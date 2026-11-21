import json
import subprocess
import time
import unittest
import uuid
from pathlib import Path
from urllib.parse import quote
from urllib.error import URLError
from urllib.request import Request, urlopen


class StaleSnapshotTest(unittest.TestCase):
    def compose(self, *args):
        return subprocess.run(
            ["docker", "compose", "-f", str(Path(__file__).with_name("compose.yaml")),
             "-p", self.project, *args],
            check=True, stdout=subprocess.PIPE, text=True, encoding="utf-8",
        ).stdout.strip()

    def sql(self, statement):
        return self.compose("exec", "-T", "postgres", "psql", "-XAtq",
                            "-U", "postgres", "-v", "ON_ERROR_STOP=1", "-c", statement)

    def api(self, method, path, data=None):
        request = Request(self.url + path, method=method,
                          headers={"X-TYPESENSE-API-KEY": "baseline-only",
                                   "Content-Type": "application/json"},
                          data=None if data is None else json.dumps(data).encode())
        with urlopen(request, timeout=5) as response:
            return json.load(response)

    def setUp(self):
        self.project = "baseline-" + uuid.uuid4().hex
        self.addCleanup(self.compose, "down", "-v")
        self.compose("up", "-d", "--wait", "--wait-timeout", "90")
        self.url = "http://" + self.compose("port", "typesense", "8108")
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                if self.api("GET", "/health").get("ok"):
                    break
            except (URLError, ConnectionError, TimeoutError):
                pass
            time.sleep(0.5)
        else:
            self.fail("Typesense did not become healthy")

    def active_hits(self):
        active = self.sql("SELECT active FROM index_control")
        results = self.api("GET", f"/collections/{active}/documents/search?q=notice&query_by=title")
        return [hit["document"]["id"] for hit in results["hits"]]

    def import_snapshot_then_delete(self):
        migrations = Path(__file__).resolve().parents[2] / "backend/src/main/resources/db"
        for migration in sorted(migrations.glob("*.sql")):
            self.sql(migration.read_text(encoding="utf-8"))
        self.sql("SELECT put_document('42', 'notice', 'expired', 'news')")
        self.sql("SELECT put_document('43', 'notice', 'keep', 'news')")
        for version in ("docs_v1", "docs_v2"):
            self.api("POST", "/collections", {"name": version, "fields": [
                {"name": "title", "type": "string"},
                {"name": "body", "type": "string"},
                {"name": "category", "type": "string", "facet": version == "docs_v2"},
            ]})
        snapshot = json.loads(self.sql("""SELECT json_build_object('seq', committed_seq,
            'docs', (SELECT json_agg(documents ORDER BY id) FROM documents)) FROM index_control"""))
        for document in snapshot["docs"]:
            self.api("POST", "/collections/docs_v1/documents", document)
        self.assertCountEqual(self.active_hits(), ["42", "43"])

        self.assertEqual(self.sql("SELECT delete_document('42')"), "3")
        self.api("DELETE", "/collections/docs_v1/documents/42")
        self.assertEqual(self.active_hits(), ["43"])

        for document in snapshot["docs"]:
            self.api("POST", "/collections/docs_v2/documents", document)
        self.assertEqual(self.sql("SELECT id FROM documents ORDER BY id"), "43")
        return snapshot["seq"]

    def test_direct_cutover_resurrects_deleted_notice(self):
        self.import_snapshot_then_delete()
        self.sql("UPDATE index_control SET active='docs_v2'")
        self.assertCountEqual(self.active_hits(), ["42", "43"])

    def test_replay_before_cutover_preserves_deletion(self):
        confirmed = self.import_snapshot_then_delete()
        def cutover():
            return self.sql(f"SELECT try_cutover('docs_v1', 'docs_v2', {confirmed})")

        self.assertEqual(cutover(), "f")
        self.assertEqual(self.sql("SELECT active FROM index_control"), "docs_v1")
        changes = self.sql(f"SELECT row_to_json(document_changes) FROM document_changes "
                           f"WHERE seq > {confirmed} ORDER BY seq")
        for line in changes.splitlines():
            event = json.loads(line)
            self.assertEqual(event["seq"], confirmed + 1)
            self.assertEqual(event["operation"], "delete")
            removed = self.api("DELETE", "/collections/docs_v2/documents/" +
                               quote(event["document_id"], safe=""))
            self.assertEqual(removed["id"], event["document_id"])
            confirmed = event["seq"]
        self.assertEqual(cutover(), "t")
        self.assertEqual(self.sql("SELECT active FROM index_control"), "docs_v2")
        self.assertEqual(self.active_hits(), ["43"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
