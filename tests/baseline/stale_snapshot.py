import json
import subprocess
import time
import unittest
import uuid
from pathlib import Path
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

    def test_direct_cutover_resurrects_deleted_notice(self):
        self.sql("""CREATE TABLE documents (id text PRIMARY KEY, title text, category text);
                    CREATE TABLE index_control (active text NOT NULL);
                    INSERT INTO documents VALUES ('42', 'notice', 'news');
                    INSERT INTO index_control VALUES ('docs_v1');""")
        for version in ("docs_v1", "docs_v2"):
            self.api("POST", "/collections", {"name": version, "fields": [
                {"name": "title", "type": "string"},
                {"name": "category", "type": "string", "facet": version == "docs_v2"},
            ]})
        snapshot = json.loads(self.sql("SELECT row_to_json(documents) FROM documents"))
        self.api("POST", "/collections/docs_v1/documents", snapshot)
        self.assertEqual(self.active_hits(), ["42"])

        self.assertEqual(self.sql("DELETE FROM documents WHERE id='42' RETURNING id"), "42")
        self.api("DELETE", "/collections/docs_v1/documents/42")
        self.assertEqual(self.active_hits(), [])

        self.api("POST", "/collections/docs_v2/documents", snapshot)
        self.sql("UPDATE index_control SET active='docs_v2'")
        self.assertEqual(self.sql("SELECT count(*) FROM documents WHERE id='42'"), "0")
        self.assertEqual(self.active_hits(), ["42"], "unsafe baseline should return the stale notice")


if __name__ == "__main__":
    unittest.main(verbosity=2)
