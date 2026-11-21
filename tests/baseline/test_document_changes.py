import subprocess
import unittest
import uuid
from pathlib import Path


class DocumentChangeTest(unittest.TestCase):
    def test_atomic_write_and_rollback(self):
        directory = Path(__file__).resolve().parent
        compose = ["docker", "compose", "-f", str(directory / "compose.yaml"),
                   "-p", "write-" + uuid.uuid4().hex]
        self.addCleanup(subprocess.run, compose + ["down", "-v"], check=True)
        subprocess.run(compose + ["up", "-d", "--wait", "postgres"], check=True)
        migration = directory.parents[1] / "backend/src/main/resources/db/001_document_changes.sql"
        for script in (migration, directory / "document_changes.sql"):
            subprocess.run(compose + ["exec", "-T", "postgres", "psql", "-X", "-U", "postgres",
                                      "-v", "ON_ERROR_STOP=1"], input=script.read_bytes(), check=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
