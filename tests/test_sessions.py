import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from tg_harvest.admin_jobs.sessions import _cleanup_isolated_worker_session


def _create_session_entities(path: Path, rows: list[tuple]) -> None:
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE entities (
            id INTEGER PRIMARY KEY,
            hash INTEGER NOT NULL,
            username TEXT,
            phone INTEGER,
            name TEXT,
            date INTEGER
        )
        """
    )
    cur.executemany(
        "INSERT INTO entities(id, hash, username, phone, name, date) VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()


def _read_entities(path: Path) -> dict[int, tuple]:
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    rows = cur.execute(
        "SELECT id, hash, username, phone, name, date FROM entities ORDER BY id ASC"
    ).fetchall()
    conn.close()
    return {int(row[0]): row for row in rows}


class WorkerSessionCleanupTests(unittest.TestCase):
    def test_cleanup_merges_worker_entities_back_into_base_session(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base_name = root / "secondary_session"
            base_path = root / "secondary_session.session"
            worker_path = root / "secondary_session_worker_job-1.session"

            _create_session_entities(
                base_path,
                [
                    (1, 11, "old_name", None, "base-old", 100),
                ],
            )
            _create_session_entities(
                worker_path,
                [
                    (1, 12, "new_name", None, "worker-new", 200),
                    (2, 22, "fresh_name", None, "worker-fresh", 150),
                ],
            )

            _cleanup_isolated_worker_session(
                SimpleNamespace(session_name=str(base_name)),
                "job-1",
            )

            entities = _read_entities(base_path)
            self.assertEqual("new_name", entities[1][2])
            self.assertEqual("worker-new", entities[1][4])
            self.assertEqual(200, entities[1][5])
            self.assertEqual("fresh_name", entities[2][2])
            self.assertFalse(worker_path.exists())

    def test_cleanup_preserves_newer_base_entity_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base_name = root / "secondary_session"
            base_path = root / "secondary_session.session"
            worker_path = root / "secondary_session_worker_job-2.session"

            _create_session_entities(
                base_path,
                [
                    (1, 99, "stable_name", None, "base-newer", 300),
                ],
            )
            _create_session_entities(
                worker_path,
                [
                    (1, 11, "older_name", None, "worker-older", 100),
                ],
            )

            _cleanup_isolated_worker_session(
                SimpleNamespace(session_name=str(base_name)),
                "job-2",
            )

            entities = _read_entities(base_path)
            self.assertEqual("stable_name", entities[1][2])
            self.assertEqual("base-newer", entities[1][4])
            self.assertEqual(300, entities[1][5])


if __name__ == "__main__":
    unittest.main()
