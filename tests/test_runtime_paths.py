import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import tg_harvest.runtime.paths as runtime_paths
from tg_harvest.storage.connection import connect_db


class RuntimePathsTests(unittest.TestCase):
    def test_ensure_runtime_layout_creates_only_active_runtime_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            runtime_root = project_root / ".runtime"
            db_dir = runtime_root / "db"
            session_dir = runtime_root / "sessions"
            model_dir = runtime_root / "models"

            for path in (runtime_root, db_dir, session_dir, model_dir):
                path.mkdir(parents=True, exist_ok=True)
                path.chmod(0o755)

            with patch.object(runtime_paths, "PROJECT_ROOT", project_root), patch.object(
                runtime_paths, "RUNTIME_ROOT", runtime_root
            ), patch.object(runtime_paths, "RUNTIME_DB_DIR", db_dir), patch.object(
                runtime_paths, "RUNTIME_SESSION_DIR", session_dir
            ):
                created_root = runtime_paths.ensure_runtime_layout()

            self.assertEqual(runtime_root, created_root)
            self.assertTrue(db_dir.is_dir())
            self.assertTrue(session_dir.is_dir())
            self.assertTrue(model_dir.is_dir())
            self.assertFalse((runtime_root / "legacy").exists())
            for path in (runtime_root, db_dir, session_dir, model_dir):
                self.assertEqual(0o700, stat.S_IMODE(path.stat().st_mode))

    def test_connect_db_restricts_database_and_sqlite_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            external_dir = root / "external"
            external_dir.mkdir(mode=0o755)
            external_dir.chmod(0o755)
            db_path = external_dir / "runtime.db"

            conn, _ = connect_db(str(db_path), cache_mb=16, mmap_mb=0)
            try:
                conn.execute("CREATE TABLE sample (value INTEGER)")
                conn.execute("INSERT INTO sample(value) VALUES (1)")
                conn.commit()

                self.assertEqual(0o755, stat.S_IMODE(external_dir.stat().st_mode))
                self.assertEqual(0o600, stat.S_IMODE(db_path.stat().st_mode))
                for suffix in ("-wal", "-shm"):
                    sidecar = Path(f"{db_path}{suffix}")
                    self.assertTrue(sidecar.exists())
                    self.assertEqual(0o600, stat.S_IMODE(sidecar.stat().st_mode))
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
