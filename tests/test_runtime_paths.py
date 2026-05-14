import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tg_harvest.runtime import paths as runtime_paths


class RuntimePathsTests(unittest.TestCase):
    def test_ensure_runtime_layout_creates_only_active_runtime_dirs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            runtime_root = project_root / ".runtime"
            db_dir = runtime_root / "db"
            session_dir = runtime_root / "sessions"

            with patch.object(runtime_paths, "PROJECT_ROOT", project_root), patch.object(
                runtime_paths, "RUNTIME_ROOT", runtime_root
            ), patch.object(runtime_paths, "RUNTIME_DB_DIR", db_dir), patch.object(
                runtime_paths, "RUNTIME_SESSION_DIR", session_dir
            ):
                created_root = runtime_paths.ensure_runtime_layout()

            self.assertEqual(runtime_root, created_root)
            self.assertTrue(db_dir.is_dir())
            self.assertTrue(session_dir.is_dir())
            self.assertFalse((runtime_root / "legacy").exists())


if __name__ == "__main__":
    unittest.main()
