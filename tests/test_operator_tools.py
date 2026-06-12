import importlib.util
import pathlib
import sys
import unittest
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

_TOOLS_ROOT = pathlib.Path(__file__).resolve().parent.parent / "tools"


def _load_tool_module(module_name: str):
    script_path = _TOOLS_ROOT / f"{module_name}.py"
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class FixMissingFilenamesToolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.module = _load_tool_module("fix_missing_filenames")

    def _install_fake_runtime_modules(self, *, backfill_result=None, backfill_error=None):
        fake_config = ModuleType("tg_harvest.config")
        fake_config.CFG = SimpleNamespace(db_name="test.sqlite3")

        conn = Mock()
        fake_connection = ModuleType("tg_harvest.storage.connection")
        fake_connection.ensure_configured_db = Mock(return_value=(conn, None))

        fake_store = ModuleType("tg_harvest.ingest.store")
        if backfill_error is not None:
            fake_store.backfill_message_search_text_from_filenames = Mock(
                side_effect=backfill_error
            )
        else:
            fake_store.backfill_message_search_text_from_filenames = Mock(
                return_value=backfill_result
            )

        modules = {
            "tg_harvest.config": fake_config,
            "tg_harvest.storage.connection": fake_connection,
            "tg_harvest.ingest.store": fake_store,
        }
        return modules, conn, fake_store, fake_connection

    def test_fix_missing_filenames_returns_zero_on_success(self) -> None:
        modules, conn, fake_store, fake_connection = self._install_fake_runtime_modules(
            backfill_result=3
        )

        with unittest.mock.patch.dict(sys.modules, modules):
            status = self.module.main([])

        self.assertEqual(0, status)
        fake_connection.ensure_configured_db.assert_called_once()
        fake_store.backfill_message_search_text_from_filenames.assert_called_once()
        conn.rollback.assert_not_called()
        conn.close.assert_called_once_with()

    def test_fix_missing_filenames_returns_nonzero_and_rolls_back_on_error(self) -> None:
        modules, conn, _fake_store, _fake_connection = self._install_fake_runtime_modules(
            backfill_error=RuntimeError("boom")
        )

        with unittest.mock.patch.dict(sys.modules, modules):
            status = self.module.main([])

        self.assertEqual(1, status)
        conn.rollback.assert_called_once_with()
        conn.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
