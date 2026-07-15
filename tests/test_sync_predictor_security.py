import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tg_harvest.ml.sync_predictor import _load_checkpoint_model, _safe_torch_load


class SafeTorchLoadTests(unittest.TestCase):
    def test_loads_checkpoint_with_restricted_unpickler(self) -> None:
        calls = []

        class FakeTorch:
            @staticmethod
            def load(path, **kwargs):
                calls.append((path, kwargs))
                return {"model_state": {}}

        path = Path("model.pt")

        result = _safe_torch_load(FakeTorch, path)

        self.assertEqual({"model_state": {}}, result)
        self.assertEqual(
            [(path, {"map_location": "cpu", "weights_only": True})],
            calls,
        )

    def test_unsupported_safe_loading_fails_closed_without_retry(self) -> None:
        calls = []

        class LegacyTorch:
            @staticmethod
            def load(path, **kwargs):
                calls.append((path, kwargs))
                raise TypeError("weights_only is unsupported")

        with self.assertRaises(TypeError):
            _safe_torch_load(LegacyTorch, Path("legacy.pt"))

        self.assertEqual(1, len(calls))
        self.assertTrue(calls[0][1]["weights_only"])

    def test_prediction_reports_checkpoint_load_failure(self) -> None:
        class BrokenTorch:
            @staticmethod
            def load(path, **kwargs):
                raise RuntimeError("invalid checkpoint")

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "model.pt"
            path.touch()
            model_row = {
                "artifact_path": str(path),
                "model_version": "temporal-batch-predictor-v3-lite",
            }
            with (
                patch(
                    "tg_harvest.ml.sync_predictor._model_state_from_db",
                    return_value=model_row,
                ),
                patch(
                    "tg_harvest.ml.sync_predictor._load_torch",
                    return_value=(BrokenTorch, object(), ""),
                ),
            ):
                torch, model, loaded_row, reason = _load_checkpoint_model(object())

        self.assertIsNone(torch)
        self.assertIsNone(model)
        self.assertIs(model_row, loaded_row)
        self.assertEqual("artifact_load_failed: RuntimeError", reason)


if __name__ == "__main__":
    unittest.main()
