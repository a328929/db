import math
import sqlite3
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest

from tg_harvest.ml import sync_predictor
from tg_harvest.storage.connection import detect_sqlite_features
from tg_harvest.storage.schema import create_schema
from tg_harvest.storage.sync_scheduler import (
    SyncObservation,
    SyncUpdateResult,
    claim_due_pending_updates,
    complete_pending_update,
    enqueue_observation,
)


def test_causal_samples_train_publish_and_load_with_real_cpu_torch(
    tmp_path, monkeypatch
):
    pytest.importorskip("torch", reason="optional CPU PyTorch is not installed")
    artifact = tmp_path / "model.pt"
    monkeypatch.setattr(sync_predictor, "default_artifact_path", lambda: str(artifact))
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        create_schema(conn, detect_sqlite_features(conn))
        conn.execute("INSERT INTO chats(chat_id,chat_title) VALUES (1,'Example')")
        conn.commit()
        base = datetime(2026, 9, 18)
        for index in range(20):
            observed = base + timedelta(seconds=index * 600)
            enqueue_observation(
                conn,
                cfg=SimpleNamespace(sync_ai_enabled=0),
                observation=SyncObservation(
                    chat_id=1,
                    reason="new_message",
                    observed_at=observed.strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
            task = claim_due_pending_updates(
                conn,
                now_text=(observed + timedelta(seconds=300)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            )[0]
            complete_pending_update(
                conn,
                task=task,
                result=SyncUpdateResult(
                    chat_id=1, added_message_count=index % 4, api_cost=2
                ),
                now_text=(observed + timedelta(seconds=310)).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
            )
        cfg = SimpleNamespace(
            sync_ai_enabled=1,
            sync_ai_shadow=1,
            sync_model_min_samples=8,
            sync_model_min_eval_samples=1,
            sync_model_max_train_samples=32,
            sync_model_train_epochs=1,
            sync_model_train_batch_size=4,
            sync_model_torch_threads=1,
        )
        trained = sync_predictor.train_sync_model(conn, cfg)
        assert trained["trained"]
        assert trained["sample_count"] == 20
        assert artifact.is_file()
        assert math.isfinite(trained["metrics"]["validation_loss"])
        suggestion = sync_predictor.predict_sync_decision(
            conn,
            cfg,
            chat_id=1,
            now_text="2026-09-18 04:00:00",
            observation_reason="new_message",
            source_account="primary",
            heuristic_delay_seconds=30,
            heuristic_priority_score=50,
        )
        assert suggestion.available
        assert not suggestion.active
        assert suggestion.model_version == sync_predictor.MODEL_VERSION
        assert len(suggestion.bucket_probabilities) == len(
            sync_predictor.DELAY_BUCKET_SECONDS
        )
        assert all(math.isfinite(value) for value in suggestion.bucket_probabilities)
        repeated = sync_predictor.train_sync_model(conn, cfg)
        assert not repeated["trained"]
        assert repeated["backend"] == "waiting_for_new_outcomes"
    finally:
        sync_predictor.invalidate_model_cache(str(artifact))
        conn.close()
