import json
import sqlite3
from concurrent.futures import Future
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from tg_harvest.ingest.parse import ParsedMessage
from tg_harvest.ingest.runner import _harvest_messages_for_entity
from tg_harvest.ml.sync_predictor import (
    _build_readiness_state,
    _encode_decision_input,
    _sample_from_outcome_row,
)
from tg_harvest.runtime.db_listener import DatabaseChatListenerRuntime
from tg_harvest.storage.connection import detect_sqlite_features
from tg_harvest.storage.schema import create_schema
from tg_harvest.storage.sync_scheduler import (
    SyncObservation,
    SyncUpdateResult,
    claim_due_pending_updates,
    complete_pending_update,
    enqueue_observation,
    fail_pending_update,
)


@pytest.fixture
def conn():
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    create_schema(connection, detect_sqlite_features(connection))
    connection.execute("INSERT INTO chats(chat_id,chat_title) VALUES (1,'Example')")
    connection.commit()
    yield connection
    connection.close()


def cfg(**overrides):
    return SimpleNamespace(
        **({"sync_ai_enabled": 0, "sync_max_active_delay_seconds": 600} | overrides)
    )


def observe(conn, seconds=0, **kwargs):
    now = (datetime(2026, 9, 18) + timedelta(seconds=seconds)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    return enqueue_observation(
        conn,
        cfg=cfg(),
        observation=SyncObservation(chat_id=1, observed_at=now, **kwargs),
    )


def claim(conn, now="2026-09-18 00:10:00"):
    return claim_due_pending_updates(conn, now_text=now)[0]


def test_continuous_events_obey_maximum_wait(conn):
    for seconds in range(0, 610, 10):
        decision = observe(conn, seconds)
    assert decision.due_at == "2026-09-18 00:10:00"
    assert claim(conn).event_count == 61


def test_new_events_do_not_erase_failure_backoff(conn):
    observe(conn)
    task = claim(conn)
    fail_pending_update(
        conn,
        cfg=cfg(),
        task=task,
        result=SyncUpdateResult(
            chat_id=1, failure_type="flood_wait", retry_after_seconds=1200
        ),
        now_text="2026-09-18 00:10:00",
    )
    observe(conn, 610)
    decision = observe(conn, 620)
    assert decision.due_at == "2026-09-18 00:30:00"
    assert not claim_due_pending_updates(conn, now_text="2026-09-18 00:20:00")


def test_training_uses_only_inputs_available_at_decision_time(conn):
    observe(conn)
    task = claim(conn)
    decision_row = conn.execute(
        "SELECT features_json FROM sync_learning_events WHERE id=?",
        (task.decision_event_id,),
    ).fetchone()
    snapshot = json.loads(decision_row[0])["decision_input"]
    expected = _encode_decision_input(conn, 1, snapshot)
    observe(conn, 601)
    complete_pending_update(
        conn,
        task=task,
        result=SyncUpdateResult(chat_id=1, added_message_count=42, api_cost=3),
        now_text="2026-09-18 00:11:00",
    )
    outcome = conn.execute(
        "SELECT * FROM sync_learning_events WHERE event_type='update_outcome'"
    ).fetchone()
    sample = _sample_from_outcome_row(conn, outcome)
    assert sample is not None
    assert sample.seq_numeric == expected["seq_numeric"]
    assert sample.static_features == expected["static_features"]
    assert sample.seq_event_type_ids[-1] == 1
    assert sample.seq_numeric[-1][3] == 0
    assert sample.added_log_target > 0


def test_untraceable_legacy_outcomes_are_not_training_samples(conn):
    conn.execute(
        "INSERT INTO sync_learning_events(chat_id,event_type) VALUES (1,'update_outcome')"
    )
    row = conn.execute("SELECT * FROM sync_learning_events").fetchone()
    assert _sample_from_outcome_row(conn, row) is None


@pytest.mark.parametrize(
    "previous,metrics,ready,mode",
    [
        (
            {},
            {
                "validation_delay_accuracy": 1,
                "validation_delay_mae_buckets": 0,
                "validation_added_mae_log": 0,
            },
            True,
            "active",
        ),
        (
            {"mode": "active", "consecutive_ready_count": 3},
            {
                "validation_delay_accuracy": 0.01,
                "validation_delay_mae_buckets": 9,
                "validation_added_mae_log": 9,
            },
            False,
            "shadow",
        ),
        (
            {"mode": "active"},
            {"validation_delay_accuracy": float("nan")},
            False,
            "shadow",
        ),
    ],
)
def test_model_readiness_handles_zero_and_demotion(previous, metrics, ready, mode):
    result = _build_readiness_state(
        cfg=cfg(
            sync_ai_shadow=0,
            sync_ai_auto_promote_enabled=1,
            sync_model_ready_consecutive_runs=1,
        ),
        previous_state=previous,
        sample_count=1000,
        validation_count=200,
        metrics=metrics,
    )
    assert result["ready"] is ready
    assert result["mode"] == mode


def test_edit_arriving_during_harvest_survives_older_acknowledgement(conn):
    observe(conn, edited_message_id=1)
    task = claim(conn)
    assert task.edited_message_ids == (1,)
    observe(conn, 601, edited_message_id=1)
    complete_pending_update(
        conn,
        task=task,
        result=SyncUpdateResult(chat_id=1),
        now_text="2026-09-18 00:11:00",
    )
    next_task = claim(conn, "2026-09-18 00:20:00")
    assert next_task.edited_message_ids == (1,)
    complete_pending_update(
        conn,
        task=next_task,
        result=SyncUpdateResult(chat_id=1),
        now_text="2026-09-18 00:21:00",
    )
    assert (
        conn.execute("SELECT COUNT(*) FROM sync_pending_message_edits").fetchone()[0]
        == 0
    )


def test_edit_batches_do_not_drop_overflow(conn):
    observe(conn)
    conn.executemany(
        "INSERT INTO sync_pending_message_edits(chat_id,message_id,generation) VALUES (1,?,1)",
        [(i,) for i in range(1, 206)],
    )
    conn.commit()
    task = claim(conn)
    assert len(task.edited_message_ids) == 200
    complete_pending_update(
        conn,
        task=task,
        result=SyncUpdateResult(chat_id=1),
        now_text="2026-09-18 00:11:00",
    )
    assert claim(conn, "2026-09-18 00:20:00").edited_message_ids == (
        201,
        202,
        203,
        204,
        205,
    )


def test_old_edit_is_fetched_outside_incremental_tail(conn):
    conn.executemany(
        "INSERT INTO messages(chat_id,message_id,msg_date_text,msg_date_ts,msg_type,content) VALUES (1,?,'date',?,'TEXT','old')",
        [(1, 1), (100000, 100000)],
    )
    conn.commit()
    client = SimpleNamespace(
        get_messages=Mock(return_value=[SimpleNamespace(id=1)]),
        iter_messages=Mock(return_value=[]),
    )
    parsed = ParsedMessage(
        msg_id=1,
        date_text="date",
        date_ts=1,
        sender_id=1,
        msg_type="TEXT",
        content="edited",
        has_media=False,
        grouped_id=None,
    )
    with patch("tg_harvest.ingest.runner.MessageParser.parse", return_value=parsed):
        _harvest_messages_for_entity(
            conn, client, SimpleNamespace(id=1), 1, edited_message_ids=(1,)
        )
    client.get_messages.assert_called_once_with(SimpleNamespace(id=1), ids=[1])
    assert client.iter_messages.call_args.kwargs["min_id"] > 1
    assert (
        conn.execute("SELECT content FROM messages WHERE message_id=1").fetchone()[0]
        == "edited"
    )


def test_worker_retries_acknowledgement_without_repeating_harvest(conn):
    observe(conn)
    task = claim(conn)

    class BorrowedConnection:
        def __getattr__(self, name):
            return getattr(conn, name)

        def close(self):
            pass

    class Executor:
        def submit(self, fn, task):
            future = Future()
            future.set_result(fn(task))
            return future

        def shutdown(self, **kwargs):
            pass

    runtime = DatabaseChatListenerRuntime(cfg=cfg(), get_conn_fn=BorrowedConnection)
    runtime._scheduler_executor = Executor()
    runtime._account_runtime.scheduler_concurrency = lambda: 1
    runtime._claim_due_scheduler_tasks = Mock(side_effect=[[task], []])
    runtime._execute_scheduler_task = Mock(return_value=SyncUpdateResult(chat_id=1))
    attempts = []

    def acknowledge(*args, **kwargs):
        attempts.append(1)
        if len(attempts) == 1:
            raise sqlite3.OperationalError("database is locked")
        complete_pending_update(*args, **kwargs)
        runtime._worker_stop.set()

    clock = iter(range(0, 1000, 10))
    with (
        patch(
            "tg_harvest.runtime.db_listener.sync_scheduler.complete_pending_update",
            side_effect=acknowledge,
        ),
        patch(
            "tg_harvest.runtime.db_listener.time.monotonic",
            side_effect=lambda: next(clock),
        ),
        patch(
            "tg_harvest.runtime.db_listener._DEFAULT_EVENT_IDLE_SLEEP_SECONDS", 0.001
        ),
    ):
        runtime._scheduler_worker_loop()
    assert len(attempts) == 2
    runtime._execute_scheduler_task.assert_called_once_with(task)
    assert conn.execute("SELECT COUNT(*) FROM sync_pending_updates").fetchone()[0] == 0
