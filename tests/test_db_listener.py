import queue
import sqlite3
import tempfile
import unittest
from inspect import iscoroutinefunction
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import tg_harvest.runtime.db_listener as db_listener
from tg_harvest.ingest.flood_wait import AccountFloodWaitError
from tg_harvest.runtime.db_listener import (
    DatabaseChatListenerRuntime,
    _ListenerAccount,
    _load_database_chat_ids,
    _PublicProbeOutcome,
    _QueuedChatUpdate,
    ensure_database_chat_listener_runtime,
)


def _connect(path: Path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _create_schema(conn):
    conn.execute(
        """
        CREATE TABLE chats (
            chat_id INTEGER PRIMARY KEY,
            chat_title TEXT NOT NULL,
            chat_username TEXT,
            last_seen_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


class DatabaseChatListenerRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        db_listener._LISTENER_SINGLETON = None

    def test_load_database_chat_ids_reads_only_positive_chat_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "listener.db"
            conn = _connect(path)
            try:
                _create_schema(conn)
                conn.executemany(
                    """
                    INSERT INTO chats(chat_id, chat_title, chat_username, last_seen_at)
                    VALUES (?, ?, ?, ?)
                    """,
                    [
                        (1, "one", "one_name", "2026-01-01 00:00:00"),
                        (2, "two", None, "2026-01-01 00:00:00"),
                    ],
                )
                conn.commit()
                chat_ids = _load_database_chat_ids(conn)
            finally:
                conn.close()

        self.assertEqual({1, 2}, chat_ids)

    def test_enqueue_chat_update_ignores_non_database_chat(self) -> None:
        runtime = DatabaseChatListenerRuntime(
            cfg=SimpleNamespace(),
            get_conn_fn=lambda: None,
        )
        runtime._db_chat_rows_by_id = {1: {"chat_id": 1}}

        runtime._enqueue_chat_update(
            chat_id=2,
            chat_title="two",
            chat_username="two_name",
            reason="new_message",
            source_account="primary",
        )

        with self.assertRaises(queue.Empty):
            runtime._queue.get_nowait()

    def test_enqueue_chat_update_dedupes_same_chat(self) -> None:
        runtime = DatabaseChatListenerRuntime(
            cfg=SimpleNamespace(),
            get_conn_fn=lambda: None,
        )
        runtime._db_chat_rows_by_id = {1: {"chat_id": 1}}

        runtime._enqueue_chat_update(
            chat_id=1,
            chat_title="one",
            chat_username="one_name",
            reason="new_message",
            source_account="primary",
        )
        runtime._enqueue_chat_update(
            chat_id=1,
            chat_title="one",
            chat_username="one_name",
            reason="message_edited",
            source_account="secondary",
        )

        item = runtime._queue.get_nowait()
        self.assertEqual(
            _QueuedChatUpdate(
                chat_id=1,
                chat_title="one",
                chat_username="one_name",
                reason="new_message",
                source_account="primary",
            ),
            item,
        )
        with self.assertRaises(queue.Empty):
            runtime._queue.get_nowait()

    def test_scheduler_completion_write_failure_does_not_convert_result_to_success(self) -> None:
        class _Conn:
            def close(self):
                return None

        runtime = DatabaseChatListenerRuntime(
            cfg=SimpleNamespace(),
            get_conn_fn=_Conn,
        )
        task = SimpleNamespace(chat_id=1, chat_title="one", chat_username="one_name")
        result = db_listener.SyncUpdateResult(
            chat_id=1,
            chat_title="one",
            chat_username="one_name",
            source_account="primary",
        )

        with patch.object(
            db_listener.sync_scheduler,
            "complete_pending_update",
            side_effect=sqlite3.OperationalError("completion write failed"),
        ) as complete_mock, patch.object(
            db_listener.sync_scheduler,
            "fail_pending_update",
        ) as fail_mock, self.assertLogs(level="ERROR") as captured:
            runtime._finish_scheduler_task(task=task, result=result)

        complete_mock.assert_called_once()
        fail_mock.assert_not_called()
        self.assertTrue(any("保持 in-flight" in line for line in captured.output))

    def test_event_chat_id_maps_back_to_database_shape(self) -> None:
        runtime = DatabaseChatListenerRuntime(
            cfg=SimpleNamespace(),
            get_conn_fn=lambda: None,
        )

        event = SimpleNamespace(chat_id=-100123456)
        self.assertEqual(123456, runtime._event_chat_id(event))

    def test_handle_message_event_enqueues_database_chat_only(self) -> None:
        runtime = DatabaseChatListenerRuntime(
            cfg=SimpleNamespace(),
            get_conn_fn=lambda: None,
        )
        runtime._db_chat_rows_by_id = {
            123456: {
                "chat_id": 123456,
                "chat_title": "cached title",
                "chat_username": "cached_name",
            }
        }

        event = SimpleNamespace(
            chat_id=-100123456,
            message=SimpleNamespace(peer_id=SimpleNamespace(channel_id=123456)),
            chat=SimpleNamespace(title="db chat", username="db_name"),
        )

        runtime._handle_message_event(
            event,
            reason="new_message",
            account_key="primary",
        )

        queued = runtime._queue.get_nowait()
        self.assertEqual(123456, queued.chat_id)
        self.assertEqual("db chat", queued.chat_title)
        self.assertEqual("db_name", queued.chat_username)

    def test_register_client_event_handlers_uses_async_callbacks(self) -> None:
        runtime = DatabaseChatListenerRuntime(
            cfg=SimpleNamespace(),
            get_conn_fn=lambda: None,
        )

        class _Client:
            def __init__(self) -> None:
                self.handlers = []

            def add_event_handler(self, callback, event_builder) -> None:
                self.handlers.append((callback, event_builder))

        client = _Client()

        runtime._register_client_event_handlers(client, account_key="primary")

        self.assertEqual(3, len(client.handlers))
        self.assertTrue(all(iscoroutinefunction(callback) for callback, _ in client.handlers))

    def test_public_probe_candidate_rows_include_joined_public_and_cached_private_chats(
        self,
    ) -> None:
        cfg = SimpleNamespace(
            session_name="primary",
            secondary_session_name="secondary",
        )
        runtime = DatabaseChatListenerRuntime(
            cfg=cfg,
            get_conn_fn=lambda: None,
        )
        runtime._db_chat_rows_by_id = {
            1: {
                "chat_id": 1,
                "chat_title": "joined public",
                "chat_username": "joined_public",
            },
            2: {
                "chat_id": 2,
                "chat_title": "cached but not joined",
                "chat_username": "cached_only",
            },
            3: {
                "chat_id": 3,
                "chat_title": "public only in db",
                "chat_username": "public_only",
            },
            4: {
                "chat_id": 4,
                "chat_title": "private no username",
                "chat_username": "",
            },
        }
        runtime._joined_chat_ids_by_account = {
            "primary": {1},
            "secondary": set(),
        }

        with patch.object(
            db_listener,
            "_read_session_cached_chat_ids",
            side_effect=[{1, 2}, {4}],
        ):
            rows = runtime._public_probe_candidate_rows()

        self.assertEqual([1, 2, 3, 4], [int(row["chat_id"]) for row in rows])
        scope_by_chat_id = {
            int(row["chat_id"]): str(row.get("probe_scope") or "") for row in rows
        }
        self.assertEqual("joined", scope_by_chat_id[1])
        self.assertEqual("public", scope_by_chat_id[2])
        self.assertEqual("public", scope_by_chat_id[3])
        self.assertEqual("cached", scope_by_chat_id[4])
        preferred_accounts_by_chat_id = {
            int(row["chat_id"]): tuple(row.get("probe_account_keys") or ())
            for row in rows
        }
        self.assertEqual(("primary",), preferred_accounts_by_chat_id[1])
        self.assertEqual(("primary",), preferred_accounts_by_chat_id[2])
        self.assertEqual(("primary", "secondary"), preferred_accounts_by_chat_id[3])
        self.assertEqual(("secondary",), preferred_accounts_by_chat_id[4])

    def test_public_probe_candidate_rows_fallback_to_session_cache_before_joined_snapshot_ready(
        self,
    ) -> None:
        cfg = SimpleNamespace(
            session_name="primary",
            secondary_session_name="",
        )
        runtime = DatabaseChatListenerRuntime(
            cfg=cfg,
            get_conn_fn=lambda: None,
        )
        runtime._db_chat_rows_by_id = {
            1: {
                "chat_id": 1,
                "chat_title": "cached public",
                "chat_username": "cached_public",
            },
            2: {
                "chat_id": 2,
                "chat_title": "probe public",
                "chat_username": "probe_public",
            },
        }

        with patch.object(
            db_listener,
            "_read_session_cached_chat_ids",
            return_value={1},
        ):
            rows = runtime._public_probe_candidate_rows()

        self.assertEqual([1, 2], [int(row["chat_id"]) for row in rows])
        row_by_chat_id = {int(row["chat_id"]): row for row in rows}
        self.assertEqual("public", row_by_chat_id[1]["probe_scope"])
        self.assertEqual(("primary",), row_by_chat_id[1]["probe_account_keys"])

    def test_account_priority_prefers_source_account(self) -> None:
        cfg = SimpleNamespace(
            session_name="primary",
            secondary_session_name="secondary",
        )
        runtime = DatabaseChatListenerRuntime(
            cfg=cfg,
            get_conn_fn=lambda: None,
        )
        item = _QueuedChatUpdate(
            chat_id=1,
            chat_title="one",
            chat_username="one_name",
            reason="new_message",
            source_account="secondary",
        )

        accounts = runtime._account_priority_for_item(item)

        self.assertEqual(["secondary", "primary"], [account.key for account in accounts])

    def test_probe_account_priority_prefers_joined_accounts(self) -> None:
        cfg = SimpleNamespace(
            session_name="primary",
            secondary_session_name="secondary",
        )
        runtime = DatabaseChatListenerRuntime(
            cfg=cfg,
            get_conn_fn=lambda: None,
        )
        row = {
            "chat_id": 1,
            "chat_title": "one",
            "chat_username": "",
            "probe_scope": "joined",
            "probe_account_keys": ("secondary",),
        }

        accounts = runtime._probe_account_priority_for_row(row)

        self.assertEqual(["secondary", "primary"], [account.key for account in accounts])

    def test_attempt_single_chat_update_disables_progress_probe_for_listener_path(
        self,
    ) -> None:
        cfg = SimpleNamespace(
            session_name="primary",
            secondary_session_name="",
        )
        runtime = DatabaseChatListenerRuntime(
            cfg=cfg,
            get_conn_fn=lambda: None,
        )
        account = _ListenerAccount(
            key="primary",
            label="主账号",
            cfg=cfg,
            session_name="primary",
        )
        item = _QueuedChatUpdate(
            chat_id=123,
            chat_title="chat-123",
            chat_username="chat_123",
            reason="new_message",
            source_account="primary",
        )

        with patch.object(
            db_listener,
            "_create_isolated_worker_client",
            return_value=object(),
        ), patch.object(
            db_listener,
            "_cleanup_isolated_worker_session",
            return_value=None,
        ), patch.object(
            db_listener,
            "_disconnect_worker_client",
            return_value=None,
        ), patch.object(
            db_listener,
            "_admin_process_single_chat_update",
        ) as update_mock:
            runtime._attempt_single_chat_update(account=account, item=item)

        update_mock.assert_called_once()
        self.assertFalse(update_mock.call_args.kwargs["enable_progress_probe"])

    def test_next_public_probe_batch_skips_recently_cooled_down_chats(self) -> None:
        cfg = SimpleNamespace(
            db_listener_public_probe_batch_size=3,
            session_name="primary",
            secondary_session_name="",
        )
        runtime = DatabaseChatListenerRuntime(
            cfg=cfg,
            get_conn_fn=lambda: None,
        )
        rows = [
            {"chat_id": 1, "chat_title": "one", "chat_username": "one"},
            {"chat_id": 2, "chat_title": "two", "chat_username": "two"},
            {"chat_id": 3, "chat_title": "three", "chat_username": "three"},
        ]
        runtime._set_public_probe_cooldown(2, seconds=3600)

        with patch.object(
            runtime,
            "_public_probe_candidate_rows",
            return_value=rows,
        ):
            batch = runtime._next_public_probe_batch()

        self.assertEqual([1, 3], [int(row["chat_id"]) for row in batch])

    def test_public_probe_pauses_when_scheduler_backpressure_active(self) -> None:
        cfg = SimpleNamespace(
            sync_scheduler_enabled=1,
            sync_scheduler_concurrency=2,
            session_name="primary",
            secondary_session_name="",
        )
        runtime = DatabaseChatListenerRuntime(
            cfg=cfg,
            get_conn_fn=lambda: None,
        )

        with patch.object(
            runtime,
            "_pending_update_counts",
            return_value={"pending": 50, "due": 0, "in_flight": 0},
        ):
            self.assertTrue(runtime._public_probe_has_pending_updates())

    def test_next_public_probe_batch_prioritizes_hot_rows_without_starving_cold_rows(
        self,
    ) -> None:
        cfg = SimpleNamespace(
            db_listener_public_probe_batch_size=4,
            session_name="primary",
            secondary_session_name="",
        )
        runtime = DatabaseChatListenerRuntime(
            cfg=cfg,
            get_conn_fn=lambda: None,
        )
        rows = [
            {
                "chat_id": 1,
                "chat_title": "hot-1",
                "chat_username": "hot1",
                "last_message_id": 1000,
                "last_seen_at": "2026-06-28 12:00:00",
            },
            {
                "chat_id": 2,
                "chat_title": "hot-2",
                "chat_username": "hot2",
                "last_message_id": 900,
                "last_seen_at": "2026-06-28 11:00:00",
            },
            {
                "chat_id": 3,
                "chat_title": "hot-3",
                "chat_username": "hot3",
                "last_message_id": 800,
                "last_seen_at": "2026-06-28 10:00:00",
            },
            {
                "chat_id": 4,
                "chat_title": "cold-1",
                "chat_username": "cold1",
                "last_message_id": 10,
                "last_seen_at": "2026-06-20 10:00:00",
            },
            {
                "chat_id": 5,
                "chat_title": "cold-2",
                "chat_username": "cold2",
                "last_message_id": 9,
                "last_seen_at": "2026-06-19 10:00:00",
            },
        ]

        with patch.object(
            runtime,
            "_public_probe_candidate_rows",
            return_value=rows,
        ):
            batch = runtime._next_public_probe_batch()

        batch_ids = [int(row["chat_id"]) for row in batch]
        self.assertEqual(4, len(batch_ids))
        self.assertIn(1, batch_ids)
        self.assertIn(2, batch_ids)
        self.assertIn(4, batch_ids)

    def test_next_public_probe_batch_reserves_slots_for_joined_and_unjoined_rows(
        self,
    ) -> None:
        cfg = SimpleNamespace(
            db_listener_public_probe_batch_size=4,
            session_name="primary",
            secondary_session_name="secondary",
        )
        runtime = DatabaseChatListenerRuntime(
            cfg=cfg,
            get_conn_fn=lambda: None,
        )
        rows = [
            {
                "chat_id": 1,
                "chat_title": "public-1",
                "chat_username": "public1",
                "last_message_id": 1000,
                "last_message_ts": 1000,
                "probe_scope": "public",
                "probe_account_keys": ("primary", "secondary"),
            },
            {
                "chat_id": 2,
                "chat_title": "public-2",
                "chat_username": "public2",
                "last_message_id": 900,
                "last_message_ts": 900,
                "probe_scope": "public",
                "probe_account_keys": ("primary", "secondary"),
            },
            {
                "chat_id": 3,
                "chat_title": "joined-1",
                "chat_username": "",
                "last_message_id": 800,
                "last_message_ts": 800,
                "probe_scope": "joined",
                "probe_account_keys": ("primary",),
            },
            {
                "chat_id": 4,
                "chat_title": "joined-2",
                "chat_username": "",
                "last_message_id": 700,
                "last_message_ts": 700,
                "probe_scope": "joined",
                "probe_account_keys": ("secondary",),
            },
        ]

        with patch.object(
            runtime,
            "_public_probe_candidate_rows",
            return_value=rows,
        ):
            batch = runtime._next_public_probe_batch()

        self.assertEqual(4, len(batch))
        scopes = {str(row.get("probe_scope") or "") for row in batch}
        self.assertIn("public", scopes)
        self.assertIn("joined", scopes)

    def test_next_public_probe_batch_reuses_hot_slots_when_no_cold_rows_available(
        self,
    ) -> None:
        cfg = SimpleNamespace(
            db_listener_public_probe_batch_size=3,
            session_name="primary",
            secondary_session_name="",
        )
        runtime = DatabaseChatListenerRuntime(
            cfg=cfg,
            get_conn_fn=lambda: None,
        )
        rows = [
            {
                "chat_id": 1,
                "chat_title": "hot-1",
                "chat_username": "hot1",
                "last_message_id": 1000,
                "last_seen_at": "2026-06-28 12:00:00",
            },
            {
                "chat_id": 2,
                "chat_title": "hot-2",
                "chat_username": "hot2",
                "last_message_id": 900,
                "last_seen_at": "2026-06-28 11:00:00",
            },
            {
                "chat_id": 3,
                "chat_title": "hot-3",
                "chat_username": "hot3",
                "last_message_id": 800,
                "last_seen_at": "2026-06-28 10:00:00",
            },
        ]

        with patch.object(
            runtime,
            "_public_probe_candidate_rows",
            return_value=rows,
        ):
            batch = runtime._next_public_probe_batch()

        batch_ids = [int(row["chat_id"]) for row in batch]
        self.assertEqual(1, batch_ids[0])
        self.assertEqual({1, 2, 3}, set(batch_ids))

    def test_probe_public_row_returns_longer_cooldown_when_unchanged(self) -> None:
        cfg = SimpleNamespace(
            db_listener_public_probe_chat_cooldown_seconds=3600,
            session_name="primary",
            secondary_session_name="",
        )
        runtime = DatabaseChatListenerRuntime(
            cfg=cfg,
            get_conn_fn=lambda: None,
        )
        row = {"chat_id": 1, "chat_title": "one", "chat_username": "one"}
        account = _ListenerAccount(
            key="primary",
            label="主账号",
            cfg=cfg,
            session_name="primary",
        )

        with patch.object(
            runtime,
            "_listener_accounts",
            return_value=[account],
        ), patch.object(
            db_listener,
            "_ensure_base_session_valid",
            return_value=True,
        ), patch.object(
            runtime,
            "_probe_public_row_with_account",
            return_value=False,
        ):
            outcome = runtime._probe_public_row(row)

        self.assertEqual(
            _PublicProbeOutcome(
                status="unchanged",
                cooldown_seconds=3600,
                source_account="primary",
            ),
            outcome,
        )

    def test_probe_public_row_returns_shorter_cooldown_when_changed(self) -> None:
        cfg = SimpleNamespace(
            db_listener_public_probe_chat_cooldown_seconds=3600,
            session_name="primary",
            secondary_session_name="",
        )
        runtime = DatabaseChatListenerRuntime(
            cfg=cfg,
            get_conn_fn=lambda: None,
        )
        row = {"chat_id": 1, "chat_title": "one", "chat_username": "one"}
        account = _ListenerAccount(
            key="primary",
            label="主账号",
            cfg=cfg,
            session_name="primary",
        )

        with patch.object(
            runtime,
            "_listener_accounts",
            return_value=[account],
        ), patch.object(
            db_listener,
            "_ensure_base_session_valid",
            return_value=True,
        ), patch.object(
            runtime,
            "_probe_public_row_with_account",
            return_value=True,
        ):
            outcome = runtime._probe_public_row(row)

        self.assertEqual("changed", outcome.status)
        self.assertEqual(900, outcome.cooldown_seconds)

    def test_probe_public_row_uses_long_inactive_cooldown_for_joined_chat(self) -> None:
        cfg = SimpleNamespace(
            db_listener_joined_probe_chat_cooldown_seconds=7200,
            db_listener_inactive_probe_chat_cooldown_seconds=43200,
            session_name="primary",
            secondary_session_name="",
        )
        runtime = DatabaseChatListenerRuntime(
            cfg=cfg,
            get_conn_fn=lambda: None,
        )
        row = {
            "chat_id": 1,
            "chat_title": "one",
            "chat_username": "",
            "probe_scope": "joined",
            "probe_inactive": True,
            "probe_account_keys": ("primary",),
        }
        account = _ListenerAccount(
            key="primary",
            label="主账号",
            cfg=cfg,
            session_name="primary",
        )

        with patch.object(
            runtime,
            "_probe_account_priority_for_row",
            return_value=[account],
        ), patch.object(
            db_listener,
            "_ensure_base_session_valid",
            return_value=True,
        ), patch.object(
            runtime,
            "_probe_public_row_with_account",
            return_value=False,
        ):
            outcome = runtime._probe_public_row(row)

        self.assertEqual("unchanged", outcome.status)
        self.assertEqual(43200, outcome.cooldown_seconds)

    def test_probe_public_row_returns_short_failure_cooldown_on_flood_wait(self) -> None:
        cfg = SimpleNamespace(
            db_listener_public_probe_chat_cooldown_seconds=3600,
            session_name="primary",
            secondary_session_name="",
        )
        runtime = DatabaseChatListenerRuntime(
            cfg=cfg,
            get_conn_fn=lambda: None,
        )
        row = {"chat_id": 1, "chat_title": "one", "chat_username": "one"}
        account = _ListenerAccount(
            key="primary",
            label="主账号",
            cfg=cfg,
            session_name="primary",
        )
        flood_exc = AccountFloodWaitError(
            seconds=7200,
            threshold_seconds=30,
            account_label="主账号",
            scope="probe",
        )

        with patch.object(
            runtime,
            "_listener_accounts",
            return_value=[account],
        ), patch.object(
            db_listener,
            "_ensure_base_session_valid",
            return_value=True,
        ), patch.object(
            runtime,
            "_probe_public_row_with_account",
            side_effect=flood_exc,
        ), patch.object(
            db_listener,
            "_remember_account_cooldown",
        ) as remember_cooldown_mock:
            outcome = runtime._probe_public_row(row)

        remember_cooldown_mock.assert_called_once()
        self.assertEqual("flood_wait", outcome.status)
        self.assertEqual(900, outcome.cooldown_seconds)

    def test_public_probe_row_waits_for_per_account_probe_gap(self) -> None:
        cfg = SimpleNamespace(
            db_listener_public_probe_chat_cooldown_seconds=3600,
            session_name="primary",
            secondary_session_name="",
        )
        runtime = DatabaseChatListenerRuntime(
            cfg=cfg,
            get_conn_fn=lambda: None,
        )
        row = {"chat_id": 1, "chat_title": "one", "chat_username": "one"}
        account = _ListenerAccount(
            key="primary",
            label="主账号",
            cfg=cfg,
            session_name="primary",
        )
        sleep_calls = []

        time_points = iter([100.0, 101.0])

        with patch.object(
            db_listener,
            "_create_isolated_worker_client",
            return_value=object(),
        ), patch.object(
            db_listener,
            "_disconnect_worker_client",
            return_value=None,
        ), patch.object(
            db_listener,
            "_cleanup_isolated_worker_session",
            return_value=None,
        ), patch.object(
            db_listener,
            "resolve_chat_entity",
            return_value=SimpleNamespace(id=1, title="one"),
        ), patch.object(
            db_listener,
            "read_latest_message_id",
            return_value=10,
        ), patch.object(
            runtime,
            "_load_local_last_message_id",
            return_value=0,
        ), patch.object(
            runtime,
            "_enqueue_chat_update",
            return_value=None,
        ), patch.object(
            db_listener.time,
            "time",
            side_effect=lambda: next(time_points),
        ), patch.object(
            db_listener.time,
            "sleep",
            side_effect=lambda seconds: sleep_calls.append(float(seconds)),
        ):
            changed_first = runtime._probe_public_row_with_account(
                row=row,
                account=account,
            )
            changed_second = runtime._probe_public_row_with_account(
                row=row,
                account=account,
            )

        self.assertTrue(changed_first)
        self.assertTrue(changed_second)
        self.assertEqual([5.0], sleep_calls)

    def test_public_probe_loop_applies_cooldown_after_probe_result(self) -> None:
        cfg = SimpleNamespace(
            db_listener_public_probe_interval_seconds=60,
            session_name="primary",
            secondary_session_name="",
        )
        runtime = DatabaseChatListenerRuntime(
            cfg=cfg,
            get_conn_fn=lambda: None,
        )
        row = {"chat_id": 123, "chat_title": "one", "chat_username": "one"}

        class _StopAfterOnePass:
            def __init__(self) -> None:
                self.calls = 0

            def wait(self, _seconds: float) -> bool:
                self.calls += 1
                return self.calls > 1

        runtime._watcher_stop = _StopAfterOnePass()

        with patch.object(
            runtime,
            "_next_public_probe_batch",
            return_value=[row],
        ), patch.object(
            runtime,
            "_probe_public_row",
            return_value=_PublicProbeOutcome(status="failed", cooldown_seconds=180),
        ) as probe_mock, patch.object(
            runtime,
            "_set_public_probe_cooldown",
        ) as cooldown_mock:
            runtime._public_probe_loop()

        probe_mock.assert_called_once_with(row)
        cooldown_mock.assert_called_once_with(123, seconds=180)

    def test_public_probe_loop_skips_round_when_update_queue_not_empty(self) -> None:
        cfg = SimpleNamespace(
            db_listener_public_probe_interval_seconds=60,
            session_name="primary",
            secondary_session_name="",
        )
        runtime = DatabaseChatListenerRuntime(
            cfg=cfg,
            get_conn_fn=lambda: None,
        )

        class _StopAfterOnePass:
            def __init__(self) -> None:
                self.calls = 0

            def wait(self, _seconds: float) -> bool:
                self.calls += 1
                return self.calls > 1

        runtime._watcher_stop = _StopAfterOnePass()
        runtime._queued_chat_ids = {123}

        with patch.object(
            runtime,
            "_next_public_probe_batch",
        ) as batch_mock, patch.object(
            runtime,
            "_probe_public_row",
        ) as probe_mock:
            runtime._public_probe_loop()

        batch_mock.assert_not_called()
        probe_mock.assert_not_called()

    def test_health_snapshot_reports_listener_and_queue_state(self) -> None:
        cfg = SimpleNamespace(
            db_listener_enabled=1,
            db_listener_public_probe_enabled=1,
            session_name="primary",
            secondary_session_name="secondary",
        )
        runtime = DatabaseChatListenerRuntime(
            cfg=cfg,
            get_conn_fn=lambda: None,
        )
        runtime._started = True
        runtime._db_chat_rows_by_id = {1: {"chat_id": 1}, 2: {"chat_id": 2}}
        runtime._queued_chat_ids = {1, 2, 3}
        runtime._listener_clients = {"primary": object()}
        runtime._listener_connected_at = {"primary": 100.0}
        runtime._listener_last_error = {"secondary": "session invalid"}
        runtime._listener_last_error_at = {"secondary": 120.0}
        runtime._last_update_success_at = 150.0
        runtime._last_probe_status = "changed"
        runtime._last_probe_result_at = 160.0

        with patch.object(db_listener.time, "time", return_value=200.0):
            snapshot = runtime.health_snapshot()

        self.assertTrue(snapshot["started"])
        self.assertEqual(2, snapshot["tracked_chat_count"])
        self.assertEqual(3, snapshot["queued_chat_count"])
        self.assertEqual(1, snapshot["active_listener_count"])
        self.assertEqual(2, snapshot["configured_listener_count"])
        self.assertEqual("1970-01-01 00:02:30", snapshot["last_update_success_at"])
        self.assertEqual("changed", snapshot["last_probe_status"])
        accounts = {item["key"]: item for item in snapshot["accounts"]}
        self.assertTrue(accounts["primary"]["connected"])
        self.assertEqual("session invalid", accounts["secondary"]["last_error"])

    def test_trigger_manual_probe_returns_probe_results(self) -> None:
        cfg = SimpleNamespace(
            session_name="primary",
            secondary_session_name="",
        )
        runtime = DatabaseChatListenerRuntime(
            cfg=cfg,
            get_conn_fn=lambda: None,
        )
        rows = [
            {"chat_id": 1, "chat_title": "one", "chat_username": "one"},
            {"chat_id": 2, "chat_title": "two", "chat_username": "two"},
        ]

        with patch.object(
            runtime,
            "_next_public_probe_batch",
            return_value=rows,
        ), patch.object(
            runtime,
            "_probe_public_row",
            side_effect=[
                _PublicProbeOutcome(status="changed", cooldown_seconds=120),
                _PublicProbeOutcome(status="unchanged", cooldown_seconds=300),
            ],
        ), patch.object(
            runtime,
            "_set_public_probe_cooldown",
        ) as cooldown_mock:
            result = runtime.trigger_manual_probe(limit=2)

        self.assertTrue(result["ok"])
        self.assertEqual(1, result["triggered"])
        self.assertEqual(2, len(result["items"]))
        cooldown_mock.assert_any_call(1, seconds=120)
        cooldown_mock.assert_any_call(2, seconds=300)

    def test_ensure_database_chat_listener_runtime_starts_once(self) -> None:
        cfg = SimpleNamespace()
        started = []

        with patch.object(
            DatabaseChatListenerRuntime,
            "start",
            side_effect=lambda self=None: started.append("started"),
        ):
            first = ensure_database_chat_listener_runtime(
                cfg=cfg,
                get_conn_fn=lambda: None,
            )
            second = ensure_database_chat_listener_runtime(
                cfg=cfg,
                get_conn_fn=lambda: None,
            )

        self.assertIs(first, second)
        self.assertEqual(["started"], started)

    def test_start_recovers_scheduler_work_before_starting_worker(self) -> None:
        cfg = SimpleNamespace(
            db_listener_enabled=1,
            sync_scheduler_enabled=1,
            db_listener_public_probe_enabled=0,
            sync_ai_enabled=0,
            session_name="primary",
            secondary_session_name="",
        )
        runtime = DatabaseChatListenerRuntime(
            cfg=cfg,
            get_conn_fn=lambda: None,
        )
        calls = []

        with patch.object(
            runtime._account_runtime,
            "sync_configured_accounts",
            side_effect=lambda: calls.append("accounts"),
        ), patch.object(
            runtime._account_runtime,
            "restore_cooldowns",
            side_effect=lambda: calls.append("cooldowns"),
        ), patch.object(
            runtime,
            "_recover_scheduler_in_flight_updates",
            side_effect=lambda: calls.append("recover"),
        ), patch.object(
            runtime,
            "_refresh_database_chat_cache",
            side_effect=lambda: calls.append("cache"),
        ), patch.object(
            runtime,
            "_refresh_joined_chat_snapshot",
            side_effect=lambda: calls.append("joined"),
        ), patch.object(
            runtime._worker_thread,
            "start",
            side_effect=lambda: calls.append("worker"),
        ), patch.object(
            runtime._refresh_thread,
            "start",
            side_effect=lambda: calls.append("refresh"),
        ), patch.object(
            runtime,
            "_start_listener_threads",
            side_effect=lambda: calls.append("listeners"),
        ):
            runtime.start()

        self.assertEqual(
            ["accounts", "cooldowns", "recover", "cache", "joined", "worker", "refresh", "listeners"],
            calls,
        )

    def test_scheduler_recovery_logs_only_when_work_was_released(self) -> None:
        cfg = SimpleNamespace(sync_scheduler_enabled=1)
        runtime = DatabaseChatListenerRuntime(
            cfg=cfg,
            get_conn_fn=lambda: object(),
        )

        with patch.object(
            db_listener.sync_scheduler,
            "recover_in_flight_pending_updates",
            return_value=0,
        ), patch.object(db_listener, "_listener_log") as log_mock:
            self.assertEqual(0, runtime._recover_scheduler_in_flight_updates())
            log_mock.assert_not_called()

        with patch.object(
            db_listener.sync_scheduler,
            "recover_in_flight_pending_updates",
            return_value=2,
        ), patch.object(db_listener, "_listener_log") as log_mock:
            self.assertEqual(2, runtime._recover_scheduler_in_flight_updates())
            log_mock.assert_called_once()
            self.assertIn("2", log_mock.call_args.args[1])

    def test_startup_recovery_clears_account_slots_when_scheduler_is_disabled(
        self,
    ) -> None:
        runtime = DatabaseChatListenerRuntime(
            cfg=SimpleNamespace(sync_scheduler_enabled=0),
            get_conn_fn=lambda: object(),
        )

        with patch.object(
            db_listener.sync_scheduler,
            "recover_in_flight_pending_updates",
            return_value=0,
        ) as recover_mock:
            self.assertEqual(0, runtime._recover_scheduler_in_flight_updates())

        recover_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
