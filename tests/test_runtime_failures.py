import asyncio
import logging
import os
import queue
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import patch

import tg_harvest.admin_jobs.core as admin_jobs_core
import tg_harvest.admin_jobs.runtime as admin_jobs_runtime
import tg_harvest.admin_jobs.store as admin_jobs_store
from tg_harvest.admin_jobs.cleanup import (
    _build_cleanup_like_patterns,
    _build_cleanup_targets_table,
    _execute_cleanup_deletion_batches,
)
from tg_harvest.admin_jobs.common import admin_error_message, resolve_chat_entity
from tg_harvest.admin_jobs.core import (
    _admin_get_active_job,
    _admin_job_append_log,
    _admin_job_create,
    _admin_job_get_logs,
    _admin_job_get_snapshot,
    _admin_job_set_status,
    _admin_job_stop_requested,
    _admin_recover_interrupted_jobs,
    _admin_request_job_stop,
    _admin_try_create_exclusive_job,
    _AdminJobThreadLogHandler,
    job_context,
    job_log_passthrough_enabled,
)
from tg_harvest.admin_jobs.range_streaming import (
    RangeHarvestAccount,
    stream_entity_ranges_to_writer,
)
from tg_harvest.admin_jobs.runners import (
    _ACCOUNT_FLOOD_COOLDOWNS,
    _admin_get_chat_message_count,
    _admin_harvest_job_runner,
    _admin_process_single_chat_update,
    _admin_update_account_start_delay,
    _admin_update_all_chats,
    _admin_update_effective_concurrency,
    _admin_update_effective_start_gap_seconds,
    _admin_update_primary_soft_cap,
    _admin_update_secondary_public_resolve_reserve,
    _admin_update_secondary_target_count,
    _admin_update_secondary_username_gap_seconds,
    _admin_update_start_gap_seconds,
    _auto_secondary_public_resolve_limit,
    _build_admin_update_account_assignments,
    _delete_chat_data,
    _delete_empty_chats_data,
    _load_admin_update_rows,
    _resolve_harvest_target_entities,
    _resolve_target_entities_for_account,
    _run_simple_admin_job_with_conn,
    _try_stream_new_chat_multi_account_ranges,
)
from tg_harvest.admin_jobs.sessions import _disconnect_worker_client
from tg_harvest.admin_jobs.store import _admin_fetch_job_snapshot_row
from tg_harvest.admin_jobs.streaming import stream_entity_harvest_to_writer
from tg_harvest.admin_jobs.update_writer import ChatUpdateWriteCoordinator
from tg_harvest.domain.dedupe import dedupe_promotional_duplicates
from tg_harvest.ingest.flood_wait import AccountFloodWaitError
from tg_harvest.ingest.media_groups import refresh_media_groups_for_chat
from tg_harvest.ingest.parse import HarvestCounters
from tg_harvest.ingest.runner import (
    _format_harvest_progress_message,
)
from tg_harvest.ingest.store import (
    batch_upsert,
    load_grouped_ids_for_messages,
)
from tg_harvest.storage.clone import (
    create_clone_migration,
    create_clone_plan,
    create_clone_run,
)
from tg_harvest.storage.connection import detect_sqlite_features
from tg_harvest.storage.schema import create_schema, refresh_chat_message_counts


class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, _sql):
        return None

    def fetchall(self):
        return list(self._rows)


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return _FakeCursor(self._rows)

    def close(self):
        return None


class _FakeClient:
    def get_entity(self, chat_id):
        return SimpleNamespace(id=chat_id, title=f"chat-{chat_id}", username=None)

    def disconnect(self):
        return None


class AdminUpdateRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        _ACCOUNT_FLOOD_COOLDOWNS.clear()

    def test_load_admin_update_rows_sorts_by_last_message_time_desc(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE chats (
                chat_id INTEGER PRIMARY KEY,
                chat_title TEXT NOT NULL,
                chat_username TEXT,
                message_count INTEGER NOT NULL DEFAULT 0,
                last_seen_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE messages (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                msg_date_text TEXT NOT NULL,
                msg_date_ts INTEGER NOT NULL,
                PRIMARY KEY(chat_id, message_id)
            )
            """
        )
        cur.executemany(
            """
            INSERT INTO chats(
                chat_id,
                chat_title,
                chat_username,
                message_count,
                last_seen_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (1, "Older but Many", None, 50, "2026-01-01 00:00:00"),
                (2, "Newer but Few", None, 3, "2026-01-02 00:00:00"),
                (3, "No Messages", None, 0, "2026-01-03 00:00:00"),
            ],
        )
        cur.executemany(
            """
            INSERT INTO messages(
                chat_id,
                message_id,
                msg_date_text,
                msg_date_ts
            )
            VALUES (?, ?, ?, ?)
            """,
            [
                (1, 1001, "2026-03-01 10:00:00", 1772359200),
                (1, 1002, "2026-03-10 10:00:00", 1773136800),
                (2, 2001, "2026-03-11 09:00:00", 1773229200),
            ],
        )
        conn.commit()

        try:
            rows = _load_admin_update_rows(conn)
        finally:
            conn.close()

        self.assertEqual(
            [2, 1, 3],
            [int(row["chat_id"]) for row in rows],
        )

    def test_admin_update_effective_concurrency_caps_each_account_to_one_worker(
        self,
    ) -> None:
        per_account, effective = _admin_update_effective_concurrency(
            SimpleNamespace(),
            configured_concurrency=4,
            active_account_count=2,
        )
        self.assertEqual(1, per_account)
        self.assertEqual(2, effective)

        per_account, effective = _admin_update_effective_concurrency(
            SimpleNamespace(),
            configured_concurrency=4,
            active_account_count=1,
        )
        self.assertEqual(4, per_account)
        self.assertEqual(4, effective)

    def test_admin_update_account_start_delay_applies_gap_per_account(self) -> None:
        next_start_at: dict[str, float] = {}

        wait = _admin_update_account_start_delay(
            next_start_at,
            "primary",
            gap_seconds=0.5,
            now=100.0,
        )
        self.assertEqual(0.0, wait)
        self.assertEqual(100.5, next_start_at["primary"])

        wait = _admin_update_account_start_delay(
            next_start_at,
            "primary",
            gap_seconds=0.5,
            now=100.2,
        )
        self.assertAlmostEqual(0.3, wait, places=6)
        self.assertEqual(101.0, next_start_at["primary"])

        wait = _admin_update_account_start_delay(
            next_start_at,
            "secondary",
            gap_seconds=0.5,
            now=100.2,
        )
        self.assertEqual(0.0, wait)
        self.assertEqual(100.7, next_start_at["secondary"])

    def test_chat_message_count_reads_chat_summary_not_messages_count(self) -> None:
        statements = []

        class _Cursor:
            def execute(self, sql, params=()):
                statements.append(" ".join(str(sql).split()))

            def fetchone(self):
                return {"cnt": 42}

        class _Conn:
            def cursor(self):
                return _Cursor()

            def close(self):
                return None

        count = _admin_get_chat_message_count(lambda: _Conn(), 1)

        self.assertEqual(42, count)
        self.assertEqual(1, len(statements))
        self.assertIn("message_count", statements[0])
        self.assertIn("FROM chats", statements[0])
        self.assertNotIn("COUNT(*)", statements[0])
        self.assertNotIn("FROM messages", statements[0])

    def test_write_coordinator_close_raises_when_writer_thread_does_not_stop(self) -> None:
        release = threading.Event()

        def blocked_get_conn():
            release.wait(timeout=1.0)
            return _FakeConn([])

        coordinator = ChatUpdateWriteCoordinator(
            job_id="job-stuck",
            get_conn_fn=blocked_get_conn,
            queue_maxsize=1,
        )
        try:
            with patch(
                "tg_harvest.admin_jobs.update_writer.CLOSE_JOIN_TIMEOUT_SEC",
                0.01,
            ), self.assertRaisesRegex(RuntimeError, "写入线程关闭超时"):
                coordinator.close()
        finally:
            release.set()
            coordinator._thread.join(timeout=1.0)

    def test_write_coordinator_close_raises_when_stop_signal_cannot_be_queued(self) -> None:
        release = threading.Event()

        def blocked_get_conn():
            release.wait(timeout=1.0)
            return _FakeConn([])

        coordinator = ChatUpdateWriteCoordinator(
            job_id="job-full",
            get_conn_fn=blocked_get_conn,
            queue_maxsize=1,
        )
        try:
            coordinator._queue.put_nowait({"kind": "batch", "chat_id": 1})
            with patch.object(
                coordinator._queue, "put", side_effect=queue.Full
            ), self.assertRaisesRegex(RuntimeError, "无法发送停止信号"):
                coordinator.close()
        finally:
            release.set()
            coordinator._thread.join(timeout=1.0)

    def test_all_chat_update_returns_false_when_any_worker_fails(self) -> None:
        rows = [
            {"chat_id": 1, "chat_title": "ok", "chat_username": "ok_name"},
            {"chat_id": 2, "chat_title": "bad", "chat_username": "bad_name"},
        ]
        cfg = SimpleNamespace(admin_update_concurrency=1, session_name="sess")
        logs = []

        def append_log(_job_id, message):
            logs.append(str(message))

        def fake_harvest(_conn, _client, entity, _chat_id, **_kwargs):
            if int(getattr(entity, "id", 0)) == 2:
                raise RuntimeError("boom")
            counters = SimpleNamespace(seen=1, written=1, parse_failures=0)
            return counters, set(), False

        class _CoordinatorStub:
            def __init__(self, **_kwargs):
                self.finalized = []

            def register_chat(self, _chat_id):
                return None

            def submit_chat_start(self, **_kwargs):
                return None

            def submit_batch(self, **_kwargs):
                return None

            def submit_finalize(self, **kwargs):
                self.finalized.append(kwargs)

            def wait_for_chat(self, _chat_id):
                return None

            def close(self):
                return None

        progress_calls = []

        with patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            return_value=_FakeClient(),
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_get_chat_message_count",
            return_value=0,
        ), patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.ingest.runner._harvest_messages_for_entity",
            side_effect=fake_harvest,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            side_effect=lambda *args, **kwargs: progress_calls.append((args, kwargs)) or True,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_stop_requested",
            return_value=False,
        ):
            ok = _admin_update_all_chats(
                "job-1",
                None,
                lambda: _FakeConn(rows),
                append_log,
                cfg,
            )

        self.assertFalse(ok)
        self.assertTrue(any("增量采集失败" in line for line in logs))
        self.assertTrue(any("bad (ID=2)" in line for line in logs))
        self.assertTrue(any("失败 1 个" in line for line in logs))
        self.assertTrue(any("失败列表：bad (ID=2)" in line for line in logs))
        self.assertTrue(any(kwargs.get("total") == 2 for _, kwargs in progress_calls))
        self.assertTrue(any(kwargs.get("stage") == "updating" for _, kwargs in progress_calls))

    def test_all_chat_update_uses_database_rows_without_joined_inventory_filter(
        self,
    ) -> None:
        rows = [
            {"chat_id": 1, "chat_title": "joined", "chat_username": "joined_name"},
            {"chat_id": 2, "chat_title": "not-joined", "chat_username": "stored_name"},
        ]
        cfg = SimpleNamespace(admin_update_concurrency=1, session_name="sess")
        logs = []
        submitted_chat_ids = []

        def append_log(_job_id, message):
            logs.append(str(message))

        class _NoInventoryClient:
            def get_entity(self, chat_id):
                return SimpleNamespace(id=chat_id, title=f"chat-{chat_id}", username=None)

            def iter_dialogs(self):
                raise AssertionError("batch update should not scan joined dialogs")

            def disconnect(self):
                return None

        class _CoordinatorStub:
            def __init__(self, **_kwargs):
                return None

            def register_chat(self, _chat_id):
                return None

            def submit_chat_start(self, **_kwargs):
                return None

            def submit_batch(self, **_kwargs):
                return None

            def submit_finalize(self, **_kwargs):
                return None

            def wait_for_chat(self, _chat_id):
                return None

            def close(self):
                return None

        def fake_harvest(_conn, _client, entity, _chat_id, **_kwargs):
            submitted_chat_ids.append(int(entity.id))
            counters = SimpleNamespace(seen=1, written=1, parse_failures=0)
            return counters, set(), False

        with patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            return_value=_NoInventoryClient(),
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_get_chat_message_count",
            return_value=0,
        ), patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.ingest.runner._harvest_messages_for_entity",
            side_effect=fake_harvest,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_stop_requested",
            return_value=False,
        ), patch(
            "tg_harvest.admin_jobs.runners.time.sleep",
            return_value=None,
        ):
            ok = _admin_update_all_chats(
                "job-1",
                None,
                lambda: _FakeConn(rows),
                append_log,
                cfg,
            )

        self.assertTrue(ok)
        self.assertEqual([1, 2], submitted_chat_ids)
        self.assertTrue(
            any("按数据库最后一条消息发送时间从新到旧逐一尝试更新" in line for line in logs)
        )
        self.assertFalse(any("本次仅更新" in line for line in logs))

    def test_all_chat_update_enables_probe_mode_only_for_two_or_fewer_chats(self) -> None:
        rows = [
            {"chat_id": 1, "chat_title": "one", "chat_username": "one_name"},
            {"chat_id": 2, "chat_title": "two", "chat_username": "two_name"},
        ]
        cfg = SimpleNamespace(admin_update_concurrency=1, session_name="sess")
        logs = []
        harvest_progress = []

        class _CoordinatorStub:
            def __init__(self, **_kwargs):
                return None

            def register_chat(self, _chat_id):
                return None

            def submit_chat_start(self, **_kwargs):
                return None

            def submit_batch(self, **_kwargs):
                return None

            def submit_finalize(self, **_kwargs):
                return None

            def wait_for_chat(self, _chat_id):
                return None

            def close(self):
                return None

        def fake_harvest(_conn, _client, entity, _chat_id, **kwargs):
            harvest_progress.append(
                (
                    int(entity.id),
                    kwargs.get("progress_total"),
                    kwargs.get("progress_prefix"),
                )
            )
            counters = SimpleNamespace(seen=1, written=1, parse_failures=0)
            return counters, set(), False

        with patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            return_value=_FakeClient(),
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_get_chat_message_count",
            return_value=0,
        ), patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.admin_jobs.runners._probe_update_progress_total",
            side_effect=[5054, 6060],
        ) as probe_mock, patch(
            "tg_harvest.ingest.runner._harvest_messages_for_entity",
            side_effect=fake_harvest,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_stop_requested",
            return_value=False,
        ), patch(
            "tg_harvest.admin_jobs.runners.time.sleep",
            return_value=None,
        ):
            ok = _admin_update_all_chats(
                "job-probe-small",
                None,
                lambda: _FakeConn(rows),
                lambda _job_id, message: logs.append(str(message)),
                cfg,
            )

        self.assertTrue(ok)
        self.assertEqual(2, probe_mock.call_count)
        self.assertEqual(
            [
                (1, 5054, "[1/2] one (ID=1) 正在采集"),
                (2, 6060, "[2/2] two (ID=2) 正在采集"),
            ],
            harvest_progress,
        )
        self.assertTrue(any("已启用探测模式" in line for line in logs))

    def test_all_chat_update_keeps_default_mode_when_chat_count_exceeds_two(self) -> None:
        rows = [
            {"chat_id": 1, "chat_title": "one", "chat_username": "one_name"},
            {"chat_id": 2, "chat_title": "two", "chat_username": "two_name"},
            {"chat_id": 3, "chat_title": "three", "chat_username": "three_name"},
        ]
        cfg = SimpleNamespace(admin_update_concurrency=1, session_name="sess")
        logs = []
        harvest_progress = []

        class _CoordinatorStub:
            def __init__(self, **_kwargs):
                return None

            def register_chat(self, _chat_id):
                return None

            def submit_chat_start(self, **_kwargs):
                return None

            def submit_batch(self, **_kwargs):
                return None

            def submit_finalize(self, **_kwargs):
                return None

            def wait_for_chat(self, _chat_id):
                return None

            def close(self):
                return None

        def fake_harvest(_conn, _client, entity, _chat_id, **kwargs):
            harvest_progress.append(
                (
                    int(entity.id),
                    kwargs.get("progress_total"),
                    kwargs.get("progress_prefix"),
                )
            )
            counters = SimpleNamespace(seen=1, written=1, parse_failures=0)
            return counters, set(), False

        with patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            return_value=_FakeClient(),
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_get_chat_message_count",
            return_value=0,
        ), patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.admin_jobs.runners._probe_update_progress_total",
        ) as probe_mock, patch(
            "tg_harvest.ingest.runner._harvest_messages_for_entity",
            side_effect=fake_harvest,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_stop_requested",
            return_value=False,
        ), patch(
            "tg_harvest.admin_jobs.runners.time.sleep",
            return_value=None,
        ):
            ok = _admin_update_all_chats(
                "job-probe-large",
                None,
                lambda: _FakeConn(rows),
                lambda _job_id, message: logs.append(str(message)),
                cfg,
            )

        self.assertTrue(ok)
        probe_mock.assert_not_called()
        self.assertEqual(
            [
                (1, None, "正在采集"),
                (2, None, "正在采集"),
                (3, None, "正在采集"),
            ],
            harvest_progress,
        )
        self.assertFalse(any("已启用探测模式" in line for line in logs))

    def test_all_chat_update_round_robins_secondary_account(self) -> None:
        rows = [
            {"chat_id": 1, "chat_title": "one", "chat_username": "one_name"},
            {"chat_id": 2, "chat_title": "two", "chat_username": "two_name"},
            {"chat_id": 3, "chat_title": "three", "chat_username": "three_name"},
        ]
        cfg = SimpleNamespace(
            admin_update_concurrency=1,
            admin_update_secondary_public_resolve_limit=1,
            session_name="main_sess",
            secondary_session_name="secondary_sess",
            api_id=1,
            api_hash="hash",
        )
        logs = []
        harvest_calls = []

        class _AccountClient:
            def __init__(self, label):
                self.label = label

            def get_entity(self, chat_id):
                return SimpleNamespace(
                    id=chat_id,
                    title=f"{self.label}-{chat_id}",
                    username=None,
                )

            def disconnect(self):
                return None

        class _CoordinatorStub:
            def __init__(self, **_kwargs):
                return None

            def register_chat(self, _chat_id):
                return None

            def submit_chat_start(self, **_kwargs):
                return None

            def submit_batch(self, **_kwargs):
                return None

            def submit_finalize(self, **_kwargs):
                return None

            def wait_for_chat(self, _chat_id):
                return None

            def close(self):
                return None

        def create_client(account_cfg, _worker_id):
            return _AccountClient(account_cfg.session_name)

        def fake_harvest(_conn, client, entity, _chat_id, **_kwargs):
            harvest_calls.append((client.label, int(entity.id)))
            counters = SimpleNamespace(seen=1, written=1, parse_failures=0)
            return counters, set(), False

        with patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            side_effect=create_client,
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_get_chat_message_count",
            return_value=0,
        ), patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.ingest.runner._harvest_messages_for_entity",
            side_effect=fake_harvest,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_stop_requested",
            return_value=False,
        ), patch(
            "tg_harvest.admin_jobs.runners.time.sleep",
            return_value=None,
        ):
            ok = _admin_update_all_chats(
                "job-accounts",
                None,
                lambda: _FakeConn(rows),
                lambda _job_id, message: logs.append(str(message)),
                cfg,
            )

        self.assertTrue(ok)
        self.assertEqual(
            [
                ("main_sess", 1),
                ("secondary_sess", 2),
                ("main_sess", 3),
            ],
            harvest_calls,
        )
        self.assertTrue(any("第二账号已加入批量更新调度" in line for line in logs))
        self.assertTrue(any("账号数：2" in line for line in logs))

    def test_all_chat_update_prefers_primary_for_rows_without_username(self) -> None:
        rows = [
            {"chat_id": 1, "chat_title": "private-ish", "chat_username": None},
            {"chat_id": 2, "chat_title": "public-a", "chat_username": "public_a"},
            {"chat_id": 3, "chat_title": "public-b", "chat_username": "public_b"},
        ]
        cfg = SimpleNamespace(
            admin_update_concurrency=1,
            admin_update_secondary_public_resolve_limit=1,
            session_name="main_sess",
            secondary_session_name="secondary_sess",
            api_id=1,
            api_hash="hash",
        )
        logs = []
        harvest_calls = []

        class _AccountClient:
            def __init__(self, label):
                self.label = label

            def get_entity(self, chat_id):
                return SimpleNamespace(
                    id=chat_id,
                    title=f"{self.label}-{chat_id}",
                    username=None,
                )

            def disconnect(self):
                return None

        class _CoordinatorStub:
            def __init__(self, **_kwargs):
                return None

            def register_chat(self, _chat_id):
                return None

            def submit_chat_start(self, **_kwargs):
                return None

            def submit_batch(self, **_kwargs):
                return None

            def submit_finalize(self, **_kwargs):
                return None

            def wait_for_chat(self, _chat_id):
                return None

            def close(self):
                return None

        def create_client(account_cfg, _worker_id):
            return _AccountClient(account_cfg.session_name)

        def fake_harvest(_conn, client, entity, _chat_id, **_kwargs):
            harvest_calls.append((client.label, int(entity.id)))
            counters = SimpleNamespace(seen=1, written=1, parse_failures=0)
            return counters, set(), False

        with patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            side_effect=create_client,
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_get_chat_message_count",
            return_value=0,
        ), patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.ingest.runner._harvest_messages_for_entity",
            side_effect=fake_harvest,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_stop_requested",
            return_value=False,
        ), patch(
            "tg_harvest.admin_jobs.runners.time.sleep",
            return_value=None,
        ):
            ok = _admin_update_all_chats(
                "job-public-plan",
                None,
                lambda: _FakeConn(rows),
                lambda _job_id, message: logs.append(str(message)),
                cfg,
            )

        self.assertTrue(ok)
        self.assertEqual(
            [
                ("main_sess", 1),
                ("main_sess", 2),
                ("secondary_sess", 3),
            ],
            harvest_calls,
        )
        self.assertTrue(any("缺少公开用户名" in line for line in logs))

    def test_all_chat_update_retries_primary_when_secondary_fails(self) -> None:
        rows = [
            {"chat_id": 1, "chat_title": "one", "chat_username": "one_name"},
            {"chat_id": 2, "chat_title": "two", "chat_username": "two_name"},
        ]
        cfg = SimpleNamespace(
            admin_update_concurrency=1,
            admin_update_secondary_public_resolve_limit=1,
            session_name="main_sess",
            secondary_session_name="secondary_sess",
            api_id=1,
            api_hash="hash",
        )
        logs = []
        harvest_calls = []

        class _AccountClient:
            def __init__(self, label):
                self.label = label

            def get_entity(self, chat_id):
                return SimpleNamespace(
                    id=chat_id,
                    title=f"{self.label}-{chat_id}",
                    username=None,
                )

            def disconnect(self):
                return None

        class _CoordinatorStub:
            def __init__(self, **_kwargs):
                return None

            def register_chat(self, _chat_id):
                return None

            def submit_chat_start(self, **_kwargs):
                return None

            def submit_batch(self, **_kwargs):
                return None

            def submit_finalize(self, **_kwargs):
                return None

            def wait_for_chat(self, _chat_id):
                return None

            def close(self):
                return None

        def create_client(account_cfg, _worker_id):
            return _AccountClient(account_cfg.session_name)

        def fake_harvest(_conn, client, entity, _chat_id, **_kwargs):
            harvest_calls.append((client.label, int(entity.id)))
            if client.label == "secondary_sess" and int(entity.id) == 2:
                raise RuntimeError("secondary cannot read")
            counters = SimpleNamespace(seen=1, written=1, parse_failures=0)
            return counters, set(), False

        with patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            side_effect=create_client,
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_get_chat_message_count",
            return_value=0,
        ), patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.ingest.runner._harvest_messages_for_entity",
            side_effect=fake_harvest,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_stop_requested",
            return_value=False,
        ), patch(
            "tg_harvest.admin_jobs.runners.time.sleep",
            return_value=None,
        ):
            ok = _admin_update_all_chats(
                "job-fallback",
                None,
                lambda: _FakeConn(rows),
                lambda _job_id, message: logs.append(str(message)),
                cfg,
            )

        self.assertTrue(ok)
        self.assertEqual(
            [
                ("main_sess", 1),
                ("secondary_sess", 2),
                ("main_sess", 2),
            ],
            harvest_calls,
        )
        self.assertTrue(any("第二账号更新失败，切换其他账号重试" in line for line in logs))
        self.assertTrue(any("成功 2 个，失败 0 个" in line for line in logs))

    def test_all_chat_update_switches_to_secondary_when_primary_fails(
        self,
    ) -> None:
        rows = [
            {"chat_id": 1, "chat_title": "one", "chat_username": "one_name"},
        ]
        cfg = SimpleNamespace(
            admin_update_concurrency=1,
            session_name="main_sess",
            secondary_session_name="secondary_sess",
            api_id=1,
            api_hash="hash",
        )
        logs = []
        harvest_calls = []

        class _AccountClient:
            def __init__(self, label):
                self.label = label

            def get_entity(self, chat_id):
                return SimpleNamespace(
                    id=chat_id,
                    title=f"{self.label}-{chat_id}",
                    username=None,
                )

            def disconnect(self):
                return None

        class _CoordinatorStub:
            def __init__(self, **_kwargs):
                return None

            def register_chat(self, _chat_id):
                return None

            def submit_chat_start(self, **_kwargs):
                return None

            def submit_batch(self, **_kwargs):
                return None

            def submit_finalize(self, **_kwargs):
                return None

            def wait_for_chat(self, _chat_id):
                return None

            def close(self):
                return None

        def create_client(account_cfg, _worker_id):
            return _AccountClient(account_cfg.session_name)

        def fake_harvest(_conn, client, entity, _chat_id, **_kwargs):
            harvest_calls.append((client.label, int(entity.id)))
            if client.label == "main_sess":
                raise RuntimeError("primary cannot read")
            counters = SimpleNamespace(seen=1, written=1, parse_failures=0)
            return counters, set(), False

        with patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            side_effect=create_client,
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_get_chat_message_count",
            return_value=0,
        ), patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.ingest.runner._harvest_messages_for_entity",
            side_effect=fake_harvest,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_stop_requested",
            return_value=False,
        ), patch(
            "tg_harvest.admin_jobs.runners.time.sleep",
            return_value=None,
        ):
            ok = _admin_update_all_chats(
                "job-primary-general-fallback",
                None,
                lambda: _FakeConn(rows),
                lambda _job_id, message: logs.append(str(message)),
                cfg,
            )

        self.assertTrue(ok)
        self.assertEqual([("main_sess", 1), ("secondary_sess", 1)], harvest_calls)
        self.assertTrue(any("主账号更新失败，切换其他账号重试" in line for line in logs))
        self.assertTrue(any("已切换账号完成采集" in line for line in logs))

    def test_all_chat_update_assignment_uses_cache_before_public_username(self) -> None:
        rows = [
            {"chat_id": 1, "chat_title": "cached-private", "chat_username": None},
            {"chat_id": 2, "chat_title": "public-a", "chat_username": "public_a"},
            {"chat_id": 3, "chat_title": "public-b", "chat_username": "public_b"},
            {"chat_id": 4, "chat_title": "public-c", "chat_username": "public_c"},
        ]
        primary = SimpleNamespace(key="primary", label="主账号")
        secondary = SimpleNamespace(key="secondary", label="第二账号")

        assignments, counts = _build_admin_update_account_assignments(
            list(enumerate(rows, start=1)),
            [primary, secondary],
            secondary_cached_chat_ids={1},
            secondary_public_resolve_limit=1,
        )

        secondary_indexes = {
            idx for idx, account in assignments.items() if account.key == "secondary"
        }
        self.assertIn(1, secondary_indexes)
        self.assertEqual(2, len(secondary_indexes))
        self.assertEqual(1, counts["secondary_cached"])
        self.assertEqual(1, counts["secondary_public"])
        self.assertEqual(1, counts["secondary_public_eligible"])
        self.assertEqual(3, counts["secondary_public_candidates"])
        self.assertEqual(2, counts["secondary_public_skipped"])

    def test_all_chat_update_assignment_can_disable_secondary_public_warmup(
        self,
    ) -> None:
        rows = [
            {"chat_id": 1, "chat_title": "public-a", "chat_username": "public_a"},
            {"chat_id": 2, "chat_title": "public-b", "chat_username": "public_b"},
        ]
        primary = SimpleNamespace(key="primary", label="主账号")
        secondary = SimpleNamespace(key="secondary", label="第二账号")

        assignments, counts = _build_admin_update_account_assignments(
            list(enumerate(rows, start=1)),
            [primary, secondary],
            secondary_cached_chat_ids=set(),
            secondary_public_resolve_limit=0,
        )

        self.assertTrue(all(account.key == "primary" for account in assignments.values()))
        self.assertEqual(0, counts["secondary"])
        self.assertEqual(0, counts["secondary_public"])
        self.assertEqual(0, counts["secondary_public_eligible"])
        self.assertEqual(2, counts["secondary_public_candidates"])
        self.assertEqual(2, counts["secondary_public_skipped"])

    def test_auto_secondary_public_resolve_limit_scales_with_large_queue(self) -> None:
        self.assertEqual(
            0,
            _auto_secondary_public_resolve_limit(
                secondary_cached_row_count=0,
                total_rows=0,
            ),
        )
        self.assertEqual(
            30,
            _auto_secondary_public_resolve_limit(
                secondary_cached_row_count=0,
                total_rows=60,
            ),
        )
        self.assertEqual(
            156,
            _auto_secondary_public_resolve_limit(
                secondary_cached_row_count=0,
                total_rows=320,
            ),
        )
        self.assertEqual(
            171,
            _auto_secondary_public_resolve_limit(
                secondary_cached_row_count=25,
                total_rows=400,
            ),
        )
        self.assertEqual(
            155,
            _auto_secondary_public_resolve_limit(
                secondary_cached_row_count=120,
                total_rows=480,
            ),
        )

    def test_admin_update_secondary_target_count_limits_primary_share_on_large_queue(
        self,
    ) -> None:
        self.assertEqual(30, _admin_update_primary_soft_cap(60))
        self.assertEqual(186, _admin_update_primary_soft_cap(320))
        self.assertEqual(0, _admin_update_secondary_public_resolve_reserve(60))
        self.assertEqual(22, _admin_update_secondary_public_resolve_reserve(320))
        self.assertEqual(
            134,
            _admin_update_secondary_target_count(
                total_rows=320,
                secondary_eligible=320,
                secondary_cached_eligible=0,
            ),
        )
        self.assertEqual(
            168,
            _admin_update_secondary_target_count(
                total_rows=400,
                secondary_eligible=400,
                secondary_cached_eligible=25,
            ),
        )
        self.assertEqual(
            116,
            _admin_update_secondary_target_count(
                total_rows=240,
                secondary_eligible=134,
                secondary_cached_eligible=80,
            ),
        )

    def test_all_chat_update_large_dual_queue_shifts_more_load_to_secondary(self) -> None:
        rows = [
            {
                "chat_id": idx,
                "chat_title": f"chat-{idx}",
                "chat_username": f"chat_{idx}",
            }
            for idx in range(1, 321)
        ]
        primary = SimpleNamespace(key="primary", label="主账号")
        secondary = SimpleNamespace(key="secondary", label="第二账号")

        assignments, counts = _build_admin_update_account_assignments(
            list(enumerate(rows, start=1)),
            [primary, secondary],
            secondary_cached_chat_ids=set(),
            secondary_public_resolve_limit=None,
        )

        secondary_indexes = {
            idx for idx, account in assignments.items() if account.key == "secondary"
        }
        self.assertEqual(134, len(secondary_indexes))
        self.assertEqual(186, counts["primary"])
        self.assertEqual(134, counts["secondary"])
        self.assertEqual(186, counts["primary_soft_cap"])
        self.assertEqual(134, counts["secondary_target"])
        self.assertEqual(134, counts["secondary_public"])
        self.assertEqual(164, counts["secondary_public_skipped"])

    def test_all_chat_update_allows_secondary_public_username_resolve(self) -> None:
        rows = [
            {"chat_id": 1, "chat_title": "one", "chat_username": "one_name"},
            {"chat_id": 2, "chat_title": "two", "chat_username": "two_name"},
        ]
        cfg = SimpleNamespace(
            admin_update_concurrency=1,
            admin_update_secondary_public_resolve_limit=2,
            session_name="main_sess",
            secondary_session_name="secondary_sess",
            api_id=1,
            api_hash="hash",
        )
        logs = []
        harvest_calls = []
        secondary_username_calls = []

        class _AccountClient:
            def __init__(self, label):
                self.label = label

            def get_entity(self, key):
                if self.label == "secondary_sess":
                    if isinstance(key, str):
                        secondary_username_calls.append(key)
                        return SimpleNamespace(id=2, title=f"{self.label}-{key}")
                    raise ValueError("could not find the input entity")
                return SimpleNamespace(id=int(key), title=f"{self.label}-{key}")

            def disconnect(self):
                return None

        class _CoordinatorStub:
            def __init__(self, **_kwargs):
                return None

            def register_chat(self, _chat_id):
                return None

            def submit_chat_start(self, **_kwargs):
                return None

            def submit_batch(self, **_kwargs):
                return None

            def submit_finalize(self, **_kwargs):
                return None

            def wait_for_chat(self, _chat_id):
                return None

            def close(self):
                return None

        def create_client(account_cfg, _worker_id):
            return _AccountClient(account_cfg.session_name)

        def fake_harvest(_conn, client, entity, _chat_id, **_kwargs):
            harvest_calls.append((client.label, int(entity.id)))
            counters = SimpleNamespace(seen=1, written=1, parse_failures=0)
            return counters, set(), False

        with patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            side_effect=create_client,
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_get_chat_message_count",
            return_value=0,
        ), patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.ingest.runner._harvest_messages_for_entity",
            side_effect=fake_harvest,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_stop_requested",
            return_value=False,
        ), patch(
            "tg_harvest.admin_jobs.runners.time.sleep",
            return_value=None,
        ):
            ok = _admin_update_all_chats(
                "job-secondary-limited-username",
                None,
                lambda: _FakeConn(rows),
                lambda _job_id, message: logs.append(str(message)),
                cfg,
            )

        self.assertTrue(ok)
        self.assertEqual(["two_name"], secondary_username_calls)
        self.assertEqual([("main_sess", 1), ("secondary_sess", 2)], harvest_calls)
        self.assertTrue(any("双账号协同策略" in line for line in logs))

    def test_all_chat_update_auto_warms_secondary_public_username_budget(
        self,
    ) -> None:
        rows = [
            {"chat_id": 1, "chat_title": "one", "chat_username": "one_name"},
            {"chat_id": 2, "chat_title": "two", "chat_username": "two_name"},
        ]
        cfg = SimpleNamespace(
            admin_update_concurrency=1,
            session_name="main_sess",
            secondary_session_name="secondary_sess",
            api_id=1,
            api_hash="hash",
        )
        logs = []
        harvest_calls = []
        secondary_username_calls = []

        class _AccountClient:
            def __init__(self, label):
                self.label = label

            def get_entity(self, key):
                if self.label == "secondary_sess" and isinstance(key, str):
                    secondary_username_calls.append(key)
                    return SimpleNamespace(id=2, title=f"{self.label}-{key}")
                if self.label == "secondary_sess" and not isinstance(key, str):
                    raise ValueError("could not find the input entity")
                return SimpleNamespace(id=int(key), title=f"{self.label}-{key}")

            def disconnect(self):
                return None

        class _CoordinatorStub:
            def __init__(self, **_kwargs):
                return None

            def register_chat(self, _chat_id):
                return None

            def submit_chat_start(self, **_kwargs):
                return None

            def submit_batch(self, **_kwargs):
                return None

            def submit_finalize(self, **_kwargs):
                return None

            def wait_for_chat(self, _chat_id):
                return None

            def close(self):
                return None

        def create_client(account_cfg, _worker_id):
            return _AccountClient(account_cfg.session_name)

        def fake_harvest(_conn, client, entity, _chat_id, **_kwargs):
            harvest_calls.append((client.label, int(entity.id)))
            counters = SimpleNamespace(seen=1, written=1, parse_failures=0)
            return counters, set(), False

        with patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            side_effect=create_client,
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_get_chat_message_count",
            return_value=0,
        ), patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.ingest.runner._harvest_messages_for_entity",
            side_effect=fake_harvest,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_stop_requested",
            return_value=False,
        ), patch(
            "tg_harvest.admin_jobs.runners.time.sleep",
            return_value=None,
        ):
            ok = _admin_update_all_chats(
                "job-secondary-defaults-to-cache-only",
                None,
                lambda: _FakeConn(rows),
                lambda _job_id, message: logs.append(str(message)),
                cfg,
            )

        self.assertTrue(ok)
        self.assertEqual(["two_name"], secondary_username_calls)
        self.assertEqual([("main_sess", 1), ("secondary_sess", 2)], harvest_calls)
        self.assertTrue(
            any("公开 username 候选" in line for line in logs),
            logs,
        )

    def test_all_chat_update_switches_to_secondary_when_primary_flood_waits(self) -> None:
        rows = [
            {"chat_id": 1, "chat_title": "one", "chat_username": "one_name"},
        ]
        cfg = SimpleNamespace(
            admin_update_concurrency=1,
            session_name="main_sess",
            secondary_session_name="secondary_sess",
            api_id=1,
            api_hash="hash",
        )
        logs = []
        harvest_calls = []

        class _AccountClient:
            def __init__(self, label):
                self.label = label

            def get_entity(self, chat_id):
                return SimpleNamespace(
                    id=chat_id,
                    title=f"{self.label}-{chat_id}",
                    username=None,
                )

            def disconnect(self):
                return None

        class _CoordinatorStub:
            def __init__(self, **_kwargs):
                return None

            def register_chat(self, _chat_id):
                return None

            def submit_chat_start(self, **_kwargs):
                return None

            def submit_batch(self, **_kwargs):
                return None

            def submit_finalize(self, **_kwargs):
                return None

            def wait_for_chat(self, _chat_id):
                return None

            def close(self):
                return None

        def create_client(account_cfg, _worker_id):
            return _AccountClient(account_cfg.session_name)

        def fake_harvest(_conn, client, entity, _chat_id, **_kwargs):
            harvest_calls.append((client.label, int(entity.id)))
            if client.label == "main_sess":
                raise AccountFloodWaitError(
                    seconds=70,
                    threshold_seconds=30,
                    account_label="primary",
                    scope="test",
                )
            counters = SimpleNamespace(seen=1, written=1, parse_failures=0)
            return counters, set(), False

        with patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            side_effect=create_client,
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_get_chat_message_count",
            return_value=0,
        ), patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.ingest.runner._harvest_messages_for_entity",
            side_effect=fake_harvest,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_stop_requested",
            return_value=False,
        ), patch(
            "tg_harvest.admin_jobs.runners.time.sleep",
            return_value=None,
        ):
            ok = _admin_update_all_chats(
                "job-primary-flood",
                None,
                lambda: _FakeConn(rows),
                lambda _job_id, message: logs.append(str(message)),
                cfg,
            )

        self.assertTrue(ok)
        self.assertEqual(
            [("main_sess", 1), ("secondary_sess", 1)],
            harvest_calls,
        )
        self.assertTrue(any("主账号 进入长等待冷却" in line for line in logs))
        self.assertTrue(any("已切换账号完成采集" in line for line in logs))

    def test_all_chat_update_keeps_secondary_username_budget_for_primary_takeover(
        self,
    ) -> None:
        rows = [
            {"chat_id": idx, "chat_title": f"chat-{idx}", "chat_username": f"name_{idx}"}
            for idx in range(1, 91)
        ]
        cfg = SimpleNamespace(
            admin_update_concurrency=1,
            session_name="main_sess",
            secondary_session_name="secondary_sess",
            api_id=1,
            api_hash="hash",
        )
        logs = []
        harvest_calls = []
        secondary_username_calls = []
        primary_attempts = {"count": 0}

        class _AccountClient:
            def __init__(self, label):
                self.label = label

            def get_entity(self, key):
                if self.label == "secondary_sess" and isinstance(key, str):
                    secondary_username_calls.append(key)
                    chat_num = int(str(key).split("_")[-1])
                    return SimpleNamespace(id=chat_num, title=f"{self.label}-{key}")
                if self.label == "secondary_sess" and not isinstance(key, str):
                    raise ValueError("could not find the input entity")
                return SimpleNamespace(id=int(key), title=f"{self.label}-{key}")

            def disconnect(self):
                return None

        class _CoordinatorStub:
            def __init__(self, **_kwargs):
                return None

            def register_chat(self, _chat_id):
                return None

            def submit_chat_start(self, **_kwargs):
                return None

            def submit_batch(self, **_kwargs):
                return None

            def submit_finalize(self, **_kwargs):
                return None

            def wait_for_chat(self, _chat_id):
                return None

            def close(self):
                return None

        def create_client(account_cfg, _worker_id):
            return _AccountClient(account_cfg.session_name)

        def fake_harvest(_conn, client, entity, _chat_id, **_kwargs):
            harvest_calls.append((client.label, int(entity.id)))
            if client.label == "main_sess":
                primary_attempts["count"] += 1
                if primary_attempts["count"] == 1:
                    raise AccountFloodWaitError(
                        seconds=70,
                        threshold_seconds=30,
                        account_label="primary",
                        scope="test",
                    )
            counters = SimpleNamespace(seen=1, written=1, parse_failures=0)
            return counters, set(), False

        with patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            side_effect=create_client,
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_get_chat_message_count",
            return_value=0,
        ), patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.ingest.runner._harvest_messages_for_entity",
            side_effect=fake_harvest,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_stop_requested",
            return_value=False,
        ), patch(
            "tg_harvest.admin_jobs.runners.time.sleep",
            return_value=None,
        ):
            ok = _admin_update_all_chats(
                "job-primary-takeover-budget",
                None,
                lambda: _FakeConn(rows),
                lambda _job_id, message: logs.append(str(message)),
                cfg,
            )

        self.assertTrue(ok)
        self.assertGreaterEqual(len(secondary_username_calls), 30)
        self.assertTrue(any(label == "secondary_sess" and chat_id > 45 for label, chat_id in harvest_calls))
        self.assertTrue(any("主账号 进入长等待冷却" in line for line in logs))
        self.assertTrue(any("已切换账号完成采集" in line for line in logs))

    def test_all_chat_update_secondary_username_resolution_has_extra_gap(self) -> None:
        rows = [
            {"chat_id": 1, "chat_title": "one", "chat_username": "one_name"},
            {"chat_id": 2, "chat_title": "two", "chat_username": "two_name"},
            {"chat_id": 3, "chat_title": "three", "chat_username": "three_name"},
            {"chat_id": 4, "chat_title": "four", "chat_username": "four_name"},
        ]
        cfg = SimpleNamespace(
            admin_update_concurrency=1,
            admin_update_min_chat_start_gap_seconds=0.0,
            admin_update_secondary_username_gap_seconds=4.5,
            admin_update_secondary_public_resolve_limit=2,
            session_name="main_sess",
            secondary_session_name="secondary_sess",
            api_id=1,
            api_hash="hash",
        )
        logs = []
        harvest_calls = []
        secondary_username_calls = []
        sleep_calls = []

        class _AccountClient:
            def __init__(self, label):
                self.label = label

            def get_entity(self, key):
                if self.label == "secondary_sess":
                    if isinstance(key, str):
                        secondary_username_calls.append(key)
                        return SimpleNamespace(
                            id=2 if key == "two_name" else 4,
                            title=f"{self.label}-{key}",
                        )
                    raise ValueError("could not find the input entity")
                return SimpleNamespace(id=int(key), title=f"{self.label}-{key}")

            def disconnect(self):
                return None

        class _CoordinatorStub:
            def __init__(self, **_kwargs):
                return None

            def register_chat(self, _chat_id):
                return None

            def submit_chat_start(self, **_kwargs):
                return None

            def submit_batch(self, **_kwargs):
                return None

            def submit_finalize(self, **_kwargs):
                return None

            def wait_for_chat(self, _chat_id):
                return None

            def close(self):
                return None

        def create_client(account_cfg, _worker_id):
            return _AccountClient(account_cfg.session_name)

        def fake_harvest(_conn, client, entity, _chat_id, **_kwargs):
            harvest_calls.append((client.label, int(entity.id)))
            counters = SimpleNamespace(seen=1, written=1, parse_failures=0)
            return counters, set(), False

        def fake_sleep(seconds):
            sleep_calls.append(float(seconds))

        with patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            side_effect=create_client,
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_get_chat_message_count",
            return_value=0,
        ), patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.ingest.runner._harvest_messages_for_entity",
            side_effect=fake_harvest,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_stop_requested",
            return_value=False,
        ), patch(
            "tg_harvest.admin_jobs.runners.time.sleep",
            side_effect=fake_sleep,
        ):
            ok = _admin_update_all_chats(
                "job-secondary-username-gap",
                None,
                lambda: _FakeConn(rows),
                lambda _job_id, message: logs.append(str(message)),
                cfg,
            )

        self.assertTrue(ok)
        self.assertEqual(["two_name", "four_name"], secondary_username_calls)
        self.assertEqual(
            [
                ("main_sess", 1),
                ("secondary_sess", 2),
                ("main_sess", 3),
                ("secondary_sess", 4),
            ],
            harvest_calls,
        )
        self.assertTrue(
            any(abs(seconds - 4.5) < 0.05 for seconds in sleep_calls),
            sleep_calls,
        )
        self.assertTrue(
            any("第二账号公开 username 解析独立节流已启用" in line for line in logs),
            logs,
        )

    def test_all_chat_update_reuses_account_after_short_cooldown(self) -> None:
        rows = [
            {"chat_id": 1, "chat_title": "one", "chat_username": None},
            {"chat_id": 2, "chat_title": "two", "chat_username": None},
            {"chat_id": 3, "chat_title": "three", "chat_username": None},
        ]
        cfg = SimpleNamespace(
            admin_update_concurrency=1,
            admin_update_max_cooldown_wait_seconds=5,
            session_name="main_sess",
            api_id=1,
            api_hash="hash",
        )
        logs = []
        harvest_calls = []
        now = [100.0]

        class _CoordinatorStub:
            def __init__(self, **_kwargs):
                return None

            def register_chat(self, _chat_id):
                return None

            def submit_chat_start(self, **_kwargs):
                return None

            def submit_batch(self, **_kwargs):
                return None

            def submit_finalize(self, **_kwargs):
                return None

            def wait_for_chat(self, _chat_id):
                return None

            def close(self):
                return None

        def fake_harvest(_conn, _client, entity, _chat_id, **_kwargs):
            chat_id = int(entity.id)
            harvest_calls.append(chat_id)
            if chat_id == 1:
                raise AccountFloodWaitError(
                    seconds=2,
                    threshold_seconds=1,
                    account_label="primary",
                    scope="test",
                )
            counters = SimpleNamespace(seen=1, written=1, parse_failures=0)
            return counters, set(), False

        def fake_sleep(seconds):
            now[0] += float(seconds)

        with patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            return_value=_FakeClient(),
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_get_chat_message_count",
            return_value=0,
        ), patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.ingest.runner._harvest_messages_for_entity",
            side_effect=fake_harvest,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_stop_requested",
            return_value=False,
        ), patch(
            "tg_harvest.admin_jobs.runners.time.time",
            side_effect=lambda: now[0],
        ), patch(
            "tg_harvest.admin_jobs.runners.time.sleep",
            side_effect=fake_sleep,
        ):
            ok = _admin_update_all_chats(
                "job-short-cooldown",
                None,
                lambda: _FakeConn(rows),
                lambda _job_id, message: logs.append(str(message)),
                cfg,
            )

        self.assertTrue(ok)
        self.assertEqual([1, 2, 3], harvest_calls)
        self.assertTrue(any("等待约 2s 后继续启动剩余群组" in line for line in logs))
        self.assertTrue(any("成功 2 个，失败 0 个，暂缓 1 个" in line for line in logs))

    def test_admin_update_start_gap_seconds_uses_safer_dual_account_default(
        self,
    ) -> None:
        self.assertEqual(
            0.25,
            _admin_update_start_gap_seconds(
                SimpleNamespace(admin_update_min_chat_start_gap_seconds=None),
                active_account_count=1,
            ),
        )
        self.assertEqual(
            1.25,
            _admin_update_start_gap_seconds(
                SimpleNamespace(admin_update_min_chat_start_gap_seconds=None),
                active_account_count=2,
            ),
        )
        self.assertEqual(
            0.5,
            _admin_update_start_gap_seconds(
                SimpleNamespace(admin_update_min_chat_start_gap_seconds=0.5),
                active_account_count=2,
            ),
        )

    def test_admin_update_secondary_username_gap_seconds_defaults_to_auto(self) -> None:
        self.assertIsNone(
            _admin_update_secondary_username_gap_seconds(
                SimpleNamespace(admin_update_secondary_username_gap_seconds=None)
            )
        )
        self.assertEqual(
            4.5,
            _admin_update_secondary_username_gap_seconds(
                SimpleNamespace(admin_update_secondary_username_gap_seconds=4.5)
            ),
        )

    def test_admin_update_effective_start_gap_seconds_progressively_slows_down(
        self,
    ) -> None:
        self.assertEqual(
            1.0,
            _admin_update_effective_start_gap_seconds(
                base_gap_seconds=1.0,
                started_chat_count=10,
                active_account_count=2,
                account_key="primary",
            ),
        )
        self.assertEqual(
            2.0,
            _admin_update_effective_start_gap_seconds(
                base_gap_seconds=1.0,
                started_chat_count=250,
                active_account_count=2,
                account_key="primary",
            ),
        )
        self.assertEqual(
            3.0,
            _admin_update_effective_start_gap_seconds(
                base_gap_seconds=1.0,
                started_chat_count=5,
                active_account_count=2,
                account_key="secondary",
                warmup_username_resolve=True,
            ),
        )
        self.assertEqual(
            4.0,
            _admin_update_effective_start_gap_seconds(
                base_gap_seconds=1.25,
                started_chat_count=370,
                active_account_count=2,
                account_key="primary",
            ),
        )
        self.assertEqual(
            2.5,
            _admin_update_effective_start_gap_seconds(
                base_gap_seconds=0.25,
                started_chat_count=650,
                active_account_count=1,
                account_key="primary",
            ),
        )

    def test_all_chat_update_stops_submitting_when_all_accounts_long_cooldown(
        self,
    ) -> None:
        rows = [
            {"chat_id": 1, "chat_title": "one", "chat_username": None},
            {"chat_id": 2, "chat_title": "two", "chat_username": None},
            {"chat_id": 3, "chat_title": "three", "chat_username": None},
        ]
        cfg = SimpleNamespace(
            admin_update_concurrency=1,
            admin_update_max_cooldown_wait_seconds=5,
            session_name="main_sess",
            api_id=1,
            api_hash="hash",
        )
        logs = []
        harvest_calls = []
        now = [100.0]

        class _CoordinatorStub:
            def __init__(self, **_kwargs):
                return None

            def register_chat(self, _chat_id):
                return None

            def submit_chat_start(self, **_kwargs):
                return None

            def submit_batch(self, **_kwargs):
                return None

            def submit_finalize(self, **_kwargs):
                return None

            def wait_for_chat(self, _chat_id):
                return None

            def close(self):
                return None

        def fake_harvest(_conn, _client, entity, _chat_id, **_kwargs):
            harvest_calls.append(int(entity.id))
            raise AccountFloodWaitError(
                seconds=120,
                threshold_seconds=1,
                account_label="primary",
                scope="test",
            )

        with patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            return_value=_FakeClient(),
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_get_chat_message_count",
            return_value=0,
        ), patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.ingest.runner._harvest_messages_for_entity",
            side_effect=fake_harvest,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_stop_requested",
            return_value=False,
        ), patch(
            "tg_harvest.admin_jobs.runners.time.time",
            side_effect=lambda: now[0],
        ), patch(
            "tg_harvest.admin_jobs.runners.time.sleep",
            return_value=None,
        ):
            ok = _admin_update_all_chats(
                "job-long-cooldown",
                None,
                lambda: _FakeConn(rows),
                lambda _job_id, message: logs.append(str(message)),
                cfg,
            )

        self.assertTrue(ok)
        self.assertEqual([1], harvest_calls)
        self.assertTrue(any("停止启动剩余群组" in line for line in logs))
        self.assertTrue(any("暂缓 1 个" in line for line in logs))
        self.assertTrue(any("未启动 2 个" in line for line in logs))
        self.assertFalse(
            any("没有可用账号执行当前群组" in line for line in logs)
        )

    def test_all_chat_update_stop_request_does_not_submit_remaining_chats(self) -> None:
        rows = [
            {"chat_id": 1, "chat_title": "one", "chat_username": None},
            {"chat_id": 2, "chat_title": "two", "chat_username": None},
            {"chat_id": 3, "chat_title": "three", "chat_username": None},
            {"chat_id": 4, "chat_title": "four", "chat_username": None},
        ]
        cfg = SimpleNamespace(admin_update_concurrency=2, session_name="sess")
        logs = []
        submitted_chat_ids = []
        stop_checks = []

        def append_log(_job_id, message):
            logs.append(str(message))

        class _CoordinatorStub:
            def __init__(self, **_kwargs):
                return None

            def register_chat(self, _chat_id):
                return None

            def submit_chat_start(self, **_kwargs):
                return None

            def submit_batch(self, **_kwargs):
                return None

            def submit_finalize(self, **_kwargs):
                return None

            def wait_for_chat(self, _chat_id):
                return None

            def close(self):
                return None

        def fake_harvest(_conn, _client, entity, _chat_id, **_kwargs):
            submitted_chat_ids.append(int(entity.id))
            counters = SimpleNamespace(seen=1, written=1, parse_failures=0)
            return counters, set(), False

        def stop_requested(_job_id):
            stop_checks.append(True)
            return len(stop_checks) > 2

        with patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            return_value=_FakeClient(),
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_get_chat_message_count",
            return_value=0,
        ), patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.ingest.runner._harvest_messages_for_entity",
            side_effect=fake_harvest,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_stop_requested",
            side_effect=stop_requested,
        ):
            ok = _admin_update_all_chats(
                "job-1",
                None,
                lambda: _FakeConn(rows),
                append_log,
                cfg,
            )

        self.assertTrue(ok)
        self.assertEqual([1, 2], sorted(submitted_chat_ids))
        self.assertTrue(any("已收到停止请求" in line for line in logs))
        self.assertTrue(any("未启动 2 个" in line for line in logs))

    def test_all_chat_update_clears_stale_username_when_chat_becomes_private(self) -> None:
        rows = [
            {"chat_id": 1, "chat_title": "was-public", "chat_username": "old_public_name"},
        ]
        cfg = SimpleNamespace(admin_update_concurrency=1, session_name="sess")

        def append_log(_job_id, _message):
            return None

        counters = SimpleNamespace(seen=1, written=1, parse_failures=0)

        class _CoordinatorStub:
            instances = []

            def __init__(self, **_kwargs):
                self.chat_starts = []
                self.finalized = []
                self.__class__.instances.append(self)

            def register_chat(self, _chat_id):
                return None

            def submit_chat_start(self, **kwargs):
                self.chat_starts.append(kwargs)

            def submit_batch(self, **_kwargs):
                return None

            def submit_finalize(self, **kwargs):
                self.finalized.append(kwargs)

            def wait_for_chat(self, _chat_id):
                return None

            def close(self):
                return None

        with patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            return_value=_FakeClient(),
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_get_chat_message_count",
            return_value=0,
        ), patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.ingest.runner._harvest_messages_for_entity",
            return_value=(counters, set(), False),
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_stop_requested",
            return_value=False,
        ):
            ok = _admin_update_all_chats(
                "job-1",
                None,
                lambda: _FakeConn(rows),
                append_log,
                cfg,
            )

        self.assertTrue(ok)
        self.assertEqual(1, len(_CoordinatorStub.instances))
        self.assertEqual(
            None,
            _CoordinatorStub.instances[0].chat_starts[0]["chat_username"],
        )

    def test_single_chat_update_streams_batches_to_write_coordinator(self) -> None:
        append_log = []

        def _append(_job_id, message):
            append_log.append(str(message))

        counters = SimpleNamespace(
            seen=1,
            written=1,
            parse_failures=0,
            parse_failure_samples=[],
            parse_failures_by_type={},
        )
        progress_kwargs = []

        class _CoordinatorStub:
            instances = []

            def __init__(self, **_kwargs):
                self.registered = []
                self.chat_starts = []
                self.batches = []
                self.finalized = []
                self.waited = []
                self.closed = False
                self.__class__.instances.append(self)

            def register_chat(self, chat_id):
                self.registered.append(chat_id)

            def submit_chat_start(self, **kwargs):
                self.chat_starts.append(kwargs)

            def submit_batch(self, **kwargs):
                self.batches.append(kwargs)

            def submit_finalize(self, **kwargs):
                self.finalized.append(kwargs)

            def wait_for_chat(self, chat_id):
                self.waited.append(chat_id)

            def close(self):
                self.closed = True

        def fake_harvest(_conn, _client, _entity, chat_id, **kwargs):
            progress_kwargs.append(
                {
                    "progress_total": kwargs.get("progress_total"),
                    "progress_prefix": kwargs.get("progress_prefix"),
                }
            )
            write_batch_fn = kwargs.get("write_batch_fn")
            self.assertIsNotNone(write_batch_fn)
            write_batch_fn(
                [
                    (
                        chat_id,
                        101,
                        "",
                        0,
                        0,
                        "",
                        "",
                        "",
                        "",
                        "TEXT",
                        9001,
                    )
                ],
                [],
            )
            return counters, {9001}, False

        with patch(
            "tg_harvest.admin_jobs.runners.resolve_chat_entity",
            return_value=SimpleNamespace(id=999, title="chat-1", username=None),
        ), patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.admin_jobs.runners._probe_update_progress_total",
            return_value=999,
        ), patch(
            "tg_harvest.ingest.runner._harvest_messages_for_entity",
            side_effect=fake_harvest,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
        ) as progress_mock:
            _admin_process_single_chat_update(
                job_id="job-1",
                client=object(),
                cfg=SimpleNamespace(flood_wait_switch_threshold=30),
                get_conn_fn=lambda: _FakeConn([]),
                admin_job_append_log_fn=_append,
                chat_id=1,
                chat_title="chat-1",
                chat_username="chat_name",
                idx=1,
                total=1,
                account_label="主账号",
            )

        self.assertEqual(1, len(_CoordinatorStub.instances))
        coordinator = _CoordinatorStub.instances[0]
        self.assertEqual([1], coordinator.registered)
        self.assertEqual(1, len(coordinator.batches))
        self.assertEqual([1], coordinator.waited)
        self.assertTrue(coordinator.closed)
        self.assertEqual(None, coordinator.chat_starts[0]["chat_username"])
        self.assertTrue(coordinator.finalized[0]["skip_postprocess_if_unchanged"])
        self.assertFalse(coordinator.finalized[0]["enable_dedupe"])
        self.assertTrue(any("准备更新群组" in line for line in append_log))
        self.assertTrue(any("群组连接成功" in line for line in append_log))
        self.assertTrue(any("边抓取边写入" in line for line in append_log))
        self.assertTrue(any("增量更新完成" in line for line in append_log))
        self.assertEqual(2, progress_mock.call_count)
        self.assertEqual(
            [{"progress_total": 999, "progress_prefix": "[1/1] chat-1 (ID=1) 正在采集"}],
            progress_kwargs,
        )

    def test_process_single_chat_update_can_skip_progress_probe(self) -> None:
        append_log = []
        progress_kwargs = []
        counters = SimpleNamespace(seen=1, written=1, parse_failures=0)

        def _append(_job_id, message):
            append_log.append(str(message))

        class _CoordinatorStub:
            instances = []

            def __init__(self, **_kwargs):
                self.registered = []
                self.chat_starts = []
                self.batches = []
                self.finalized = []
                self.waited = []
                self.closed = False
                self.__class__.instances.append(self)

            def register_chat(self, chat_id):
                self.registered.append(chat_id)

            def submit_chat_start(self, **kwargs):
                self.chat_starts.append(kwargs)

            def submit_batch(self, **kwargs):
                self.batches.append(kwargs)

            def submit_finalize(self, **kwargs):
                self.finalized.append(kwargs)

            def wait_for_chat(self, chat_id):
                self.waited.append(chat_id)

            def close(self):
                self.closed = True

        def fake_harvest(_conn, _client, _entity, chat_id, **kwargs):
            progress_kwargs.append(
                {
                    "progress_total": kwargs.get("progress_total"),
                    "progress_prefix": kwargs.get("progress_prefix"),
                }
            )
            write_batch_fn = kwargs.get("write_batch_fn")
            self.assertIsNotNone(write_batch_fn)
            write_batch_fn(
                [
                    (
                        chat_id,
                        101,
                        "",
                        0,
                        0,
                        "",
                        "",
                        "",
                        "",
                        "TEXT",
                        9001,
                    )
                ],
                [],
            )
            return counters, {9001}, False

        with patch(
            "tg_harvest.admin_jobs.runners.resolve_chat_entity",
            return_value=SimpleNamespace(id=999, title="chat-1", username=None),
        ), patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.admin_jobs.runners._probe_update_progress_total",
        ) as probe_mock, patch(
            "tg_harvest.ingest.runner._harvest_messages_for_entity",
            side_effect=fake_harvest,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
        ):
            _admin_process_single_chat_update(
                job_id="job-1",
                client=object(),
                cfg=SimpleNamespace(flood_wait_switch_threshold=30),
                get_conn_fn=lambda: _FakeConn([]),
                admin_job_append_log_fn=_append,
                chat_id=1,
                chat_title="chat-1",
                chat_username="chat_name",
                idx=1,
                total=1,
                account_label="主账号",
                enable_progress_probe=False,
            )

        probe_mock.assert_not_called()
        self.assertEqual(
            [{"progress_total": None, "progress_prefix": "正在采集"}],
            progress_kwargs,
        )
        self.assertTrue(any("增量更新完成" in line for line in append_log))

    def test_streaming_helper_finalizes_partial_batches_after_harvest_failure(self) -> None:
        class _CoordinatorStub:
            def __init__(self):
                self.finalized = []
                self.batches = []
                self.waited = []
                self.registered = []

            def register_chat(self, chat_id):
                self.registered.append(chat_id)

            def submit_chat_start(self, **_kwargs):
                return None

            def submit_batch(self, **kwargs):
                self.batches.append(kwargs)

            def submit_finalize(self, **kwargs):
                self.finalized.append(kwargs)

            def wait_for_chat(self, chat_id):
                self.waited.append(chat_id)

        def fake_harvest(_conn, _client, _entity, chat_id, **kwargs):
            kwargs["write_batch_fn"](
                [
                    (
                        chat_id,
                        201,
                        "",
                        0,
                        0,
                        "",
                        "",
                        "",
                        "",
                        "TEXT",
                        7001,
                    )
                ],
                [],
            )
            raise RuntimeError("network break")

        coordinator = _CoordinatorStub()
        with patch(
            "tg_harvest.ingest.runner._harvest_messages_for_entity",
            side_effect=fake_harvest,
        ), self.assertRaisesRegex(RuntimeError, "network break"):
            stream_entity_harvest_to_writer(
                write_coordinator=coordinator,
                get_conn_fn=lambda: _FakeConn([]),
                client=object(),
                entity=SimpleNamespace(id=7, title="partial", username=None),
                idx=1,
                total=1,
                skip_postprocess_if_unchanged=True,
                enable_dedupe=False,
            )

        self.assertEqual([7], coordinator.registered)
        self.assertEqual(1, len(coordinator.batches))
        self.assertEqual(1, len(coordinator.finalized))
        self.assertEqual(1, coordinator.finalized[0]["counters"].written)
        self.assertEqual({7001}, coordinator.finalized[0]["touched_groups"])
        self.assertFalse(coordinator.finalized[0]["enable_dedupe"])
        self.assertEqual([7], coordinator.waited)

    def test_streaming_helper_binds_client_event_loop_for_single_chat_harvest(
        self,
    ) -> None:
        class _CoordinatorStub:
            def __init__(self):
                self.finalized = []

            def register_chat(self, _chat_id):
                return None

            def submit_chat_start(self, **_kwargs):
                return None

            def submit_batch(self, **_kwargs):
                return None

            def submit_finalize(self, **kwargs):
                self.finalized.append(kwargs)

            def wait_for_chat(self, _chat_id):
                return None

        previous_loop = asyncio.new_event_loop()
        client_loop = asyncio.new_event_loop()
        client = SimpleNamespace(_tg_harvest_loop=client_loop)
        observed = []

        def fake_harvest(_conn, harvest_client, _entity, _chat_id, **_kwargs):
            observed.append(asyncio.get_event_loop() is harvest_client._tg_harvest_loop)
            return HarvestCounters(seen=1, written=0), set(), False

        try:
            asyncio.set_event_loop(previous_loop)
            coordinator = _CoordinatorStub()
            with patch(
                "tg_harvest.ingest.runner._harvest_messages_for_entity",
                side_effect=fake_harvest,
            ):
                stream_entity_harvest_to_writer(
                    write_coordinator=coordinator,
                    get_conn_fn=lambda: _FakeConn([]),
                    client=client,
                    entity=SimpleNamespace(id=7, title="looped", username=None),
                    idx=1,
                    total=1,
                    skip_postprocess_if_unchanged=True,
                    enable_dedupe=False,
                )

            self.assertEqual([True], observed)
            self.assertIs(asyncio.get_event_loop(), previous_loop)
            self.assertEqual(1, len(coordinator.finalized))
        finally:
            asyncio.set_event_loop(None)
            client_loop.close()
            previous_loop.close()

    def test_resolve_chat_entity_binds_client_event_loop(self) -> None:
        previous_loop = asyncio.new_event_loop()
        client_loop = asyncio.new_event_loop()
        observed = []

        class _Client:
            _tg_harvest_loop = client_loop

            def get_entity(self, chat_id):
                observed.append(asyncio.get_event_loop() is self._tg_harvest_loop)
                return SimpleNamespace(id=chat_id, title="resolved")

        try:
            asyncio.set_event_loop(previous_loop)
            entity = resolve_chat_entity(_Client(), 123)

            self.assertEqual(123, entity.id)
            self.assertEqual([True], observed)
            self.assertIs(asyncio.get_event_loop(), previous_loop)
        finally:
            asyncio.set_event_loop(None)
            client_loop.close()
            previous_loop.close()

    def test_target_resolution_binds_client_event_loop(self) -> None:
        previous_loop = asyncio.new_event_loop()
        client_loop = asyncio.new_event_loop()
        client = SimpleNamespace(_tg_harvest_loop=client_loop)
        expected_entity = SimpleNamespace(id=321)
        observed = []

        def fake_resolve_entities(resolve_client, _target):
            observed.append(asyncio.get_event_loop() is resolve_client._tg_harvest_loop)
            return [expected_entity]

        try:
            asyncio.set_event_loop(previous_loop)
            with patch(
                "tg_harvest.ingest.parse.resolve_target_entities",
                side_effect=fake_resolve_entities,
            ):
                entities = _resolve_target_entities_for_account(
                    client,
                    "target",
                    cfg=SimpleNamespace(flood_wait_switch_threshold=30),
                    account_label="primary",
                    scope="test-resolve",
                )

            self.assertEqual([expected_entity], entities)
            self.assertEqual([True], observed)
            self.assertIs(asyncio.get_event_loop(), previous_loop)
        finally:
            asyncio.set_event_loop(None)
            client_loop.close()
            previous_loop.close()

    def test_new_chat_dual_account_probe_binds_client_event_loops(self) -> None:
        previous_loop = asyncio.new_event_loop()
        primary_loop = asyncio.new_event_loop()
        secondary_loop = asyncio.new_event_loop()
        primary_client = SimpleNamespace(_tg_harvest_loop=primary_loop)
        secondary_client = SimpleNamespace(_tg_harvest_loop=secondary_loop)
        entity = SimpleNamespace(id=44, title="new", username="new_name")
        secondary_entity = SimpleNamespace(id=44, title="new", username="new_name")
        observed = []

        class _CoordinatorStub:
            pass

        def fake_probe(client, _entity, **kwargs):
            observed.append(
                (
                    kwargs["account_label"],
                    asyncio.get_event_loop() is client._tg_harvest_loop,
                )
            )
            return SimpleNamespace(
                can_read_history=True,
                latest_message_id=6000,
                reason="",
            )

        try:
            asyncio.set_event_loop(previous_loop)
            with patch(
                "tg_harvest.admin_jobs.runners._get_last_message_id",
                return_value=0,
            ), patch(
                "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
                return_value=True,
            ), patch(
                "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
                return_value=secondary_client,
            ), patch(
                "tg_harvest.admin_jobs.runners._disconnect_worker_client",
                return_value=None,
            ), patch(
                "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
                return_value=None,
            ), patch(
                "tg_harvest.admin_jobs.runners._resolve_matching_entity_for_account",
                return_value=secondary_entity,
            ), patch(
                "tg_harvest.admin_jobs.runners.probe_history_access",
                side_effect=fake_probe,
            ), patch(
                "tg_harvest.admin_jobs.runners.stream_entity_ranges_to_writer",
                return_value=SimpleNamespace(submitted_message_count=0),
            ) as stream_ranges_mock:
                used_ranges = _try_stream_new_chat_multi_account_ranges(
                    job_id="job-loop-probe",
                    target="new_name",
                    cfg=SimpleNamespace(
                        session_name="main_sess",
                        secondary_session_name="secondary_sess",
                        api_id=1,
                        api_hash="hash",
                        flood_wait_switch_threshold=30,
                        multi_account_min_message_id=5000,
                        multi_account_range_chunk_size=1000,
                    ),
                    get_conn_fn=lambda: _FakeConn([]),
                    admin_job_append_log_fn=lambda *_args: None,
                    write_coordinator=_CoordinatorStub(),
                    primary_client=primary_client,
                    entity=entity,
                    entity_title="new",
                    idx=1,
                    total=1,
                )

            self.assertTrue(used_ranges)
            self.assertEqual(
                [("primary", True), ("secondary", True)],
                observed,
            )
            self.assertTrue(stream_ranges_mock.called)
            self.assertIs(asyncio.get_event_loop(), previous_loop)
        finally:
            asyncio.set_event_loop(None)
            secondary_loop.close()
            primary_loop.close()
            previous_loop.close()

    def test_range_streaming_requeues_chunk_when_account_flood_waits(self) -> None:
        class _SyncExecutor:
            def __init__(self, max_workers=None):
                self.max_workers = max_workers

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def submit(self, fn, *args, **kwargs):
                future = Future()
                try:
                    future.set_result(fn(*args, **kwargs))
                except Exception as exc:
                    future.set_exception(exc)
                return future

        class _CoordinatorStub:
            def __init__(self):
                self.finalized = []
                self.registered = []

            def register_chat(self, chat_id):
                self.registered.append(chat_id)

            def submit_chat_start(self, **_kwargs):
                return None

            def submit_batch(self, **_kwargs):
                return None

            def submit_finalize(self, **kwargs):
                self.finalized.append(kwargs)

            def wait_for_chat(self, _chat_id):
                return None

        calls = []

        def fake_harvest_range(**kwargs):
            label = kwargs["account_label"]
            message_range = kwargs["message_range"]
            calls.append((label, message_range.start_id, message_range.end_id))
            if label == "primary":
                raise AccountFloodWaitError(
                    seconds=70,
                    threshold_seconds=30,
                    account_label=label,
                    scope="test-range",
                )
            return HarvestCounters(seen=1, written=1), set()

        coordinator = _CoordinatorStub()
        with patch(
            "tg_harvest.admin_jobs.range_streaming.ThreadPoolExecutor",
            _SyncExecutor,
        ), patch(
            "tg_harvest.admin_jobs.range_streaming.harvest_message_id_range",
            side_effect=fake_harvest_range,
        ):
            result = stream_entity_ranges_to_writer(
                job_id="job-range-flood",
                write_coordinator=coordinator,
                accounts=[
                    RangeHarvestAccount("primary", object(), object()),
                    RangeHarvestAccount("secondary", object(), object()),
                ],
                idx=1,
                total=1,
                chat_id=99,
                chat_title="range",
                chat_username="range_name",
                chat_type="Channel",
                latest_message_id=20,
                chunk_size=10,
                enable_dedupe=False,
            )

        self.assertEqual("primary", calls[0][0])
        self.assertTrue(all(call[0] == "secondary" for call in calls[1:]))
        self.assertEqual(2, result.counters.written)
        self.assertEqual([99], coordinator.registered)
        self.assertEqual(1, len(coordinator.finalized))
        self.assertEqual(2, coordinator.finalized[0]["counters"].written)

    def test_range_streaming_binds_client_event_loop_in_worker_threads(self) -> None:
        class _CoordinatorStub:
            def __init__(self):
                self.finalized = []

            def register_chat(self, _chat_id):
                return None

            def submit_chat_start(self, **_kwargs):
                return None

            def submit_batch(self, **_kwargs):
                return None

            def submit_finalize(self, **kwargs):
                self.finalized.append(kwargs)

            def wait_for_chat(self, _chat_id):
                return None

        primary_loop = asyncio.new_event_loop()
        secondary_loop = asyncio.new_event_loop()
        primary_client = SimpleNamespace(_tg_harvest_loop=primary_loop)
        secondary_client = SimpleNamespace(_tg_harvest_loop=secondary_loop)
        barrier = threading.Barrier(2)
        observed = []

        def fake_harvest_range(**kwargs):
            barrier.wait(timeout=5)
            observed.append(
                (
                    kwargs["account_label"],
                    asyncio.get_event_loop() is kwargs["client"]._tg_harvest_loop,
                )
            )
            return HarvestCounters(seen=1, written=1), set()

        try:
            coordinator = _CoordinatorStub()
            with patch(
                "tg_harvest.admin_jobs.range_streaming.harvest_message_id_range",
                side_effect=fake_harvest_range,
            ):
                result = stream_entity_ranges_to_writer(
                    job_id="job-range-loop",
                    write_coordinator=coordinator,
                    accounts=[
                        RangeHarvestAccount("primary", primary_client, object()),
                        RangeHarvestAccount("secondary", secondary_client, object()),
                    ],
                    idx=1,
                    total=1,
                    chat_id=99,
                    chat_title="range",
                    chat_username="range_name",
                    chat_type="Channel",
                    latest_message_id=2,
                    chunk_size=1,
                    enable_dedupe=False,
                )
        finally:
            primary_loop.close()
            secondary_loop.close()

        self.assertEqual(2, result.counters.written)
        self.assertEqual(
            [("primary", True), ("secondary", True)],
            sorted(observed),
        )
        self.assertEqual(1, len(coordinator.finalized))

    def test_harvest_job_runner_streams_new_targets_through_shared_writer(self) -> None:
        statuses = []
        logs = []
        progress_calls = []
        client = object()
        entities = [
            SimpleNamespace(id=11, title="new-1", username="u1"),
            SimpleNamespace(id=12, title="new-2", username=None),
        ]

        class _HeartbeatStop:
            def set(self):
                return None

        class _HeartbeatThread:
            def join(self, timeout=None):
                return None

        class _CoordinatorStub:
            instances = []

            def __init__(self, **kwargs):
                self.kwargs = kwargs
                self.closed = False
                self.__class__.instances.append(self)

            def close(self):
                self.closed = True

        stream_calls = []

        def fake_stream(**kwargs):
            stream_calls.append(kwargs)
            entity = kwargs["entity"]
            return SimpleNamespace(
                chat_id=entity.id,
                chat_title=entity.title,
                chat_username=entity.username,
                counters=SimpleNamespace(written=0),
                touched_groups=set(),
                first_sync=True,
                submitted_message_count=0,
            )

        with patch(
            "tg_harvest.admin_jobs.runners._start_job_heartbeat",
            return_value=(_HeartbeatStop(), _HeartbeatThread()),
        ), patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            return_value=client,
        ), patch(
            "tg_harvest.admin_jobs.runners._disconnect_worker_client",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.ingest.parse.resolve_target_entities",
            return_value=entities,
        ), patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.admin_jobs.runners.stream_entity_harvest_to_writer",
            side_effect=fake_stream,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            side_effect=lambda *args, **kwargs: progress_calls.append((args, kwargs)) or True,
        ):
            _admin_harvest_job_runner(
                "job-new",
                "target",
                cfg=SimpleNamespace(session_name="sess"),
                get_conn_fn=lambda: _FakeConn([]),
                admin_make_job_log_handler_fn=lambda _job_id: logging.NullHandler(),
                admin_job_set_status_fn=lambda _job_id, status: statuses.append(status) or True,
                admin_job_append_log_fn=lambda _job_id, message: logs.append(str(message)),
            )

        self.assertEqual(["running", "done"], statuses)
        self.assertEqual(1, len(_CoordinatorStub.instances))
        self.assertTrue(_CoordinatorStub.instances[0].closed)
        self.assertEqual(2, len(stream_calls))
        self.assertTrue(
            all(
                call["write_coordinator"] is _CoordinatorStub.instances[0]
                for call in stream_calls
            )
        )
        self.assertTrue(all(call["enable_dedupe"] for call in stream_calls))
        self.assertTrue(
            all(not call["skip_postprocess_if_unchanged"] for call in stream_calls)
        )
        self.assertTrue(any("边抓取边写入" in line for line in logs))
        self.assertTrue(any(kwargs.get("stage") == "done" for _, kwargs in progress_calls))

    def test_harvest_job_runner_switches_to_secondary_when_primary_stream_fails(
        self,
    ) -> None:
        statuses = []
        logs = []
        progress_calls = []
        primary_entity = SimpleNamespace(id=11, title="new-1", username="u1")
        secondary_entity = SimpleNamespace(id=11, title="new-1", username="u1")

        class _AccountClient:
            def __init__(self, label):
                self.label = label

            def get_entity(self, key):
                if str(key).strip().lstrip("@") == "u1":
                    return secondary_entity
                raise ValueError("could not find the input entity")

        primary_client = _AccountClient("main_sess")
        secondary_client = _AccountClient("secondary_sess")

        class _HeartbeatStop:
            def set(self):
                return None

        class _HeartbeatThread:
            def join(self, timeout=None):
                return None

        class _CoordinatorStub:
            instances = []

            def __init__(self, **kwargs):
                self.kwargs = kwargs
                self.closed = False
                self.__class__.instances.append(self)

            def close(self):
                self.closed = True

        stream_calls = []

        def fake_stream(**kwargs):
            client = kwargs["client"]
            entity = kwargs["entity"]
            stream_calls.append((client.label, entity.id))
            if client.label == "main_sess":
                raise RuntimeError("primary stream failed")
            return SimpleNamespace(
                chat_id=entity.id,
                chat_title=entity.title,
                chat_username=entity.username,
                counters=SimpleNamespace(written=0),
                touched_groups=set(),
                first_sync=True,
                submitted_message_count=0,
            )

        with patch(
            "tg_harvest.admin_jobs.runners._start_job_heartbeat",
            return_value=(_HeartbeatStop(), _HeartbeatThread()),
        ), patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            side_effect=[primary_client, secondary_client],
        ), patch(
            "tg_harvest.admin_jobs.runners._disconnect_worker_client",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.ingest.parse.resolve_target_entities",
            side_effect=[[primary_entity], [secondary_entity]],
        ), patch(
            "tg_harvest.admin_jobs.runners._try_stream_new_chat_multi_account_ranges",
            return_value=False,
        ), patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.admin_jobs.runners.stream_entity_harvest_to_writer",
            side_effect=fake_stream,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            side_effect=lambda *args, **kwargs: progress_calls.append((args, kwargs)) or True,
        ):
            _admin_harvest_job_runner(
                "job-new-fallback",
                "u1",
                cfg=SimpleNamespace(
                    session_name="main_sess",
                    secondary_session_name="secondary_sess",
                    api_id=1,
                    api_hash="hash",
                ),
                get_conn_fn=lambda: _FakeConn([]),
                admin_make_job_log_handler_fn=lambda _job_id: logging.NullHandler(),
                admin_job_set_status_fn=lambda _job_id, status: statuses.append(status) or True,
                admin_job_append_log_fn=lambda _job_id, message: logs.append(str(message)),
            )

        self.assertEqual(["running", "done"], statuses)
        self.assertEqual(
            [("main_sess", 11), ("secondary_sess", 11)],
            stream_calls,
        )
        self.assertTrue(_CoordinatorStub.instances[0].closed)
        self.assertTrue(any("主账号新增采集失败" in line for line in logs))
        self.assertTrue(any("已切换账号继续新增采集" in line for line in logs))
        self.assertTrue(any(kwargs.get("stage") == "done" for _, kwargs in progress_calls))

    def test_harvest_job_runner_uses_dual_account_ranges_for_large_new_target(self) -> None:
        statuses = []
        logs = []
        progress_calls = []
        primary_client = object()
        secondary_entity = SimpleNamespace(id=11, title="new-1", username="u1")

        class _SecondaryClient:
            def get_entity(self, key):
                if str(key).strip().lstrip("@") == "u1":
                    return secondary_entity
                raise ValueError("could not find the input entity")

        secondary_client = _SecondaryClient()
        entity = SimpleNamespace(id=11, title="new-1", username="u1")
        entities = [entity]

        class _HeartbeatStop:
            def set(self):
                return None

        class _HeartbeatThread:
            def join(self, timeout=None):
                return None

        class _CoordinatorStub:
            instances = []

            def __init__(self, **kwargs):
                self.kwargs = kwargs
                self.closed = False
                self.__class__.instances.append(self)

            def close(self):
                self.closed = True

        with patch(
            "tg_harvest.admin_jobs.runners._start_job_heartbeat",
            return_value=(_HeartbeatStop(), _HeartbeatThread()),
        ), patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            side_effect=[True, True],
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            side_effect=[primary_client, secondary_client],
        ), patch(
            "tg_harvest.admin_jobs.runners._disconnect_worker_client",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.ingest.parse.resolve_target_entities",
            side_effect=[entities, []],
        ), patch(
            "tg_harvest.admin_jobs.runners._get_last_message_id",
            return_value=0,
        ), patch(
            "tg_harvest.admin_jobs.runners.probe_history_access",
            side_effect=[
                SimpleNamespace(
                    can_read_history=True,
                    latest_message_id=6000,
                    reason="",
                ),
                SimpleNamespace(
                    can_read_history=True,
                    latest_message_id=6000,
                    reason="",
                ),
            ],
        ) as probe_mock, patch(
            "tg_harvest.admin_jobs.runners.stream_entity_ranges_to_writer",
            return_value=SimpleNamespace(
                chat_id=11,
                chat_title="new-1",
                chat_username="u1",
                counters=SimpleNamespace(written=0),
                touched_groups=set(),
                first_sync=True,
                submitted_message_count=0,
            ),
        ) as stream_ranges_mock, patch(
            "tg_harvest.admin_jobs.runners.stream_entity_harvest_to_writer"
        ) as stream_single_mock, patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            side_effect=lambda *args, **kwargs: progress_calls.append((args, kwargs)) or True,
        ):
            _admin_harvest_job_runner(
                "job-range",
                "target",
                cfg=SimpleNamespace(
                    session_name="main_sess",
                    secondary_session_name="secondary_sess",
                    api_id=1,
                    api_hash="hash",
                    multi_account_min_message_id=6000,
                    multi_account_range_chunk_size=777,
                ),
                get_conn_fn=lambda: _FakeConn([]),
                admin_make_job_log_handler_fn=lambda _job_id: logging.NullHandler(),
                admin_job_set_status_fn=lambda _job_id, status: statuses.append(status) or True,
                admin_job_append_log_fn=lambda _job_id, message: logs.append(str(message)),
            )

        self.assertEqual(["running", "done"], statuses)
        self.assertEqual(1, len(_CoordinatorStub.instances))
        self.assertTrue(_CoordinatorStub.instances[0].closed)
        self.assertEqual(2, probe_mock.call_count)
        self.assertTrue(
            all(
                call.kwargs["min_history_message_id"] == 6000
                for call in probe_mock.call_args_list
            )
        )
        self.assertEqual(1, stream_ranges_mock.call_count)
        self.assertFalse(stream_single_mock.called)
        self.assertEqual(6000, stream_ranges_mock.call_args.kwargs["latest_message_id"])
        self.assertEqual(777, stream_ranges_mock.call_args.kwargs["chunk_size"])
        self.assertTrue(any("双账号区间拉取" in line for line in logs))
        self.assertTrue(any(kwargs.get("stage") == "done" for _, kwargs in progress_calls))

    def test_harvest_job_runner_falls_back_when_secondary_resolve_flood_waits(self) -> None:
        statuses = []
        logs = []
        progress_calls = []
        primary_client = object()
        secondary_client = object()
        entity = SimpleNamespace(id=11, title="new-1", username="u1")

        class _FloodWaitError(Exception):
            seconds = 68816

            def __str__(self):
                return (
                    "A wait of 68816 seconds is required "
                    "(caused by InvokeWithoutUpdatesRequest(ResolveUsernameRequest))"
                )

        class _HeartbeatStop:
            def set(self):
                return None

        class _HeartbeatThread:
            def join(self, timeout=None):
                return None

        class _CoordinatorStub:
            instances = []

            def __init__(self, **kwargs):
                self.kwargs = kwargs
                self.closed = False
                self.__class__.instances.append(self)

            def close(self):
                self.closed = True

        with patch(
            "tg_harvest.admin_jobs.runners._start_job_heartbeat",
            return_value=(_HeartbeatStop(), _HeartbeatThread()),
        ), patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            side_effect=[True, True],
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            side_effect=[primary_client, secondary_client],
        ), patch(
            "tg_harvest.admin_jobs.runners._disconnect_worker_client",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.ingest.parse.resolve_target_entities",
            side_effect=[[entity], _FloodWaitError()],
        ), patch(
            "tg_harvest.admin_jobs.runners._get_last_message_id",
            return_value=0,
        ), patch(
            "tg_harvest.admin_jobs.runners.probe_history_access",
            return_value=SimpleNamespace(
                can_read_history=True,
                latest_message_id=6000,
                reason="",
            ),
        ) as probe_mock, patch(
            "tg_harvest.admin_jobs.runners.stream_entity_ranges_to_writer",
            return_value=None,
        ) as stream_ranges_mock, patch(
            "tg_harvest.admin_jobs.runners.stream_entity_harvest_to_writer",
            return_value=None,
        ) as stream_single_mock, patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            side_effect=lambda *args, **kwargs: progress_calls.append((args, kwargs)) or True,
        ):
            _admin_harvest_job_runner(
                "job-secondary-resolve-flood",
                "https://t.me/fancha07",
                cfg=SimpleNamespace(
                    session_name="main_sess",
                    secondary_session_name="secondary_sess",
                    api_id=1,
                    api_hash="hash",
                    flood_wait_switch_threshold=30,
                    multi_account_min_message_id=5000,
                    multi_account_range_chunk_size=777,
                ),
                get_conn_fn=lambda: _FakeConn([]),
                admin_make_job_log_handler_fn=lambda _job_id: logging.NullHandler(),
                admin_job_set_status_fn=lambda _job_id, status: statuses.append(status) or True,
                admin_job_append_log_fn=lambda _job_id, message: logs.append(str(message)),
            )

        self.assertEqual(["running", "done"], statuses)
        self.assertEqual(1, len(_CoordinatorStub.instances))
        self.assertTrue(_CoordinatorStub.instances[0].closed)
        self.assertEqual(1, probe_mock.call_count)
        self.assertFalse(stream_ranges_mock.called)
        self.assertEqual(1, stream_single_mock.call_count)
        self.assertIs(stream_single_mock.call_args.kwargs["client"], primary_client)
        self.assertTrue(any("第二账号解析目标触发长等待" in line for line in logs))
        self.assertTrue(any("回退主账号单账号拉取" in line for line in logs))
        self.assertTrue(any(kwargs.get("stage") == "done" for _, kwargs in progress_calls))

    def test_harvest_job_runner_uses_secondary_when_primary_target_resolve_flood_waits(
        self,
    ) -> None:
        statuses = []
        logs = []
        progress_calls = []
        primary_client = object()
        secondary_client = object()
        secondary_entity = SimpleNamespace(id=55, title="secondary-target", username="sec")

        class _HeartbeatStop:
            def set(self):
                return None

        class _HeartbeatThread:
            def join(self, timeout=None):
                return None

        class _CoordinatorStub:
            instances = []

            def __init__(self, **kwargs):
                self.kwargs = kwargs
                self.closed = False
                self.__class__.instances.append(self)

            def close(self):
                self.closed = True

        with patch(
            "tg_harvest.admin_jobs.runners._start_job_heartbeat",
            return_value=(_HeartbeatStop(), _HeartbeatThread()),
        ), patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            side_effect=[True, True],
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            side_effect=[primary_client, secondary_client],
        ), patch(
            "tg_harvest.admin_jobs.runners._disconnect_worker_client",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.ingest.parse.resolve_target_entities",
            side_effect=[
                AccountFloodWaitError(
                    seconds=30153,
                    threshold_seconds=30,
                    account_label="primary",
                    scope="resolve-harvest-target",
                ),
                [secondary_entity],
            ],
        ) as resolve_mock, patch(
            "tg_harvest.admin_jobs.runners.stream_entity_harvest_to_writer",
            return_value=None,
        ) as stream_single_mock, patch(
            "tg_harvest.admin_jobs.runners.stream_entity_ranges_to_writer"
        ) as stream_ranges_mock, patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            side_effect=lambda *args, **kwargs: progress_calls.append((args, kwargs)) or True,
        ):
            _admin_harvest_job_runner(
                "job-primary-resolve-flood",
                "https://t.me/sec",
                cfg=SimpleNamespace(
                    session_name="main_sess",
                    secondary_session_name="secondary_sess",
                    api_id=1,
                    api_hash="hash",
                    flood_wait_switch_threshold=30,
                    multi_account_min_message_id=5000,
                    multi_account_range_chunk_size=777,
                ),
                get_conn_fn=lambda: _FakeConn([]),
                admin_make_job_log_handler_fn=lambda _job_id: logging.NullHandler(),
                admin_job_set_status_fn=lambda _job_id, status: statuses.append(status) or True,
                admin_job_append_log_fn=lambda _job_id, message: logs.append(str(message)),
            )

        self.assertEqual(["running", "done"], statuses)
        self.assertEqual(2, resolve_mock.call_count)
        self.assertEqual(1, stream_single_mock.call_count)
        self.assertIs(stream_single_mock.call_args.kwargs["client"], secondary_client)
        self.assertIs(stream_single_mock.call_args.kwargs["entity"], secondary_entity)
        self.assertFalse(stream_ranges_mock.called)
        self.assertTrue(_CoordinatorStub.instances[0].closed)
        self.assertTrue(any("主账号处于长等待，第二账号已解析目标" in line for line in logs))
        self.assertTrue(any("账号=第二账号" in line for line in logs))
        self.assertFalse(any("主账号确认第二账号解析结果" in line for line in logs))
        self.assertTrue(any(kwargs.get("stage") == "done" for _, kwargs in progress_calls))

    def test_harvest_job_runner_uses_secondary_when_primary_confirmation_flood_waits(
        self,
    ) -> None:
        statuses = []
        logs = []
        progress_calls = []
        primary_client = object()
        secondary_client = object()
        secondary_entity = SimpleNamespace(id=56, title="secondary-title", username="sec2")

        class _HeartbeatStop:
            def set(self):
                return None

        class _HeartbeatThread:
            def join(self, timeout=None):
                return None

        class _CoordinatorStub:
            instances = []

            def __init__(self, **kwargs):
                self.kwargs = kwargs
                self.closed = False
                self.__class__.instances.append(self)

            def close(self):
                self.closed = True

        with patch(
            "tg_harvest.admin_jobs.runners._start_job_heartbeat",
            return_value=(_HeartbeatStop(), _HeartbeatThread()),
        ), patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            side_effect=[True, True],
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            side_effect=[primary_client, secondary_client],
        ), patch(
            "tg_harvest.admin_jobs.runners._disconnect_worker_client",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.ingest.parse.resolve_target_entities",
            side_effect=[
                [],
                [secondary_entity],
                AccountFloodWaitError(
                    seconds=30150,
                    threshold_seconds=30,
                    account_label="primary",
                    scope="resolve-matching-target",
                ),
            ],
        ), patch(
            "tg_harvest.admin_jobs.runners.stream_entity_harvest_to_writer",
            return_value=None,
        ) as stream_single_mock, patch(
            "tg_harvest.admin_jobs.runners.stream_entity_ranges_to_writer"
        ) as stream_ranges_mock, patch(
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            side_effect=lambda *args, **kwargs: progress_calls.append((args, kwargs)) or True,
        ):
            _admin_harvest_job_runner(
                "job-primary-confirm-flood",
                "secondary-title",
                cfg=SimpleNamespace(
                    session_name="main_sess",
                    secondary_session_name="secondary_sess",
                    api_id=1,
                    api_hash="hash",
                    flood_wait_switch_threshold=30,
                    multi_account_min_message_id=5000,
                    multi_account_range_chunk_size=777,
                ),
                get_conn_fn=lambda: _FakeConn([]),
                admin_make_job_log_handler_fn=lambda _job_id: logging.NullHandler(),
                admin_job_set_status_fn=lambda _job_id, status: statuses.append(status) or True,
                admin_job_append_log_fn=lambda _job_id, message: logs.append(str(message)),
            )

        self.assertEqual(["running", "done"], statuses)
        self.assertEqual(1, stream_single_mock.call_count)
        self.assertIs(stream_single_mock.call_args.kwargs["client"], secondary_client)
        self.assertIs(stream_single_mock.call_args.kwargs["entity"], secondary_entity)
        self.assertFalse(stream_ranges_mock.called)
        self.assertTrue(_CoordinatorStub.instances[0].closed)
        self.assertTrue(any("改用第二账号直接采集" in line for line in logs))
        self.assertTrue(any("账号=第二账号" in line for line in logs))
        self.assertTrue(any(kwargs.get("stage") == "done" for _, kwargs in progress_calls))

    def test_harvest_target_resolution_can_start_from_secondary_title_match(self) -> None:
        logs = []
        primary_entity = SimpleNamespace(id=22, title="public-title", username="public_chan")
        secondary_entity = SimpleNamespace(id=22, title="public-title", username="public_chan")

        class _PrimaryClient:
            def get_entity(self, key):
                if str(key).strip().lstrip("@") == "public_chan":
                    return primary_entity
                raise ValueError("could not find the input entity")

        with patch(
            "tg_harvest.admin_jobs.runners._ensure_base_session_valid",
            return_value=True,
        ), patch(
            "tg_harvest.admin_jobs.runners._create_isolated_worker_client",
            return_value=object(),
        ), patch(
            "tg_harvest.admin_jobs.runners._disconnect_worker_client",
            return_value=None,
        ), patch(
            "tg_harvest.admin_jobs.runners._cleanup_isolated_worker_session",
            return_value=None,
        ), patch(
            "tg_harvest.ingest.parse.resolve_target_entities",
            side_effect=[
                [],
                [secondary_entity],
                [],
            ],
        ):
            entities = _resolve_harvest_target_entities(
                job_id="job-resolve",
                target="public-title",
                cfg=SimpleNamespace(
                    session_name="main_sess",
                    secondary_session_name="secondary_sess",
                    api_id=1,
                    api_hash="hash",
                ),
                primary_client=_PrimaryClient(),
                admin_job_append_log_fn=lambda _job_id, message: logs.append(str(message)),
            )

        self.assertEqual([primary_entity], entities)
        self.assertTrue(any("第二账号解析并在主账号确认" in line for line in logs))


class HarvestProgressTests(unittest.TestCase):
    def test_format_harvest_progress_message_with_total(self) -> None:
        self.assertEqual(
            "正在采集 1000/25000",
            _format_harvest_progress_message(1000, 25000),
        )

    def test_format_harvest_progress_message_clamps_over_total(self) -> None:
        self.assertEqual(
            "正在采集 25000/25000",
            _format_harvest_progress_message(26000, 25000),
        )


class SearchableMediaCleanupTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE messages (
                pk INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                grouped_id INTEGER,
                msg_type TEXT,
                content TEXT,
                content_norm TEXT,
                pure_hash TEXT,
                dedupe_hash TEXT,
                is_promo INTEGER NOT NULL DEFAULT 0,
                promo_score INTEGER NOT NULL DEFAULT 0,
                promo_reasons TEXT,
                dedupe_eligible INTEGER NOT NULL DEFAULT 0,
                guard_reason TEXT,
                text_len INTEGER NOT NULL DEFAULT 0,
                has_media INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE message_media (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                file_name TEXT,
                file_unique_id TEXT,
                media_fingerprint TEXT,
                PRIMARY KEY (chat_id, message_id)
            )
            """
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()

    def test_load_grouped_ids_for_messages_reads_existing_album_ids(self) -> None:
        cur = self.conn.cursor()
        cur.executemany(
            """
            INSERT INTO messages(chat_id, message_id, grouped_id, msg_type, content, content_norm, has_media)
            VALUES (1, ?, ?, 'PHOTO', '', '', 1)
            """,
            [(105, 7001), (106, None), (107, 7002)],
        )
        self.conn.commit()

        grouped_ids = load_grouped_ids_for_messages(
            self.conn,
            [(1, 105), (1, 106), (1, 107), (1, 999)],
        )

        self.assertEqual({7001, 7002}, grouped_ids)

    def test_unsearchable_cleanup_targets_media_with_only_identity_metadata(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO messages(chat_id, message_id, content, content_norm, has_media)
            VALUES (1, 200, '', '', 1)
            """
        )
        cur.execute(
            """
            INSERT INTO message_media(chat_id, message_id, file_name, file_unique_id, media_fingerprint)
            VALUES (1, 200, '', 'u-only', 'fp-only')
            """
        )
        self.conn.commit()

        target_count = _build_cleanup_targets_table(
            cur,
            "empty_media",
            "",
            [],
            "",
        )

        self.assertEqual(1, target_count)

    def test_unsearchable_cleanup_targets_broken_media_placeholders(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO messages(chat_id, message_id, content, content_norm, has_media)
            VALUES (1, 201, '', '', 1)
            """
        )
        cur.execute(
            """
            INSERT INTO message_media(chat_id, message_id, file_name, file_unique_id, media_fingerprint)
            VALUES (1, 201, '', '', '')
            """
        )
        self.conn.commit()

        target_count = _build_cleanup_targets_table(
            cur,
            "empty_media",
            "",
            [],
            "",
        )

        self.assertEqual(1, target_count)

    def test_cleanup_targets_table_supports_runner_call_signature(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO messages(chat_id, message_id, content, content_norm, has_media)
            VALUES (1, 202, '', '', 1)
            """
        )
        self.conn.commit()

        target_count = _build_cleanup_targets_table(
            cur,
            "empty_media",
            "",
            [],
            "",
        )

        self.assertEqual(1, target_count)

    def test_keyword_cleanup_treats_like_wildcards_as_literals(self) -> None:
        cur = self.conn.cursor()
        cur.executemany(
            """
            INSERT INTO messages(chat_id, message_id, content, content_norm, has_media)
            VALUES (?, ?, ?, ?, 0)
            """,
            [
                (1, 210, "plain text", "plain text"),
                (1, 211, "100% real", "100% real"),
            ],
        )
        self.conn.commit()

        target_count = _build_cleanup_targets_table(
            cur,
            "keyword",
            "",
            [],
            _build_cleanup_like_patterns("%"),
        )

        self.assertEqual(1, target_count)
        cur.execute("SELECT message_id FROM temp_cleanup_targets")
        self.assertEqual([211], [int(row["message_id"]) for row in cur.fetchall()])

    def test_unsearchable_cleanup_targets_blank_album_members_with_media_metadata(self) -> None:
        cur = self.conn.cursor()
        cur.executemany(
            """
            INSERT INTO messages(chat_id, message_id, grouped_id, content, content_norm, has_media)
            VALUES (?, ?, ?, ?, ?, 1)
            """,
            [
                (1, 220, 900, "album caption", "album caption"),
                (1, 221, 900, "", ""),
                (1, 222, 901, "", ""),
            ],
        )
        cur.executemany(
            """
            INSERT INTO message_media(chat_id, message_id, file_name, file_unique_id, media_fingerprint)
            VALUES (?, ?, '', ?, ?)
            """,
            [
                (1, 221, "u-221", "fp-221"),
                (1, 222, "u-222", "fp-222"),
            ],
        )
        self.conn.commit()

        target_count = _build_cleanup_targets_table(
            cur,
            "empty_media",
            "",
            [],
            "",
        )

        self.assertEqual(2, target_count)
        cur.execute("SELECT message_id FROM temp_cleanup_targets")
        self.assertEqual([221, 222], [int(row["message_id"]) for row in cur.fetchall()])

    def test_unsearchable_cleanup_keeps_filename_search_text(self) -> None:
        cur = self.conn.cursor()
        cur.executemany(
            """
            INSERT INTO messages(chat_id, message_id, grouped_id, content, content_norm, has_media)
            VALUES (?, ?, ?, ?, ?, 1)
            """,
            [
                (1, 230, 910, "real caption", "real caption"),
                (1, 231, 910, "photo_001.jpg", "photo_001.jpg"),
                (1, 232, None, "standalone.jpg", "standalone.jpg"),
            ],
        )
        cur.executemany(
            """
            INSERT INTO message_media(chat_id, message_id, file_name, file_unique_id, media_fingerprint)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (1, 230, "", "u-230", "fp-230"),
                (1, 231, "photo_001.jpg", "u-231", "fp-231"),
                (1, 232, "standalone.jpg", "u-232", "fp-232"),
            ],
        )
        self.conn.commit()

        target_count = _build_cleanup_targets_table(
            cur,
            "empty_media",
            "",
            [],
            "",
        )

        self.assertEqual(0, target_count)

    def test_unsearchable_cleanup_targets_blank_non_media_messages(self) -> None:
        cur = self.conn.cursor()
        cur.executemany(
            """
            INSERT INTO messages(chat_id, message_id, content, content_norm, has_media)
            VALUES (?, ?, ?, ?, 0)
            """,
            [
                (1, 240, "", ""),
                (1, 241, "searchable text", "searchable text"),
                (1, 242, "   ", "   "),
            ],
        )
        self.conn.commit()

        target_count = _build_cleanup_targets_table(
            cur,
            "empty_media",
            "",
            [],
            "",
        )

        self.assertEqual(2, target_count)
        cur.execute("SELECT message_id FROM temp_cleanup_targets")
        self.assertEqual([240, 242], [int(row["message_id"]) for row in cur.fetchall()])


class AdminRunnerHelperTests(unittest.TestCase):
    def test_job_is_marked_error_when_done_status_cannot_be_persisted(self) -> None:
        statuses = []
        logs = []

        class _Conn:
            def rollback(self):
                return None

            def close(self):
                return None

        class _HeartbeatStop:
            def set(self):
                return None

        class _HeartbeatThread:
            def join(self, timeout=None):
                return None

        def set_status(_job_id, status):
            statuses.append(status)
            return status != "done"

        with patch(
            "tg_harvest.admin_jobs.runners.start_admin_job_heartbeat",
            return_value=(_HeartbeatStop(), _HeartbeatThread()),
        ):
            _run_simple_admin_job_with_conn(
                "job-status-write",
                get_conn_fn=_Conn,
                admin_job_set_status_fn=set_status,
                admin_job_append_log_fn=lambda _job_id, message: logs.append(str(message)),
                run_fn=lambda _conn: None,
                error_prefix="执行失败：",
            )

        self.assertEqual(["running", "done", "error"], statuses)
        self.assertTrue(any("任务状态持久化失败" in message for message in logs))

    def test_admin_error_message_maps_known_telegram_errors(self) -> None:
        self.assertEqual(
            "触发 Telegram 频控限制，请稍后再试",
            admin_error_message(RuntimeError("FloodWaitError: retry later")),
        )

    def test_admin_error_message_keeps_unknown_error_context(self) -> None:
        self.assertEqual(
            "RuntimeError: boom",
            admin_error_message(RuntimeError("boom")),
        )

    def test_disconnect_worker_client_closes_attached_event_loop(self) -> None:
        class _Loop:
            def __init__(self):
                self.closed = False

            def is_closed(self):
                return self.closed

            def close(self):
                self.closed = True

        class _Client:
            def __init__(self):
                self.disconnected = False
                self._tg_harvest_loop = _Loop()

            def disconnect(self):
                self.disconnected = True

        client = _Client()

        _disconnect_worker_client(client)

        self.assertTrue(client.disconnected)
        self.assertTrue(client._tg_harvest_loop.closed)

    def test_disconnect_worker_client_restores_previous_event_loop(self) -> None:
        previous_loop = asyncio.new_event_loop()
        worker_loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(previous_loop)

            class _Client:
                def __init__(self):
                    self.disconnected = False
                    self._tg_harvest_loop = worker_loop
                    self._tg_harvest_previous_loop = previous_loop

                def disconnect(self):
                    self.disconnected = True

            client = _Client()

            _disconnect_worker_client(client)

            self.assertTrue(client.disconnected)
            self.assertTrue(worker_loop.is_closed())
            self.assertIs(asyncio.get_event_loop(), previous_loop)
            self.assertFalse(previous_loop.is_closed())
        finally:
            asyncio.set_event_loop(None)
            if not worker_loop.is_closed():
                worker_loop.close()
            if not previous_loop.is_closed():
                previous_loop.close()

    def test_disconnect_worker_client_without_saved_previous_loop_keeps_current_loop(
        self,
    ) -> None:
        previous_loop = asyncio.new_event_loop()
        worker_loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(previous_loop)

            class _Client:
                def __init__(self):
                    self.disconnected = False
                    self._tg_harvest_loop = worker_loop

                def disconnect(self):
                    self.disconnected = True

            client = _Client()

            _disconnect_worker_client(client)

            self.assertTrue(client.disconnected)
            self.assertTrue(worker_loop.is_closed())
            self.assertIs(asyncio.get_event_loop(), previous_loop)
            self.assertFalse(previous_loop.is_closed())
        finally:
            asyncio.set_event_loop(None)
            if not worker_loop.is_closed():
                worker_loop.close()
            if not previous_loop.is_closed():
                previous_loop.close()


class PersistentAdminJobsTests(unittest.TestCase):
    def setUp(self) -> None:
        fd, self.db_path = tempfile.mkstemp(suffix=".sqlite3")
        os.close(fd)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        feats = detect_sqlite_features(conn)
        create_schema(conn, feats)
        conn.close()

        admin_jobs_core.ADMIN_JOBS.clear()
        admin_jobs_runtime.configure_admin_job_runtime("test-runtime")

        self.patchers = [
            patch.object(admin_jobs_core.CFG, "db_name", self.db_path),
            patch.object(admin_jobs_core.CFG, "sqlite_cache_mb", 16),
            patch.object(admin_jobs_core.CFG, "sqlite_mmap_mb", 0),
        ]
        for patcher in self.patchers:
            patcher.start()

    def tearDown(self) -> None:
        for patcher in reversed(self.patchers):
            patcher.stop()
        admin_jobs_core.ADMIN_JOBS.clear()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_snapshot_and_logs_survive_process_memory_loss(self) -> None:
        job = _admin_job_create("cleanup", target_chat_id=123, target_label="chat-123")
        job_id = str(job["job_id"])

        _admin_job_append_log(job_id, "line-2")
        _admin_job_set_status(job_id, "running")

        admin_jobs_core.ADMIN_JOBS.clear()

        snapshot = _admin_job_get_snapshot(job_id)
        logs = _admin_job_get_logs(job_id, after_seq=0)

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual("running", snapshot["status"])
        self.assertEqual(2, snapshot["log_count"])
        self.assertEqual(2, snapshot["last_seq"])
        self.assertEqual(
            ["任务已创建（占位）", "line-2"],
            [item["message"] for item in logs or []],
        )

    def test_snapshot_row_uses_indexed_log_aggregates_without_join_group(self) -> None:
        job = _admin_job_create("cleanup", target_chat_id=123, target_label="chat-123")
        job_id = str(job["job_id"])
        _admin_job_append_log(job_id, "line-2")

        statements = []
        real_admin_connect = admin_jobs_store._admin_connect

        def traced_admin_connect():
            conn = real_admin_connect()
            conn.set_trace_callback(
                lambda sql: statements.append(" ".join(str(sql).split()))
            )
            return conn

        with patch.object(admin_jobs_store, "_admin_connect", traced_admin_connect):
            row = _admin_fetch_job_snapshot_row(job_id)

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual(2, int(row["log_count"]))
        self.assertEqual(2, int(row["last_seq"]))
        snapshot_sql = next(
            sql
            for sql in statements
            if sql.upper().startswith("SELECT") and "FROM admin_jobs j" in sql
        )
        self.assertIn("SELECT COUNT(*) FROM admin_job_logs", snapshot_sql)
        self.assertIn("SELECT COALESCE(MAX(l.seq), 0)", snapshot_sql)
        self.assertNotIn("LEFT JOIN admin_job_logs", snapshot_sql)
        self.assertNotIn("GROUP BY", snapshot_sql)

    def test_recover_interrupted_jobs_marks_running_jobs_as_error(self) -> None:
        job = _admin_job_create("update", target_chat_id=321, target_label="chat-321")
        job_id = str(job["job_id"])
        _admin_job_set_status(job_id, "running")

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE admin_jobs
            SET heartbeat_at = '2000-01-01T00:00:00+00:00',
                updated_at = '2000-01-01T00:00:00+00:00'
            WHERE job_id = ?
            """,
            (job_id,),
        )
        conn.commit()
        conn.close()

        admin_jobs_core.ADMIN_JOBS.clear()

        recovered = _admin_recover_interrupted_jobs()
        snapshot = _admin_job_get_snapshot(job_id)
        logs = _admin_job_get_logs(job_id, after_seq=0) or []

        self.assertEqual(1, recovered)
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual("error", snapshot["status"])
        self.assertTrue(
            any("进程重启或退出而中断" in item["message"] for item in logs),
        )

    def test_recover_interrupted_jobs_ignores_fresh_running_jobs(self) -> None:
        job = _admin_job_create("update", target_chat_id=654, target_label="chat-654")
        job_id = str(job["job_id"])
        _admin_job_set_status(job_id, "running")

        recovered = _admin_recover_interrupted_jobs()
        snapshot = _admin_job_get_snapshot(job_id)

        self.assertEqual(0, recovered)
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual("running", snapshot["status"])

    def test_request_job_stop_marks_active_job(self) -> None:
        job = _admin_job_create("update", target_chat_id=None, target_label="all")
        job_id = str(job["job_id"])
        _admin_job_set_status(job_id, "running")

        ok, error_message = _admin_request_job_stop(job_id)
        snapshot = _admin_job_get_snapshot(job_id)
        active_job = _admin_get_active_job()

        self.assertTrue(ok)
        self.assertIsNone(error_message)
        self.assertTrue(_admin_job_stop_requested(job_id))
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertTrue(snapshot["stop_requested"])
        self.assertIsNotNone(active_job)
        assert active_job is not None
        self.assertTrue(active_job["stop_requested"])

    def test_recover_interrupted_jobs_marks_previous_runtime_job_immediately(self) -> None:
        admin_jobs_runtime.configure_admin_job_runtime("old-runtime")
        job = _admin_job_create("update", target_chat_id=654, target_label="chat-654")
        job_id = str(job["job_id"])
        _admin_job_set_status(job_id, "running")

        admin_jobs_runtime.configure_admin_job_runtime("new-runtime")
        admin_jobs_core.ADMIN_JOBS.clear()

        with patch.object(
            admin_jobs_runtime,
            "_admin_owner_is_alive",
            return_value=False,
        ):
            recovered = _admin_recover_interrupted_jobs()
        snapshot = _admin_job_get_snapshot(job_id)

        self.assertEqual(1, recovered)
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual("error", snapshot["status"])

    def test_recover_interrupted_jobs_keeps_fresh_foreign_live_owner(self) -> None:
        admin_jobs_runtime.configure_admin_job_runtime("old-runtime")
        job = _admin_job_create("update", target_chat_id=654, target_label="chat-654")
        job_id = str(job["job_id"])
        _admin_job_set_status(job_id, "running")

        admin_jobs_runtime.configure_admin_job_runtime("new-runtime")
        admin_jobs_core.ADMIN_JOBS.clear()
        with patch.object(
            admin_jobs_runtime,
            "_admin_owner_is_alive",
            return_value=True,
        ):
            recovered = _admin_recover_interrupted_jobs()

        snapshot = _admin_job_get_snapshot(job_id)
        self.assertEqual(0, recovered)
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual("running", snapshot["status"])

    def test_log_and_stop_request_do_not_take_over_job_owner(self) -> None:
        admin_jobs_runtime.configure_admin_job_runtime("owner-runtime")
        job = _admin_job_create("cleanup", target_chat_id=None, target_label="all")
        job_id = str(job["job_id"])

        admin_jobs_runtime.configure_admin_job_runtime("request-runtime")
        _admin_job_append_log(job_id, "external request log")
        self.assertTrue(_admin_request_job_stop(job_id)[0])

        conn = sqlite3.connect(self.db_path)
        try:
            owner = conn.execute(
                """
                SELECT owner_instance_id, owner_pid, owner_host
                FROM admin_jobs
                WHERE job_id = ?
                """,
                (job_id,),
            ).fetchone()
        finally:
            conn.close()
        self.assertEqual("owner-runtime", owner[0])
        self.assertEqual(os.getpid(), owner[1])
        self.assertTrue(owner[2])

    def test_try_create_exclusive_job_rejects_second_active_job(self) -> None:
        first_job, existing = _admin_try_create_exclusive_job(
            "cleanup", target_chat_id=None, target_label="all"
        )
        second_job, second_existing = _admin_try_create_exclusive_job(
            "cleanup_empty", target_chat_id=None, target_label="all"
        )

        self.assertIsNotNone(first_job)
        self.assertIsNone(existing)
        self.assertIsNone(second_job)
        self.assertIsNotNone(second_existing)
        assert second_existing is not None
        self.assertEqual("cleanup", second_existing["job_type"])

    def test_try_create_exclusive_job_keeps_fresh_previous_runtime_job_as_conflict(self) -> None:
        admin_jobs_runtime.configure_admin_job_runtime("old-runtime")
        old_job = _admin_job_create("cleanup", target_chat_id=None, target_label="old")
        old_job_id = str(old_job["job_id"])
        _admin_job_set_status(old_job_id, "running")

        admin_jobs_runtime.configure_admin_job_runtime("new-runtime")
        admin_jobs_core.ADMIN_JOBS.clear()

        new_job, existing = _admin_try_create_exclusive_job(
            "cleanup_empty", target_chat_id=None, target_label="new"
        )

        self.assertIsNone(new_job)
        self.assertIsNotNone(existing)
        assert existing is not None
        self.assertEqual(old_job_id, existing["job_id"])
        old_snapshot = _admin_job_get_snapshot(old_job_id)
        self.assertIsNotNone(old_snapshot)
        assert old_snapshot is not None
        self.assertEqual("running", old_snapshot["status"])

    def test_try_create_exclusive_job_recovers_stale_job_before_conflict_check(self) -> None:
        old_job = _admin_job_create("cleanup", target_chat_id=None, target_label="old")
        old_job_id = str(old_job["job_id"])
        _admin_job_set_status(old_job_id, "running")

        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """
                UPDATE admin_jobs
                SET heartbeat_at = '2000-01-01T00:00:00+00:00',
                    updated_at = '2000-01-01T00:00:00+00:00'
                WHERE job_id = ?
                """,
                (old_job_id,),
            )
            conn.commit()
        finally:
            conn.close()

        admin_jobs_core.ADMIN_JOBS.clear()
        new_job, existing = _admin_try_create_exclusive_job(
            "cleanup_empty", target_chat_id=None, target_label="new"
        )

        self.assertIsNotNone(new_job)
        self.assertIsNone(existing)
        old_snapshot = _admin_job_get_snapshot(old_job_id)
        self.assertIsNotNone(old_snapshot)
        assert old_snapshot is not None
        self.assertEqual("error", old_snapshot["status"])

    def test_has_any_active_job_keeps_fresh_running_job_active(self) -> None:
        job = _admin_job_create("cleanup", target_chat_id=None, target_label="fresh")
        _admin_job_set_status(str(job["job_id"]), "running")

        self.assertTrue(admin_jobs_core._admin_has_any_active_job())

    def test_has_any_active_job_recovers_stale_job_before_reporting_active_state(self) -> None:
        old_job = _admin_job_create("cleanup", target_chat_id=None, target_label="old")
        old_job_id = str(old_job["job_id"])
        _admin_job_set_status(old_job_id, "running")

        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """
                UPDATE admin_jobs
                SET heartbeat_at = '2000-01-01T00:00:00+00:00',
                    updated_at = '2000-01-01T00:00:00+00:00'
                WHERE job_id = ?
                """,
                (old_job_id,),
            )
            conn.commit()
        finally:
            conn.close()

        admin_jobs_core.ADMIN_JOBS.clear()

        self.assertFalse(admin_jobs_core._admin_has_any_active_job())
        old_snapshot = _admin_job_get_snapshot(old_job_id)
        self.assertIsNotNone(old_snapshot)
        assert old_snapshot is not None
        self.assertEqual("error", old_snapshot["status"])

    def test_interrupted_clone_job_marks_migration_record_error(self) -> None:
        job = _admin_job_create(
            "clone_timeline_migration",
            target_chat_id=100,
            target_label="clone",
        )
        job_id = str(job["job_id"])
        _admin_job_set_status(job_id, "running")

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute(
                """
                INSERT INTO chats(
                    chat_id, chat_title, chat_type, message_count,
                    first_seen_at, last_seen_at
                ) VALUES (100, 'Source', 'Megagroup', 1, '2026-01-01', '2026-01-01')
                """
            )
            create_clone_run(
                conn,
                run_id="run-interrupted",
                job_id="structure-job",
                source_chat={
                    "chat_id": 100,
                    "chat_title": "Source",
                    "chat_type": "Megagroup",
                    "message_count": 1,
                },
                target_title="Target",
                target_kind="megagroup",
                target_owner_session="secondary",
            )
            conn.execute(
                """
                UPDATE admin_clone_runs
                SET status = 'done', phase = 'done'
                WHERE run_id = 'run-interrupted'
                """
            )
            create_clone_plan(
                conn,
                plan_id="plan-interrupted",
                run_id="run-interrupted",
                job_id="plan-job",
                status="done",
            )
            create_clone_migration(
                conn,
                migration_id="migration-interrupted",
                run_id="run-interrupted",
                plan_id="plan-interrupted",
                job_id=job_id,
                status="running",
                phase="replaying_timeline",
            )
            conn.execute(
                """
                UPDATE admin_jobs
                SET heartbeat_at = '2000-01-01T00:00:00+00:00',
                    updated_at = '2000-01-01T00:00:00+00:00'
                WHERE job_id = ?
                """,
                (job_id,),
            )
            conn.commit()
        finally:
            conn.close()

        self.assertEqual(1, _admin_recover_interrupted_jobs())

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            migration = conn.execute(
                """
                SELECT status, phase, error_message, completed_at
                FROM admin_clone_migrations
                WHERE migration_id = 'migration-interrupted'
                """
            ).fetchone()
        finally:
            conn.close()

        self.assertEqual("error", migration["status"])
        self.assertEqual("interrupted", migration["phase"])
        self.assertIn("服务进程重启或退出", migration["error_message"])
        self.assertTrue(migration["completed_at"])

        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """
                UPDATE admin_clone_migrations
                SET status = 'running', phase = 'replaying_timeline',
                    error_message = '', completed_at = NULL
                WHERE migration_id = 'migration-interrupted'
                """
            )
            conn.commit()
        finally:
            conn.close()

        self.assertEqual(0, _admin_recover_interrupted_jobs())
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            reconciled = conn.execute(
                """
                SELECT status, phase, error_message
                FROM admin_clone_migrations
                WHERE migration_id = 'migration-interrupted'
                """
            ).fetchone()
        finally:
            conn.close()

        self.assertEqual("error", reconciled["status"])
        self.assertEqual("interrupted", reconciled["phase"])
        self.assertIn("所属后台任务已失败", reconciled["error_message"])

class AdminJobLogHandlerTests(unittest.TestCase):
    def test_handler_skips_passthrough_when_disabled(self) -> None:
        messages = []

        with patch(
            "tg_harvest.admin_jobs.core._admin_job_append_log",
            side_effect=lambda job_id, message: messages.append((job_id, message)),
        ):
            handler = _AdminJobThreadLogHandler("job-1")
            record = logging.LogRecord(
                name="root",
                level=logging.INFO,
                pathname=__file__,
                lineno=1,
                msg="hidden-line",
                args=(),
                exc_info=None,
            )
            job_token = job_context.set("job-1")
            passthrough_token = job_log_passthrough_enabled.set(False)
            try:
                handler.emit(record)
            finally:
                job_log_passthrough_enabled.reset(passthrough_token)
                job_context.reset(job_token)

        self.assertEqual([], messages)


class _RecordingLock:
    def __init__(self) -> None:
        self.acquired = 0
        self.released = 0

    def acquire(self, timeout=None):
        self.acquired += 1
        return True

    def release(self):
        self.released += 1


class WriteConsistencyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        feats = detect_sqlite_features(self.conn)
        create_schema(self.conn, feats)

        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO chats(chat_id, chat_title, message_count)
            VALUES (1, 'Chat 1', 0)
            """
        )
        cur.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type,
                content, content_norm, has_media, is_promo, dedupe_eligible
            )
            VALUES (1, 10, '2026-01-01 00:00:00', 1, 'TEXT', 'hello', 'hello', 0, 0, 0)
            """
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()

    def test_refresh_chat_message_counts_uses_global_write_lock(self) -> None:
        lock = _RecordingLock()

        with patch("tg_harvest.storage.connection.DB_WRITE_LOCK", lock):
            refresh_chat_message_counts(self.conn, [1])

        self.assertEqual(1, lock.acquired)
        self.assertEqual(1, lock.released)

        cur = self.conn.cursor()
        cur.execute("SELECT message_count FROM chats WHERE chat_id = 1")
        self.assertEqual(1, int(cur.fetchone()["message_count"]))

    def test_dedupe_promotional_duplicates_uses_global_write_lock(self) -> None:
        lock = _RecordingLock()

        with patch("tg_harvest.storage.connection.DB_WRITE_LOCK", lock):
            deleted, solo, group_txt, group_med, affected_groups = (
                dedupe_promotional_duplicates(self.conn, chat_id=1)
            )

        self.assertEqual((0, 0, 0, 0, set()), (deleted, solo, group_txt, group_med, affected_groups))
        self.assertEqual(1, lock.acquired)
        self.assertEqual(1, lock.released)

    def test_dedupe_deletes_every_target_across_batches(self) -> None:
        rows = [
            (
                1,
                1000 + idx,
                "2026-01-01 00:00:00",
                idx,
                "TEXT",
                "same promo",
                "same promo",
                "hash-all",
                "hash-all",
                0,
                1,
                1,
                10,
            )
            for idx in range(501)
        ]
        self.conn.executemany(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type,
                content, content_norm, pure_hash, dedupe_hash, has_media,
                is_promo, dedupe_eligible, promo_score
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()

        deleted, solo, group_txt, group_med, affected_groups = (
            dedupe_promotional_duplicates(
                self.conn,
                chat_id=1,
                mode="PURGE_ALL",
                threshold=2,
            )
        )

        self.assertEqual((501, 1, 0, 0, set()), (deleted, solo, group_txt, group_med, affected_groups))
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM messages WHERE dedupe_hash = 'hash-all'")
        self.assertEqual(0, int(cur.fetchone()["c"]))
        cur.execute("SELECT COUNT(*) AS c FROM dedupe_actions WHERE dedupe_hash = 'hash-all'")
        self.assertEqual(501, int(cur.fetchone()["c"]))

    def test_batch_upsert_removes_stale_media_when_message_becomes_text(self) -> None:
        media_message = (
            1,
            900,
            "2026-01-01 00:00:00",
            1,
            1,
            "photo caption",
            "photo caption",
            "pure-photo",
            "dedupe-photo",
            "PHOTO",
            None,
            1,
            0,
            0,
            "[]",
            0,
            "",
            13,
        )
        media_row = (
            1,
            900,
            "PHOTO",
            "file-1",
            "photo.jpg",
            ".jpg",
            "image/jpeg",
            123,
            640,
            480,
            None,
            None,
            "media-fp-1",
            "{}",
        )
        text_message = (
            1,
            900,
            "2026-01-01 00:01:00",
            2,
            1,
            "edited text",
            "edited text",
            "pure-text",
            "dedupe-text",
            "TEXT",
            None,
            0,
            0,
            0,
            "[]",
            0,
            "",
            11,
        )

        batch_upsert(self.conn, [media_message], [media_row])
        batch_upsert(self.conn, [text_message], [])

        cur = self.conn.cursor()
        cur.execute(
            "SELECT has_media, msg_type FROM messages WHERE chat_id = 1 AND message_id = 900"
        )
        message_row = cur.fetchone()
        self.assertEqual(0, int(message_row["has_media"]))
        self.assertEqual("TEXT", message_row["msg_type"])
        cur.execute(
            "SELECT COUNT(*) AS c FROM message_media WHERE chat_id = 1 AND message_id = 900"
        )
        self.assertEqual(0, int(cur.fetchone()["c"]))

    def test_batch_upsert_rolls_back_message_rows_when_later_media_write_fails(self) -> None:
        message_row = (
            1,
            905,
            "2026-01-01 00:00:00",
            1,
            1,
            "atomic message",
            "atomic message",
            "pure-atomic",
            "dedupe-atomic",
            "TEXT",
            None,
            0,
            0,
            0,
            "[]",
            0,
            "",
            14,
        )

        with patch(
            "tg_harvest.ingest.store._batch_upsert_media",
            side_effect=sqlite3.OperationalError("media write failed"),
        ), self.assertRaises(sqlite3.OperationalError):
            batch_upsert(self.conn, [message_row], [])

        row = self.conn.execute(
            "SELECT 1 FROM messages WHERE chat_id = ? AND message_id = ?",
            (1, 905),
        ).fetchone()
        self.assertIsNone(row)

    def test_batch_upsert_refreshes_chat_message_summary(self) -> None:
        refresh_chat_message_counts(self.conn, [1])
        rows = [
            (
                1,
                910,
                "2026-01-01 00:00:00",
                1,
                1,
                "first",
                "first",
                "pure-first",
                "dedupe-first",
                "TEXT",
                None,
                0,
                0,
                0,
                "[]",
                0,
                "",
                5,
            ),
            (
                1,
                911,
                "2026-01-01 00:01:00",
                2,
                1,
                "second",
                "second",
                "pure-second",
                "dedupe-second",
                "TEXT",
                None,
                0,
                0,
                0,
                "[]",
                0,
                "",
                6,
            ),
        ]

        batch_upsert(self.conn, rows, [])

        cur = self.conn.cursor()
        cur.execute(
            "SELECT message_count, last_message_created_at FROM chats WHERE chat_id = 1"
        )
        chat_row = cur.fetchone()
        self.assertEqual(3, int(chat_row["message_count"]))
        self.assertTrue(str(chat_row["last_message_created_at"] or ""))

    def test_batch_upsert_deletes_stale_media_in_key_batches(self) -> None:
        media_messages = [
            (
                1,
                2000 + idx,
                "2026-01-01 00:00:00",
                idx,
                1,
                f"photo caption {idx}",
                f"photo caption {idx}",
                f"pure-photo-{idx}",
                f"dedupe-photo-{idx}",
                "PHOTO",
                None,
                1,
                0,
                0,
                "[]",
                0,
                "",
                15,
            )
            for idx in range(9)
        ]
        media_rows = [
            (
                1,
                2000 + idx,
                "PHOTO",
                f"file-{idx}",
                f"photo-{idx}.jpg",
                ".jpg",
                "image/jpeg",
                100 + idx,
                640,
                480,
                None,
                None,
                f"media-fp-{idx}",
                "{}",
            )
            for idx in range(9)
        ]
        text_messages = [
            (
                1,
                2000 + idx,
                "2026-01-01 00:01:00",
                idx + 100,
                1,
                f"edited text {idx}",
                f"edited text {idx}",
                f"pure-text-{idx}",
                f"dedupe-text-{idx}",
                "TEXT",
                None,
                0,
                0,
                0,
                "[]",
                0,
                "",
                13,
            )
            for idx in range(9)
        ]

        batch_upsert(self.conn, media_messages, media_rows)
        statements = []
        self.conn.set_trace_callback(
            lambda sql: statements.append(" ".join(str(sql).split()))
        )
        try:
            batch_upsert(self.conn, text_messages, [])
        finally:
            self.conn.set_trace_callback(None)

        stale_media_deletes = [
            sql for sql in statements if sql.startswith("DELETE FROM message_media")
        ]
        self.assertEqual(1, len(stale_media_deletes))
        self.assertIn("(chat_id, message_id) IN", stale_media_deletes[0])
        cur = self.conn.cursor()
        cur.execute(
            "SELECT COUNT(*) AS c FROM message_media WHERE chat_id = 1 AND message_id >= 2000"
        )
        self.assertEqual(0, int(cur.fetchone()["c"]))

    def test_delete_chat_data_uses_one_global_write_lock_for_whole_transaction(self) -> None:
        self.conn.execute(
            """
            INSERT INTO message_media(chat_id, message_id)
            VALUES (1, 10)
            """
        )
        cur = self.conn.cursor()
        try:
            cur.execute("DROP TRIGGER IF EXISTS trg_message_terms_delete")
            cur.execute(
                "SELECT pk FROM messages WHERE chat_id = 1 AND message_id = 10"
            )
            message_pk = int(cur.fetchone()["pk"])
            cur.execute(
                "INSERT INTO message_search_terms(pk, term) VALUES (?, ?)",
                (message_pk, "hello"),
            )
            cur.execute(
                """
                INSERT INTO message_search_terms_rebuild_queue(pk, reason)
                VALUES (?, ?)
                ON CONFLICT(pk) DO UPDATE SET reason = excluded.reason
                """,
                (message_pk, "legacy"),
            )
        finally:
            cur.close()
        self.conn.commit()
        lock = _RecordingLock()

        with patch("tg_harvest.storage.connection.DB_WRITE_LOCK", lock):
            deleted = _delete_chat_data(self.conn, 1)

        self.assertEqual(1, deleted)
        self.assertEqual(1, lock.acquired)
        self.assertEqual(1, lock.released)
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM messages WHERE chat_id = 1")
        self.assertEqual(0, int(cur.fetchone()["c"]))
        cur.execute("SELECT COUNT(*) AS c FROM chats WHERE chat_id = 1")
        self.assertEqual(0, int(cur.fetchone()["c"]))
        cur.execute("SELECT COUNT(*) AS c FROM message_search_terms")
        self.assertEqual(0, int(cur.fetchone()["c"]))
        cur.execute("SELECT COUNT(*) AS c FROM message_search_terms_rebuild_queue")
        self.assertEqual(0, int(cur.fetchone()["c"]))

    def test_delete_chat_data_bulk_path_keeps_fts_clean_and_restores_triggers(self) -> None:
        cur = self.conn.cursor()
        try:
            cur.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='messages_fts'"
            )
            if cur.fetchone() is None:
                self.skipTest("SQLite build does not provide FTS5")
            cur.execute(
                "SELECT rowid FROM messages_fts WHERE messages_fts MATCH ?",
                ("hello",),
            )
            self.assertEqual([1], [int(row["rowid"]) for row in cur.fetchall()])
        finally:
            cur.close()

        with patch("tg_harvest.admin_jobs.runners.DELETE_CHAT_FAST_PATH_THRESHOLD", 1):
            deleted = _delete_chat_data(self.conn, 1)

        self.assertEqual(1, deleted)
        cur = self.conn.cursor()
        try:
            cur.execute(
                "SELECT rowid FROM messages_fts WHERE messages_fts MATCH ?",
                ("hello",),
            )
            self.assertEqual([], cur.fetchall())
            cur.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type='trigger'
                  AND name IN ('trg_messages_fts_delete', 'trg_message_terms_delete')
                """
            )
            self.assertEqual(
                {"trg_messages_fts_delete", "trg_message_terms_delete"},
                {row["name"] for row in cur.fetchall()},
            )
        finally:
            cur.close()

    def test_delete_empty_chats_only_removes_chats_with_no_messages(self) -> None:
        self.conn.execute(
            """
            INSERT INTO chats(chat_id, chat_title, message_count)
            VALUES (2, 'Empty', 0)
            """
        )
        self.conn.execute(
            """
            INSERT INTO chats(chat_id, chat_title, message_count)
            VALUES (3, 'Stale Count', 0)
            """
        )
        self.conn.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type,
                content, content_norm, has_media, is_promo, dedupe_eligible
            )
            VALUES (3, 30, '2026-01-01 00:00:00', 30, 'TEXT', 'keep', 'keep', 0, 0, 0)
            """
        )
        self.conn.execute(
            """
            INSERT INTO media_groups(chat_id, grouped_id, item_count, active_items)
            VALUES (2, 77, 0, 0)
            """
        )
        self.conn.commit()

        stats = _delete_empty_chats_data(self.conn)

        self.assertEqual(1, stats["deleted_chats"])
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT COUNT(*) AS c FROM chats WHERE chat_id = 2")
            self.assertEqual(0, int(cur.fetchone()["c"]))
            cur.execute("SELECT COUNT(*) AS c FROM chats WHERE chat_id = 3")
            self.assertEqual(1, int(cur.fetchone()["c"]))
            cur.execute("SELECT COUNT(*) AS c FROM messages WHERE chat_id = 3")
            self.assertEqual(1, int(cur.fetchone()["c"]))
            cur.execute("SELECT COUNT(*) AS c FROM media_groups WHERE chat_id = 2")
            self.assertEqual(0, int(cur.fetchone()["c"]))
        finally:
            cur.close()


class _SQLiteSideEffectLogHandler(logging.Handler):
    def __init__(self, db_path: str) -> None:
        super().__init__()
        self.db_path = db_path
        self.errors = []

    def emit(self, record: logging.LogRecord) -> None:
        conn = sqlite3.connect(self.db_path, timeout=0.2)
        try:
            conn.execute("PRAGMA busy_timeout=200")
            conn.execute(
                "INSERT INTO side_logs(message) VALUES (?)",
                (str(record.getMessage()),),
            )
            conn.commit()
        except Exception as exc:
            self.errors.append(exc)
        finally:
            conn.close()


class AdminJobLoggingLockRegressionTests(unittest.TestCase):
    def setUp(self) -> None:
        fd, self.db_path = tempfile.mkstemp(suffix=".sqlite3")
        os.close(fd)
        self.conn = sqlite3.connect(self.db_path, timeout=0.2)
        self.conn.row_factory = sqlite3.Row
        feats = detect_sqlite_features(self.conn)
        create_schema(self.conn, feats)
        self.conn.execute("CREATE TABLE side_logs(message TEXT NOT NULL)")
        self.conn.commit()
        self.root_logger = logging.getLogger()
        self.original_root_level = self.root_logger.level
        self.root_logger.setLevel(logging.INFO)

    def tearDown(self) -> None:
        self.root_logger.setLevel(self.original_root_level)
        self.conn.close()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def _side_log_count(self) -> int:
        cur = self.conn.cursor()
        try:
            cur.execute("SELECT COUNT(*) AS c FROM side_logs")
            return int(cur.fetchone()["c"] or 0)
        finally:
            cur.close()

    def test_refresh_media_groups_does_not_log_while_write_tx_is_open(self) -> None:
        self.conn.execute(
            """
            INSERT INTO chats(chat_id, chat_title, message_count)
            VALUES (1, 'Chat 1', 1)
            """
        )
        self.conn.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type,
                grouped_id, content, content_norm, has_media, is_promo, dedupe_eligible
            )
            VALUES (1, 10, '2026-01-01 00:00:00', 1, 'PHOTO', 99, 'caption', 'caption', 1, 0, 0)
            """
        )
        self.conn.execute(
            """
            INSERT INTO message_media(
                chat_id, message_id, media_kind, media_fingerprint
            )
            VALUES (1, 10, 'photo', 'fingerprint-1')
            """
        )
        self.conn.commit()

        handler = _SQLiteSideEffectLogHandler(self.db_path)
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        try:
            refresh_media_groups_for_chat(
                self.conn,
                chat_id=1,
                cfg=SimpleNamespace(
                    media_caption_guard_len=58,
                    promo_score_threshold=0,
                    disable_promo_filter=1,
                ),
                grouped_ids=None,
            )
        finally:
            root_logger.removeHandler(handler)

        self.assertEqual([], handler.errors)
        self.assertGreaterEqual(self._side_log_count(), 2)

    def test_dedupe_does_not_log_while_write_tx_is_open(self) -> None:
        self.conn.execute(
            """
            INSERT INTO chats(chat_id, chat_title, message_count)
            VALUES (1, 'Chat 1', 1)
            """
        )
        self.conn.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type,
                content, content_norm, has_media, is_promo, dedupe_eligible, promo_score
            )
            VALUES (1, 10, '2026-01-01 00:00:00', 1, 'TEXT', 'hello', 'hello', 0, 1, 1, 10)
            """
        )
        self.conn.commit()

        handler = _SQLiteSideEffectLogHandler(self.db_path)
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        try:
            dedupe_promotional_duplicates(self.conn, chat_id=1)
        finally:
            root_logger.removeHandler(handler)

        self.assertEqual([], handler.errors)
        self.assertGreaterEqual(self._side_log_count(), 1)

    def test_cleanup_batches_commit_before_appending_progress_logs(self) -> None:
        self.conn.execute(
            """
            INSERT INTO chats(chat_id, chat_title, message_count)
            VALUES (1, 'Chat 1', 2)
            """
        )
        self.conn.executemany(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type,
                content, content_norm, has_media, is_promo, dedupe_eligible
            )
            VALUES (?, ?, '2026-01-01 00:00:00', ?, 'TEXT', ?, ?, 0, 0, 0)
            """,
            [
                (1, 10, 1, "spam", "spam"),
                (1, 11, 2, "spam", "spam"),
            ],
        )
        self.conn.commit()

        cur = self.conn.cursor()
        try:
            target_count = _build_cleanup_targets_table(
                cur,
                "keyword",
                "",
                [],
                "%spam%",
            )
            self.conn.commit()

            def append_log(_job_id: str, message: str) -> None:
                conn = sqlite3.connect(self.db_path, timeout=0.2)
                try:
                    conn.execute("PRAGMA busy_timeout=200")
                    conn.execute(
                        "INSERT INTO side_logs(message) VALUES (?)",
                        (str(message),),
                    )
                    conn.commit()
                finally:
                    conn.close()

            deleted = _execute_cleanup_deletion_batches(
                self.conn,
                cur,
                "job-1",
                target_count,
                append_log,
            )
        finally:
            cur.close()

        self.assertEqual(2, deleted)
        self.assertGreaterEqual(self._side_log_count(), 1)

    def test_cleanup_refreshes_partially_deleted_media_groups(self) -> None:
        self.conn.execute(
            """
            INSERT INTO chats(chat_id, chat_title, message_count)
            VALUES (1, 'Chat 1', 2)
            """
        )
        self.conn.executemany(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type,
                grouped_id, content, content_norm, has_media, is_promo, dedupe_eligible
            )
            VALUES (?, ?, '2026-01-01 00:00:00', ?, 'PHOTO', 77, ?, ?, 1, 0, 0)
            """,
            [
                (1, 20, 1, "keep", "keep"),
                (1, 21, 2, "delete-me", "delete-me"),
            ],
        )
        self.conn.executemany(
            """
            INSERT INTO message_media(
                chat_id, message_id, media_kind, media_fingerprint
            )
            VALUES (?, ?, 'PHOTO', ?)
            """,
            [
                (1, 20, "fp-20"),
                (1, 21, "fp-21"),
            ],
        )
        self.conn.commit()
        refresh_media_groups_for_chat(
            self.conn,
            chat_id=1,
            cfg=SimpleNamespace(
                media_caption_guard_len=58,
                promo_score_threshold=0,
                disable_promo_filter=1,
            ),
            grouped_ids=None,
        )

        cur = self.conn.cursor()
        try:
            target_count = _build_cleanup_targets_table(
                cur,
                "keyword",
                "",
                [],
                _build_cleanup_like_patterns("delete-me"),
            )
            self.conn.commit()
            deleted = _execute_cleanup_deletion_batches(
                self.conn,
                cur,
                "job-1",
                target_count,
                lambda *_args: None,
            )
        finally:
            cur.close()

        self.assertEqual(1, deleted)
        cur = self.conn.cursor()
        try:
            cur.execute(
                """
                SELECT item_count, first_message_id, last_message_id, captions_concat
                FROM media_groups
                WHERE chat_id = 1 AND grouped_id = 77
                """
            )
            row = cur.fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(1, int(row["item_count"]))
            self.assertEqual(20, int(row["first_message_id"]))
            self.assertEqual(20, int(row["last_message_id"]))
            self.assertEqual("keep", row["captions_concat"])
        finally:
            cur.close()


if __name__ == "__main__":
    unittest.main()
