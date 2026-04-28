import asyncio
import importlib.util
import logging
import os
import pathlib
import sqlite3
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import patch

from tg_harvest.admin_jobs.core import _admin_job_append_log
from tg_harvest.admin_jobs.core import _admin_job_create
from tg_harvest.admin_jobs.core import _admin_job_get_logs
from tg_harvest.admin_jobs.core import _admin_job_get_snapshot
from tg_harvest.admin_jobs.core import _admin_job_set_status
from tg_harvest.admin_jobs.core import _admin_try_create_exclusive_job
from tg_harvest.admin_jobs.core import _admin_recover_interrupted_jobs
from tg_harvest.admin_jobs.core import _AdminJobThreadLogHandler
from tg_harvest.admin_jobs.core import job_context
from tg_harvest.admin_jobs.core import job_log_passthrough_enabled
from tg_harvest.admin_jobs import core as admin_jobs_core
from tg_harvest.admin_jobs.runners import _admin_error_message
from tg_harvest.admin_jobs.runners import _admin_process_single_chat_update
from tg_harvest.admin_jobs.runners import _admin_update_all_chats
from tg_harvest.admin_jobs.runners import _build_cleanup_targets_table
from tg_harvest.storage.schema import create_schema
from tg_harvest.storage.schema import detect_sqlite_features
from tg_harvest.storage.schema import refresh_chat_message_counts
from tg_harvest.domain.dedupe import dedupe_promotional_duplicates
from tg_harvest.ingest.runner import _format_harvest_progress_message
from tg_harvest.ingest.runner import _read_target_message_total
from tg_harvest.ingest.store import refresh_media_groups_for_chat
from tg_harvest.ingest.store import backfill_message_search_text_from_filenames
from tg_harvest.admin_jobs.cleanup import _build_cleanup_like_patterns
from tg_harvest.admin_jobs.cleanup import _execute_cleanup_deletion_batches


_BACKFILL_MODULE_PATH = (
    pathlib.Path(__file__).resolve().parent.parent
    / "tools"
    / "backfill_message_media_from_telegram.py"
)
_BACKFILL_SPEC = importlib.util.spec_from_file_location(
    "backfill_message_media_from_telegram", _BACKFILL_MODULE_PATH
)
assert _BACKFILL_SPEC is not None and _BACKFILL_SPEC.loader is not None
_BACKFILL_MODULE = importlib.util.module_from_spec(_BACKFILL_SPEC)
_BACKFILL_SPEC.loader.exec_module(_BACKFILL_MODULE)
_fetch_chunk_messages = _BACKFILL_MODULE._fetch_chunk_messages


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
            "tg_harvest.admin_jobs.runners._ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.ingest.runner._harvest_messages_for_entity",
            side_effect=fake_harvest,
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            side_effect=lambda *args, **kwargs: progress_calls.append((args, kwargs)) or True,
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
        self.assertTrue(any("失败 1 个" in line for line in logs))
        self.assertTrue(any(kwargs.get("total") == 2 for _, kwargs in progress_calls))
        self.assertTrue(any(kwargs.get("stage") == "updating" for _, kwargs in progress_calls))

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
            "tg_harvest.admin_jobs.runners._ChatUpdateWriteCoordinator",
            _CoordinatorStub,
        ), patch(
            "tg_harvest.ingest.runner._harvest_messages_for_entity",
            return_value=(counters, set(), False),
        ), patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
            return_value=True,
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

    def test_single_chat_update_uses_lightweight_incremental_postprocess(self) -> None:
        append_log = []

        def _append(_job_id, message):
            append_log.append(str(message))

        with patch(
            "tg_harvest.admin_jobs.runners._resolve_chat_entity",
            return_value=SimpleNamespace(title="chat-1"),
        ), patch(
            "tg_harvest.ingest.runner._process_entity",
            return_value=None,
        ) as process_mock, patch(
            "tg_harvest.admin_jobs.runners._admin_job_update_progress",
        ) as progress_mock:
            _admin_process_single_chat_update(
                job_id="job-1",
                client=object(),
                get_conn_fn=lambda: _FakeConn([]),
                admin_job_append_log_fn=_append,
                chat_id=1,
                chat_title="chat-1",
                chat_username="chat_name",
                idx=1,
                total=1,
            )

        process_mock.assert_called_once()
        _, kwargs = process_mock.call_args
        self.assertTrue(kwargs["skip_postprocess_if_unchanged"])
        self.assertFalse(kwargs["enable_dedupe"])
        self.assertEqual(2, progress_mock.call_count)


class _CountResult:
    def __init__(self, total):
        self.total = total


class _CountClient:
    def __init__(self, result=None, exc=None):
        self._result = result
        self._exc = exc
        self.calls = []

    def get_messages(self, entity, **kwargs):
        self.calls.append(kwargs)
        if self._exc is not None:
            raise self._exc
        return self._result


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

    def test_read_target_message_total_full_sync(self) -> None:
        client = _CountClient(result=_CountResult(25000))

        total = _read_target_message_total(
            client,
            object(),
            first_sync=True,
            scan_from_id=0,
        )

        self.assertEqual(25000, total)
        self.assertEqual([{"limit": 0}], client.calls)

    def test_read_target_message_total_incremental_uses_min_id(self) -> None:
        client = _CountClient(result=_CountResult(123))

        total = _read_target_message_total(
            client,
            object(),
            first_sync=False,
            scan_from_id=456,
        )

        self.assertEqual(123, total)
        self.assertEqual([{"limit": 0, "min_id": 456}], client.calls)

    def test_read_target_message_total_degrades_to_none_on_error(self) -> None:
        client = _CountClient(exc=RuntimeError("boom"))

        with self.assertLogs(level="WARNING") as captured:
            total = _read_target_message_total(
                client,
                object(),
                first_sync=True,
                scan_from_id=0,
            )

        self.assertIsNone(total)
        self.assertTrue(any("读取目标总消息数失败" in line for line in captured.output))


class _RetryClient:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0

    async def get_messages(self, entity, ids):
        self.calls += 1
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class BackfillRetryTests(unittest.TestCase):
    def test_fetch_chunk_messages_retries_then_succeeds(self) -> None:
        client = _RetryClient([RuntimeError("temporary"), ["ok"]])

        with patch.object(_BACKFILL_MODULE.asyncio, "sleep", new=AsyncMock()):
            result = asyncio.run(
                _fetch_chunk_messages(
                    client,
                    object(),
                    [101, 100],
                    chat_id=42,
                    max_retries=3,
                )
            )

        self.assertEqual(["ok"], result)
        self.assertEqual(2, client.calls)

    def test_fetch_chunk_messages_raises_after_retry_exhausted(self) -> None:
        client = _RetryClient([RuntimeError("a"), RuntimeError("b")])

        with patch.object(_BACKFILL_MODULE.asyncio, "sleep", new=AsyncMock()):
            with self.assertRaises(RuntimeError):
                asyncio.run(
                    _fetch_chunk_messages(
                        client,
                        object(),
                        [11, 10],
                        chat_id=7,
                        max_retries=2,
                    )
                )

        self.assertEqual(2, client.calls)


class SearchableFilenameBackfillTests(unittest.TestCase):
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
                content TEXT,
                content_norm TEXT,
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

    def test_backfill_message_search_text_from_filenames_populates_search_text(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO messages(chat_id, message_id, content, content_norm, has_media)
            VALUES (1, 100, '', '', 1)
            """
        )
        cur.execute(
            """
            INSERT INTO message_media(chat_id, message_id, file_name, file_unique_id, media_fingerprint)
            VALUES (1, 100, 'movie.mp4', 'u1', 'fp1')
            """
        )
        self.conn.commit()

        updated = backfill_message_search_text_from_filenames(self.conn, chat_id=1)
        self.assertEqual(1, updated)

        cur.execute(
            "SELECT content, content_norm FROM messages WHERE chat_id = 1 AND message_id = 100"
        )
        row = cur.fetchone()
        self.assertEqual("movie.mp4", row["content"])
        self.assertEqual("movie.mp4", row["content_norm"])

    def test_empty_media_cleanup_targets_messages_without_searchable_text(self) -> None:
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
            "job-clean",
            "empty_media",
            "",
            [],
            "",
            lambda *_args: None,
        )

        self.assertEqual(1, target_count)

    def test_cleanup_targets_table_supports_runner_call_signature(self) -> None:
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
            VALUES (1, 201, '', 'u-direct', 'fp-direct')
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

    def test_empty_media_cleanup_targets_blank_album_members_even_with_caption(self) -> None:
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


class AdminRunnerHelperTests(unittest.TestCase):
    def test_admin_error_message_maps_known_telegram_errors(self) -> None:
        self.assertEqual(
            "触发 Telegram 频控限制，请稍后再试",
            _admin_error_message(RuntimeError("FloodWaitError: retry later")),
        )

    def test_admin_error_message_keeps_unknown_error_context(self) -> None:
        self.assertEqual(
            "RuntimeError: boom",
            _admin_error_message(RuntimeError("boom")),
        )


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
        admin_jobs_core.configure_admin_job_runtime("test-runtime")

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

    def test_recover_interrupted_jobs_marks_previous_runtime_job_immediately(self) -> None:
        admin_jobs_core.configure_admin_job_runtime("old-runtime")
        job = _admin_job_create("update", target_chat_id=654, target_label="chat-654")
        job_id = str(job["job_id"])
        _admin_job_set_status(job_id, "running")

        admin_jobs_core.configure_admin_job_runtime("new-runtime")
        admin_jobs_core.ADMIN_JOBS.clear()

        recovered = _admin_recover_interrupted_jobs()
        snapshot = _admin_job_get_snapshot(job_id)

        self.assertEqual(1, recovered)
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual("error", snapshot["status"])

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

        with patch("tg_harvest.storage.schema.DB_WRITE_LOCK", lock):
            refresh_chat_message_counts(self.conn, [1])

        self.assertEqual(1, lock.acquired)
        self.assertEqual(1, lock.released)

        cur = self.conn.cursor()
        cur.execute("SELECT message_count FROM chats WHERE chat_id = 1")
        self.assertEqual(1, int(cur.fetchone()["message_count"]))

    def test_dedupe_promotional_duplicates_uses_global_write_lock(self) -> None:
        lock = _RecordingLock()

        with patch("tg_harvest.storage.schema.DB_WRITE_LOCK", lock):
            deleted, solo, group_txt, group_med, affected_groups = (
                dedupe_promotional_duplicates(self.conn, chat_id=1)
            )

        self.assertEqual((0, 0, 0, 0, set()), (deleted, solo, group_txt, group_med, affected_groups))
        self.assertEqual(1, lock.acquired)
        self.assertEqual(1, lock.released)


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
