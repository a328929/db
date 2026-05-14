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
from tg_harvest.admin_jobs import runtime as admin_jobs_runtime
from tg_harvest.admin_jobs.common import admin_error_message
from tg_harvest.admin_jobs.sessions import _disconnect_worker_client
from tg_harvest.admin_jobs.runners import _admin_harvest_job_runner
from tg_harvest.admin_jobs.runners import _admin_process_single_chat_update
from tg_harvest.admin_jobs.runners import _admin_update_all_chats
from tg_harvest.admin_jobs.runners import _delete_chat_data
from tg_harvest.admin_jobs.streaming import stream_entity_harvest_to_writer
from tg_harvest.admin_jobs.cleanup import _build_cleanup_targets_table
from tg_harvest.storage.connection import detect_sqlite_features
from tg_harvest.storage.schema import create_schema
from tg_harvest.storage.schema import refresh_chat_message_counts
from tg_harvest.domain.dedupe import dedupe_promotional_duplicates
from tg_harvest.ingest.runner import _format_harvest_progress_message
from tg_harvest.ingest.runner import _read_target_message_total
from tg_harvest.ingest.media_groups import refresh_media_groups_for_chat
from tg_harvest.ingest.store import backfill_message_search_text_from_filenames
from tg_harvest.ingest.store import batch_upsert
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
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
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
            "tg_harvest.admin_jobs.runners.ChatUpdateWriteCoordinator",
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
            "tg_harvest.ingest.runner._harvest_messages_for_entity",
            side_effect=fake_harvest,
        ), patch(
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

        self.assertEqual(1, len(_CoordinatorStub.instances))
        coordinator = _CoordinatorStub.instances[0]
        self.assertEqual([1], coordinator.registered)
        self.assertEqual(1, len(coordinator.batches))
        self.assertEqual([1], coordinator.waited)
        self.assertTrue(coordinator.closed)
        self.assertEqual(None, coordinator.chat_starts[0]["chat_username"])
        self.assertTrue(coordinator.finalized[0]["skip_postprocess_if_unchanged"])
        self.assertFalse(coordinator.finalized[0]["enable_dedupe"])
        self.assertTrue(any("边抓取边写入" in line for line in append_log))
        self.assertEqual(2, progress_mock.call_count)

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
        ):
            with self.assertRaisesRegex(RuntimeError, "network break"):
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

    def test_backfill_message_search_text_from_filenames_advances_by_pk(self) -> None:
        cur = self.conn.cursor()
        cur.executemany(
            """
            INSERT INTO messages(chat_id, message_id, content, content_norm, has_media)
            VALUES (1, ?, '', '', 1)
            """,
            [(101,), (102,), (103,)],
        )
        cur.executemany(
            """
            INSERT INTO message_media(chat_id, message_id, file_name, file_unique_id, media_fingerprint)
            VALUES (1, ?, ?, ?, ?)
            """,
            [
                (101, "a.jpg", "u101", "fp101"),
                (102, "", "u102", "fp102"),
                (103, "c.jpg", "u103", "fp103"),
            ],
        )
        self.conn.commit()

        updated = backfill_message_search_text_from_filenames(
            self.conn,
            chat_id=1,
            batch_size=1,
        )

        self.assertEqual(2, updated)
        cur.execute(
            "SELECT message_id, content FROM messages ORDER BY message_id"
        )
        self.assertEqual(
            [(101, "a.jpg"), (102, ""), (103, "c.jpg")],
            [(int(row["message_id"]), row["content"]) for row in cur.fetchall()],
        )

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

    def test_unsearchable_cleanup_keeps_media_after_filename_backfill(self) -> None:
        cur = self.conn.cursor()
        cur.executemany(
            """
            INSERT INTO messages(chat_id, message_id, grouped_id, content, content_norm, has_media)
            VALUES (?, ?, ?, ?, ?, 1)
            """,
            [
                (1, 235, 920, "", ""),
                (1, 236, 920, "", ""),
            ],
        )
        cur.executemany(
            """
            INSERT INTO message_media(chat_id, message_id, file_name, file_unique_id, media_fingerprint)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (1, 235, "kept_by_filename.jpg", "u-235", "fp-235"),
                (1, 236, "", "u-236", "fp-236"),
            ],
        )
        self.conn.commit()

        backfilled = backfill_message_search_text_from_filenames(self.conn, chat_id=1)
        target_count = _build_cleanup_targets_table(
            cur,
            "empty_media",
            "",
            [],
            "",
        )

        self.assertEqual(1, backfilled)
        self.assertEqual(1, target_count)
        cur.execute("SELECT message_id FROM temp_cleanup_targets")
        self.assertEqual([236], [int(row["message_id"]) for row in cur.fetchall()])

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
        admin_jobs_runtime.configure_admin_job_runtime("old-runtime")
        job = _admin_job_create("update", target_chat_id=654, target_label="chat-654")
        job_id = str(job["job_id"])
        _admin_job_set_status(job_id, "running")

        admin_jobs_runtime.configure_admin_job_runtime("new-runtime")
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

    def test_job_trim_handles_legacy_naive_sqlite_timestamps(self) -> None:
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute(
                """
                INSERT INTO admin_jobs(
                    job_id, job_type, status, target_chat_id, target_label,
                    created_at, updated_at, heartbeat_at,
                    progress_current, progress_total, progress_stage, last_logged_current
                )
                VALUES (
                    'legacy-naive', 'cleanup', 'done', NULL, 'legacy',
                    '2000-01-01 00:00:00', '2000-01-01 00:00:00', '2000-01-01 00:00:00',
                    0, NULL, 'done', 0
                )
                """
            )
            conn.commit()
        finally:
            conn.close()

        created = _admin_job_create("cleanup", target_chat_id=None, target_label="new")

        self.assertTrue(created["job_id"])
        conn = sqlite3.connect(self.db_path)
        try:
            row = conn.execute(
                "SELECT 1 FROM admin_jobs WHERE job_id = 'legacy-naive'"
            ).fetchone()
        finally:
            conn.close()
        self.assertIsNone(row)


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
