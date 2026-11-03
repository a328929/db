import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tg_harvest.admin_jobs.cleanup import (
    _admin_cleanup_job_runner,
    _build_cleanup_like_patterns,
    _build_cleanup_targets_table,
    _execute_cleanup_deletion_batches,
    _refresh_cleanup_denormalized_state_locked,
)
from tg_harvest.ingest.media_groups import refresh_media_groups_for_chat
from tg_harvest.storage.cleanup import (
    EMPTY_SEARCH_TEXT_SQL,
    load_cleanup_scan_window,
)
from tg_harvest.storage.connection import detect_sqlite_features
from tg_harvest.storage.schema import create_schema


class _ObservingLock:
    def __init__(self) -> None:
        self.acquired = 0
        self.released = 0
        self.held = False

    def acquire(self, timeout=None):
        self.acquired += 1
        self.held = True
        return True

    def release(self):
        self.released += 1
        self.held = False


class CleanupWriteLockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        create_schema(self.conn, detect_sqlite_features(self.conn))
        self.conn.execute(
            "INSERT INTO chats(chat_id, chat_title, message_count) VALUES (1, 'Chat 1', 3)"
        )
        self.conn.executemany(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type,
                content, content_norm, has_media, is_promo, dedupe_eligible
            )
            VALUES (1, ?, '2026-01-01 00:00:00', ?, 'TEXT', 'spam', 'spam', 0, 0, 0)
            """,
            [(10, 1), (11, 2), (12, 3)],
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()

    def test_empty_cleanup_uses_covering_partial_index_and_tracks_text_edits(self) -> None:
        self.conn.executemany(
            """
            INSERT INTO messages(chat_id, message_id, msg_date_text, msg_date_ts,
                                 msg_type, content, content_norm)
            VALUES (1, ?, '', 0, 'TEXT', ?, ?)
            """,
            [(20, None, None), (21, '   ', ''), (22, 'keep', ''), (23, '', 'keep')],
        )
        self.conn.commit()
        for scope_sql, params in [("", []), (" AND m.chat_id = ?", [1])]:
            statements = []
            self.conn.set_trace_callback(statements.append)
            try:
                count = _build_cleanup_targets_table(
                    self.conn.cursor(), "empty_media", scope_sql, params, ""
                )
            finally:
                self.conn.set_trace_callback(None)
            self.assertEqual(count, 2)
            query = next(sql for sql in statements if "SELECT m.chat_id" in sql)
            plan = " ".join(
                str(row[3]) for row in self.conn.execute("EXPLAIN QUERY PLAN " + query)
            )
            if not scope_sql:
                self.assertIn("idx_messages_cleanup_empty", plan)

        self.conn.execute("UPDATE messages SET content = 'now searchable' WHERE message_id = 20")
        self.conn.execute("UPDATE messages SET content = '' WHERE message_id = 22")
        count = _build_cleanup_targets_table(self.conn.cursor(), "empty_media", "", [], "")
        self.assertEqual(count, 2)
        self.assertEqual(
            {row[0] for row in self.conn.execute("SELECT message_id FROM temp_cleanup_targets")},
            {21, 22},
        )
        # The index covers all needed values without reading message bodies.
        instructions = self.conn.execute(
            "EXPLAIN SELECT chat_id, pk, message_id, grouped_id FROM messages "
            f"WHERE {EMPTY_SEARCH_TEXT_SQL}"
        ).fetchall()
        self.assertFalse(any(row[1] == "Column" and row[2] == 0 for row in instructions))

    def test_cleanup_target_removal_uses_integer_primary_key(self) -> None:
        count = _build_cleanup_targets_table(
            self.conn.cursor(), "keyword", "", [], _build_cleanup_like_patterns("spam")
        )
        self.assertEqual(count, 3)
        plan = " ".join(
            str(row[3]) for row in self.conn.execute(
                "EXPLAIN QUERY PLAN DELETE FROM temp_cleanup_targets WHERE pk IN (1, 2)"
            )
        )
        self.assertIn("INTEGER PRIMARY KEY", plan)
        self.assertNotIn("SCAN temp_cleanup_targets", plan)

    def test_cleanup_media_deletion_queues_only_final_delete_operation(self) -> None:
        self.conn.execute(
            "INSERT INTO message_media(chat_id, message_id, media_kind) VALUES (1, 10, 'PHOTO')"
        )
        self.conn.execute("DELETE FROM manticore_search_outbox")
        self.conn.commit()
        cur = self.conn.cursor()
        count = _build_cleanup_targets_table(
            cur, "keyword", "", [], _build_cleanup_like_patterns("spam")
        )
        self.conn.commit()
        self.assertEqual(
            _execute_cleanup_deletion_batches(self.conn, cur, "job", count, lambda *_: None),
            3,
        )
        rows = self.conn.execute(
            "SELECT operation, revision FROM manticore_search_outbox"
        ).fetchall()
        self.assertEqual([tuple(row) for row in rows], [("delete", 1)] * 3)

    def test_cleanup_batches_stay_below_999_variables_and_remove_media(self) -> None:
        self.conn.executemany(
            """
            INSERT INTO messages(chat_id, message_id, msg_date_text, msg_date_ts,
                                 msg_type, content)
            VALUES (1, ?, '', 0, 'PHOTO', 'spam')
            """,
            [(number,) for number in range(100, 1005)],
        )
        self.conn.executemany(
            "INSERT INTO message_media(chat_id, message_id) VALUES (1, ?)",
            [(number,) for number in range(100, 1005)],
        )
        self.conn.commit()
        cur = self.conn.cursor()
        count = _build_cleanup_targets_table(
            cur, "keyword", "", [], _build_cleanup_like_patterns("spam")
        )
        self.conn.commit()
        previous_limit = self.conn.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999)
        try:
            deleted = _execute_cleanup_deletion_batches(
                self.conn, cur, "job", count, lambda *_: None
            )
        finally:
            self.conn.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, previous_limit)
        self.assertEqual(deleted, 908)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM message_media").fetchone()[0], 0)

    def test_cleanup_logs_after_releasing_write_lock(self) -> None:
        lock = _ObservingLock()
        log_lock_states = []
        cur = self.conn.cursor()
        try:
            target_count = _build_cleanup_targets_table(
                cur,
                "keyword",
                "",
                [],
                _build_cleanup_like_patterns("spam"),
            )
            self.conn.commit()

            def append_log(_job_id: str, _message: str) -> None:
                log_lock_states.append(lock.held)

            with patch("tg_harvest.storage.connection.DB_WRITE_LOCK", lock):
                deleted = _execute_cleanup_deletion_batches(
                    self.conn,
                    cur,
                    "job-1",
                    target_count,
                    append_log,
                )
        finally:
            cur.close()

        self.assertEqual(deleted, 3)
        self.assertGreaterEqual(lock.acquired, 2)
        self.assertEqual(lock.acquired, lock.released)
        self.assertTrue(log_lock_states)
        self.assertFalse(any(log_lock_states))

    def test_unsearchable_cleanup_runner_adapts_context_logging(self) -> None:
        self.conn.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type,
                content, content_norm, has_media, is_promo, dedupe_eligible
            )
            VALUES (1, 20, '2026-01-01 00:00:00', 20, 'PHOTO', '', '', 1, 0, 0)
            """
        )
        self.conn.commit()
        messages = []
        context = SimpleNamespace(
            job_id="job-1",
            get_connection=lambda: self.conn,
            log=messages.append,
        )

        result = _admin_cleanup_job_runner.__wrapped__(
            context,
            keyword="",
            scope="all",
            chat_id=None,
            target_label="全部数据",
            cleanup_mode="empty_media",
        )

        self.assertTrue(result.success)
        self.assertEqual(result.message, "清理完成：共删除 1 条数据")
        self.assertIn("进度：已清理 1/1", messages)

    def test_cleanup_deletes_message_media_without_foreign_key_pragmas(self) -> None:
        self.conn.execute("PRAGMA foreign_keys=OFF")
        self.conn.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type,
                content, content_norm, has_media, is_promo, dedupe_eligible
            )
            VALUES (1, 20, '2026-01-01 00:00:00', 20, 'PHOTO', 'delete-media', 'delete-media', 1, 0, 0)
            """
        )
        self.conn.execute(
            """
            INSERT INTO message_media(chat_id, message_id, media_kind)
            VALUES (1, 20, 'PHOTO')
            """
        )
        self.conn.commit()

        cur = self.conn.cursor()
        try:
            target_count = _build_cleanup_targets_table(
                cur,
                "keyword",
                "",
                [],
                _build_cleanup_like_patterns("delete-media"),
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

        self.assertEqual(deleted, 1)
        cur = self.conn.cursor()
        try:
            cur.execute(
                "SELECT COUNT(*) AS c FROM message_media WHERE chat_id = 1 AND message_id = 20"
            )
            self.assertEqual(int(cur.fetchone()["c"]), 0)
        finally:
            cur.close()

    def test_cleanup_deletes_fully_removed_media_group_without_rebuild(self) -> None:
        self.conn.executemany(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type,
                grouped_id, content, content_norm, has_media, is_promo, dedupe_eligible
            )
            VALUES (1, ?, '2026-01-01 00:00:00', ?, 'PHOTO', 77, 'delete-album', 'delete-album', 1, 0, 0)
            """,
            [(20, 20), (21, 21)],
        )
        self.conn.executemany(
            """
            INSERT INTO message_media(chat_id, message_id, media_kind)
            VALUES (1, ?, 'PHOTO')
            """,
            [(20,), (21,)],
        )
        self.conn.execute(
            """
            INSERT INTO media_groups(chat_id, grouped_id, item_count, active_items)
            VALUES (1, 77, 2, 2)
            """
        )
        self.conn.commit()

        cur = self.conn.cursor()
        try:
            target_count = _build_cleanup_targets_table(
                cur,
                "keyword",
                "",
                [],
                _build_cleanup_like_patterns("delete-album"),
            )
            self.conn.commit()
            with patch(
                "tg_harvest.admin_jobs.cleanup._refresh_media_groups_for_cursor"
            ) as refresh_mock:
                deleted = _execute_cleanup_deletion_batches(
                    self.conn,
                    cur,
                    "job-1",
                    target_count,
                    lambda *_args: None,
                )
        finally:
            cur.close()

        self.assertEqual(deleted, 2)
        refresh_mock.assert_not_called()
        cur = self.conn.cursor()
        try:
            cur.execute(
                "SELECT COUNT(*) AS c FROM media_groups WHERE chat_id = 1 AND grouped_id = 77"
            )
            self.assertEqual(int(cur.fetchone()["c"]), 0)
        finally:
            cur.close()

    def test_unsearchable_cleanup_preserves_clone_media_group_anchors(self) -> None:
        self.conn.executemany(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type,
                grouped_id, content, content_norm, has_media, is_promo,
                dedupe_eligible
            )
            VALUES (1, ?, ?, ?, 'PHOTO', 88, '', '', 1, 0, 0)
            """,
            [
                (30, "2026-01-01 00:00:30", 30),
                (31, "2026-01-01 00:00:31", 31),
            ],
        )
        self.conn.execute(
            """
            INSERT INTO media_groups(
                chat_id, grouped_id, first_message_id, first_msg_date_ts,
                last_message_id, last_msg_date_ts, item_count, active_items
            )
            VALUES (1, 88, 30, 30, 31, 31, 2, 2)
            """
        )
        self.conn.commit()

        cur = self.conn.cursor()
        try:
            target_count = _build_cleanup_targets_table(
                cur,
                "empty_media",
                "",
                [],
                "",
            )
            self.conn.commit()
            deleted = _execute_cleanup_deletion_batches(
                self.conn,
                cur,
                "job-1",
                target_count,
                lambda *_args: None,
                cleanup_mode="empty_media",
            )
        finally:
            cur.close()

        self.assertEqual(deleted, 2)
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM messages WHERE grouped_id = 88"
            ).fetchone()[0],
            0,
        )
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM media_groups WHERE grouped_id = 88"
            ).fetchone()[0],
            0,
        )
        anchors = self.conn.execute(
            """
            SELECT grouped_id, message_id, msg_date_ts, cleanup_reason
            FROM clone_media_group_anchors
            WHERE chat_id = 1 AND grouped_id = 88
            ORDER BY message_id
            """
        ).fetchall()
        self.assertEqual(
            [tuple(row) for row in anchors],
            [
                (88, 30, 30, "empty_media"),
                (88, 31, 31, "empty_media"),
            ],
        )

    def test_cleanup_removes_stale_media_group_id_from_media_metadata(self) -> None:
        """A stale message_media grouped_id must not leave an orphan aggregate."""
        self.conn.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type,
                content, content_norm, has_media, is_promo, dedupe_eligible
            )
            VALUES (1, 20, '2026-01-01 00:00:00', 20, 'PHOTO',
                    'orphan-media-group', 'orphan-media-group', 1, 0, 0)
            """
        )
        self.conn.execute(
            """
            INSERT INTO message_media(chat_id, message_id, media_kind, grouped_id)
            VALUES (1, 20, 'PHOTO', 777)
            """
        )
        self.conn.execute(
            """
            INSERT INTO media_groups(chat_id, grouped_id, item_count, active_items)
            VALUES (1, 777, 1, 1)
            """
        )
        self.conn.commit()

        cur = self.conn.cursor()
        try:
            target_count = _build_cleanup_targets_table(
                cur,
                "keyword",
                "",
                [],
                _build_cleanup_like_patterns("orphan-media-group"),
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

        self.assertEqual(deleted, 1)
        self.assertIsNone(
            self.conn.execute(
                "SELECT 1 FROM message_media WHERE chat_id = 1 AND message_id = 20"
            ).fetchone()
        )
        self.assertIsNone(
            self.conn.execute(
                "SELECT 1 FROM media_groups WHERE chat_id = 1 AND grouped_id = 777"
            ).fetchone()
        )

    def test_cleanup_media_group_tail_chunks_large_group_id_sets(self) -> None:
        setlimit = getattr(self.conn, "setlimit", None)
        if setlimit is None or not hasattr(sqlite3, "SQLITE_LIMIT_VARIABLE_NUMBER"):
            self.skipTest("SQLite variable limit API is unavailable")

        grouped_ids = set(range(1, 1001))
        self.conn.executemany(
            "INSERT INTO media_groups(chat_id, grouped_id) VALUES (1, ?)",
            [(grouped_id,) for grouped_id in sorted(grouped_ids)],
        )
        self.conn.commit()
        previous_limit = setlimit(
            sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER,
            999,
        )
        try:
            result = _refresh_cleanup_denormalized_state_locked(
                self.conn,
                {1},
                {1: grouped_ids},
            )
        finally:
            setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, previous_limit)

        self.assertEqual(result, (1000, 1000, 0))
        self.assertEqual(
            self.conn.execute(
                "SELECT COUNT(*) FROM media_groups WHERE chat_id = 1"
            ).fetchone()[0],
            0,
        )

    def test_cleanup_continues_and_repairs_summaries_when_progress_logging_fails(
        self,
    ) -> None:
        cur = self.conn.cursor()
        try:
            target_count = _build_cleanup_targets_table(
                cur,
                "keyword",
                "",
                [],
                _build_cleanup_like_patterns("spam"),
            )
            self.conn.commit()

            def fail_progress_log(_job_id: str, message: str) -> None:
                if message.startswith("进度："):
                    raise RuntimeError("log unavailable")

            with patch(
                "tg_harvest.admin_jobs.cleanup.CLEANUP_DELETE_BATCH_SIZE", 1
            ):
                deleted = _execute_cleanup_deletion_batches(
                    self.conn,
                    cur,
                    "job-1",
                    target_count,
                    fail_progress_log,
                )
        finally:
            cur.close()

        row = self.conn.execute(
            "SELECT message_count FROM chats WHERE chat_id = 1"
        ).fetchone()
        self.assertEqual(int(row["message_count"]), 0)
        remaining = self.conn.execute(
            "SELECT COUNT(*) AS c FROM messages WHERE chat_id = 1"
        ).fetchone()
        self.assertEqual(deleted, 3)
        self.assertEqual(int(remaining["c"]), 0)

    def test_cleanup_repairs_summary_when_media_group_rebuild_fails(self) -> None:
        self.conn.executemany(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type,
                grouped_id, content, content_norm, has_media, is_promo,
                dedupe_eligible
            )
            VALUES (1, ?, '2026-01-01 00:00:00', ?, 'PHOTO', 99, ?, ?, 1, 0, 0)
            """,
            [
                (20, 20, "delete-group", "delete-group"),
                (21, 21, "keep-group", "keep-group"),
            ],
        )
        self.conn.execute(
            "INSERT INTO media_groups(chat_id, grouped_id, item_count, active_items) VALUES (1, 99, 2, 2)"
        )
        self.conn.commit()
        cur = self.conn.cursor()
        try:
            target_count = _build_cleanup_targets_table(
                cur,
                "keyword",
                "",
                [],
                _build_cleanup_like_patterns("delete-group"),
            )
            self.conn.commit()
            with patch(
                "tg_harvest.admin_jobs.cleanup._refresh_media_groups_for_cursor",
                side_effect=RuntimeError("media group trigger failed"),
            ), self.assertRaisesRegex(RuntimeError, "media group trigger failed"):
                _execute_cleanup_deletion_batches(
                    self.conn,
                    cur,
                    "job-1",
                    target_count,
                    lambda *_args: None,
                )
        finally:
            cur.close()

        remaining = self.conn.execute(
            "SELECT COUNT(*) AS c FROM messages WHERE chat_id = 1"
        ).fetchone()
        summary = self.conn.execute(
            "SELECT message_count FROM chats WHERE chat_id = 1"
        ).fetchone()
        self.assertEqual(int(remaining["c"]), 4)
        self.assertEqual(int(summary["message_count"]), 4)

    def test_unchanged_media_group_refresh_does_not_rewrite_aggregate(self) -> None:
        self.conn.executemany(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type,
                grouped_id, content, content_norm, has_media, is_promo,
                dedupe_eligible
            )
            VALUES (1, ?, '2026-01-01 00:00:00', ?, 'PHOTO', 88, ?, ?, 1, 0, 0)
            """,
            [
                (30, 30, "caption one", "caption one"),
                (31, 31, "caption two", "caption two"),
            ],
        )
        self.conn.executemany(
            """
            INSERT INTO message_media(
                chat_id, message_id, media_kind, media_fingerprint
            ) VALUES (1, ?, 'PHOTO', ?)
            """,
            [(30, "fp-30"), (31, "fp-31")],
        )
        self.conn.commit()
        cfg = SimpleNamespace(
            media_caption_guard_len=58,
            promo_score_threshold=0,
            disable_promo_filter=1,
        )
        refresh_media_groups_for_chat(
            self.conn,
            chat_id=1,
            cfg=cfg,
            grouped_ids={88},
        )
        self.conn.execute("CREATE TABLE media_group_audit(action TEXT NOT NULL)")
        self.conn.execute(
            """
            CREATE TRIGGER audit_media_group_update
            AFTER UPDATE ON media_groups
            BEGIN
                INSERT INTO media_group_audit(action) VALUES ('update');
            END
            """
        )
        self.conn.execute(
            """
            CREATE TRIGGER audit_media_group_delete
            AFTER DELETE ON media_groups
            BEGIN
                INSERT INTO media_group_audit(action) VALUES ('delete');
            END
            """
        )
        self.conn.commit()

        refresh_media_groups_for_chat(
            self.conn,
            chat_id=1,
            cfg=cfg,
            grouped_ids={88},
        )

        actions = self.conn.execute("SELECT action FROM media_group_audit").fetchall()
        self.assertEqual(actions, [])


class CleanupIncrementalScanTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db_path = str(Path(self.tmp.name) / "cleanup.db")
        self.conn = sqlite3.connect(self.db_path)
        self.addCleanup(self.conn.close)
        self.conn.execute("PRAGMA journal_mode=WAL")
        create_schema(self.conn, detect_sqlite_features(self.conn))
        self.conn.executemany(
            "INSERT INTO chats(chat_id, chat_title) VALUES (?, ?)",
            [(1, "One"), (2, "Two")],
        )
        self.conn.commit()

    def _insert(self, message_id, content, *, chat_id=1, pk=None, content_norm=""):
        self.conn.execute(
            """
            INSERT INTO messages(pk, chat_id, message_id, msg_date_text, msg_date_ts,
                                 msg_type, content, content_norm)
            VALUES (?, ?, ?, '', 0, 'TEXT', ?, ?)
            """,
            (pk, chat_id, message_id, content, content_norm),
        )
        self.conn.commit()

    def _run(self, keyword="spam", *, chat_id=None):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        statements = []
        conn.set_trace_callback(statements.append)
        result = _admin_cleanup_job_runner.__wrapped__(
            SimpleNamespace(job_id="job", get_connection=lambda: conn, log=lambda *_: None),
            keyword=keyword,
            scope="all" if chat_id is None else "chat",
            chat_id=chat_id,
            target_label="test",
        )
        return result, statements

    def _window(self, keyword="spam", *, chat_id=None):
        return load_cleanup_scan_window(
            self.conn.cursor(),
            "all" if chat_id is None else f"chat:{chat_id}",
            _build_cleanup_like_patterns(keyword),
        )

    def test_repeat_scan_only_reads_new_messages_and_preserves_literal_matching(self):
        self._insert(1, "keep")
        self._insert(2, "SpAm")
        self._insert(3, "raw text", content_norm="normalized spam")
        result, _ = self._run()
        self.assertEqual(result.message, "清理完成：共删除 2 条数据")
        self.assertEqual(self._window().after_pk, 3)
        self._insert(4, "spam new")
        result, statements = self._run()
        self.assertEqual(result.message, "清理完成：共删除 1 条数据")
        query = next(sql for sql in statements if "INSERT INTO temp_cleanup_targets" in sql)
        self.assertIn("m.pk > 3", query)
        # The job's connection owns the temporary table. Explain the same
        # bounded SELECT on this connection to verify a primary-key range scan.
        select_sql = query[query.index("SELECT chat_id"):]
        plan = " ".join(str(row[3]) for row in self.conn.execute("EXPLAIN QUERY PLAN " + select_sql))
        self.assertIn("INTEGER PRIMARY KEY", plan)
        self.assertNotIn("SCAN m", plan)
        self.assertEqual(self.conn.execute("SELECT message_id FROM messages").fetchall(), [(1,)])

    def test_no_match_scan_is_cached_and_scopes_and_keywords_remain_independent(self):
        self._insert(1, "other", chat_id=1)
        self._insert(2, "spam", chat_id=2)
        result, _ = self._run(chat_id=1)
        self.assertEqual(result.message, "未发现符合条件的数据，任务结束")
        self.assertEqual(self._window(chat_id=1).after_pk, 2)
        self.assertIsNone(self._window(chat_id=2).after_pk)
        self.assertIsNone(self._window().after_pk)
        self.assertIsNone(self._window("other", chat_id=1).after_pk)
        result, _ = self._run()
        self.assertEqual(result.message, "清理完成：共删除 1 条数据")

    def test_changed_old_text_invalidates_scan_but_identical_writes_do_not(self):
        self._insert(1, "keep")
        self._run()
        self.conn.execute("UPDATE messages SET content = content, content_norm = content_norm")
        self.conn.commit()
        self.assertEqual(self._window().after_pk, 1)
        self.conn.execute("UPDATE messages SET content_norm = 'spam'")
        self.conn.commit()
        self.assertIsNone(self._window().after_pk)
        result, _ = self._run()
        self.assertEqual(result.message, "清理完成：共删除 1 条数据")

    def test_moving_message_into_previously_scanned_chat_invalidates_scan(self):
        self._insert(1, "spam", chat_id=2)
        self._run(chat_id=1)
        self.conn.execute("UPDATE messages SET chat_id = 1 WHERE message_id = 1")
        self.conn.commit()
        self.assertIsNone(self._window(chat_id=1).after_pk)
        result, _ = self._run(chat_id=1)
        self.assertEqual(result.message, "清理完成：共删除 1 条数据")

    def test_reusing_old_primary_key_invalidates_scan(self):
        self._insert(1, "spam", pk=10)
        self._run()
        self._insert(2, "spam", pk=5)
        self.assertIsNone(self._window().after_pk)
        result, _ = self._run()
        self.assertEqual(result.message, "清理完成：共删除 1 条数据")

    def test_primary_key_edits_update_scan_upper_bound(self):
        self._insert(1, "keep")
        self._run()
        self.conn.execute("UPDATE messages SET pk = 100, content = 'spam'")
        self.conn.commit()
        self.assertEqual(self._window().upper_pk, 100)
        result, _ = self._run()
        self.assertEqual(result.message, "清理完成：共删除 1 条数据")

    def test_edits_during_scan_are_not_hidden_by_saved_watermark(self):
        self._insert(1, "keep")
        original_builder = _build_cleanup_targets_table

        def edit_after_scan(*args, **kwargs):
            count = original_builder(*args, **kwargs)
            self.conn.execute("UPDATE messages SET content = 'spam'")
            self.conn.commit()
            return count

        with patch("tg_harvest.admin_jobs.cleanup._build_cleanup_targets_table", edit_after_scan):
            self._run()
        self.assertIsNone(self._window().after_pk)
        result, _ = self._run()
        self.assertEqual(result.message, "清理完成：共删除 1 条数据")

    def test_messages_appended_during_scan_are_checked_by_next_scan(self):
        self._insert(1, "keep")
        original_builder = _build_cleanup_targets_table

        def append_after_scan(*args, **kwargs):
            count = original_builder(*args, **kwargs)
            self._insert(2, "spam")
            return count

        with patch("tg_harvest.admin_jobs.cleanup._build_cleanup_targets_table", append_after_scan):
            self._run()
        self.assertEqual(self._window().after_pk, 1)
        result, _ = self._run()
        self.assertEqual(result.message, "清理完成：共删除 1 条数据")

    def test_failed_deletion_does_not_advance_scan(self):
        self._insert(1, "keep")
        self._run()
        self._insert(2, "spam")
        with patch(
            "tg_harvest.admin_jobs.cleanup._execute_cleanup_deletion_batches",
            side_effect=RuntimeError("interrupted"),
        ), self.assertRaisesRegex(RuntimeError, "interrupted"):
            self._run()
        self.assertEqual(self._window().after_pk, 1)
        result, _ = self._run()
        self.assertEqual(result.message, "清理完成：共删除 1 条数据")

    def test_literal_percent_underscore_backslash_and_normalized_patterns(self):
        for keyword in ("100%_done", r"a\b", "ＳＰＡＭ"):
            with self.subTest(keyword=keyword):
                self._insert(100, keyword)
                result, _ = self._run(keyword)
                self.assertEqual(result.message, "清理完成：共删除 1 条数据")
                self._insert(101, keyword)
                result, _ = self._run(keyword)
                self.assertEqual(result.message, "清理完成：共删除 1 条数据")

    def test_scan_records_are_bounded_and_survive_schema_initialization(self):
        self._insert(1, "keep")
        for number in range(140):
            self._run(f"missing-{number}")
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM admin_cleanup_scans").fetchone()[0], 128)
        self.assertEqual(self._window("missing-139").after_pk, 1)
        create_schema(self.conn, detect_sqlite_features(self.conn))
        self.assertEqual(self._window("missing-139").after_pk, 1)


if __name__ == "__main__":
    unittest.main()
