import sqlite3
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tg_harvest.admin_jobs.cleanup import (
    _build_cleanup_like_patterns,
    _build_cleanup_targets_table,
    _execute_cleanup_deletion_batches,
    _refresh_cleanup_denormalized_state_locked,
)
from tg_harvest.ingest.media_groups import refresh_media_groups_for_chat
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

        self.assertEqual(3, deleted)
        self.assertGreaterEqual(lock.acquired, 2)
        self.assertEqual(lock.acquired, lock.released)
        self.assertTrue(log_lock_states)
        self.assertFalse(any(log_lock_states))

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

        self.assertEqual(1, deleted)
        cur = self.conn.cursor()
        try:
            cur.execute(
                "SELECT COUNT(*) AS c FROM message_media WHERE chat_id = 1 AND message_id = 20"
            )
            self.assertEqual(0, int(cur.fetchone()["c"]))
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

        self.assertEqual(2, deleted)
        refresh_mock.assert_not_called()
        cur = self.conn.cursor()
        try:
            cur.execute(
                "SELECT COUNT(*) AS c FROM media_groups WHERE chat_id = 1 AND grouped_id = 77"
            )
            self.assertEqual(0, int(cur.fetchone()["c"]))
        finally:
            cur.close()

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

        self.assertEqual(1, deleted)
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

        self.assertEqual((1000, 1000, 0), result)
        self.assertEqual(
            0,
            self.conn.execute(
                "SELECT COUNT(*) FROM media_groups WHERE chat_id = 1"
            ).fetchone()[0],
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
        self.assertEqual(0, int(row["message_count"]))
        remaining = self.conn.execute(
            "SELECT COUNT(*) AS c FROM messages WHERE chat_id = 1"
        ).fetchone()
        self.assertEqual(3, deleted)
        self.assertEqual(0, int(remaining["c"]))

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
        self.assertEqual(4, int(remaining["c"]))
        self.assertEqual(4, int(summary["message_count"]))

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
        self.assertEqual([], actions)


if __name__ == "__main__":
    unittest.main()
