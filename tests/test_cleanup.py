import sqlite3
import unittest
from unittest.mock import patch

from tg_harvest.admin_jobs.cleanup import (
    _build_cleanup_like_patterns,
    _build_cleanup_targets_table,
    _execute_cleanup_deletion_batches,
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
                "tg_harvest.admin_jobs.cleanup.refresh_media_groups_for_chat"
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


if __name__ == "__main__":
    unittest.main()
