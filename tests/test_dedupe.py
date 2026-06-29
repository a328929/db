import sqlite3
import unittest

from tg_harvest.domain.dedupe import dedupe_promotional_duplicates
from tg_harvest.storage.connection import detect_sqlite_features
from tg_harvest.storage.schema import create_schema


class DedupeBatchDeletionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        create_schema(self.conn, detect_sqlite_features(self.conn))
        self.conn.execute(
            "INSERT INTO chats(chat_id, chat_title) VALUES (1, 'Chat 1')"
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()

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

        self.assertEqual(
            (501, 1, 0, 0, set()),
            (deleted, solo, group_txt, group_med, affected_groups),
        )
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM messages WHERE dedupe_hash = 'hash-all'")
        self.assertEqual(0, int(cur.fetchone()["c"]))
        cur.execute(
            "SELECT COUNT(*) AS c FROM dedupe_actions WHERE dedupe_hash = 'hash-all'"
        )
        self.assertEqual(501, int(cur.fetchone()["c"]))
        cur.execute(
            "SELECT message_count, last_message_created_at FROM chats WHERE chat_id = 1"
        )
        chat_row = cur.fetchone()
        self.assertEqual(0, int(chat_row["message_count"]))
        self.assertEqual("", chat_row["last_message_created_at"])

    def test_dedupe_deletes_message_media_without_foreign_key_pragmas(self) -> None:
        self.conn.execute("PRAGMA foreign_keys=OFF")
        rows = [
            (
                1,
                message_id,
                "2026-01-01 00:00:00",
                message_id,
                "PHOTO",
                "same promo",
                "same promo",
                "hash-media",
                "hash-media",
                1,
                1,
                1,
                10,
            )
            for message_id in (200, 201)
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
        self.conn.executemany(
            """
            INSERT INTO message_media(chat_id, message_id, media_kind)
            VALUES (1, ?, 'PHOTO')
            """,
            [(200,), (201,)],
        )
        self.conn.commit()

        deleted, _solo, _group_txt, _group_med, _affected_groups = (
            dedupe_promotional_duplicates(
                self.conn,
                chat_id=1,
                mode="PURGE_ALL",
                threshold=2,
            )
        )

        self.assertEqual(2, deleted)
        cur = self.conn.cursor()
        cur.execute(
            "SELECT COUNT(*) AS c FROM message_media WHERE chat_id = 1 AND message_id IN (200, 201)"
        )
        self.assertEqual(0, int(cur.fetchone()["c"]))
        cur.execute(
            "SELECT message_count, last_message_created_at FROM chats WHERE chat_id = 1"
        )
        chat_row = cur.fetchone()
        self.assertEqual(0, int(chat_row["message_count"]))
        self.assertEqual("", chat_row["last_message_created_at"])


if __name__ == "__main__":
    unittest.main()
