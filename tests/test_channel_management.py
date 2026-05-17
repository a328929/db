import sqlite3
import unittest

from tg_harvest.domain.chat_inventory import ChatInventoryRow
from tg_harvest.storage.channel_management import (
    list_absent_chat_scan_results,
    list_database_channels,
    list_missing_chat_scan_results,
    normalize_channel_sort,
    replace_absent_chat_scan_results,
    replace_missing_chat_scan_results,
)


class ChannelManagementStorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE chats (
                chat_id INTEGER PRIMARY KEY,
                chat_title TEXT NOT NULL,
                chat_username TEXT,
                chat_type TEXT,
                message_count INTEGER NOT NULL DEFAULT 0,
                last_seen_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE admin_missing_chats (
                chat_id INTEGER PRIMARY KEY,
                chat_title TEXT NOT NULL,
                chat_username TEXT,
                chat_type TEXT,
                is_public INTEGER NOT NULL DEFAULT 0,
                scan_job_id TEXT,
                scanned_at TEXT NOT NULL
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
                msg_type TEXT NOT NULL DEFAULT 'TEXT',
                PRIMARY KEY(chat_id, message_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE admin_absent_chats (
                chat_id INTEGER PRIMARY KEY,
                chat_title TEXT NOT NULL,
                chat_username TEXT,
                chat_type TEXT,
                message_count INTEGER NOT NULL DEFAULT 0,
                last_seen_at TEXT,
                scan_reason TEXT,
                scan_job_id TEXT,
                scanned_at TEXT NOT NULL
            )
            """
        )
        cur.executemany(
            """
            INSERT INTO chats(
                chat_id,
                chat_title,
                chat_username,
                chat_type,
                message_count,
                last_seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (1, "Small", None, "Channel", 2, "2026-03-01 00:00:00"),
                (2, "Large", "large", "Channel", 20, "2026-01-01 00:00:00"),
                (3, "Fresh", None, "Chat", 8, "2026-02-01 00:00:00"),
            ],
        )
        cur.executemany(
            """
            INSERT INTO messages(
                chat_id,
                message_id,
                msg_date_text,
                msg_date_ts,
                msg_type
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (1, 101, "2026-01-10 00:00:00", 1768003200, "TEXT"),
                (1, 102, "2026-01-15 00:00:00", 1768435200, "TEXT"),
                (2, 201, "2026-03-10 00:00:00", 1773100800, "TEXT"),
                (3, 301, "2026-02-10 00:00:00", 1770681600, "TEXT"),
            ],
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()

    def test_normalize_channel_sort_falls_back_to_default(self) -> None:
        self.assertEqual("message_count_desc", normalize_channel_sort("bad"))

    def test_list_database_channels_sorts_by_message_count(self) -> None:
        channels = list_database_channels(self.conn, sort="message_count_asc")

        self.assertEqual(["Small", "Fresh", "Large"], [c["chat_title"] for c in channels])

    def test_list_database_channels_sorts_by_last_message_time(self) -> None:
        channels = list_database_channels(self.conn, sort="updated_desc")

        self.assertEqual(["Large", "Fresh", "Small"], [c["chat_title"] for c in channels])
        self.assertEqual("2026-03-10 00:00:00", channels[0]["last_message_at"])
        self.assertEqual(1773100800, channels[0]["last_message_ts"])

        channels = list_database_channels(self.conn, sort="updated_asc")
        self.assertEqual(["Small", "Fresh", "Large"], [c["chat_title"] for c in channels])

    def test_replace_and_list_missing_chat_scan_results(self) -> None:
        count = replace_missing_chat_scan_results(
            self.conn,
            [
                ChatInventoryRow(
                    chat_id=9,
                    chat_title="Missing",
                    chat_username="missing",
                    chat_type="Channel",
                    is_public=1,
                )
            ],
            scan_job_id="job-1",
            scanned_at="2026-04-01T00:00:00+00:00",
        )

        rows = list_missing_chat_scan_results(self.conn)
        self.assertEqual(1, count)
        self.assertEqual(1, len(rows))
        self.assertEqual("Missing", rows[0]["chat_title"])
        self.assertEqual("missing", rows[0]["chat_username"])
        self.assertEqual(1, rows[0]["is_public"])

    def test_replace_and_list_absent_chat_scan_results(self) -> None:
        count = replace_absent_chat_scan_results(
            self.conn,
            [
                {
                    "chat_id": 1,
                    "chat_title": "Small",
                    "chat_username": "",
                    "chat_type": "Channel",
                    "message_count": 2,
                    "last_seen_at": "2026-01-01 00:00:00",
                },
                {
                    "chat_id": 2,
                    "chat_title": "Large",
                    "chat_username": "large",
                    "chat_type": "Channel",
                    "message_count": 20,
                    "last_seen_at": "2026-02-01 00:00:00",
                    "scan_reason": "Telegram 限制显示：违规不可用",
                },
            ],
            scan_job_id="job-2",
            scanned_at="2026-04-02T00:00:00+00:00",
        )

        rows = list_absent_chat_scan_results(self.conn)
        self.assertEqual(2, count)
        self.assertEqual(["Large", "Small"], [row["chat_title"] for row in rows])
        self.assertEqual("large", rows[0]["chat_username"])
        self.assertEqual(20, rows[0]["message_count"])
        self.assertEqual("Telegram 限制显示：违规不可用", rows[0]["scan_reason"])
        self.assertEqual("账号未加入", rows[1]["scan_reason"])

    def test_list_absent_chat_scan_results_hides_deleted_chat(self) -> None:
        replace_absent_chat_scan_results(
            self.conn,
            [
                {
                    "chat_id": 999,
                    "chat_title": "Deleted",
                    "message_count": 1,
                    "last_seen_at": "2026-01-01 00:00:00",
                }
            ],
            scan_job_id="job-3",
            scanned_at="2026-04-03T00:00:00+00:00",
        )

        rows = list_absent_chat_scan_results(self.conn)
        self.assertEqual([], rows)


if __name__ == "__main__":
    unittest.main()
