import sqlite3
import unittest

from tg_harvest.domain.chat_inventory import (
    ChatInventoryRow,
    RestrictedChatInventoryRow,
)
from tg_harvest.storage.channel_management import (
    list_database_channels,
    list_missing_chat_scan_results,
    list_restricted_chat_scan_results,
    normalize_channel_sort,
    replace_missing_chat_scan_results,
    replace_restricted_chat_scan_results,
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
                unavailable_reason TEXT,
                last_message_at TEXT,
                last_message_ts INTEGER,
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
            CREATE TABLE admin_restricted_chats (
                chat_id INTEGER PRIMARY KEY,
                chat_title TEXT NOT NULL,
                chat_username TEXT,
                chat_type TEXT,
                is_public INTEGER NOT NULL DEFAULT 0,
                restriction_platforms TEXT,
                restriction_reasons TEXT,
                restriction_text TEXT,
                risk_flags TEXT,
                membership_scope TEXT NOT NULL DEFAULT 'joined',
                last_message_at TEXT,
                last_message_ts INTEGER,
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

    def test_list_database_channels_uses_one_latest_message_lookup(self) -> None:
        statements = []
        self.conn.set_trace_callback(
            lambda sql: statements.append(" ".join(str(sql).split()))
        )
        try:
            list_database_channels(self.conn, sort="updated_desc")
        finally:
            self.conn.set_trace_callback(None)

        select_sql = next(sql for sql in statements if "FROM chats c" in sql)
        self.assertEqual(1, select_sql.count("SELECT m.message_id FROM messages m"))
        self.assertNotIn("SELECT m.msg_date_text FROM messages m", select_sql)
        self.assertNotIn("SELECT m.msg_date_ts FROM messages m", select_sql)

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
                    last_message_at="2026-04-01 10:00:00",
                    last_message_ts=1775037600,
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
        self.assertEqual("2026-04-01 10:00:00", rows[0]["last_message_at"])
        self.assertEqual(1775037600, rows[0]["last_message_ts"])
        self.assertEqual("", rows[0]["unavailable_reason"])

    def test_list_missing_chat_scan_results_hides_imported_chat(self) -> None:
        replace_missing_chat_scan_results(
            self.conn,
            [
                ChatInventoryRow(
                    chat_id=1,
                    chat_title="Small",
                    chat_username="small",
                    chat_type="Channel",
                    is_public=1,
                    last_message_at="2026-04-01 10:00:00",
                    last_message_ts=1775037600,
                ),
                ChatInventoryRow(
                    chat_id=9,
                    chat_title="Still Missing",
                    chat_username="missing",
                    chat_type="Channel",
                    is_public=1,
                    last_message_at="2026-04-01 11:00:00",
                    last_message_ts=1775041200,
                ),
            ],
            scan_job_id="job-1",
            scanned_at="2026-04-01T00:00:00+00:00",
        )

        rows = list_missing_chat_scan_results(self.conn)
        self.assertEqual(["Still Missing"], [row["chat_title"] for row in rows])

    def test_list_missing_chat_scan_results_keeps_same_numeric_id_with_other_type(self) -> None:
        replace_missing_chat_scan_results(
            self.conn,
            [
                ChatInventoryRow(
                    chat_id=3,
                    chat_title="Channel With Chat Id Collision",
                    chat_type="Channel",
                    unavailable_reason="Telegram 返回该会话不可访问",
                )
            ],
            scan_job_id="job-1",
            scanned_at="2026-04-01T00:00:00+00:00",
        )

        rows = list_missing_chat_scan_results(self.conn)
        self.assertEqual(1, len(rows))
        self.assertEqual("Channel With Chat Id Collision", rows[0]["chat_title"])
        self.assertEqual("Telegram 返回该会话不可访问", rows[0]["unavailable_reason"])

    def test_replace_and_list_restricted_chat_scan_results(self) -> None:
        count = replace_restricted_chat_scan_results(
            self.conn,
            [
                RestrictedChatInventoryRow(
                    chat_id=8,
                    chat_title="Restricted",
                    chat_username="restricted",
                    chat_type="Channel",
                    is_public=1,
                    restriction_platforms="all",
                    restriction_reasons="porn",
                    restriction_text="This channel can't be displayed.",
                    risk_flags="restricted",
                    last_message_at="2026-04-04 10:00:00",
                    last_message_ts=1775296800,
                )
            ],
            scan_job_id="job-4",
            scanned_at="2026-04-04T00:00:00+00:00",
        )

        rows = list_restricted_chat_scan_results(self.conn)
        self.assertEqual(1, count)
        self.assertEqual(1, len(rows))
        self.assertEqual("Restricted", rows[0]["chat_title"])
        self.assertEqual("restricted", rows[0]["chat_username"])
        self.assertEqual("all", rows[0]["restriction_platforms"])
        self.assertEqual("porn", rows[0]["restriction_reasons"])
        self.assertEqual("This channel can't be displayed.", rows[0]["restriction_text"])
        self.assertEqual("restricted", rows[0]["risk_flags"])
        self.assertEqual("joined", rows[0]["membership_scope"])
        self.assertEqual(0, rows[0]["in_database"])
        self.assertEqual("2026-04-04 10:00:00", rows[0]["last_message_at"])
        self.assertEqual(1775296800, rows[0]["last_message_ts"])

    def test_list_restricted_chat_scan_results_groups_membership_scopes(self) -> None:
        replace_restricted_chat_scan_results(
            self.conn,
            [
                RestrictedChatInventoryRow(
                    chat_id=8,
                    chat_title="Zulu Joined",
                    chat_type="Channel",
                    membership_scope="joined",
                ),
                RestrictedChatInventoryRow(
                    chat_id=9,
                    chat_title="Alpha Public",
                    chat_type="Channel",
                    membership_scope="public_unjoined",
                ),
                RestrictedChatInventoryRow(
                    chat_id=1,
                    chat_title="Alpha Joined",
                    chat_type="Channel",
                    membership_scope="joined",
                ),
            ],
            scan_job_id="job-5",
            scanned_at="2026-04-05T00:00:00+00:00",
        )

        rows = list_restricted_chat_scan_results(self.conn)

        self.assertEqual(
            ["Alpha Joined", "Zulu Joined", "Alpha Public"],
            [row["chat_title"] for row in rows],
        )
        self.assertEqual(
            [1, 0, 0],
            [row["in_database"] for row in rows],
        )
        self.assertEqual(
            [2, 0, 0],
            [row["message_count"] for row in rows],
        )

    def test_list_restricted_chat_scan_results_normalizes_signed_entity_id(self) -> None:
        self.conn.execute(
            """
            INSERT INTO admin_restricted_chats(
                chat_id, chat_title, chat_type, membership_scope, scanned_at
            ) VALUES (-1002, 'Legacy Signed Channel', 'Channel', 'joined', ?)
            """,
            ("2026-04-06T00:00:00+00:00",),
        )
        self.conn.commit()

        rows = list_restricted_chat_scan_results(self.conn)

        row = next(item for item in rows if item["chat_title"] == "Legacy Signed Channel")
        self.assertEqual(2, row["chat_id"])
        self.assertEqual(1, row["in_database"])
        self.assertEqual(20, row["message_count"])
        self.assertEqual("2026-03-10 00:00:00", row["last_message_at"])

    def test_list_restricted_chat_scan_results_maps_to_signed_database_id(self) -> None:
        self.conn.execute(
            """
            INSERT INTO chats(
                chat_id, chat_title, chat_type, message_count, last_seen_at
            ) VALUES (-10044, 'Signed Stored Channel', 'Channel', 7, ?)
            """,
            ("2026-04-07 00:00:00",),
        )
        self.conn.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type
            ) VALUES (-10044, 1, '2026-04-07 01:00:00', 1775523600, 'TEXT')
            """
        )
        self.conn.execute(
            """
            INSERT INTO admin_restricted_chats(
                chat_id, chat_title, chat_type, membership_scope, scanned_at
            ) VALUES (44, 'Positive Scan Id', 'Channel', 'joined', ?)
            """,
            ("2026-04-07T00:00:00+00:00",),
        )
        self.conn.commit()

        row = list_restricted_chat_scan_results(self.conn)[0]

        self.assertEqual(-10044, row["chat_id"])
        self.assertEqual(1, row["in_database"])
        self.assertEqual(7, row["message_count"])
        self.assertEqual("2026-04-07 01:00:00", row["last_message_at"])

    def test_list_restricted_chat_scan_results_prefers_exact_id_and_blocks_ambiguous_alias(
        self,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO chats(
                chat_id, chat_title, chat_type, message_count, last_seen_at
            ) VALUES (-1002, 'Duplicate Signed Channel', 'Channel', 7, ?)
            """,
            ("2026-04-08 00:00:00",),
        )
        self.conn.execute(
            """
            INSERT INTO admin_restricted_chats(
                chat_id, chat_title, chat_type, membership_scope, scanned_at
            ) VALUES (2, 'Exact Positive Scan', 'Channel', 'joined', ?)
            """,
            ("2026-04-08T00:00:00+00:00",),
        )
        self.conn.commit()

        exact = list_restricted_chat_scan_results(self.conn)[0]
        self.assertEqual(2, exact["chat_id"])
        self.assertEqual(20, exact["message_count"])
        self.assertEqual(0, exact["database_match_ambiguous"])

        self.conn.execute("DELETE FROM admin_restricted_chats")
        self.conn.execute(
            """
            INSERT INTO admin_restricted_chats(
                chat_id, chat_title, chat_type, membership_scope, scanned_at
            ) VALUES (-2, 'Ambiguous Alias Scan', 'Channel', 'joined', ?)
            """,
            ("2026-04-08T01:00:00+00:00",),
        )
        self.conn.commit()

        ambiguous = list_restricted_chat_scan_results(self.conn)[0]
        self.assertEqual(-2, ambiguous["chat_id"])
        self.assertEqual(0, ambiguous["in_database"])
        self.assertEqual(1, ambiguous["database_match_ambiguous"])
        self.assertEqual(0, ambiguous["message_count"])


if __name__ == "__main__":
    unittest.main()
