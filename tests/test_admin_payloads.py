import sqlite3
import unittest

from tg_harvest.app.admin_payloads import build_admin_stats_payload


class AdminPayloadPerformanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(
            """
            CREATE TABLE chats(
                chat_id INTEGER PRIMARY KEY,
                chat_title TEXT NOT NULL,
                message_count INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        self.conn.executemany(
            "INSERT INTO chats(chat_id, chat_title, message_count) VALUES (?, ?, ?)",
            [(1, "A", 3), (2, "B", 5)],
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()

    def test_all_scope_stats_use_single_aggregate_query(self) -> None:
        statements = []
        self.conn.set_trace_callback(
            lambda sql: statements.append(" ".join(str(sql).split()))
        )
        try:
            payload, status_code = build_admin_stats_payload(self.conn, None)
        finally:
            self.conn.set_trace_callback(None)

        select_statements = [
            sql for sql in statements if sql.upper().startswith("SELECT")
        ]
        self.assertEqual(200, status_code)
        self.assertEqual(2, payload["chat_count"])
        self.assertEqual(8, payload["message_count"])
        self.assertEqual(1, len(select_statements))
        self.assertIn("COUNT(*) AS chat_count", select_statements[0])
        self.assertIn("SUM(message_count)", select_statements[0])


if __name__ == "__main__":
    unittest.main()
