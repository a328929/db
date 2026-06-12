import importlib.util
import pathlib
import sqlite3
import sys
import unittest

_SCRIPT_PATH = (
    pathlib.Path(__file__).resolve().parent.parent
    / "tools"
    / "recover_missing_media.py"
)
_SPEC = importlib.util.spec_from_file_location("recover_missing_media", _SCRIPT_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)


class RecoverMissingMediaTargetTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE chats (
                chat_id INTEGER PRIMARY KEY,
                chat_title TEXT NOT NULL,
                chat_username TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE messages (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                PRIMARY KEY(chat_id, message_id)
            )
            """
        )
        cur.executemany(
            "INSERT INTO chats(chat_id, chat_title, chat_username) VALUES (?, ?, ?)",
            [
                (1, "chat-one", "one"),
                (2, "chat-two", None),
                (3, "chat-three", "three"),
            ],
        )
        cur.executemany(
            "INSERT INTO messages(chat_id, message_id) VALUES (?, ?)",
            [
                (1, 1),
                (1, 2),
                (1, 5),
                (2, 2),
                (2, 5),
                (2, 6),
                (3, 1),
                (3, 2),
                (3, 3),
            ],
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()

    def test_load_chat_gap_summaries_orders_by_gap_size(self) -> None:
        summaries = _MODULE.load_chat_gap_summaries(self.conn)

        self.assertEqual([2, 1], [summary.chat_id for summary in summaries])
        self.assertEqual([3, 2], [summary.missing_count for summary in summaries])
        self.assertEqual(["chat-two", "chat-one"], [s.chat_title for s in summaries])
        self.assertEqual([None, "one"], [s.chat_username for s in summaries])

    def test_load_chat_gap_summaries_filters_by_chat_and_min_missing(self) -> None:
        summaries = _MODULE.load_chat_gap_summaries(
            self.conn,
            chat_id=1,
            min_missing=2,
        )

        self.assertEqual(1, len(summaries))
        self.assertEqual(1, summaries[0].chat_id)
        self.assertEqual(2, summaries[0].missing_count)

    def test_iter_missing_message_id_chunks_streams_descending_gaps(self) -> None:
        chunks = list(
            _MODULE.iter_missing_message_id_chunks(
                self.conn,
                chat_id=2,
                last_id=6,
                chunk_size=2,
            )
        )

        self.assertEqual([[4, 3], [1]], chunks)

    def test_iter_missing_message_id_chunks_respects_limit(self) -> None:
        chunks = list(
            _MODULE.iter_missing_message_id_chunks(
                self.conn,
                chat_id=2,
                last_id=6,
                chunk_size=10,
                limit=2,
            )
        )

        self.assertEqual([[4, 3]], chunks)


if __name__ == "__main__":
    unittest.main()
