import importlib.util
import pathlib
import sqlite3
import unittest
from types import SimpleNamespace
from unittest.mock import patch

_SCRIPT_PATH = (
    pathlib.Path(__file__).resolve().parent.parent
    / "tools"
    / "resync_chats_with_missing_media.py"
)
_SPEC = importlib.util.spec_from_file_location(
    "resync_chats_with_missing_media", _SCRIPT_PATH
)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)


class MissingMediaResyncTargetTests(unittest.TestCase):
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
                message_count INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE messages (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                msg_type TEXT NOT NULL,
                grouped_id INTEGER,
                has_media INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(chat_id, message_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE message_media (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                media_kind TEXT,
                file_unique_id TEXT,
                mime_type TEXT,
                file_size INTEGER,
                width INTEGER,
                height INTEGER,
                duration_sec INTEGER,
                grouped_id INTEGER,
                media_fingerprint TEXT,
                PRIMARY KEY(chat_id, message_id)
            )
            """
        )
        cur.executemany(
            """
            INSERT INTO chats(chat_id, chat_title, chat_username, message_count)
            VALUES (?, ?, ?, ?)
            """,
            [
                (1, "chat-one", "one", 100),
                (2, "chat-two", "two", 200),
                (3, "chat-three", None, 300),
            ],
        )
        cur.executemany(
            """
            INSERT INTO messages(chat_id, message_id, msg_type, has_media)
            VALUES (?, ?, ?, 1)
            """,
            [
                (1, 10, "VIDEO"),
                (1, 11, "PHOTO"),
                (2, 20, "VIDEO"),
                (2, 21, "VIDEO"),
                (2, 22, "FILE"),
                (3, 30, "POLL"),
            ],
        )
        cur.executemany(
            """
            INSERT INTO message_media(
                chat_id, message_id, media_kind, file_unique_id,
                mime_type, file_size, width, height, duration_sec,
                grouped_id, media_fingerprint
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (1, 10, "VIDEO", "", "video/mp4", 100, 640, 480, 30, None, ""),
                (1, 11, "PHOTO", "u11", "image/jpeg", 100, None, None, None, None, "fp11"),
                (2, 20, "VIDEO", "", "video/mp4", 100, 640, 480, 30, None, ""),
                (2, 21, "VIDEO", "", "video/mp4", 100, 640, 480, 30, None, ""),
                (2, 22, "FILE", "u22", "application/pdf", None, None, None, None, None, "fp22"),
            ],
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()

    def test_load_missing_media_chats_orders_by_missing_count_desc(self) -> None:
        chats = _MODULE.load_missing_media_chats(
            self.conn,
            target_mode=_MODULE.TARGET_MODE_IDENTITY,
        )

        self.assertEqual([2, 1], [chat.chat_id for chat in chats])
        self.assertEqual([2, 1], [chat.missing_count for chat in chats])
        self.assertEqual(["two", "one"], [chat.chat_username for chat in chats])

    def test_full_mode_includes_size_only_gaps(self) -> None:
        chats = _MODULE.load_missing_media_chats(
            self.conn,
            target_mode=_MODULE.TARGET_MODE_FULL,
        )

        self.assertEqual([2, 1], [chat.chat_id for chat in chats])
        self.assertEqual([3, 2], [chat.missing_count for chat in chats])

    def test_create_ranking_index_preserves_target_ordering(self) -> None:
        created = _MODULE.ensure_missing_identity_ranking_index(self.conn)

        self.assertTrue(created)
        cur = self.conn.cursor()
        cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",
            (_MODULE.MISSING_IDENTITY_INDEX_NAME,),
        )
        self.assertIsNotNone(cur.fetchone())

        chats = _MODULE.load_missing_media_chats(
            self.conn,
            target_mode=_MODULE.TARGET_MODE_IDENTITY,
        )
        self.assertEqual([2, 1], [chat.chat_id for chat in chats])


class MissingMediaResyncExecutionTests(unittest.TestCase):
    def test_resync_one_chat_skips_without_deleting_when_entity_unresolvable(self) -> None:
        chat = _MODULE.MissingMediaChat(
            chat_id=1,
            chat_title="chat-one",
            chat_username="one",
            message_count=100,
            missing_count=10,
        )

        with patch.object(
            _MODULE,
            "resolve_chat_entity",
            side_effect=ValueError("could not find the input entity"),
        ), patch.object(_MODULE, "_delete_chat") as delete_mock, patch.object(
            _MODULE, "_admin_process_single_chat_update"
        ) as update_mock:
            result = _MODULE._resync_one_chat(
                object(),
                "job-1",
                chat,
                1,
                1,
                target_mode=_MODULE.TARGET_MODE_IDENTITY,
            )

        self.assertEqual("skipped", result.status)
        delete_mock.assert_not_called()
        update_mock.assert_not_called()

    def test_resync_one_chat_deletes_then_calls_existing_single_chat_update(self) -> None:
        chat = _MODULE.MissingMediaChat(
            chat_id=2,
            chat_title="chat-two",
            chat_username="two",
            message_count=200,
            missing_count=20,
        )
        calls = []

        class _Conn:
            def close(self):
                calls.append("close")

        def fake_delete(chat_id):
            calls.append(("delete", chat_id))
            return 200

        def fake_update(**kwargs):
            calls.append(("update", kwargs["chat_id"], kwargs["chat_username"]))

        with patch.object(
            _MODULE,
            "resolve_chat_entity",
            return_value=SimpleNamespace(id=2, title="chat-two", username="two"),
        ), patch.object(_MODULE, "_delete_chat", side_effect=fake_delete), patch.object(
            _MODULE, "_admin_process_single_chat_update", side_effect=fake_update
        ), patch.object(
            _MODULE, "_open_conn", return_value=_Conn()
        ), patch.object(
            _MODULE, "count_messages", return_value=198
        ), patch.object(
            _MODULE, "count_missing_media_messages", return_value=0
        ):
            result = _MODULE._resync_one_chat(
                object(),
                "job-1",
                chat,
                1,
                1,
                target_mode=_MODULE.TARGET_MODE_IDENTITY,
            )

        self.assertEqual("success", result.status)
        self.assertEqual(200, result.deleted_messages)
        self.assertEqual(198, result.after_messages)
        self.assertEqual(0, result.missing_after)
        self.assertLess(
            calls.index(("delete", 2)),
            calls.index(("update", 2, "two")),
        )


if __name__ == "__main__":
    unittest.main()
