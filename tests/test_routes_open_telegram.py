import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from flask import Flask

from tg_harvest.web.routes.open_telegram import register_open_telegram_routes


class OpenTelegramRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite3")
        os.close(fd)
        Path(db_path).unlink(missing_ok=True)
        self.db_path = db_path
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE chats (
                chat_id INTEGER PRIMARY KEY,
                chat_title TEXT NOT NULL,
                chat_username TEXT,
                chat_type TEXT
            )
            """
        )
        cur.executemany(
            "INSERT INTO chats(chat_id, chat_title, chat_username, chat_type) VALUES (?, ?, ?, ?)",
            [
                (1, "Public Chat", "public_chat", "Channel"),
                (2, "Private Chat", "", "Channel"),
                (-1002202633364, "Signed Private Chat", "", "Channel"),
                (3, "Basic Group", "", "Chat"),
            ],
        )
        cur.execute(
            """
            CREATE TABLE messages (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                grouped_id INTEGER,
                PRIMARY KEY(chat_id, message_id)
            )
            """
        )
        cur.executemany(
            "INSERT INTO messages(chat_id, message_id, grouped_id) VALUES (?, ?, ?)",
            [
                (1, 123, None),
                (1, 124, 9001),
                (2, 456, None),
                (-1002202633364, 456, None),
                (3, 789, None),
            ],
        )
        conn.commit()
        conn.close()

        self.app = Flask(__name__, template_folder="/root/db/templates")
        register_open_telegram_routes(
            self.app,
            logger=_LoggerStub(),
            get_conn_fn=self._connect,
        )
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        Path(self.db_path).unlink(missing_ok=True)

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def test_open_telegram_route_renders_public_app_link(self) -> None:
        response = self.client.get("/open/telegram?chat_id=1&message_id=123")

        self.assertEqual(200, response.status_code)
        body = response.get_data(as_text=True)
        self.assertIn("tg://resolve?domain=public_chat&amp;post=123", body)
        self.assertIn("tg://privatepost?channel=1&amp;post=123", body)
        self.assertIn("https://t.me/public_chat/123", body)
        self.assertIn("备用 App 链接", body)

    def test_open_telegram_route_marks_grouped_media_links_as_single(self) -> None:
        response = self.client.get("/open/telegram?chat_id=1&message_id=124")

        self.assertEqual(200, response.status_code)
        body = response.get_data(as_text=True)
        self.assertIn("tg://resolve?domain=public_chat&amp;post=124&amp;single", body)
        self.assertIn("tg://privatepost?channel=1&amp;post=124&amp;single", body)
        self.assertIn("https://t.me/public_chat/124?single", body)

    def test_open_telegram_route_renders_basic_chat_openmessage_link(self) -> None:
        response = self.client.get("/open/telegram?chat_id=3&message_id=789")

        self.assertEqual(200, response.status_code)
        body = response.get_data(as_text=True)
        self.assertIn("Basic Group", body)
        self.assertIn("tg://openmessage?chat_id=3&amp;message_id=789", body)
        self.assertNotIn("tg://privatepost", body)
        self.assertNotIn("https://t.me/c/3/789", body)
        self.assertNotIn("备用网页链接", body)

    def test_open_telegram_route_renders_private_app_link(self) -> None:
        response = self.client.get("/open/telegram?chat_id=2&message_id=456")

        self.assertEqual(200, response.status_code)
        body = response.get_data(as_text=True)
        self.assertIn("tg://privatepost?channel=2&amp;post=456", body)
        self.assertIn("https://t.me/c/2/456", body)
        self.assertNotIn("备用 App 链接", body)

    def test_open_telegram_route_accepts_signed_private_chat_id(self) -> None:
        response = self.client.get(
            "/open/telegram?chat_id=-1002202633364&message_id=456"
        )

        self.assertEqual(200, response.status_code)
        body = response.get_data(as_text=True)
        self.assertIn("Signed Private Chat", body)
        self.assertIn("tg://privatepost?channel=2202633364&amp;post=456", body)
        self.assertIn("https://t.me/c/2202633364/456", body)

    def test_open_telegram_route_rejects_invalid_params(self) -> None:
        response = self.client.get("/open/telegram?chat_id=bad&message_id=1")

        self.assertEqual(400, response.status_code)
        self.assertEqual("chat_id 参数非法", response.get_json()["error"])


class _LoggerStub:
    def exception(self, _message):
        return None


if __name__ == "__main__":
    unittest.main()
