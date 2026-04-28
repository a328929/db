import unittest
import os
import sqlite3
import tempfile
from pathlib import Path

from flask import Flask

from tg_harvest.search.result_mapper import _map_search_items
from tg_harvest.web.auth import register_auth_routes
from tg_harvest.web.routes.context import register_context_routes
from tg_harvest.web.routes import search as search_routes_module
from tg_harvest.web.routes.search import register_search_routes


class AuthRoutesValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = Flask(__name__)
        self.app.secret_key = "test-secret"
        register_auth_routes(self.app)
        self.client = self.app.test_client()

    def test_login_rejects_non_json_requests(self) -> None:
        response = self.client.post("/api/admin/auth/login", data="password=1")

        self.assertEqual(400, response.status_code)
        self.assertEqual("请求必须为 JSON", response.get_json()["error"])

    def test_login_rejects_non_object_json(self) -> None:
        response = self.client.post("/api/admin/auth/login", json=["bad"])

        self.assertEqual(400, response.status_code)
        self.assertEqual("请求 JSON 格式错误", response.get_json()["error"])

    def test_login_rejects_non_string_password(self) -> None:
        response = self.client.post("/api/admin/auth/login", json={"password": 123})

        self.assertEqual(400, response.status_code)
        self.assertEqual("password 参数必须为字符串", response.get_json()["error"])


class SearchRoutesValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        with search_routes_module._rate_lock:
            search_routes_module._search_rate_tracker.clear()
        self.app = Flask(__name__)
        register_search_routes(
            self.app,
            logger=_LoggerStub(),
            get_conn_fn=lambda: _ConnStub(),
            has_fts_fn=lambda _conn: False,
            from_sql="FROM messages m",
            page_size=100,
            max_count=50000000,
            map_search_items_fn=lambda rows, detail_level="lite": rows,
            parse_search_params_fn=lambda data: data,
            search_payload_service_fn=lambda *_args, **_kwargs: {"ok": True, "items": []},
        )
        self.client = self.app.test_client()

    def test_search_rejects_non_json_requests(self) -> None:
        response = self.client.post("/api/search", data="query=hello")

        self.assertEqual(400, response.status_code)
        self.assertEqual("请求必须为 JSON", response.get_json()["error"])

    def test_search_rejects_non_object_json(self) -> None:
        response = self.client.post("/api/search", json=["hello"])

        self.assertEqual(400, response.status_code)
        self.assertEqual("请求 JSON 格式错误", response.get_json()["error"])

    def test_search_rate_limit_ignores_background_count_requests(self) -> None:
        ip = "198.51.100.10"
        for _ in range(search_routes_module.SEARCH_RATE_LIMIT + 5):
            response = self.client.post(
                "/api/search",
                json={"query": "hello", "count_only": True},
                environ_base={"REMOTE_ADDR": ip},
            )
            self.assertEqual(200, response.status_code)

        response = self.client.post(
            "/api/search",
            json={"query": "hello"},
            environ_base={"REMOTE_ADDR": ip},
        )

        self.assertEqual(200, response.status_code)


class ContextRoutesTests(unittest.TestCase):
    def setUp(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite3")
        os.close(fd)
        self.db_path = db_path
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
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
                pk INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                msg_date_text TEXT NOT NULL,
                msg_date_ts INTEGER NOT NULL,
                msg_type TEXT NOT NULL,
                content TEXT,
                grouped_id INTEGER,
                is_promo INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE message_media (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                file_name TEXT,
                file_size INTEGER,
                mime_type TEXT,
                media_kind TEXT,
                duration_sec INTEGER,
                PRIMARY KEY(chat_id, message_id)
            )
            """
        )
        cur.execute("INSERT INTO chats(chat_id, chat_title) VALUES (1, 'Chat 1')")
        cur.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type, content, is_promo
            ) VALUES (1, 10, '2026-01-01 00:00:00', 1, 'TEXT', 'promo text', 1)
            """
        )
        conn.commit()
        conn.close()

        self.app = Flask(__name__, template_folder="/root/db/templates")
        register_context_routes(
            self.app,
            logger=_LoggerStub(),
            get_conn_fn=self._connect,
            from_sql=(
                "FROM messages m "
                "LEFT JOIN chats c ON c.chat_id = m.chat_id "
                "LEFT JOIN message_media mm ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id"
            ),
            map_search_items_fn=_map_search_items,
        )
        self.client = self.app.test_client()

    def tearDown(self) -> None:
        Path(self.db_path).unlink(missing_ok=True)

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def test_context_api_returns_promo_flag_for_local_view(self) -> None:
        response = self.client.get("/api/chat/1/context?msg_id=10&direction=around")

        self.assertEqual(200, response.status_code)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(1, payload["items"][0]["is_promo"])


class _ConnStub:
    def close(self):
        return None


class _LoggerStub:
    def exception(self, _message):
        return None


if __name__ == "__main__":
    unittest.main()
