import unittest
import hashlib
import os
import sqlite3
import tempfile
from collections import deque
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

from tg_harvest.search.result_mapper import _map_search_items
from tg_harvest.app import factory as app_factory
from tg_harvest.web import auth as auth_module
from tg_harvest.web.auth import register_auth_routes
from tg_harvest.web.routes.context import register_context_routes
from tg_harvest.web.routes import search as search_routes_module
from tg_harvest.web.routes.search import register_search_routes


class AuthRoutesValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        with auth_module._admin_login_failure_lock:
            auth_module._admin_login_failure_tracker.clear()
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

    def test_login_rejects_when_admin_password_is_unconfigured(self) -> None:
        with patch(
            "tg_harvest.web.auth._get_auth_config",
            return_value=SimpleNamespace(admin_password="", admin_session_expiry=60),
        ):
            response = self.client.post("/api/admin/auth/login", json={})

        self.assertEqual(503, response.status_code)
        self.assertEqual("后台密码未配置", response.get_json()["error"])

    def test_login_accepts_configured_password(self) -> None:
        with patch(
            "tg_harvest.web.auth._get_auth_config",
            return_value=SimpleNamespace(admin_password="secret", admin_session_expiry=60),
        ):
            response = self.client.post(
                "/api/admin/auth/login",
                json={"password": "secret"},
            )

        self.assertEqual(200, response.status_code)
        self.assertTrue(response.get_json()["ok"])
        self.assertNotIn("token", response.get_json())

    def test_login_rate_limits_repeated_password_failures(self) -> None:
        ip = "203.0.113.10"
        with patch(
            "tg_harvest.web.auth._get_auth_config",
            return_value=SimpleNamespace(admin_password="secret", admin_session_expiry=60),
        ):
            for _ in range(auth_module.ADMIN_LOGIN_FAILURE_LIMIT):
                response = self.client.post(
                    "/api/admin/auth/login",
                    json={"password": "bad"},
                    environ_base={"REMOTE_ADDR": ip},
                )
                self.assertEqual(403, response.status_code)

            response = self.client.post(
                "/api/admin/auth/login",
                json={"password": "secret"},
                environ_base={"REMOTE_ADDR": ip},
            )

        self.assertEqual(429, response.status_code)
        self.assertEqual("登录失败次数过多，请稍后再试", response.get_json()["error"])

    def test_successful_login_clears_prior_failures(self) -> None:
        ip = "203.0.113.11"
        with patch(
            "tg_harvest.web.auth._get_auth_config",
            return_value=SimpleNamespace(admin_password="secret", admin_session_expiry=60),
        ):
            for _ in range(auth_module.ADMIN_LOGIN_FAILURE_LIMIT - 1):
                response = self.client.post(
                    "/api/admin/auth/login",
                    json={"password": "bad"},
                    environ_base={"REMOTE_ADDR": ip},
                )
                self.assertEqual(403, response.status_code)

            response = self.client.post(
                "/api/admin/auth/login",
                json={"password": "secret"},
                environ_base={"REMOTE_ADDR": ip},
            )
            self.assertEqual(200, response.status_code)

            response = self.client.post(
                "/api/admin/auth/login",
                json={"password": "bad"},
                environ_base={"REMOTE_ADDR": ip},
            )

        self.assertEqual(403, response.status_code)

    def test_login_rate_limiter_prunes_expired_client_keys(self) -> None:
        with auth_module._admin_login_failure_lock:
            auth_module._admin_login_failure_tracker["old"] = deque([1.0])

        with patch("tg_harvest.web.auth.time.time", return_value=10_000.0):
            limited = auth_module._admin_login_is_limited("fresh")

        self.assertFalse(limited)
        with auth_module._admin_login_failure_lock:
            self.assertNotIn("old", auth_module._admin_login_failure_tracker)

    def test_admin_password_fingerprint_is_keyed_by_flask_secret(self) -> None:
        with patch(
            "tg_harvest.web.auth._get_auth_config",
            return_value=SimpleNamespace(admin_password="secret", admin_session_expiry=60),
        ):
            with self.app.test_request_context("/"):
                self.app.secret_key = "secret-key-a"
                fingerprint_a = auth_module._admin_password_fingerprint()
                self.app.secret_key = "secret-key-b"
                fingerprint_b = auth_module._admin_password_fingerprint()

        unsalted = hashlib.blake2b(b"secret", digest_size=16).hexdigest()
        self.assertNotEqual(fingerprint_a, fingerprint_b)
        self.assertNotEqual(unsalted, fingerprint_a)

    def test_admin_password_fingerprint_accepts_long_flask_secret(self) -> None:
        with patch(
            "tg_harvest.web.auth._get_auth_config",
            return_value=SimpleNamespace(admin_password="secret", admin_session_expiry=60),
        ):
            with self.app.test_request_context("/"):
                self.app.secret_key = "x" * 200
                fingerprint = auth_module._admin_password_fingerprint()

        self.assertTrue(fingerprint)


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

    def test_search_count_only_requests_have_their_own_rate_limit(self) -> None:
        ip = "198.51.100.11"
        for _ in range(search_routes_module.SEARCH_COUNT_ONLY_RATE_LIMIT):
            response = self.client.post(
                "/api/search",
                json={"query": "hello", "count_only": True},
                environ_base={"REMOTE_ADDR": ip},
            )
            self.assertEqual(200, response.status_code)

        response = self.client.post(
            "/api/search",
            json={"query": "hello", "count_only": True},
            environ_base={"REMOTE_ADDR": ip},
        )

        self.assertEqual(429, response.status_code)

    def test_search_rate_limiter_prunes_expired_client_keys(self) -> None:
        with search_routes_module._rate_lock:
            search_routes_module._search_rate_tracker["search:old"] = deque([1.0])

        with patch("tg_harvest.web.routes.search.time.time", return_value=10_000.0):
            limited = search_routes_module._is_rate_limited("fresh")

        self.assertFalse(limited)
        with search_routes_module._rate_lock:
            self.assertNotIn("search:old", search_routes_module._search_rate_tracker)


class AppFactoryRuntimeInitTests(unittest.TestCase):
    def test_run_web_server_marks_global_app_ready_after_preinit(self) -> None:
        app_factory.app.extensions["tg_db_ready"] = False

        with patch.object(app_factory, "_ensure_db") as ensure_db_mock, patch.object(
            app_factory.app, "run", return_value=None
        ) as run_mock:
            app_factory.run_web_server(host="127.0.0.1", port=9999, debug=False)

        ensure_db_mock.assert_called_once_with()
        run_mock.assert_called_once_with(host="127.0.0.1", port=9999, debug=False)
        self.assertTrue(app_factory.app.extensions["tg_db_ready"])


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
        cur.executemany(
            "INSERT INTO chats(chat_id, chat_title) VALUES (?, ?)",
            [
                (1, "Chat 1"),
                (-1002202633364, "Signed Chat"),
            ],
        )
        cur.executemany(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type, content, is_promo
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (1, 10, "2026-01-01 00:00:00", 1, "TEXT", "promo text", 1),
                (
                    -1002202633364,
                    11,
                    "2026-01-01 00:00:01",
                    2,
                    "TEXT",
                    "signed chat text",
                    0,
                ),
            ],
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

    def test_context_page_accepts_signed_chat_id(self) -> None:
        response = self.client.get("/chat/-1002202633364?msg_id=11")

        self.assertEqual(200, response.status_code)
        self.assertIn('data-chat-id="-1002202633364"', response.get_data(as_text=True))

    def test_context_api_accepts_signed_chat_id(self) -> None:
        response = self.client.get(
            "/api/chat/-1002202633364/context?msg_id=11&direction=around"
        )

        self.assertEqual(200, response.status_code)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(-1002202633364, payload["items"][0]["chat_id"])
        self.assertEqual("signed chat text", payload["items"][0]["content"])


class _ConnStub:
    def close(self):
        return None


class _LoggerStub:
    def exception(self, _message):
        return None


if __name__ == "__main__":
    unittest.main()
