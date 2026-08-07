import gzip
import hashlib
import json
import os
import sqlite3
import tempfile
import unittest
from collections import deque
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

import tg_harvest.app.factory as app_factory
import tg_harvest.web.auth as auth_module
import tg_harvest.web.routes.search as search_routes_module
from tg_harvest.search.manticore_client import ManticoreError
from tg_harvest.search.result_mapper import _map_search_items
from tg_harvest.web.auth import register_auth_routes
from tg_harvest.web.responses import MAX_JSON_BODY_BYTES
from tg_harvest.web.routes.context import register_context_routes
from tg_harvest.web.routes.pages import register_page_routes
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

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "请求必须为 JSON")

    def test_login_rejects_oversized_json_body(self) -> None:
        response = self.client.post(
            "/api/admin/auth/login",
            data='{"password":"x"}' + " " * MAX_JSON_BODY_BYTES,
            content_type="application/json",
        )

        self.assertEqual(response.status_code, 413)
        self.assertEqual(response.get_json()["error"], "请求体过大")

    def test_login_rejects_non_object_json(self) -> None:
        response = self.client.post("/api/admin/auth/login", json=["bad"])

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "请求 JSON 格式错误")

    def test_login_rejects_non_string_password(self) -> None:
        response = self.client.post("/api/admin/auth/login", json={"password": 123})

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "password 参数必须为字符串")

    def test_login_rejects_when_admin_password_is_unconfigured(self) -> None:
        with patch(
            "tg_harvest.web.auth._get_auth_config",
            return_value=SimpleNamespace(admin_password="", admin_session_expiry=60),
        ):
            response = self.client.post("/api/admin/auth/login", json={})

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.get_json()["error"], "后台密码未配置")

    def test_login_accepts_configured_password(self) -> None:
        with patch(
            "tg_harvest.web.auth._get_auth_config",
            return_value=SimpleNamespace(admin_password="secret", admin_session_expiry=60),
        ):
            response = self.client.post(
                "/api/admin/auth/login",
                json={"password": "secret"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["ok"])
        self.assertNotIn("token", response.get_json())
        self.assertIsInstance(response.get_json()["csrf_token"], str)
        self.assertTrue(response.get_json()["csrf_token"])

    def test_auth_check_returns_csrf_token_for_authenticated_session(self) -> None:
        with patch(
            "tg_harvest.web.auth._get_auth_config",
            return_value=SimpleNamespace(admin_password="secret", admin_session_expiry=60),
        ):
            login_response = self.client.post(
                "/api/admin/auth/login",
                json={"password": "secret"},
            )
            check_response = self.client.get("/api/admin/auth/check")

        self.assertEqual(login_response.status_code, 200)
        self.assertEqual(check_response.status_code, 200)
        self.assertTrue(check_response.get_json()["authenticated"])
        self.assertEqual(
            login_response.get_json()["csrf_token"],
            check_response.get_json()["csrf_token"],
        )

    def test_admin_write_request_rejects_missing_csrf_token(self) -> None:
        @self.app.post("/api/admin/protected-write")
        def _protected_write():
            return {"ok": True}

        with patch(
            "tg_harvest.web.auth._get_auth_config",
            return_value=SimpleNamespace(admin_password="secret", admin_session_expiry=60),
        ):
            login_response = self.client.post(
                "/api/admin/auth/login",
                json={"password": "secret"},
            )
            response = self.client.post("/api/admin/protected-write", json={})

        self.assertEqual(login_response.status_code, 200)
        self.assertEqual(response.status_code, 403)
        self.assertTrue(response.get_json()["csrf_required"])

    def test_admin_write_request_accepts_valid_csrf_token(self) -> None:
        @self.app.post("/api/admin/protected-write")
        def _protected_write():
            return {"ok": True}

        with patch(
            "tg_harvest.web.auth._get_auth_config",
            return_value=SimpleNamespace(admin_password="secret", admin_session_expiry=60),
        ):
            login_response = self.client.post(
                "/api/admin/auth/login",
                json={"password": "secret"},
            )
            csrf_token = login_response.get_json()["csrf_token"]
            response = self.client.post(
                "/api/admin/protected-write",
                json={},
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["ok"])

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
                self.assertEqual(response.status_code, 403)

            response = self.client.post(
                "/api/admin/auth/login",
                json={"password": "secret"},
                environ_base={"REMOTE_ADDR": ip},
            )

        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.get_json()["error"], "登录失败次数过多，请稍后再试")

    def test_login_rate_limiter_rejects_new_keys_at_capacity(self) -> None:
        with auth_module._admin_login_failure_lock:
            auth_module._admin_login_failure_tracker.clear()
        try:
            with patch.object(
                auth_module, "ADMIN_LOGIN_FAILURE_MAX_TRACKED_KEYS", 1
            ):
                self.assertFalse(auth_module._admin_login_is_limited("first"))
                auth_module._admin_login_record_failure("first")
                self.assertTrue(auth_module._admin_login_is_limited("second"))
        finally:
            with auth_module._admin_login_failure_lock:
                auth_module._admin_login_failure_tracker.clear()

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
                self.assertEqual(response.status_code, 403)

            response = self.client.post(
                "/api/admin/auth/login",
                json={"password": "secret"},
                environ_base={"REMOTE_ADDR": ip},
            )
            self.assertEqual(response.status_code, 200)

            response = self.client.post(
                "/api/admin/auth/login",
                json={"password": "bad"},
                environ_base={"REMOTE_ADDR": ip},
            )

        self.assertEqual(response.status_code, 403)

    def test_proxy_clients_have_independent_login_failure_buckets(self) -> None:
        with patch.dict(
            "os.environ",
            {"TG_TRUST_PROXY_CIDRS": "127.0.0.1/32"},
        ), patch(
            "tg_harvest.web.auth._get_auth_config",
            return_value=SimpleNamespace(admin_password="secret", admin_session_expiry=60),
        ):
            for _ in range(auth_module.ADMIN_LOGIN_FAILURE_LIMIT):
                response = self.client.post(
                    "/api/admin/auth/login",
                    json={"password": "bad"},
                    environ_base={
                        "REMOTE_ADDR": "127.0.0.1",
                        "HTTP_X_FORWARDED_FOR": "203.0.113.20",
                    },
                )
                self.assertEqual(response.status_code, 403)

            response = self.client.post(
                "/api/admin/auth/login",
                json={"password": "secret"},
                environ_base={
                    "REMOTE_ADDR": "127.0.0.1",
                    "HTTP_X_FORWARDED_FOR": "203.0.113.21",
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["ok"])

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
        ), self.app.test_request_context("/"):
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
        ), self.app.test_request_context("/"):
            self.app.secret_key = "x" * 200
            fingerprint = auth_module._admin_password_fingerprint()

        self.assertTrue(fingerprint)

    def test_normalize_admin_next_path_rejects_external_urls(self) -> None:
        self.assertEqual(
            auth_module.normalize_admin_next_path("https://evil.example/admin/manage"),
            "/admin/manage",
        )
        self.assertEqual(
            auth_module.normalize_admin_next_path("//evil.example/admin/manage"),
            "/admin/manage",
        )

    def test_normalize_admin_next_path_allows_known_admin_pages_only(self) -> None:
        self.assertEqual(
            auth_module.normalize_admin_next_path("/admin/channels?sort=updated_desc"),
            "/admin/channels?sort=updated_desc",
        )
        self.assertEqual(
            auth_module.normalize_admin_next_path("/admin/recovery"),
            "/admin/recovery",
        )
        self.assertEqual(
            auth_module.normalize_admin_next_path("/admin/sync"),
            "/admin/sync",
        )
        self.assertEqual(
            auth_module.normalize_admin_next_path("/admin/clone"),
            "/admin/clone",
        )
        self.assertEqual(
            auth_module.normalize_admin_next_path("/admin/clone/create"),
            "/admin/clone/create",
        )
        self.assertEqual(
            auth_module.normalize_admin_next_path("/admin/clone/migrate"),
            "/admin/clone/migrate",
        )
        self.assertEqual(
            auth_module.normalize_admin_next_path("/admin/clone/runs/manage"),
            "/admin/clone/runs/manage",
        )
        self.assertEqual(
            auth_module.normalize_admin_next_path("/admin/clone/runs/detail?run_id=run-existing"),
            "/admin/clone/runs/detail?run_id=run-existing",
        )
        self.assertEqual(
            auth_module.normalize_admin_next_path("/api/admin/chats"),
            "/admin/manage",
        )


class AdminPageRoutesTests(unittest.TestCase):
    def setUp(self) -> None:
        with auth_module._admin_login_failure_lock:
            auth_module._admin_login_failure_tracker.clear()
        self.app = Flask(__name__, template_folder="/root/db/templates")
        self.app.secret_key = "test-secret"
        register_auth_routes(self.app)
        register_page_routes(self.app, page_size=100)
        self.client = self.app.test_client()

    def _auth_config_patch(self):
        return patch(
            "tg_harvest.web.auth._get_auth_config",
            return_value=SimpleNamespace(
                admin_password="secret",
                admin_session_expiry=60,
            ),
        )

    def _login_admin(self) -> None:
        response = self.client.post(
            "/api/admin/auth/login",
            json={"password": "secret"},
        )
        self.assertEqual(response.status_code, 200)

    def test_admin_manage_page_redirects_to_login_when_unauthenticated(self) -> None:
        with self._auth_config_patch():
            response = self.client.get("/admin/manage")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, "/admin/login?next=%2Fadmin%2Fmanage")

    def test_admin_login_page_sanitizes_next_parameter(self) -> None:
        with self._auth_config_patch():
            response = self.client.get("/admin/login?next=https://evil.example/")

        self.assertEqual(response.status_code, 200)
        body = response.get_data(as_text=True)
        self.assertIn('id="admin-login-page"', body)
        self.assertIn('data-next-path="/admin/manage"', body)
        self.assertNotIn("evil.example", body)

    def test_authenticated_login_page_redirects_to_allowed_next_page(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/admin/login?next=/admin/clone")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, "/admin/clone")

    def test_authenticated_login_page_redirects_to_clone_runs_manage_page(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/admin/login?next=/admin/clone/runs/manage")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, "/admin/clone/runs/manage")

    def test_authenticated_login_page_redirects_to_clone_run_detail_page(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/admin/login?next=/admin/clone/runs/detail?run_id=run-existing")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, "/admin/clone/runs/detail?run_id=run-existing")

    def test_authenticated_login_page_redirects_to_clone_create_page(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/admin/login?next=/admin/clone/create")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, "/admin/clone/create")

    def test_authenticated_login_page_preserves_legacy_clone_migrate_page(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/admin/login?next=/admin/clone/migrate")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, "/admin/clone/migrate")

    def test_authenticated_admin_manage_page_renders(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/admin/manage")

        self.assertEqual(response.status_code, 200)
        self.assertIn("后台数据库管理", response.get_data(as_text=True))

    def test_authenticated_admin_sync_page_renders(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/admin/sync")

        self.assertEqual(response.status_code, 200)
        self.assertIn("消息同步统计", response.get_data(as_text=True))


class SearchRoutesValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        with search_routes_module._rate_lock:
            search_routes_module._search_rate_tracker.clear()
        self.app = Flask(__name__)
        self.search_payload = {"ok": True, "items": []}
        register_search_routes(
            self.app,
            logger=_LoggerStub(),
            get_conn_fn=lambda: _ConnStub(),
            page_size=100,
            max_count=50000000,
            map_search_items_fn=lambda rows, detail_level="lite": rows,
            parse_search_params_fn=lambda data: data,
            search_payload_service_fn=self._search_payload_service,
        )
        self.client = self.app.test_client()

    def _search_payload_service(self, *_args, **_kwargs):
        if isinstance(self.search_payload, Exception):
            raise self.search_payload
        return self.search_payload

    def test_search_rejects_non_json_requests(self) -> None:
        response = self.client.post("/api/search", data="query=hello")

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "请求必须为 JSON")

    def test_search_rejects_non_object_json(self) -> None:
        response = self.client.post("/api/search", json=["hello"])

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "请求 JSON 格式错误")

    def test_search_returns_retryable_status_when_index_is_unavailable(self) -> None:
        self.search_payload = ManticoreError("index is stale")

        response = self.client.post("/api/search", json={"query": "hello"})

        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.get_json()["error"], "搜索索引暂不可用，请稍后重试"
        )

    def test_search_compresses_large_json_for_gzip_clients(self) -> None:
        self.search_payload = {
            "ok": True,
            "items": [{"title": "完整正文" * 1000}],
        }
        response = self.client.post(
            "/api/search",
            json={"query": "hello"},
            headers={"Accept-Encoding": "gzip"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("Content-Encoding"), "gzip")
        self.assertIn("Accept-Encoding", response.headers.get("Vary", ""))
        payload = json.loads(gzip.decompress(response.data).decode("utf-8"))
        self.assertEqual("完整正文" * 1000, payload["items"][0]["title"])

    def test_search_rate_limit_ignores_background_count_requests(self) -> None:
        ip = "198.51.100.10"
        for _ in range(search_routes_module.SEARCH_RATE_LIMIT + 5):
            response = self.client.post(
                "/api/search",
                json={"query": "hello", "count_only": "yes"},
                environ_base={"REMOTE_ADDR": ip},
            )
            self.assertEqual(response.status_code, 200)

        response = self.client.post(
            "/api/search",
            json={"query": "hello"},
            environ_base={"REMOTE_ADDR": ip},
        )

        self.assertEqual(response.status_code, 200)

    def test_search_count_only_requests_have_their_own_rate_limit(self) -> None:
        ip = "198.51.100.11"
        for _ in range(search_routes_module.SEARCH_COUNT_ONLY_RATE_LIMIT):
            response = self.client.post(
                "/api/search",
                json={"query": "hello", "count_only": True},
                environ_base={"REMOTE_ADDR": ip},
            )
            self.assertEqual(response.status_code, 200)

        response = self.client.post(
            "/api/search",
            json={"query": "hello", "count_only": True},
            environ_base={"REMOTE_ADDR": ip},
        )

        self.assertEqual(response.status_code, 429)

    def test_search_rate_limiter_prunes_expired_client_keys(self) -> None:
        with search_routes_module._rate_lock:
            search_routes_module._search_rate_tracker["search:old"] = deque([1.0])

        with patch("tg_harvest.web.routes.search.time.time", return_value=10_000.0):
            limited = search_routes_module._is_rate_limited("fresh")

        self.assertFalse(limited)
        with search_routes_module._rate_lock:
            self.assertNotIn("search:old", search_routes_module._search_rate_tracker)


class AppFactoryRuntimeInitTests(unittest.TestCase):
    def test_search_page_size_is_limited_to_fifty_complete_messages(self) -> None:
        self.assertEqual(app_factory.PAGE_SIZE, 50)

    def test_create_app_requires_security_config_when_production_flag_is_enabled(self) -> None:
        with patch.dict(
            os.environ,
            {"TG_REQUIRE_SECURE_CONFIG": "1", "FLASK_SECRET_KEY": ""},
            clear=False,
        ), patch.object(app_factory.CFG, "admin_password", ""), (
            self.assertRaisesRegex(
                RuntimeError, "FLASK_SECRET_KEY.*TG_ADMIN_PASSWORD"
            )
        ):
            app_factory.create_app(init_db=False)

    def test_create_app_enables_secure_session_cookie_in_production(self) -> None:
        with patch.dict(
            os.environ,
            {
                "TG_REQUIRE_SECURE_CONFIG": "1",
                "FLASK_SECRET_KEY": "stable-test-secret",
            },
            clear=False,
        ), patch.object(app_factory.CFG, "admin_password", "secret"):
            app = app_factory.create_app(init_db=False)

        self.assertTrue(app.config["SESSION_COOKIE_SECURE"])

    def test_create_app_allows_secure_cookie_override_for_local_tls_termination(self) -> None:
        with patch.dict(
            os.environ,
            {
                "TG_REQUIRE_SECURE_CONFIG": "1",
                "FLASK_SECRET_KEY": "stable-test-secret",
                "TG_SESSION_COOKIE_SECURE": "0",
            },
            clear=False,
        ), patch.object(app_factory.CFG, "admin_password", "secret"):
            app = app_factory.create_app(init_db=False)

        self.assertFalse(app.config["SESSION_COOKIE_SECURE"])

    def test_run_web_server_creates_ready_app_for_serving(self) -> None:
        created_apps = []
        original_create_app = app_factory.create_app

        def capture_create_app(*, init_db: bool = False):
            app = original_create_app(init_db=init_db)
            created_apps.append(app)
            return app

        with patch.object(app_factory, "_ensure_db") as ensure_db_mock, patch.object(
            app_factory, "ensure_database_chat_listener_runtime"
        ) as listener_mock, patch.object(
            app_factory, "create_app", side_effect=capture_create_app
        ), patch("flask.Flask.run", return_value=None) as run_mock:
            app_factory.run_web_server(host="127.0.0.1", port=9999, debug=False)

        ensure_db_mock.assert_called_once_with()
        listener_mock.assert_not_called()
        run_mock.assert_called_once_with(host="127.0.0.1", port=9999, debug=False)
        self.assertEqual(len(created_apps), 1)
        self.assertTrue(created_apps[0].extensions["tg_db_ready"])

    def test_debug_reloader_parent_does_not_start_runtime_workers(self) -> None:
        created_apps = []
        original_create_app = app_factory.create_app

        def capture_create_app(*, init_db: bool = False):
            app = original_create_app(init_db=init_db)
            created_apps.append(app)
            return app

        with patch.dict(os.environ, {"WERKZEUG_RUN_MAIN": ""}, clear=False), patch.object(
            app_factory, "_ensure_db"
        ) as ensure_db_mock, patch.object(
            app_factory, "create_app", side_effect=capture_create_app
        ) as create_app_mock, patch("flask.Flask.run", return_value=None) as run_mock:
            app_factory.run_web_server(host="127.0.0.1", port=9999, debug=True)

        create_app_mock.assert_called_once_with(init_db=False)
        ensure_db_mock.assert_not_called()
        run_mock.assert_called_once_with(host="127.0.0.1", port=9999, debug=True)
        self.assertEqual(len(created_apps), 1)
        self.assertFalse(created_apps[0].extensions["tg_db_ready"])

    def test_debug_reloader_child_starts_runtime_workers(self) -> None:
        created_apps = []
        original_create_app = app_factory.create_app

        def capture_create_app(*, init_db: bool = False):
            app = original_create_app(init_db=init_db)
            created_apps.append(app)
            return app

        with patch.dict(
            os.environ, {"WERKZEUG_RUN_MAIN": "true"}, clear=False
        ), patch.object(app_factory, "_ensure_db") as ensure_db_mock, patch.object(
            app_factory, "create_app", side_effect=capture_create_app
        ) as create_app_mock, patch("flask.Flask.run", return_value=None):
            app_factory.run_web_server(host="127.0.0.1", port=9999, debug=True)

        create_app_mock.assert_called_once_with(init_db=True)
        ensure_db_mock.assert_called_once_with()
        self.assertEqual(len(created_apps), 1)
        self.assertTrue(created_apps[0].extensions["tg_db_ready"])

    def test_db_free_routes_do_not_trigger_runtime_db_initialization(self) -> None:
        with patch.dict(
            os.environ,
            {"FLASK_SECRET_KEY": "stable-test-secret"},
            clear=False,
        ), patch.object(app_factory.CFG, "admin_password", "secret"):
            app = app_factory.create_app(init_db=False)

        client = app.test_client()
        with patch.object(app_factory, "_ensure_db") as ensure_db_mock:
            login_response = client.get("/admin/login")
            auth_check_response = client.get("/api/admin/auth/check")
            admin_response = client.get("/admin/manage")
            clone_create_response = client.get("/admin/clone/create")
            clone_migrate_response = client.get("/admin/clone/migrate")
            clone_detail_response = client.get("/admin/clone/runs/detail")
            static_response = client.get("/static/admin_login.js")
            missing_response = client.get("/not-a-route")

        self.assertEqual(login_response.status_code, 200)
        self.assertEqual(auth_check_response.status_code, 200)
        self.assertEqual(admin_response.status_code, 302)
        self.assertEqual(clone_create_response.status_code, 302)
        self.assertEqual(clone_migrate_response.status_code, 302)
        self.assertEqual(clone_detail_response.status_code, 302)
        self.assertEqual(static_response.status_code, 200)
        self.assertEqual(missing_response.status_code, 404)
        ensure_db_mock.assert_not_called()
        self.assertFalse(app.extensions["tg_db_ready"])

    def test_database_api_triggers_runtime_db_initialization_once(self) -> None:
        with patch.dict(
            os.environ,
            {"FLASK_SECRET_KEY": "stable-test-secret"},
            clear=False,
        ), patch.object(app_factory.CFG, "admin_password", "secret"):
            app = app_factory.create_app(init_db=False)

        client = app.test_client()
        with patch.object(app_factory, "_ensure_db") as ensure_db_mock:
            response = client.get("/api/meta")
            second_response = client.get("/api/meta")

        self.assertNotEqual(response.status_code, 404)
        self.assertNotEqual(second_response.status_code, 404)
        ensure_db_mock.assert_called_once_with()
        self.assertTrue(app.extensions["tg_db_ready"])

    def test_unauthenticated_admin_api_does_not_trigger_runtime_db_initialization(
        self,
    ) -> None:
        with patch.dict(
            os.environ,
            {"FLASK_SECRET_KEY": "stable-test-secret"},
            clear=False,
        ), patch.object(app_factory.CFG, "admin_password", "secret"):
            app = app_factory.create_app(init_db=False)

        client = app.test_client()
        with patch.object(app_factory, "_ensure_db") as ensure_db_mock:
            response = client.get("/api/admin/chats")

        self.assertEqual(response.status_code, 401)
        ensure_db_mock.assert_not_called()
        self.assertFalse(app.extensions["tg_db_ready"])


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

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["items"][0]["is_promo"], 1)

    def test_context_page_accepts_signed_chat_id(self) -> None:
        response = self.client.get("/chat/-1002202633364?msg_id=11")

        self.assertEqual(response.status_code, 200)
        self.assertIn('data-chat-id="-1002202633364"', response.get_data(as_text=True))

    def test_context_api_accepts_signed_chat_id(self) -> None:
        response = self.client.get(
            "/api/chat/-1002202633364/context?msg_id=11&direction=around"
        )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(-1002202633364, payload["items"][0]["chat_id"])
        self.assertEqual(payload["items"][0]["content"], "signed chat text")


class _ConnStub:
    def close(self):
        return None


class _LoggerStub:
    def exception(self, _message):
        return None


if __name__ == "__main__":
    unittest.main()
