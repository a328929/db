import unittest
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

import tg_harvest.web.auth as auth_module
from tg_harvest.web.auth import register_auth_routes
from tg_harvest.web.routes.channels import register_channel_routes


class _LoggerStub:
    def exception(self, _message):
        return None


class _ConnStub:
    def close(self):
        return None


class _Bundle:
    def __init__(self, app_link, web_link):
        self.app_link = app_link
        self.web_link = web_link


class ChannelRoutesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = Flask(__name__, template_folder="/root/db/templates")
        self.app.secret_key = "test"

        def build_bundle(chat_id, chat_username=None):
            username = str(chat_username or "")
            return _Bundle(
                app_link=f"tg://resolve?domain={username}" if username else f"tg://openmessage?chat_id={chat_id}",
                web_link=f"https://t.me/{username}" if username else "",
            )

        register_auth_routes(self.app)
        register_channel_routes(
            self.app,
            logger=_LoggerStub(),
            get_conn_fn=lambda: _ConnStub(),
            cfg=object(),
            list_database_channels_fn=lambda _conn, sort: [
                {
                    "chat_id": 1,
                    "chat_title": "Public",
                    "chat_username": "public",
                    "chat_type": "Channel",
                    "message_count": 10,
                    "last_seen_at": "2026-01-01 00:00:00",
                    "last_message_at": "2026-01-02 00:00:00",
                    "last_message_ts": 1767312000,
                }
            ],
            list_missing_chat_scan_results_fn=lambda _conn: [],
            list_restricted_chat_scan_results_fn=lambda _conn: [
                {
                    "chat_id": 3,
                    "chat_title": "Restricted",
                    "chat_username": "restricted",
                    "chat_type": "Channel",
                    "is_public": 1,
                    "restriction_platforms": "all",
                    "restriction_reasons": "porn",
                    "restriction_text": "This channel can't be displayed.",
                    "risk_flags": "restricted",
                    "membership_scope": "joined",
                    "last_message_at": "2026-03-01 00:00:00",
                    "last_message_ts": 1772323200,
                    "scan_job_id": "job-3",
                    "scanned_at": "2026-03-02 00:00:00",
                }
            ],
            build_telegram_chat_link_bundle_fn=build_bundle,
            admin_try_create_exclusive_job_fn=lambda *_args, **_kwargs: (
                {"job_id": "job-1"},
                None,
            ),
            admin_job_get_snapshot_fn=lambda job_id: {"job_id": job_id},
            admin_job_append_log_fn=lambda *_args, **_kwargs: None,
            admin_job_set_status_fn=lambda *_args, **_kwargs: True,
            admin_start_missing_chats_scan_job_thread_fn=lambda *_args, **_kwargs: None,
            admin_start_restricted_chats_scan_job_thread_fn=(
                lambda *_args, **_kwargs: None
            ),
        )
        self.client = self.app.test_client()

    def _auth_config_patch(self):
        return patch(
            "tg_harvest.web.auth._get_auth_config",
            return_value=SimpleNamespace(
                admin_password="secret",
                admin_session_expiry=60,
            ),
        )

    def _login_admin(self) -> str:
        response = self.client.post(
            "/api/admin/auth/login",
            json={"password": "secret"},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        return str(payload["csrf_token"])

    def test_database_channels_api_includes_telegram_links(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/api/admin/channels")

        self.assertEqual(response.status_code, 200)
        item = response.get_json()["channels"][0]
        self.assertEqual(item["telegram_app_link"], "tg://resolve?domain=public")
        self.assertEqual(item["telegram_web_link"], "https://t.me/public")
        self.assertEqual(item["last_message_at"], "2026-01-02 00:00:00")
        self.assertTrue(item["has_public_link"])

    def test_channels_page_redirects_to_login_when_unauthenticated(self) -> None:
        with self._auth_config_patch():
            response = self.client.get("/admin/channels")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, "/admin/login?next=%2Fadmin%2Fchannels")

    def test_channels_page_renders_when_authenticated(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/admin/channels")

        self.assertEqual(response.status_code, 200)
        self.assertIn("频道管理", response.get_data(as_text=True))

    def test_channel_scan_routes_reject_unauthenticated_requests(self) -> None:
        with self._auth_config_patch():
            response = self.client.post("/api/admin/channels/restricted/scan")

        self.assertEqual(response.status_code, 401)
        self.assertTrue(response.get_json()["auth_required"])

    def test_channel_scan_routes_reject_missing_csrf_token(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.post("/api/admin/channels/restricted/scan")

        self.assertEqual(response.status_code, 403)
        self.assertTrue(response.get_json()["csrf_required"])

    def test_channel_scan_routes_accept_real_login_and_csrf_token(self) -> None:
        endpoints = [
            "/api/admin/channels/missing/scan",
            "/api/admin/channels/restricted/scan",
        ]

        with self._auth_config_patch():
            csrf_token = self._login_admin()
            for endpoint in endpoints:
                with self.subTest(endpoint=endpoint):
                    response = self.client.post(
                        endpoint,
                        headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
                    )
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.get_json()["job"], {"job_id": "job-1"})

    def test_removed_absent_channels_routes_return_not_found(self) -> None:
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            get_response = self.client.get("/api/admin/channels/absent")
            scan_response = self.client.post(
                "/api/admin/channels/absent/scan",
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(get_response.status_code, 404)
        self.assertEqual(scan_response.status_code, 404)


    def test_restricted_channels_api_includes_telegram_links(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/api/admin/channels/restricted")

        self.assertEqual(response.status_code, 200)
        item = response.get_json()["items"][0]
        self.assertEqual(item["chat_title"], "Restricted")
        self.assertEqual(item["telegram_app_link"], "tg://resolve?domain=restricted")
        self.assertEqual(item["telegram_web_link"], "https://t.me/restricted")
        self.assertTrue(item["has_public_link"])
        self.assertEqual(item["restriction_reasons"], "porn")
        self.assertEqual(item["risk_flags"], "restricted")
        self.assertEqual(item["membership_scope"], "joined")
        self.assertEqual(item["last_message_at"], "2026-03-01 00:00:00")


if __name__ == "__main__":
    unittest.main()
