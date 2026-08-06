import unittest
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

import tg_harvest.web.auth as auth_module
from tg_harvest.web.auth import register_auth_routes
from tg_harvest.web.routes.recovery import register_recovery_routes


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


class RecoveryRoutesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.started_harvests = []
        self.started_scans = []
        self.started_restores = []
        self.logs = []
        self.app = Flask(__name__, template_folder="/root/db/templates")
        self.app.secret_key = "test"

        def build_bundle(chat_id, chat_username=None):
            username = str(chat_username or "")
            return _Bundle(
                app_link=(
                    f"tg://resolve?domain={username}"
                    if username
                    else f"tg://openmessage?chat_id={chat_id}"
                ),
                web_link=f"https://t.me/{username}" if username else "",
            )

        register_auth_routes(self.app)
        register_recovery_routes(
            self.app,
            logger=_LoggerStub(),
            get_conn_fn=lambda: _ConnStub(),
            cfg=object(),
            list_recovery_chat_candidates_fn=lambda _conn: [
                {
                    "chat_id": 1,
                    "chat_title": "Recoverable",
                    "chat_username": "recoverable",
                    "chat_type": "SessionEntity",
                    "is_public": 1,
                    "source_session": "my_session.session",
                    "source_entity_id": -1001,
                    "source_access_hash": 9001,
                    "availability_reason": "",
                    "session_entity_date": "2026-04-01 10:00:00",
                    "session_entity_ts": 1775037600,
                    "recovered_at": "",
                    "recovered_job_id": "",
                    "scan_job_id": "scan-1",
                    "scanned_at": "2026-04-01T00:00:00+00:00",
                    "in_database": 0,
                    "message_count": 0,
                    "database_last_seen_at": "",
                }
            ],
            build_recovery_overview_fn=lambda _conn: {
                "total_count": 1,
                "pending_count": 1,
                "in_database_count": 0,
                "recovered_count": 0,
                "last_scanned_at": "2026-04-01T00:00:00+00:00",
            },
            build_telegram_chat_link_bundle_fn=build_bundle,
            admin_try_create_exclusive_job_fn=lambda *_args, **_kwargs: (
                {"job_id": "job-1"},
                None,
            ),
            admin_job_get_snapshot_fn=lambda job_id: {"job_id": job_id},
            admin_job_append_log_fn=lambda job_id, message: self.logs.append(
                (job_id, str(message))
            ),
            admin_job_set_status_fn=lambda *_args, **_kwargs: True,
            admin_start_harvest_job_thread_fn=(
                lambda *args, **kwargs: self.started_harvests.append((args, kwargs))
            ),
            admin_make_job_log_handler_fn=lambda _job_id: None,
            admin_harvest_target_max_len=128,
            admin_start_recovery_scan_job_thread_fn=(
                lambda *args, **kwargs: self.started_scans.append((args, kwargs))
            ),
            admin_start_recovery_restore_job_thread_fn=(
                lambda *args, **kwargs: self.started_restores.append((args, kwargs))
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

    def test_recovery_page_redirects_to_login_when_unauthenticated(self) -> None:
        with self._auth_config_patch():
            response = self.client.get("/admin/recovery")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, "/admin/login?next=%2Fadmin%2Frecovery")

    def test_recovery_candidates_api_includes_links_and_overview(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/api/admin/recovery")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["overview"]["pending_count"], 1)
        item = payload["items"][0]
        self.assertEqual(item["chat_title"], "Recoverable")
        self.assertEqual(item["telegram_app_link"], "tg://resolve?domain=recoverable")
        self.assertEqual(item["telegram_web_link"], "https://t.me/recoverable")

    def test_recovery_scan_rejects_missing_csrf_token(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.post("/api/admin/recovery/scan")

        self.assertEqual(response.status_code, 403)
        self.assertTrue(response.get_json()["csrf_required"])

    def test_recovery_scan_accepts_csrf_token(self) -> None:
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.post(
                "/api/admin/recovery/scan",
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["job"], {"job_id": "job-1"})
        self.assertEqual(len(self.started_scans), 1)
        self.assertIn(("job-1", "已接收 Session 群组恢复扫描请求"), self.logs)

    def test_recovery_add_starts_harvest_job_with_recovery_specific_route(self) -> None:
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.post(
                "/api/admin/recovery/add",
                json={
                    "target": "  @recoverable  ",
                    "chat_id": 1,
                    "chat_title": "Recoverable",
                    "chat_username": "recoverable",
                    "source_session": "my_session.session",
                    "source_entity_id": -1001,
                    "source_access_hash": 9001,
                },
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["job"], {"job_id": "job-1"})
        self.assertEqual(len(self.started_harvests), 1)
        args, kwargs = self.started_harvests[0]
        self.assertEqual(args[:2], ("job-1", "@recoverable"))
        self.assertEqual(
            kwargs["harvest_hint"],
            {
                "chat_id": 1,
                "chat_title": "Recoverable",
                "chat_username": "recoverable",
                "source_session": "my_session.session",
                "source_entity_id": -1001,
                "source_access_hash": 9001,
            },
        )
        self.assertEqual(
            self.logs[:2],
            [
                ("job-1", "已接收恢复候选添加入库请求"),
                ("job-1", "抓取目标：@recoverable"),
            ],
        )

    def test_recovery_restore_selected_requires_confirmation(self) -> None:
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.post(
                "/api/admin/recovery/restore",
                json={"scope": "selected", "chat_ids": [1], "confirm": "bad"},
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.get_json()["error"], "confirm 参数不匹配")

    def test_recovery_restore_selected_starts_job(self) -> None:
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.post(
                "/api/admin/recovery/restore",
                json={
                    "scope": "selected",
                    "chat_ids": [1],
                    "confirm": "RECOVER:selected:1",
                },
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["job"], {"job_id": "job-1"})
        self.assertEqual(len(self.started_restores), 1)
        _args, kwargs = self.started_restores[0]
        self.assertEqual(kwargs["chat_ids"], [1])
        self.assertEqual(kwargs["target_label"], "1 个恢复候选")


if __name__ == "__main__":
    unittest.main()
