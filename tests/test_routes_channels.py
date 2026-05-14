import unittest

from flask import Flask

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
            list_absent_chat_scan_results_fn=lambda _conn: [
                {
                    "chat_id": 2,
                    "chat_title": "Absent",
                    "chat_username": "",
                    "chat_type": "Channel",
                    "message_count": 5,
                    "last_seen_at": "2026-02-01 00:00:00",
                    "scan_job_id": "job-2",
                    "scanned_at": "2026-02-02 00:00:00",
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
            admin_start_absent_chats_scan_job_thread_fn=lambda *_args, **_kwargs: None,
        )
        self.client = self.app.test_client()
        with self.client.session_transaction() as session:
            session["admin_token"] = "token"
            session["admin_expiry"] = 9999999999
            session["admin_auth_fp"] = "fp"

    def test_database_channels_api_includes_telegram_links(self) -> None:
        from unittest.mock import patch

        with patch("tg_harvest.web.auth.is_authenticated", return_value=True):
            response = self.client.get("/api/admin/channels")

        self.assertEqual(200, response.status_code)
        item = response.get_json()["channels"][0]
        self.assertEqual("tg://resolve?domain=public", item["telegram_app_link"])
        self.assertEqual("https://t.me/public", item["telegram_web_link"])
        self.assertEqual("2026-01-02 00:00:00", item["last_message_at"])
        self.assertTrue(item["has_public_link"])

    def test_absent_channels_api_includes_telegram_links(self) -> None:
        from unittest.mock import patch

        with patch("tg_harvest.web.auth.is_authenticated", return_value=True):
            response = self.client.get("/api/admin/channels/absent")

        self.assertEqual(200, response.status_code)
        item = response.get_json()["items"][0]
        self.assertEqual("Absent", item["chat_title"])
        self.assertEqual("tg://openmessage?chat_id=2", item["telegram_app_link"])
        self.assertEqual("", item["telegram_web_link"])
        self.assertFalse(item["has_public_link"])

    def test_absent_channels_scan_creates_job(self) -> None:
        from unittest.mock import patch

        with patch("tg_harvest.web.auth.is_authenticated", return_value=True):
            response = self.client.post("/api/admin/channels/absent/scan")

        self.assertEqual(200, response.status_code)
        self.assertEqual({"job_id": "job-1"}, response.get_json()["job"])


if __name__ == "__main__":
    unittest.main()
