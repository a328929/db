import unittest
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

from tg_harvest.app.services import AdminRouteServices
from tg_harvest.web.routes.admin import AdminRoutesHandler


class _ConnStub:
    def close(self):
        return None


class _LoggerStub:
    def exception(self, _message):
        return None


class AdminRoutesHandlerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = Flask(__name__)
        self.logs = []
        self.started_jobs = []
        self.started_harvests = []
        self.started_updates = []

        def append_log(job_id, message):
            self.logs.append((job_id, str(message)))

        def start_harvest(*args, **kwargs):
            self.started_harvests.append({"args": args, **kwargs})

        def start_update(*args, **kwargs):
            self.started_updates.append({"args": args, **kwargs})

        def start_cleanup_empty(**kwargs):
            self.started_jobs.append(kwargs)

        def start_delete_empty_chats(*args, **kwargs):
            self.started_jobs.append({"args": args, **kwargs})

        self.services = AdminRouteServices(
            logger=_LoggerStub(),
            cfg=object(),
            get_conn_fn=lambda: _ConnStub(),
            parse_admin_chat_id_fn=lambda value: value,
            build_admin_chats_payload_fn=lambda _conn: {"ok": True, "items": []},
            build_admin_stats_payload_fn=lambda _conn, _chat_id: ({"ok": True}, 200),
            build_admin_sync_stats_payload_fn=lambda _conn, **_kwargs: {"ok": True, "windows": []},
            build_admin_sync_live_messages_payload_fn=lambda _conn, **_kwargs: {"ok": True, "items": []},
            get_sync_health_snapshot_fn=lambda: {"status": "healthy", "reasons": [], "actions": [], "listener": {}},
            trigger_sync_remediation_fn=lambda: {"ok": True, "triggered": 0, "items": [], "message": "done"},
            admin_get_chat_brief_fn=lambda _conn, chat_id: {
                "chat_id": chat_id,
                "chat_title": f"chat-{chat_id}",
            },
            admin_job_get_snapshot_fn=lambda job_id: {
                "job_id": job_id,
                "status": "queued",
            },
            admin_job_get_logs_fn=lambda *_args, **_kwargs: [],
            admin_get_active_job_fn=lambda: None,
            admin_request_job_stop_fn=lambda *_args, **_kwargs: (True, None),
            admin_has_any_active_job_fn=lambda: False,
            admin_try_create_exclusive_job_fn=lambda *_args, **_kwargs: (
                {"job_id": "job-1"},
                None,
            ),
            admin_create_chat_job_if_absent_fn=lambda *_args, **_kwargs: (None, None),
            admin_job_create_fn=lambda *_args, **_kwargs: {"job_id": "job-1"},
            admin_job_append_log_fn=append_log,
            admin_start_harvest_job_thread_fn=start_harvest,
            admin_start_update_job_thread_fn=start_update,
            admin_start_delete_job_thread_fn=lambda *_args, **_kwargs: None,
            admin_start_delete_empty_chats_job_thread_fn=start_delete_empty_chats,
            admin_start_cleanup_job_thread_fn=lambda *_args, **_kwargs: None,
            admin_start_cleanup_empty_job_thread_fn=start_cleanup_empty,
            admin_make_job_log_handler_fn=lambda _job_id: None,
            admin_job_set_status_fn=lambda *_args, **_kwargs: True,
            admin_harvest_target_max_len=128,
            admin_cleanup_keyword_max_len=64,
        )
        self.handler = AdminRoutesHandler(services=self.services)

    def test_harvest_success_starts_job_with_normalized_target(self) -> None:
        with self.app.test_request_context(
            "/api/admin/jobs/harvest",
            method="POST",
            json={"target": "  @example_group  "},
        ):
            response = self.handler.api_admin_job_create_harvest.__wrapped__(
                self.handler
            )

        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(
            [("job-1", "已接收抓取目标：@example_group")],
            self.logs,
        )
        self.assertEqual(1, len(self.started_harvests))
        self.assertEqual(("job-1", "@example_group"), self.started_harvests[0]["args"])

    def test_harvest_marks_job_error_when_thread_start_fails(self) -> None:
        status_updates = []

        def fail_start(*_args, **_kwargs):
            raise RuntimeError("thread unavailable")

        self.handler.admin_start_harvest_job_thread_fn = fail_start
        self.handler.admin_job_set_status_fn = (
            lambda job_id, status: status_updates.append((job_id, status)) or True
        )

        with self.app.test_request_context(
            "/api/admin/jobs/harvest",
            method="POST",
            json={"target": "@example_group"},
        ):
            response, status_code = self.handler.api_admin_job_create_harvest.__wrapped__(
                self.handler
            )

        self.assertEqual(500, status_code)
        self.assertEqual("任务启动失败", response.get_json()["error"])
        self.assertEqual([("job-1", "error")], status_updates)
        self.assertIn(("job-1", "后台任务启动失败，已标记为失败"), self.logs)

    def test_update_all_success_starts_job_with_all_scope(self) -> None:
        with self.app.test_request_context(
            "/api/admin/jobs/update",
            method="POST",
            json={"chat_id": "all"},
        ):
            response = self.handler.api_admin_job_create_update.__wrapped__(
                self.handler
            )

        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(
            [
                ("job-1", "已接收增量更新请求"),
                ("job-1", "目标范围：全部群聊"),
            ],
            self.logs,
        )
        self.assertEqual(1, len(self.started_updates))
        self.assertEqual(("job-1", "all", "全部群聊"), self.started_updates[0]["args"])

    def test_update_single_chat_success_starts_job_with_resolved_target(self) -> None:
        with self.app.test_request_context(
            "/api/admin/jobs/update",
            method="POST",
            json={"chat_id": 42},
        ):
            response = self.handler.api_admin_job_create_update.__wrapped__(
                self.handler
            )

        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(
            [
                ("job-1", "已接收增量更新请求"),
                ("job-1", "目标群组：chat-42 (42)"),
            ],
            self.logs,
        )
        self.assertEqual(1, len(self.started_updates))
        self.assertEqual(("job-1", 42, "chat-42"), self.started_updates[0]["args"])

    def test_update_preflight_returns_summary(self) -> None:
        with patch(
            "tg_harvest.web.routes.admin.sync_scheduler.build_update_preflight",
            return_value={
                "ok": True,
                "target": {"target_count": 5},
                "account_capacity": {"available": 1, "configured": 2},
            },
        ) as preflight_mock, self.app.test_request_context(
            "/api/admin/jobs/update/preflight?chat_id=all",
            method="GET",
        ):
            response, status_code = self.handler.api_admin_update_preflight.__wrapped__(
                self.handler
            )

        payload = response.get_json()
        self.assertEqual(200, status_code)
        self.assertTrue(payload["ok"])
        self.assertEqual(5, payload["target"]["target_count"])
        self.assertEqual("all", preflight_mock.call_args.kwargs["chat_id"])

    def test_cleanup_empty_requires_json_body(self) -> None:
        with self.app.test_request_context("/api/admin/jobs/cleanup-empty", method="POST"):
            response, status_code = self.handler.api_admin_job_create_cleanup_empty.__wrapped__(self.handler)

        self.assertEqual(400, status_code)
        self.assertEqual("请求必须为 JSON", response.get_json()["error"])

    def test_cleanup_empty_chat_scope_returns_not_found_for_unknown_chat(self) -> None:
        self.handler.admin_get_chat_brief_fn = lambda _conn, _chat_id: None

        with self.app.test_request_context(
            "/api/admin/jobs/cleanup-empty",
            method="POST",
            json={"scope": "chat", "chat_id": 42},
        ):
            response, status_code = self.handler.api_admin_job_create_cleanup_empty.__wrapped__(self.handler)

        self.assertEqual(404, status_code)
        self.assertEqual("chat_id 不存在", response.get_json()["error"])

    def test_cleanup_empty_success_uses_shared_scope_resolution(self) -> None:
        with self.app.test_request_context(
            "/api/admin/jobs/cleanup-empty",
            method="POST",
            json={"scope": "chat", "chat_id": 42, "confirm": "CLEANUP_EMPTY:chat:42"},
        ):
            response = self.handler.api_admin_job_create_cleanup_empty.__wrapped__(self.handler)

        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(
            {
                "scope": "chat",
                "chat_id": 42,
                "target_label": "chat-42",
            },
            payload["request"],
        )
        self.assertEqual(1, len(self.started_jobs))
        self.assertEqual("chat-42", self.started_jobs[0]["target_label"])
        self.assertEqual(
            [
                ("job-1", "已接收不可搜索数据清理请求"),
                ("job-1", "作用范围：当前群组"),
                ("job-1", "目标：chat-42 (42)"),
            ],
            self.logs,
        )

    def test_delete_returns_conflict_response_from_shared_helper(self) -> None:
        self.handler.admin_try_create_exclusive_job_fn = lambda *_args, **_kwargs: (
            None,
            {"job_id": "existing-1", "status": "running"},
        )

        with self.app.test_request_context(
            "/api/admin/jobs/delete",
            method="POST",
            json={"chat_id": 7, "confirm": "DELETE:7"},
        ):
            response, status_code = self.handler.api_admin_job_create_delete.__wrapped__(self.handler)

        payload = response.get_json()
        self.assertEqual(409, status_code)
        self.assertEqual("当前已有进行中的任务，请等待完成后再试", payload["error"])
        self.assertEqual("existing-1", payload["existing_job"]["job_id"])

    def test_delete_requires_server_side_confirmation(self) -> None:
        with self.app.test_request_context(
            "/api/admin/jobs/delete",
            method="POST",
            json={"chat_id": 7},
        ):
            response, status_code = self.handler.api_admin_job_create_delete.__wrapped__(self.handler)

        self.assertEqual(400, status_code)
        self.assertEqual("confirm 参数不匹配", response.get_json()["error"])

    def test_delete_empty_chats_requires_server_side_confirmation(self) -> None:
        with self.app.test_request_context(
            "/api/admin/jobs/delete-empty-chats",
            method="POST",
            json={},
        ):
            response, status_code = (
                self.handler.api_admin_job_create_delete_empty_chats.__wrapped__(
                    self.handler
                )
            )

        self.assertEqual(400, status_code)
        self.assertEqual("confirm 参数不匹配", response.get_json()["error"])

    def test_delete_empty_chats_success_starts_job(self) -> None:
        with self.app.test_request_context(
            "/api/admin/jobs/delete-empty-chats",
            method="POST",
            json={"confirm": "DELETE_EMPTY_CHATS"},
        ):
            response = self.handler.api_admin_job_create_delete_empty_chats.__wrapped__(
                self.handler
            )

        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(
            [("job-1", "已接收零消息群组删除请求")],
            self.logs,
        )
        self.assertEqual(1, len(self.started_jobs))
        self.assertEqual(("job-1",), self.started_jobs[0]["args"])

    def test_active_job_returns_current_snapshot(self) -> None:
        self.handler.admin_get_active_job_fn = lambda: {
            "job_id": "job-active",
            "status": "running",
        }
        self.handler.admin_job_get_snapshot_fn = lambda job_id: {
            "job_id": job_id,
            "status": "running",
            "stop_requested": False,
        }

        with self.app.test_request_context("/api/admin/jobs/active", method="GET"):
            response = self.handler.api_admin_active_job.__wrapped__(self.handler)

        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual("job-active", payload["job"]["job_id"])

    def test_sync_stats_reads_payload_from_service(self) -> None:
        self.handler.build_admin_sync_stats_payload_fn = lambda _conn, **_kwargs: {
            "ok": True,
            "default_window_key": "live",
            "windows": [{"window_key": "10m", "message_count": 3}],
        }

        with self.app.test_request_context("/api/admin/sync/stats", method="GET"):
            response = self.handler.api_admin_sync_stats.__wrapped__(self.handler)

        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual("10m", payload["windows"][0]["window_key"])
        self.assertEqual(3, payload["windows"][0]["message_count"])

    def test_sync_live_messages_reads_payload_from_service(self) -> None:
        self.handler.build_admin_sync_live_messages_payload_fn = lambda _conn, **_kwargs: {
            "ok": True,
            "items": [{"chat_id": 1, "message_id": 101}],
        }

        with self.app.test_request_context(
            "/api/admin/sync/messages?limit=20",
            method="GET",
        ):
            response = self.handler.api_admin_sync_live_messages.__wrapped__(
                self.handler
            )

        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(1, payload["items"][0]["chat_id"])
        self.assertEqual(101, payload["items"][0]["message_id"])

    def test_sync_diagnose_returns_payload_from_service(self) -> None:
        self.handler.trigger_sync_remediation_fn = lambda: {
            "ok": True,
            "triggered": 1,
            "items": [{"chat_id": 1, "status": "changed"}],
            "message": "已完成即时轮巡探测",
        }

        with self.app.test_request_context("/api/admin/sync/diagnose", method="POST"):
            response, status_code = self.handler.api_admin_sync_diagnose.__wrapped__(
                self.handler
            )

        payload = response.get_json()
        self.assertEqual(200, status_code)
        self.assertTrue(payload["ok"])
        self.assertEqual(1, payload["triggered"])

    def test_sync_scheduler_returns_scheduler_summary(self) -> None:
        with patch(
            "tg_harvest.web.routes.admin.sync_scheduler.build_scheduler_summary",
            return_value={"enabled": True, "pending_count": 2},
        ) as build_summary, self.app.test_request_context(
            "/api/admin/sync/scheduler",
            method="GET",
        ):
            response = self.handler.api_admin_sync_scheduler.__wrapped__(
                self.handler
            )

        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(2, payload["scheduler"]["pending_count"])
        self.assertEqual(1, build_summary.call_count)

    def test_sync_chats_returns_filtered_scheduler_rows(self) -> None:
        with patch(
            "tg_harvest.web.routes.admin.sync_scheduler.list_scheduler_chats",
            return_value={
                "ok": True,
                "items": [{"chat_id": 7, "membership_scope": "both_joined"}],
                "count": 1,
            },
        ) as list_chats, self.app.test_request_context(
            "/api/admin/sync/chats?membership=both_joined&status=pending&limit=20&offset=5",
            method="GET",
        ):
            response = self.handler.api_admin_sync_chats.__wrapped__(self.handler)

        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(7, payload["items"][0]["chat_id"])
        self.assertEqual("both_joined", list_chats.call_args.kwargs["membership"])
        self.assertEqual("pending", list_chats.call_args.kwargs["status"])
        self.assertEqual(20, list_chats.call_args.kwargs["limit"])
        self.assertEqual(5, list_chats.call_args.kwargs["offset"])

    def test_sync_chat_probe_uses_listener_runtime(self) -> None:
        runtime = SimpleNamespace(
            trigger_manual_chat_probe=lambda chat_id: {
                "ok": True,
                "triggered": 1,
                "items": [{"chat_id": chat_id, "status": "changed"}],
                "message": "done",
            }
        )
        with patch(
            "tg_harvest.web.routes.admin.get_database_chat_listener_runtime",
            return_value=runtime,
        ), self.app.test_request_context(
            "/api/admin/sync/chats/42/probe",
            method="POST",
        ):
            response, status_code = self.handler.api_admin_sync_chat_probe.__wrapped__(
                self.handler,
                42,
            )

        payload = response.get_json()
        self.assertEqual(200, status_code)
        self.assertTrue(payload["ok"])
        self.assertEqual(42, payload["items"][0]["chat_id"])

    def test_sync_model_reset_returns_reset_payload(self) -> None:
        with patch(
            "tg_harvest.web.routes.admin.sync_scheduler.reset_model_state",
            return_value={"ok": True, "removed_artifact": True},
        ), self.app.test_request_context("/api/admin/sync/model/reset", method="POST"):
            response = self.handler.api_admin_sync_model_reset.__wrapped__(
                self.handler
            )

        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertTrue(payload["removed_artifact"])

    def test_job_stop_marks_active_job_and_logs_request(self) -> None:
        stop_calls = []
        self.handler.admin_job_get_snapshot_fn = lambda job_id: {
            "job_id": job_id,
            "status": "running",
            "stop_requested": bool(stop_calls),
        }
        self.handler.admin_request_job_stop_fn = (
            lambda job_id: stop_calls.append(job_id) or (True, None)
        )

        with self.app.test_request_context("/api/admin/jobs/job-1/stop", method="POST"):
            response = self.handler.api_admin_job_stop.__wrapped__(
                self.handler, "job-1"
            )

        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(["job-1"], stop_calls)
        self.assertIn(
            ("job-1", "已收到停止请求，当前群组完成后停止派发新群组"),
            self.logs,
        )


if __name__ == "__main__":
    unittest.main()
