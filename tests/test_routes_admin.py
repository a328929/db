import unittest

from flask import Flask

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

        def append_log(job_id, message):
            self.logs.append((job_id, str(message)))

        def start_cleanup_empty(**kwargs):
            self.started_jobs.append(kwargs)

        def start_delete_empty_chats(*args, **kwargs):
            self.started_jobs.append({"args": args, **kwargs})

        self.handler = AdminRoutesHandler(
            logger=_LoggerStub(),
            cfg=object(),
            get_conn_fn=lambda: _ConnStub(),
            admin_make_job_log_handler_fn=lambda _job_id: None,
            admin_job_set_status_fn=lambda *_args, **_kwargs: True,
            admin_job_append_log_fn=append_log,
            admin_job_get_snapshot_fn=lambda job_id: {"job_id": job_id, "status": "queued"},
            admin_job_get_logs_fn=lambda *_args, **_kwargs: [],
            admin_get_active_job_fn=lambda: None,
            admin_request_job_stop_fn=lambda *_args, **_kwargs: (True, None),
            admin_has_any_active_job_fn=lambda: False,
            admin_try_create_exclusive_job_fn=lambda *_args, **_kwargs: (
                {"job_id": "job-1"},
                None,
            ),
            admin_job_create_fn=lambda *_args, **_kwargs: {"job_id": "job-1"},
            admin_start_harvest_job_thread_fn=lambda *_args, **_kwargs: None,
            admin_start_update_job_thread_fn=lambda *_args, **_kwargs: None,
            admin_start_delete_job_thread_fn=lambda *_args, **_kwargs: None,
            admin_start_delete_empty_chats_job_thread_fn=start_delete_empty_chats,
            admin_start_cleanup_job_thread_fn=lambda *_args, **_kwargs: None,
            admin_start_cleanup_empty_job_thread_fn=start_cleanup_empty,
            admin_get_chat_brief_fn=lambda _conn, chat_id: {
                "chat_id": chat_id,
                "chat_title": f"chat-{chat_id}",
            },
            admin_create_chat_job_if_absent_fn=lambda *_args, **_kwargs: (None, None),
            parse_admin_chat_id_fn=lambda value: value,
            build_admin_chats_payload_fn=lambda _conn: {"ok": True, "items": []},
            build_admin_stats_payload_fn=lambda _conn, _chat_id: ({"ok": True}, 200),
            admin_harvest_target_max_len=128,
            admin_cleanup_keyword_max_len=64,
        )

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
