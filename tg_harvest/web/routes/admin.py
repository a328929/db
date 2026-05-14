# -*- coding: utf-8 -*-
import sqlite3
from contextlib import closing
from typing import Any, Optional

from flask import jsonify, request
from tg_harvest.app.services import AdminRouteServices
from tg_harvest.web.auth import admin_login_required


class AdminRoutesHandler:
    services: AdminRouteServices

    def __init__(
        self,
        *,
        services: Optional[AdminRouteServices] = None,
        **kwargs,
    ):
        self.services = services or AdminRouteServices(**kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.services, name)

    def _json_error(self, message: str, status_code: int):
        return jsonify({"ok": False, "error": str(message)}), int(status_code)

    def _require_json_dict(self):
        if not request.is_json:
            return None, self._json_error("请求必须为 JSON", 400)

        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return None, self._json_error("请求 JSON 格式错误", 400)
        return data, None

    def _conflict_response(self, existing_job: Any):
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "当前已有进行中的任务，请等待完成后再试",
                    "existing_job": existing_job,
                }
            ),
            409,
        )

    def _created_job_snapshot_response(self, job_id: str, **extra):
        snapshot = self.admin_job_get_snapshot_fn(job_id)
        if snapshot is None:
            return self._json_error("任务创建失败", 500)

        payload = {"ok": True, "job": snapshot}
        payload.update(extra)
        return jsonify(payload)

    def _create_exclusive_job_or_response(
        self,
        job_type: str,
        *,
        target_chat_id: Optional[int] = None,
        target_label: Optional[str] = None,
    ):
        job, existing_job = self.admin_try_create_exclusive_job_fn(
            job_type,
            target_chat_id=target_chat_id,
            target_label=target_label,
        )
        if existing_job is not None:
            return None, self._conflict_response(existing_job)
        return str(job.get("job_id") or ""), None

    def _load_chat_brief_or_response(self, chat_id: int):
        try:
            with closing(self.get_conn_fn()) as conn:
                chat_brief = self.admin_get_chat_brief_fn(conn, int(chat_id))
        except sqlite3.Error:
            self.logger.exception("读取群信息失败")
            return None, self._json_error("读取群信息失败", 500)
        except Exception:
            self.logger.exception("系统异常")
            return None, self._json_error("系统异常", 500)

        if chat_brief is None:
            return None, self._json_error("chat_id 不存在", 404)
        return chat_brief, None

    def _resolve_chat_target_or_response(self, raw_chat_id: Any):
        try:
            chat_id = int(raw_chat_id)
        except (TypeError, ValueError):
            return None, None, self._json_error("chat_id 参数非法", 400)

        chat_brief, error_response = self._load_chat_brief_or_response(chat_id)
        if error_response is not None:
            return None, None, error_response
        return chat_id, str(chat_brief["chat_title"]), None

    def _resolve_scope_target_or_response(self, data: dict):
        raw_scope = data.get("scope", "")
        scope = str(raw_scope or "").strip().lower()
        if scope not in {"all", "chat"}:
            return None, None, None, self._json_error(
                "scope 参数必须为 all 或 chat", 400
            )

        if scope == "all":
            return scope, None, "全部数据", None

        chat_id, chat_title, error_response = self._resolve_chat_target_or_response(
            data.get("chat_id")
        )
        if error_response is not None:
            return None, None, None, error_response
        return scope, chat_id, chat_title, None

    def _require_confirmation(self, data: dict, expected: str):
        supplied = str(data.get("confirm") or "").strip()
        if supplied != expected:
            return self._json_error("confirm 参数不匹配", 400)
        return None

    @admin_login_required
    def api_admin_chats(self):
        try:
            with closing(self.get_conn_fn()) as conn:
                payload = self.build_admin_chats_payload_fn(conn)
            return jsonify(payload)
        except sqlite3.Error:
            self.logger.exception("读取后台群列表失败")
            return jsonify({"ok": False, "error": "读取后台群列表失败"}), 500
        except Exception:
            self.logger.exception("系统异常")
            return jsonify({"ok": False, "error": "系统异常"}), 500

    @admin_login_required
    def api_admin_stats(self):
        try:
            chat_id = self.parse_admin_chat_id_fn(request.args.get("chat_id"))
        except (ValueError, TypeError):
            return jsonify({"ok": False, "error": "chat_id 参数非法"}), 400

        try:
            with closing(self.get_conn_fn()) as conn:
                payload, status_code = self.build_admin_stats_payload_fn(conn, chat_id)
            return jsonify(payload), status_code
        except sqlite3.Error:
            self.logger.exception("读取后台统计失败")
            return jsonify({"ok": False, "error": "读取后台统计失败"}), 500
        except Exception:
            self.logger.exception("系统异常")
            return jsonify({"ok": False, "error": "系统异常"}), 500

    @admin_login_required
    def api_admin_job_snapshot(self, job_id: str):
        snapshot = self.admin_job_get_snapshot_fn(job_id)
        if snapshot is None:
            return jsonify({"ok": False, "error": "任务不存在"}), 404
        return jsonify({"ok": True, "job": snapshot})

    @admin_login_required
    def api_admin_job_logs(self, job_id: str):
        raw_after_seq = (request.args.get("after_seq") or "").strip()
        try:
            after_seq = int(raw_after_seq) if raw_after_seq else 0
            if after_seq < 0:
                raise ValueError()
        except (ValueError, TypeError):
            return jsonify({"ok": False, "error": "after_seq 参数非法"}), 400

        logs = self.admin_job_get_logs_fn(job_id, after_seq=after_seq)
        if logs is None:
            return jsonify({"ok": False, "error": "任务不存在"}), 404
        return jsonify(
            {"ok": True, "job_id": job_id, "after_seq": after_seq, "logs": logs}
        )

    @admin_login_required
    def api_admin_job_create_harvest(self):
        data, error_response = self._require_json_dict()
        if error_response is not None:
            return error_response

        raw_target = data.get("target", "")
        if not isinstance(raw_target, str):
            return self._json_error("target 参数必须为字符串", 400)

        target = raw_target.strip()
        if not target:
            return self._json_error("target 不能为空", 400)
        if len(target) > self.admin_harvest_target_max_len:
            return self._json_error(
                f"target 长度不能超过 {self.admin_harvest_target_max_len}",
                400,
            )

        job_id, error_response = self._create_exclusive_job_or_response(
            "harvest",
            target_chat_id=None,
            target_label=target,
        )
        if error_response is not None:
            return error_response

        self.admin_job_append_log_fn(job_id, f"已接收抓取目标：{target}")
        self.admin_start_harvest_job_thread_fn(
            job_id,
            target,
            cfg=self.cfg,
            get_conn_fn=self.get_conn_fn,
            admin_make_job_log_handler_fn=self.admin_make_job_log_handler_fn,
            admin_job_set_status_fn=self.admin_job_set_status_fn,
            admin_job_append_log_fn=self.admin_job_append_log_fn,
        )
        return self._created_job_snapshot_response(job_id)

    @admin_login_required
    def api_admin_job_create_update(self):
        data, error_response = self._require_json_dict()
        if error_response is not None:
            return error_response

        raw_chat_id = data.get("chat_id")
        is_all_scope = (
            isinstance(raw_chat_id, str) and raw_chat_id.strip().lower() == "all"
        )

        if is_all_scope:
            job_id, error_response = self._create_exclusive_job_or_response(
                "update",
                target_chat_id=None,
                target_label="全部群聊",
            )
            if error_response is not None:
                return error_response

            self.admin_job_append_log_fn(job_id, "已接收增量更新请求")
            self.admin_job_append_log_fn(job_id, "目标范围：全部群聊")
            self.admin_start_update_job_thread_fn(
                job_id,
                "all",
                "全部群聊",
                cfg=self.cfg,
                get_conn_fn=self.get_conn_fn,
                admin_make_job_log_handler_fn=self.admin_make_job_log_handler_fn,
                admin_job_set_status_fn=self.admin_job_set_status_fn,
                admin_job_append_log_fn=self.admin_job_append_log_fn,
            )
            return self._created_job_snapshot_response(job_id)

        chat_id, chat_title, error_response = self._resolve_chat_target_or_response(
            raw_chat_id
        )
        if error_response is not None:
            return error_response

        job_id, error_response = self._create_exclusive_job_or_response(
            "update",
            target_chat_id=chat_id,
            target_label=chat_title,
        )
        if error_response is not None:
            return error_response

        self.admin_job_append_log_fn(job_id, "已接收增量更新请求")
        self.admin_job_append_log_fn(job_id, f"目标群组：{chat_title} ({chat_id})")
        self.admin_start_update_job_thread_fn(
            job_id,
            chat_id,
            chat_title,
            cfg=self.cfg,
            get_conn_fn=self.get_conn_fn,
            admin_make_job_log_handler_fn=self.admin_make_job_log_handler_fn,
            admin_job_set_status_fn=self.admin_job_set_status_fn,
            admin_job_append_log_fn=self.admin_job_append_log_fn,
        )
        return self._created_job_snapshot_response(job_id)

    @admin_login_required
    def api_admin_job_create_delete(self):
        data, error_response = self._require_json_dict()
        if error_response is not None:
            return error_response

        chat_id, chat_title, error_response = self._resolve_chat_target_or_response(
            data.get("chat_id")
        )
        if error_response is not None:
            return error_response

        error_response = self._require_confirmation(data, f"DELETE:{chat_id}")
        if error_response is not None:
            return error_response

        job_id, error_response = self._create_exclusive_job_or_response(
            "delete",
            target_chat_id=chat_id,
            target_label=chat_title,
        )
        if error_response is not None:
            return error_response

        self.admin_job_append_log_fn(job_id, "已接收删除请求")
        self.admin_job_append_log_fn(job_id, f"目标群组：{chat_title} ({chat_id})")
        self.admin_start_delete_job_thread_fn(
            job_id,
            chat_id,
            chat_title,
            get_conn_fn=self.get_conn_fn,
            admin_job_set_status_fn=self.admin_job_set_status_fn,
            admin_job_append_log_fn=self.admin_job_append_log_fn,
        )
        return self._created_job_snapshot_response(job_id)

    @admin_login_required
    def api_admin_job_create_cleanup(self):
        data, error_response = self._require_json_dict()
        if error_response is not None:
            return error_response

        raw_keyword = data.get("keyword")
        if not isinstance(raw_keyword, str):
            return self._json_error("keyword 参数必须为字符串", 400)

        keyword = raw_keyword.strip()
        if not keyword:
            return self._json_error("keyword 不能为空", 400)
        if len(keyword) > self.admin_cleanup_keyword_max_len:
            return self._json_error(
                f"keyword 长度不能超过 {self.admin_cleanup_keyword_max_len}",
                400,
            )

        scope, chat_id, target_label, error_response = (
            self._resolve_scope_target_or_response(data)
        )
        if error_response is not None:
            return error_response

        confirm_target = "all" if chat_id is None else str(chat_id)
        error_response = self._require_confirmation(
            data, f"CLEANUP:{scope}:{confirm_target}:{keyword}"
        )
        if error_response is not None:
            return error_response

        job_id, error_response = self._create_exclusive_job_or_response(
            "cleanup",
            target_chat_id=chat_id,
            target_label=target_label,
        )
        if error_response is not None:
            return error_response

        self.admin_job_append_log_fn(job_id, "已接收垃圾清理请求")
        scope_label = {"all": "全部数据", "chat": "当前群组"}.get(scope, scope)
        self.admin_job_append_log_fn(job_id, f"作用范围：{scope_label}")
        chat_suffix = "" if chat_id is None else f" ({chat_id})"
        self.admin_job_append_log_fn(job_id, f"目标：{target_label}{chat_suffix}")
        self.admin_job_append_log_fn(job_id, f"关键字：{keyword}")
        self.admin_start_cleanup_job_thread_fn(
            job_id=job_id,
            keyword=keyword,
            scope=scope,
            chat_id=chat_id,
            target_label=target_label,
            get_conn_fn=self.get_conn_fn,
            admin_job_set_status_fn=self.admin_job_set_status_fn,
            admin_job_append_log_fn=self.admin_job_append_log_fn,
        )
        return self._created_job_snapshot_response(
            job_id,
            request={
                "scope": scope,
                "chat_id": chat_id,
                "target_label": target_label,
                "keyword": keyword,
            },
        )

    @admin_login_required
    def api_admin_job_create_cleanup_empty(self):
        data, error_response = self._require_json_dict()
        if error_response is not None:
            return error_response

        scope, chat_id, target_label, error_response = (
            self._resolve_scope_target_or_response(data)
        )
        if error_response is not None:
            return error_response

        confirm_target = "all" if chat_id is None else str(chat_id)
        error_response = self._require_confirmation(
            data, f"CLEANUP_EMPTY:{scope}:{confirm_target}"
        )
        if error_response is not None:
            return error_response

        job_id, error_response = self._create_exclusive_job_or_response(
            "cleanup_empty",
            target_chat_id=chat_id,
            target_label=target_label,
        )
        if error_response is not None:
            return error_response

        self.admin_job_append_log_fn(job_id, "已接收不可搜索数据清理请求")
        scope_label = {"all": "全部数据", "chat": "当前群组"}.get(scope, scope)
        self.admin_job_append_log_fn(job_id, f"作用范围：{scope_label}")
        chat_suffix = "" if chat_id is None else f" ({chat_id})"
        self.admin_job_append_log_fn(job_id, f"目标：{target_label}{chat_suffix}")
        self.admin_start_cleanup_empty_job_thread_fn(
            job_id=job_id,
            scope=scope,
            chat_id=chat_id,
            target_label=target_label,
            get_conn_fn=self.get_conn_fn,
            admin_job_set_status_fn=self.admin_job_set_status_fn,
            admin_job_append_log_fn=self.admin_job_append_log_fn,
        )
        return self._created_job_snapshot_response(
            job_id,
            request={
                "scope": scope,
                "chat_id": chat_id,
                "target_label": target_label,
            },
        )


def register_admin_routes(app, *, services: Optional[AdminRouteServices] = None, **kwargs) -> None:
    handler = AdminRoutesHandler(services=services, **kwargs)

    app.get("/api/admin/chats")(handler.api_admin_chats)
    app.get("/api/admin/stats")(handler.api_admin_stats)
    app.get("/api/admin/jobs/<job_id>")(handler.api_admin_job_snapshot)
    app.get("/api/admin/jobs/<job_id>/logs")(handler.api_admin_job_logs)
    app.post("/api/admin/jobs/harvest")(handler.api_admin_job_create_harvest)
    app.post("/api/admin/jobs/update")(handler.api_admin_job_create_update)
    app.post("/api/admin/jobs/delete")(handler.api_admin_job_create_delete)
    app.post("/api/admin/jobs/cleanup")(handler.api_admin_job_create_cleanup)
    app.post("/api/admin/jobs/cleanup-empty")(
        handler.api_admin_job_create_cleanup_empty
    )
