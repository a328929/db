import sqlite3
from contextlib import closing
from typing import Any

from flask import jsonify, request

from tg_harvest.app.services import AdminRouteServices
from tg_harvest.runtime.db_listener import get_database_chat_listener_runtime
from tg_harvest.storage import sync_scheduler
from tg_harvest.web.auth import admin_login_required
from tg_harvest.web.responses import (
    create_started_exclusive_job_response,
    json_error,
    require_json_dict,
)


class AdminRoutesHandler:
    services: AdminRouteServices

    def __init__(
        self,
        *,
        services: AdminRouteServices,
    ):
        self.services = services
        self.logger = services.logger
        self.cfg = services.cfg
        self.get_conn_fn = services.get_conn_fn
        self.parse_admin_chat_id_fn = services.parse_admin_chat_id_fn
        self.build_admin_chats_payload_fn = services.build_admin_chats_payload_fn
        self.build_admin_stats_payload_fn = services.build_admin_stats_payload_fn
        self.build_admin_sync_stats_payload_fn = (
            services.build_admin_sync_stats_payload_fn
        )
        self.build_admin_sync_live_messages_payload_fn = (
            services.build_admin_sync_live_messages_payload_fn
        )
        self.build_admin_storage_health_payload_fn = (
            services.build_admin_storage_health_payload_fn
        )
        self.get_sync_health_snapshot_fn = services.get_sync_health_snapshot_fn
        self.trigger_sync_remediation_fn = services.trigger_sync_remediation_fn
        self.admin_get_chat_brief_fn = services.admin_get_chat_brief_fn
        self.admin_job_get_snapshot_fn = services.admin_job_get_snapshot_fn
        self.admin_job_get_logs_fn = services.admin_job_get_logs_fn
        self.admin_get_active_job_fn = services.admin_get_active_job_fn
        self.admin_request_job_stop_fn = services.admin_request_job_stop_fn
        self.admin_has_any_active_job_fn = services.admin_has_any_active_job_fn
        self.admin_try_create_exclusive_job_fn = (
            services.admin_try_create_exclusive_job_fn
        )
        self.admin_create_chat_job_if_absent_fn = (
            services.admin_create_chat_job_if_absent_fn
        )
        self.admin_job_create_fn = services.admin_job_create_fn
        self.admin_job_append_log_fn = services.admin_job_append_log_fn
        self.admin_start_harvest_job_thread_fn = (
            services.admin_start_harvest_job_thread_fn
        )
        self.admin_start_update_job_thread_fn = (
            services.admin_start_update_job_thread_fn
        )
        self.admin_start_delete_job_thread_fn = (
            services.admin_start_delete_job_thread_fn
        )
        self.admin_start_delete_empty_chats_job_thread_fn = (
            services.admin_start_delete_empty_chats_job_thread_fn
        )
        self.admin_start_cleanup_job_thread_fn = (
            services.admin_start_cleanup_job_thread_fn
        )
        self.admin_start_cleanup_empty_job_thread_fn = (
            services.admin_start_cleanup_empty_job_thread_fn
        )
        self.admin_make_job_log_handler_fn = services.admin_make_job_log_handler_fn
        self.admin_job_set_status_fn = services.admin_job_set_status_fn
        self.admin_harvest_target_max_len = services.admin_harvest_target_max_len
        self.admin_cleanup_keyword_max_len = services.admin_cleanup_keyword_max_len

    def _json_error(self, message: str, status_code: int):
        return json_error(message, status_code)

    def _require_json_dict(self):
        return require_json_dict()

    def _create_started_exclusive_job_response(
        self,
        *,
        job_type: str,
        target_chat_id: int | None = None,
        target_label: str | None = None,
        initial_logs: tuple[str, ...] | list[str] = (),
        start_job_fn,
        response_extra: dict[str, Any] | None = None,
    ):
        return create_started_exclusive_job_response(
            self.admin_try_create_exclusive_job_fn,
            self.admin_job_get_snapshot_fn,
            job_type=job_type,
            target_chat_id=target_chat_id,
            target_label=target_label,
            append_log_fn=self.admin_job_append_log_fn,
            set_status_fn=self.admin_job_set_status_fn,
            initial_logs=initial_logs,
            start_job_fn=start_job_fn,
            response_extra=response_extra,
        )

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

    def _resolve_harvest_target_or_response(self, data: dict):
        raw_target = data.get("target", "")
        if not isinstance(raw_target, str):
            return None, self._json_error("target 参数必须为字符串", 400)

        target = raw_target.strip()
        if not target:
            return None, self._json_error("target 不能为空", 400)
        if len(target) > self.admin_harvest_target_max_len:
            return None, self._json_error(
                f"target 长度不能超过 {self.admin_harvest_target_max_len}",
                400,
            )
        return target, None

    def _build_scope_target_logs(
        self,
        *,
        scope: str,
        chat_id: int | None,
        target_label: str,
    ) -> list[str]:
        scope_label = {"all": "全部数据", "chat": "当前群组"}.get(scope, scope)
        chat_suffix = "" if chat_id is None else f" ({chat_id})"
        return [
            f"作用范围：{scope_label}",
            f"目标：{target_label}{chat_suffix}",
        ]

    @admin_login_required
    def api_admin_chats(self):
        try:
            with closing(self.get_conn_fn()) as conn:
                payload = self.build_admin_chats_payload_fn(conn)
            return jsonify(payload)
        except sqlite3.Error:
            self.logger.exception("读取后台群列表失败")
            return self._json_error("读取后台群列表失败", 500)
        except Exception:
            self.logger.exception("系统异常")
            return self._json_error("系统异常", 500)

    @admin_login_required
    def api_admin_stats(self):
        try:
            chat_id = self.parse_admin_chat_id_fn(request.args.get("chat_id"))
        except (ValueError, TypeError):
            return self._json_error("chat_id 参数非法", 400)

        try:
            with closing(self.get_conn_fn()) as conn:
                payload, status_code = self.build_admin_stats_payload_fn(conn, chat_id)
            return jsonify(payload), status_code
        except sqlite3.Error:
            self.logger.exception("读取后台统计失败")
            return self._json_error("读取后台统计失败", 500)
        except Exception:
            self.logger.exception("系统异常")
            return self._json_error("系统异常", 500)

    @admin_login_required
    def api_admin_sync_stats(self):
        try:
            with closing(self.get_conn_fn()) as conn:
                payload = self.build_admin_sync_stats_payload_fn(
                    conn,
                    health_snapshot=self.get_sync_health_snapshot_fn(),
                )
            return jsonify(payload)
        except sqlite3.Error:
            self.logger.exception("读取消息同步统计失败")
            return self._json_error("读取消息同步统计失败", 500)
        except Exception:
            self.logger.exception("系统异常")
            return self._json_error("系统异常", 500)

    @admin_login_required
    def api_admin_sync_live_messages(self):
        raw_limit = str(request.args.get("limit", "") or "").strip()
        try:
            limit = int(raw_limit) if raw_limit else 50
        except (TypeError, ValueError):
            return self._json_error("limit 参数非法", 400)
        if limit <= 0:
            return self._json_error("limit 参数非法", 400)

        try:
            with closing(self.get_conn_fn()) as conn:
                payload = self.build_admin_sync_live_messages_payload_fn(
                    conn,
                    limit=limit,
                )
            return jsonify(payload)
        except sqlite3.Error:
            self.logger.exception("读取实时同步消息失败")
            return self._json_error("读取实时同步消息失败", 500)
        except Exception:
            self.logger.exception("系统异常")
            return self._json_error("系统异常", 500)

    @admin_login_required
    def api_admin_storage_health(self):
        if self.build_admin_storage_health_payload_fn is None:
            return self._json_error("数据库健康状态未配置", 503)
        try:
            with closing(self.get_conn_fn()) as conn:
                payload = self.build_admin_storage_health_payload_fn(
                    conn,
                    cfg=self.cfg,
                )
            return jsonify(payload)
        except sqlite3.Error:
            self.logger.exception("读取数据库容量健康状态失败")
            return self._json_error("读取数据库容量健康状态失败", 500)
        except Exception:
            self.logger.exception("系统异常")
            return self._json_error("系统异常", 500)

    @admin_login_required
    def api_admin_sync_diagnose(self):
        try:
            payload = self.trigger_sync_remediation_fn()
            status_code = 200 if bool(payload.get("ok")) else 503
            return jsonify(payload), status_code
        except Exception:
            self.logger.exception("执行消息同步诊断失败")
            return self._json_error("执行消息同步诊断失败", 500)

    @admin_login_required
    def api_admin_sync_scheduler(self):
        try:
            health_snapshot = self.get_sync_health_snapshot_fn()
            with closing(self.get_conn_fn()) as conn:
                scheduler_payload = sync_scheduler.build_scheduler_summary(
                    conn,
                    health_snapshot=health_snapshot,
                )
            return jsonify({"ok": True, "scheduler": scheduler_payload})
        except sqlite3.Error:
            self.logger.exception("读取同步调度状态失败")
            return self._json_error("读取同步调度状态失败", 500)
        except Exception:
            self.logger.exception("系统异常")
            return self._json_error("系统异常", 500)

    @admin_login_required
    def api_admin_sync_chats(self):
        raw_limit = str(request.args.get("limit", "") or "").strip()
        raw_offset = str(request.args.get("offset", "") or "").strip()
        try:
            limit = int(raw_limit) if raw_limit else 100
            offset = int(raw_offset) if raw_offset else 0
        except (TypeError, ValueError):
            return self._json_error("分页参数非法", 400)
        try:
            with closing(self.get_conn_fn()) as conn:
                payload = sync_scheduler.list_scheduler_chats(
                    conn,
                    membership=str(request.args.get("membership", "") or "").strip(),
                    status=str(request.args.get("status", "") or "").strip(),
                    limit=limit,
                    offset=offset,
                )
            return jsonify(payload)
        except sqlite3.Error:
            self.logger.exception("读取同步调度群组状态失败")
            return self._json_error("读取同步调度群组状态失败", 500)
        except Exception:
            self.logger.exception("系统异常")
            return self._json_error("系统异常", 500)

    @admin_login_required
    def api_admin_sync_chat_probe(self, chat_id: int):
        try:
            safe_chat_id = int(chat_id)
        except (TypeError, ValueError):
            return self._json_error("chat_id 参数非法", 400)
        runtime = get_database_chat_listener_runtime()
        if runtime is None:
            return self._json_error("数据库监听运行时尚未初始化", 503)
        try:
            payload = runtime.trigger_manual_chat_probe(safe_chat_id)
            status_code = 200 if bool(payload.get("ok")) else 503
            if str(payload.get("message") or "") == "chat_id 不存在":
                status_code = 404
            return jsonify(payload), status_code
        except Exception:
            self.logger.exception("执行单群同步调度诊断失败")
            return self._json_error("执行单群同步调度诊断失败", 500)

    @admin_login_required
    def api_admin_sync_model_reset(self):
        try:
            with closing(self.get_conn_fn()) as conn:
                payload = sync_scheduler.reset_model_state(conn)
            return jsonify(payload)
        except sqlite3.Error:
            self.logger.exception("重置同步模型状态失败")
            return self._json_error("重置同步模型状态失败", 500)
        except Exception:
            self.logger.exception("系统异常")
            return self._json_error("系统异常", 500)

    @admin_login_required
    def api_admin_job_snapshot(self, job_id: str):
        snapshot = self.admin_job_get_snapshot_fn(job_id)
        if snapshot is None:
            return self._json_error("任务不存在", 404)
        return jsonify({"ok": True, "job": snapshot})

    @admin_login_required
    def api_admin_job_logs(self, job_id: str):
        raw_after_seq = (request.args.get("after_seq") or "").strip()
        try:
            after_seq = int(raw_after_seq) if raw_after_seq else 0
            if after_seq < 0:
                raise ValueError()
        except (ValueError, TypeError):
            return self._json_error("after_seq 参数非法", 400)

        logs = self.admin_job_get_logs_fn(job_id, after_seq=after_seq)
        if logs is None:
            return self._json_error("任务不存在", 404)
        return jsonify(
            {"ok": True, "job_id": job_id, "after_seq": after_seq, "logs": logs}
        )

    @admin_login_required
    def api_admin_active_job(self):
        active_job = self.admin_get_active_job_fn()
        if active_job is None:
            return jsonify({"ok": True, "job": None})

        job_id = str(active_job.get("job_id") or "")
        snapshot = self.admin_job_get_snapshot_fn(job_id) if job_id else None
        return jsonify({"ok": True, "job": snapshot or active_job})

    @admin_login_required
    def api_admin_job_stop(self, job_id: str):
        snapshot = self.admin_job_get_snapshot_fn(job_id)
        if snapshot is None:
            return self._json_error("任务不存在", 404)

        status = str(snapshot.get("status") or "").lower()
        if status not in {"queued", "running"}:
            return self._json_error("任务已结束，不能停止", 409)

        ok, error_message = self.admin_request_job_stop_fn(job_id)
        if not ok:
            status_code = 404 if error_message == "任务不存在" else 409
            return self._json_error(error_message or "停止请求失败", status_code)

        self.admin_job_append_log_fn(job_id, "已收到停止请求，当前群组完成后停止派发新群组")
        snapshot = self.admin_job_get_snapshot_fn(job_id)
        return jsonify({"ok": True, "job": snapshot})

    @admin_login_required
    def api_admin_job_create_harvest(self):
        data, error_response = self._require_json_dict()
        if error_response is not None:
            return error_response

        target, error_response = self._resolve_harvest_target_or_response(data)
        if error_response is not None:
            return error_response

        return self._create_started_exclusive_job_response(
            job_type="harvest",
            target_chat_id=None,
            target_label=target,
            initial_logs=[f"已接收抓取目标：{target}"],
            start_job_fn=lambda job_id: self.admin_start_harvest_job_thread_fn(
                job_id,
                target,
                cfg=self.cfg,
                get_conn_fn=self.get_conn_fn,
                admin_make_job_log_handler_fn=self.admin_make_job_log_handler_fn,
                admin_job_set_status_fn=self.admin_job_set_status_fn,
                admin_job_append_log_fn=self.admin_job_append_log_fn,
            ),
        )

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
            return self._create_started_exclusive_job_response(
                job_type="update",
                target_chat_id=None,
                target_label="全部群聊",
                initial_logs=[
                    "已接收增量更新请求",
                    "目标范围：全部群聊",
                ],
                start_job_fn=lambda job_id: self.admin_start_update_job_thread_fn(
                    job_id,
                    "all",
                    "全部群聊",
                    cfg=self.cfg,
                    get_conn_fn=self.get_conn_fn,
                    admin_make_job_log_handler_fn=self.admin_make_job_log_handler_fn,
                    admin_job_set_status_fn=self.admin_job_set_status_fn,
                    admin_job_append_log_fn=self.admin_job_append_log_fn,
                ),
            )

        chat_id, chat_title, error_response = self._resolve_chat_target_or_response(
            raw_chat_id
        )
        if error_response is not None:
            return error_response

        return self._create_started_exclusive_job_response(
            job_type="update",
            target_chat_id=chat_id,
            target_label=chat_title,
            initial_logs=[
                "已接收增量更新请求",
                f"目标群组：{chat_title} ({chat_id})",
            ],
            start_job_fn=lambda job_id: self.admin_start_update_job_thread_fn(
                job_id,
                chat_id,
                chat_title,
                cfg=self.cfg,
                get_conn_fn=self.get_conn_fn,
                admin_make_job_log_handler_fn=self.admin_make_job_log_handler_fn,
                admin_job_set_status_fn=self.admin_job_set_status_fn,
                admin_job_append_log_fn=self.admin_job_append_log_fn,
            ),
        )

    @admin_login_required
    def api_admin_update_preflight(self):
        raw_chat_id = str(request.args.get("chat_id", "all") or "all").strip()
        if not raw_chat_id:
            raw_chat_id = "all"
        try:
            with closing(self.get_conn_fn()) as conn:
                payload = sync_scheduler.build_update_preflight(
                    conn,
                    self.cfg,
                    chat_id=raw_chat_id,
                    health_snapshot=self.get_sync_health_snapshot_fn(),
                )
            status_code = 200 if bool(payload.get("ok", True)) else 404
            return jsonify(payload), status_code
        except (TypeError, ValueError):
            return self._json_error("chat_id 参数非法", 400)
        except sqlite3.Error:
            self.logger.exception("读取批量更新预检失败")
            return self._json_error("读取批量更新预检失败", 500)
        except Exception:
            self.logger.exception("系统异常")
            return self._json_error("系统异常", 500)

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

        return self._create_started_exclusive_job_response(
            job_type="delete",
            target_chat_id=chat_id,
            target_label=chat_title,
            initial_logs=[
                "已接收删除请求",
                f"目标群组：{chat_title} ({chat_id})",
            ],
            start_job_fn=lambda job_id: self.admin_start_delete_job_thread_fn(
                job_id,
                chat_id,
                chat_title,
                get_conn_fn=self.get_conn_fn,
                admin_job_set_status_fn=self.admin_job_set_status_fn,
                admin_job_append_log_fn=self.admin_job_append_log_fn,
            ),
        )

    @admin_login_required
    def api_admin_job_create_delete_empty_chats(self):
        data, error_response = self._require_json_dict()
        if error_response is not None:
            return error_response

        error_response = self._require_confirmation(data, "DELETE_EMPTY_CHATS")
        if error_response is not None:
            return error_response

        return self._create_started_exclusive_job_response(
            job_type="delete_empty_chats",
            target_chat_id=None,
            target_label="零消息群组",
            initial_logs=["已接收零消息群组删除请求"],
            start_job_fn=lambda job_id: self.admin_start_delete_empty_chats_job_thread_fn(
                job_id,
                get_conn_fn=self.get_conn_fn,
                admin_job_set_status_fn=self.admin_job_set_status_fn,
                admin_job_append_log_fn=self.admin_job_append_log_fn,
            ),
        )

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

        initial_logs = ["已接收垃圾清理请求"]
        initial_logs.extend(
            self._build_scope_target_logs(
                scope=scope,
                chat_id=chat_id,
                target_label=target_label,
            )
        )
        initial_logs.append(f"关键字：{keyword}")
        return self._create_started_exclusive_job_response(
            job_type="cleanup",
            target_chat_id=chat_id,
            target_label=target_label,
            initial_logs=initial_logs,
            start_job_fn=lambda job_id: self.admin_start_cleanup_job_thread_fn(
                job_id=job_id,
                keyword=keyword,
                scope=scope,
                chat_id=chat_id,
                target_label=target_label,
                get_conn_fn=self.get_conn_fn,
                admin_job_set_status_fn=self.admin_job_set_status_fn,
                admin_job_append_log_fn=self.admin_job_append_log_fn,
            ),
            response_extra={
                "request": {
                    "scope": scope,
                    "chat_id": chat_id,
                    "target_label": target_label,
                    "keyword": keyword,
                }
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

        initial_logs = ["已接收不可搜索数据清理请求"]
        initial_logs.extend(
            self._build_scope_target_logs(
                scope=scope,
                chat_id=chat_id,
                target_label=target_label,
            )
        )
        return self._create_started_exclusive_job_response(
            job_type="cleanup_empty",
            target_chat_id=chat_id,
            target_label=target_label,
            initial_logs=initial_logs,
            start_job_fn=lambda job_id: self.admin_start_cleanup_empty_job_thread_fn(
                job_id=job_id,
                scope=scope,
                chat_id=chat_id,
                target_label=target_label,
                get_conn_fn=self.get_conn_fn,
                admin_job_set_status_fn=self.admin_job_set_status_fn,
                admin_job_append_log_fn=self.admin_job_append_log_fn,
            ),
            response_extra={
                "request": {
                    "scope": scope,
                    "chat_id": chat_id,
                    "target_label": target_label,
                }
            },
        )


def register_admin_routes(app, *, services: AdminRouteServices) -> None:
    handler = AdminRoutesHandler(services=services)

    app.get("/api/admin/chats")(handler.api_admin_chats)
    app.get("/api/admin/stats")(handler.api_admin_stats)
    app.get("/api/admin/sync/stats")(handler.api_admin_sync_stats)
    app.get("/api/admin/sync/messages")(handler.api_admin_sync_live_messages)
    app.get("/api/admin/storage-health")(handler.api_admin_storage_health)
    app.post("/api/admin/sync/diagnose")(handler.api_admin_sync_diagnose)
    app.get("/api/admin/sync/scheduler")(handler.api_admin_sync_scheduler)
    app.get("/api/admin/sync/chats")(handler.api_admin_sync_chats)
    app.post("/api/admin/sync/chats/<int:chat_id>/probe")(
        handler.api_admin_sync_chat_probe
    )
    app.post("/api/admin/sync/model/reset")(handler.api_admin_sync_model_reset)
    app.get("/api/admin/jobs/active")(handler.api_admin_active_job)
    app.get("/api/admin/jobs/<job_id>")(handler.api_admin_job_snapshot)
    app.get("/api/admin/jobs/<job_id>/logs")(handler.api_admin_job_logs)
    app.post("/api/admin/jobs/<job_id>/stop")(handler.api_admin_job_stop)
    app.post("/api/admin/jobs/harvest")(handler.api_admin_job_create_harvest)
    app.get("/api/admin/jobs/update/preflight")(handler.api_admin_update_preflight)
    app.post("/api/admin/jobs/update")(handler.api_admin_job_create_update)
    app.post("/api/admin/jobs/delete")(handler.api_admin_job_create_delete)
    app.post("/api/admin/jobs/delete-empty-chats")(
        handler.api_admin_job_create_delete_empty_chats
    )
    app.post("/api/admin/jobs/cleanup")(handler.api_admin_job_create_cleanup)
    app.post("/api/admin/jobs/cleanup-empty")(
        handler.api_admin_job_create_cleanup_empty
    )
