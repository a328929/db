import sqlite3
from contextlib import closing

from flask import jsonify, render_template

from tg_harvest.app.services import RecoveryRouteServices
from tg_harvest.web.auth import admin_login_required, admin_page_login_required
from tg_harvest.web.responses import (
    create_started_exclusive_job_response,
    json_error,
    logged_json_error,
    require_json_dict,
)
from tg_harvest.web.routes.chat_links import with_chat_links


def _parse_restore_chat_ids(payload: dict) -> tuple[list[int] | None, object | None]:
    raw_chat_ids = payload.get("chat_ids")
    if raw_chat_ids is None:
        raw_chat_id = payload.get("chat_id")
        if raw_chat_id is None:
            return None, None
        raw_chat_ids = [raw_chat_id]

    if not isinstance(raw_chat_ids, list):
        return None, json_error("chat_ids 参数必须为数组", 400)

    chat_ids = []
    for raw_chat_id in raw_chat_ids:
        try:
            chat_id = int(raw_chat_id)
        except (TypeError, ValueError):
            return None, json_error("chat_id 参数非法", 400)
        if chat_id == 0:
            return None, json_error("chat_id 参数非法", 400)
        chat_ids.append(chat_id)

    if not chat_ids:
        return None, json_error("chat_ids 不能为空", 400)
    return sorted(set(chat_ids)), None


def _parse_recovery_harvest_target(
    payload: dict,
    *,
    max_len: int,
) -> tuple[str | None, object | None]:
    raw_target = payload.get("target", "")
    if not isinstance(raw_target, str):
        return None, json_error("target 参数必须为字符串", 400)

    target = raw_target.strip()
    if not target:
        return None, json_error("target 不能为空", 400)
    if len(target) > max_len:
        return None, json_error(f"target 长度不能超过 {max_len}", 400)
    return target, None


def _resolve_recovery_services(
    *,
    services: RecoveryRouteServices | None,
    logger=None,
    get_conn_fn=None,
    cfg=None,
    list_recovery_chat_candidates_fn=None,
    build_recovery_overview_fn=None,
    build_telegram_chat_link_bundle_fn=None,
    admin_try_create_exclusive_job_fn=None,
    admin_job_get_snapshot_fn=None,
    admin_job_append_log_fn=None,
    admin_job_set_status_fn=None,
    admin_start_harvest_job_thread_fn=None,
    admin_make_job_log_handler_fn=None,
    admin_harvest_target_max_len=None,
    admin_start_recovery_scan_job_thread_fn=None,
    admin_start_recovery_restore_job_thread_fn=None,
) -> RecoveryRouteServices:
    if services is not None:
        return services
    return RecoveryRouteServices(
        logger=logger,
        get_conn_fn=get_conn_fn,
        cfg=cfg,
        list_recovery_chat_candidates_fn=list_recovery_chat_candidates_fn,
        build_recovery_overview_fn=build_recovery_overview_fn,
        build_telegram_chat_link_bundle_fn=build_telegram_chat_link_bundle_fn,
        admin_try_create_exclusive_job_fn=admin_try_create_exclusive_job_fn,
        admin_job_get_snapshot_fn=admin_job_get_snapshot_fn,
        admin_job_append_log_fn=admin_job_append_log_fn,
        admin_job_set_status_fn=admin_job_set_status_fn,
        admin_start_harvest_job_thread_fn=admin_start_harvest_job_thread_fn,
        admin_make_job_log_handler_fn=admin_make_job_log_handler_fn,
        admin_harvest_target_max_len=int(admin_harvest_target_max_len or 300),
        admin_start_recovery_scan_job_thread_fn=(
            admin_start_recovery_scan_job_thread_fn
        ),
        admin_start_recovery_restore_job_thread_fn=(
            admin_start_recovery_restore_job_thread_fn
        ),
    )


def register_recovery_routes(
    app,
    *,
    services: RecoveryRouteServices | None = None,
    logger=None,
    get_conn_fn=None,
    cfg=None,
    list_recovery_chat_candidates_fn=None,
    build_recovery_overview_fn=None,
    build_telegram_chat_link_bundle_fn=None,
    admin_try_create_exclusive_job_fn=None,
    admin_job_get_snapshot_fn=None,
    admin_job_append_log_fn=None,
    admin_job_set_status_fn=None,
    admin_start_harvest_job_thread_fn=None,
    admin_make_job_log_handler_fn=None,
    admin_harvest_target_max_len=None,
    admin_start_recovery_scan_job_thread_fn=None,
    admin_start_recovery_restore_job_thread_fn=None,
) -> None:
    services = _resolve_recovery_services(
        services=services,
        logger=logger,
        get_conn_fn=get_conn_fn,
        cfg=cfg,
        list_recovery_chat_candidates_fn=list_recovery_chat_candidates_fn,
        build_recovery_overview_fn=build_recovery_overview_fn,
        build_telegram_chat_link_bundle_fn=build_telegram_chat_link_bundle_fn,
        admin_try_create_exclusive_job_fn=admin_try_create_exclusive_job_fn,
        admin_job_get_snapshot_fn=admin_job_get_snapshot_fn,
        admin_job_append_log_fn=admin_job_append_log_fn,
        admin_job_set_status_fn=admin_job_set_status_fn,
        admin_start_harvest_job_thread_fn=admin_start_harvest_job_thread_fn,
        admin_make_job_log_handler_fn=admin_make_job_log_handler_fn,
        admin_harvest_target_max_len=admin_harvest_target_max_len,
        admin_start_recovery_scan_job_thread_fn=(
            admin_start_recovery_scan_job_thread_fn
        ),
        admin_start_recovery_restore_job_thread_fn=(
            admin_start_recovery_restore_job_thread_fn
        ),
    )

    @app.get("/admin/recovery")
    @admin_page_login_required
    def admin_recovery_page():
        return render_template("admin_recovery.html")

    @app.get("/api/admin/recovery")
    @admin_login_required
    def api_admin_recovery_candidates():
        try:
            with closing(services.get_conn_fn()) as conn:
                items = services.list_recovery_chat_candidates_fn(conn)
                overview = services.build_recovery_overview_fn(conn)
            return jsonify(
                {
                    "ok": True,
                    "items": with_chat_links(
                        items,
                        services.build_telegram_chat_link_bundle_fn,
                    ),
                    "overview": overview,
                }
            )
        except sqlite3.Error:
            return logged_json_error(
                services.logger,
                "读取群组恢复候选失败",
                "读取群组恢复候选失败",
            )
        except Exception:
            return logged_json_error(services.logger, "系统异常", "系统异常")

    @app.post("/api/admin/recovery/scan")
    @admin_login_required
    def api_admin_recovery_scan():
        return create_started_exclusive_job_response(
            services.admin_try_create_exclusive_job_fn,
            services.admin_job_get_snapshot_fn,
            job_type="recovery_scan",
            target_chat_id=None,
            target_label="Session 群组恢复扫描",
            append_log_fn=services.admin_job_append_log_fn,
            initial_logs=["已接收 Session 群组恢复扫描请求"],
            start_job_fn=lambda job_id: services.admin_start_recovery_scan_job_thread_fn(
                job_id,
                cfg=services.cfg,
                get_conn_fn=services.get_conn_fn,
                admin_job_set_status_fn=services.admin_job_set_status_fn,
                admin_job_append_log_fn=services.admin_job_append_log_fn,
            ),
        )

    @app.post("/api/admin/recovery/add")
    @admin_login_required
    def api_admin_recovery_add():
        data, error_response = require_json_dict()
        if error_response is not None:
            return error_response

        target, error_response = _parse_recovery_harvest_target(
            data,
            max_len=services.admin_harvest_target_max_len,
        )
        if error_response is not None:
            return error_response

        return create_started_exclusive_job_response(
            services.admin_try_create_exclusive_job_fn,
            services.admin_job_get_snapshot_fn,
            job_type="harvest",
            target_chat_id=None,
            target_label=target,
            append_log_fn=services.admin_job_append_log_fn,
            initial_logs=[
                "已接收恢复候选添加入库请求",
                f"抓取目标：{target}",
            ],
            start_job_fn=lambda job_id: services.admin_start_harvest_job_thread_fn(
                job_id,
                target,
                cfg=services.cfg,
                get_conn_fn=services.get_conn_fn,
                admin_make_job_log_handler_fn=services.admin_make_job_log_handler_fn,
                admin_job_set_status_fn=services.admin_job_set_status_fn,
                admin_job_append_log_fn=services.admin_job_append_log_fn,
            ),
        )

    @app.post("/api/admin/recovery/restore")
    @admin_login_required
    def api_admin_recovery_restore():
        data, error_response = require_json_dict()
        if error_response is not None:
            return error_response

        scope = str(data.get("scope") or "").strip().lower()
        if scope not in {"all", "selected"}:
            return json_error("scope 参数必须为 all 或 selected", 400)

        if scope == "all":
            chat_ids = None
            target_label = "全部恢复候选"
            expected_confirm = "RECOVER:all"
        else:
            chat_ids, parse_error = _parse_restore_chat_ids(data)
            if parse_error is not None:
                return parse_error
            target_label = f"{len(chat_ids or [])} 个恢复候选"
            expected_confirm = "RECOVER:selected:" + ",".join(
                str(chat_id) for chat_id in (chat_ids or [])
            )

        if str(data.get("confirm") or "").strip() != expected_confirm:
            return json_error("confirm 参数不匹配", 400)

        return create_started_exclusive_job_response(
            services.admin_try_create_exclusive_job_fn,
            services.admin_job_get_snapshot_fn,
            job_type="recovery_restore",
            target_chat_id=None,
            target_label=target_label,
            append_log_fn=services.admin_job_append_log_fn,
            initial_logs=[
                "已接收群组恢复请求",
                f"目标：{target_label}",
            ],
            start_job_fn=lambda job_id: services.admin_start_recovery_restore_job_thread_fn(
                job_id,
                chat_ids=chat_ids,
                target_label=target_label,
                get_conn_fn=services.get_conn_fn,
                admin_job_set_status_fn=services.admin_job_set_status_fn,
                admin_job_append_log_fn=services.admin_job_append_log_fn,
            ),
        )
