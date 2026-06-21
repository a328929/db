import sqlite3
from contextlib import closing

from flask import jsonify, render_template

from tg_harvest.web.auth import admin_login_required, admin_page_login_required
from tg_harvest.web.responses import (
    create_exclusive_job_or_response,
    created_job_snapshot_response,
    json_error,
    logged_json_error,
    require_json_dict,
)


def _with_chat_links(rows, build_telegram_chat_link_bundle_fn):
    items = []
    for row in rows:
        item = dict(row)
        bundle = build_telegram_chat_link_bundle_fn(
            chat_id=int(item["chat_id"]),
            chat_username=item.get("chat_username"),
        )
        item["telegram_app_link"] = bundle.app_link
        item["telegram_web_link"] = bundle.web_link
        item["has_public_link"] = bool(item["telegram_web_link"])
        items.append(item)
    return items


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


def register_recovery_routes(
    app,
    *,
    logger,
    get_conn_fn,
    cfg,
    list_recovery_chat_candidates_fn,
    build_recovery_overview_fn,
    build_telegram_chat_link_bundle_fn,
    admin_try_create_exclusive_job_fn,
    admin_job_get_snapshot_fn,
    admin_job_append_log_fn,
    admin_job_set_status_fn,
    admin_start_recovery_scan_job_thread_fn,
    admin_start_recovery_restore_job_thread_fn,
) -> None:
    @app.get("/admin/recovery")
    @admin_page_login_required
    def admin_recovery_page():
        return render_template("admin_recovery.html")

    @app.get("/api/admin/recovery")
    @admin_login_required
    def api_admin_recovery_candidates():
        try:
            with closing(get_conn_fn()) as conn:
                items = list_recovery_chat_candidates_fn(conn)
                overview = build_recovery_overview_fn(conn)
            return jsonify(
                {
                    "ok": True,
                    "items": _with_chat_links(
                        items,
                        build_telegram_chat_link_bundle_fn,
                    ),
                    "overview": overview,
                }
            )
        except sqlite3.Error:
            return logged_json_error(
                logger,
                "读取群组恢复候选失败",
                "读取群组恢复候选失败",
            )
        except Exception:
            return logged_json_error(logger, "系统异常", "系统异常")

    @app.post("/api/admin/recovery/scan")
    @admin_login_required
    def api_admin_recovery_scan():
        job_id, error_response = create_exclusive_job_or_response(
            admin_try_create_exclusive_job_fn,
            "recovery_scan",
            target_chat_id=None,
            target_label="Session 群组恢复扫描",
        )
        if error_response is not None:
            return error_response

        admin_job_append_log_fn(job_id, "已接收 Session 群组恢复扫描请求")
        admin_start_recovery_scan_job_thread_fn(
            job_id,
            cfg=cfg,
            get_conn_fn=get_conn_fn,
            admin_job_set_status_fn=admin_job_set_status_fn,
            admin_job_append_log_fn=admin_job_append_log_fn,
        )
        return created_job_snapshot_response(job_id, admin_job_get_snapshot_fn)

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

        job_id, error_response = create_exclusive_job_or_response(
            admin_try_create_exclusive_job_fn,
            "recovery_restore",
            target_chat_id=None,
            target_label=target_label,
        )
        if error_response is not None:
            return error_response

        admin_job_append_log_fn(job_id, "已接收群组恢复请求")
        admin_job_append_log_fn(job_id, f"目标：{target_label}")
        admin_start_recovery_restore_job_thread_fn(
            job_id,
            chat_ids=chat_ids,
            target_label=target_label,
            get_conn_fn=get_conn_fn,
            admin_job_set_status_fn=admin_job_set_status_fn,
            admin_job_append_log_fn=admin_job_append_log_fn,
        )
        return created_job_snapshot_response(job_id, admin_job_get_snapshot_fn)
