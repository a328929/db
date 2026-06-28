import sqlite3
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from typing import Any

from flask import jsonify, render_template, request

from tg_harvest.admin_jobs.clone import (
    CLONE_TARGET_TITLE_MAX_LEN,
    normalize_clone_target_kind,
    normalize_clone_target_title,
)
from tg_harvest.app.services import CloneRouteServices
from tg_harvest.domain.clone_plan import (
    CLONE_TEXT_MIGRATION_DEFAULT_SEND_DELAY_MS,
    CLONE_TEXT_MIGRATION_MAX_MESSAGE_LIMIT,
    CLONE_TEXT_MIGRATION_MAX_SEND_DELAY_MS,
    clone_plan_media_relay,
    clone_plan_timeline_readiness,
)
from tg_harvest.web.auth import admin_login_required, admin_page_login_required
from tg_harvest.web.responses import (
    create_exclusive_job_or_response,
    created_job_snapshot_response,
    json_error,
    logged_json_error,
    require_json_dict,
)
from tg_harvest.web.routes.chat_links import with_chat_links, with_prefixed_chat_links

_ALLOWED_TARGET_KINDS = {"channel", "megagroup"}


@dataclass(frozen=True)
class _CloneRouteDeps:
    logger: Any
    get_conn_fn: Any
    cfg: Any
    list_clone_source_chats_fn: Any
    build_clone_preflight_report_fn: Any
    create_clone_run_fn: Any
    load_clone_run_fn: Any
    list_clone_runs_fn: Any
    count_clone_runs_fn: Any
    load_clone_run_detail_fn: Any
    list_clone_message_mappings_fn: Any
    count_clone_message_mappings_fn: Any
    delete_clone_run_fn: Any
    create_clone_plan_fn: Any
    load_latest_clone_plan_fn: Any
    create_clone_migration_fn: Any
    load_latest_clone_migration_fn: Any
    build_clone_timeline_replay_preview_fn: Any
    build_telegram_chat_link_bundle_fn: Any
    admin_try_create_exclusive_job_fn: Any
    admin_job_get_snapshot_fn: Any
    admin_job_append_log_fn: Any
    admin_job_set_status_fn: Any
    admin_start_clone_structure_job_thread_fn: Any
    admin_start_clone_deep_preflight_job_thread_fn: Any
    admin_start_clone_timeline_migration_job_thread_fn: Any


def _with_clone_run_links(rows, build_telegram_chat_link_bundle_fn):
    items = []
    for row in rows:
        item = dict(row)
        with_prefixed_chat_links(
            item,
            prefix="source",
            build_telegram_chat_link_bundle_fn=build_telegram_chat_link_bundle_fn,
            chat_id=item.get("source_chat_id"),
            chat_username=item.get("source_chat_username"),
        )
        with_prefixed_chat_links(
            item,
            prefix="target",
            build_telegram_chat_link_bundle_fn=build_telegram_chat_link_bundle_fn,
            chat_id=item.get("target_chat_id"),
            chat_username=item.get("target_username"),
        )
        items.append(item)
    return items


def _with_clone_run_link(row, build_telegram_chat_link_bundle_fn):
    items = _with_clone_run_links([row], build_telegram_chat_link_bundle_fn)
    return items[0] if items else dict(row)


def _parse_chat_id(raw_chat_id: Any):
    try:
        chat_id = int(raw_chat_id)
    except (TypeError, ValueError):
        return None, json_error("chat_id 参数非法", 400)
    if chat_id == 0:
        return None, json_error("chat_id 参数非法", 400)
    return chat_id, None


def _parse_optional_limit(raw_limit: Any, *, default: int = 20):
    text = str(raw_limit or "").strip()
    if not text:
        return default, None
    try:
        limit = int(text)
    except (TypeError, ValueError):
        return None, json_error("limit 参数非法", 400)
    if limit <= 0:
        return None, json_error("limit 参数非法", 400)
    return min(limit, 100), None


def _parse_optional_offset(raw_offset: Any):
    text = str(raw_offset or "").strip()
    if not text:
        return 0, None
    try:
        offset = int(text)
    except (TypeError, ValueError):
        return None, json_error("offset 参数非法", 400)
    if offset < 0:
        return None, json_error("offset 参数非法", 400)
    return offset, None


def _parse_run_id(raw_run_id: Any):
    normalized_run_id = str(raw_run_id or "").strip()
    if not normalized_run_id:
        return None, json_error("run_id 参数非法", 400)
    return normalized_run_id, None


def _clone_run_delete_confirm_text(run: dict) -> str:
    return "DELETE-CLONE-RUN:" + str(run.get("run_id") or "").strip()


def _normalize_requested_target_kind(raw_kind: Any, *, source_chat_type: Any):
    normalized_raw = str(raw_kind or "").strip().lower()
    if normalized_raw and normalized_raw not in _ALLOWED_TARGET_KINDS:
        return None, json_error("target_kind 参数必须为 channel 或 megagroup", 400)
    return (
        normalize_clone_target_kind(
            normalized_raw,
            source_chat_type=source_chat_type,
        ),
        None,
    )


def _normalize_requested_target_title(raw_title: Any, *, fallback_title: str):
    if raw_title is not None and not isinstance(raw_title, str):
        return None, json_error("target_title 参数必须为字符串", 400)

    title = str(raw_title or "").strip()
    if len(title) > CLONE_TARGET_TITLE_MAX_LEN:
        return (
            None,
            json_error(f"target_title 长度不能超过 {CLONE_TARGET_TITLE_MAX_LEN}", 400),
        )
    return normalize_clone_target_title(title, fallback_title=fallback_title), None


def _parse_optional_nonnegative_int(
    raw_value: Any,
    *,
    field_name: str,
    default: int = 0,
    max_value: int,
):
    if raw_value in (None, ""):
        return int(default), None
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        return None, json_error(f"{field_name} 参数必须为非负整数", 400)
    if value < 0:
        return None, json_error(f"{field_name} 参数必须为非负整数", 400)
    if value > max_value:
        return None, json_error(f"{field_name} 参数不能超过 {max_value}", 400)
    return value, None


def _parse_timeline_migration_options():
    data = request.get_json(silent=True) if request.is_json else {}
    if data is None:
        data = {}
    if not isinstance(data, dict):
        return None, json_error("请求 JSON 格式错误", 400)

    message_limit, error_response = _parse_optional_nonnegative_int(
        data.get("message_limit"),
        field_name="message_limit",
        default=0,
        max_value=CLONE_TEXT_MIGRATION_MAX_MESSAGE_LIMIT,
    )
    if error_response is not None:
        return None, error_response

    send_delay_ms, error_response = _parse_optional_nonnegative_int(
        data.get("send_delay_ms"),
        field_name="send_delay_ms",
        default=CLONE_TEXT_MIGRATION_DEFAULT_SEND_DELAY_MS,
        max_value=CLONE_TEXT_MIGRATION_MAX_SEND_DELAY_MS,
    )
    if error_response is not None:
        return None, error_response

    return {
        "message_limit": int(message_limit or 0),
        "send_delay_ms": int(send_delay_ms or 0),
    }, None


def _load_clone_run_or_response(conn, load_clone_run_fn, run_id: str):
    clone_run = load_clone_run_fn(conn, run_id)
    if clone_run is None:
        return None, json_error("克隆运行记录不存在", 404)
    return clone_run, None


def _load_clone_preflight_report_or_response(
    deps: _CloneRouteDeps,
    chat_id: int,
    *,
    db_error_message: str,
):
    try:
        with closing(deps.get_conn_fn()) as conn:
            report = deps.build_clone_preflight_report_fn(
                conn,
                chat_id=chat_id,
                cfg=deps.cfg,
            )
        return report, None
    except ValueError as exc:
        return None, json_error(str(exc), 404)
    except sqlite3.Error:
        return None, logged_json_error(
            deps.logger,
            db_error_message,
            db_error_message,
        )
    except Exception:
        return None, logged_json_error(deps.logger, "系统异常", "系统异常")


def _mark_job_start_failed(
    job_id: str,
    *,
    admin_job_append_log_fn,
    admin_job_set_status_fn,
    message: str,
) -> None:
    admin_job_append_log_fn(job_id, f"{message}，任务未启动")
    admin_job_set_status_fn(job_id, "error")


def _clone_target_summary(clone_run: dict) -> tuple[int, str, str, str]:
    source_chat_id = int(clone_run.get("source_chat_id") or 0)
    source_title = str(clone_run.get("source_title") or source_chat_id)
    target_title = str(clone_run.get("target_title") or "未创建目标")
    return source_chat_id, source_title, target_title, f"{source_title} -> {target_title}"


def _job_creation_failed_response(deps: _CloneRouteDeps, job_id: str, *, message: str):
    _mark_job_start_failed(
        job_id,
        admin_job_append_log_fn=deps.admin_job_append_log_fn,
        admin_job_set_status_fn=deps.admin_job_set_status_fn,
        message=message,
    )


def _create_clone_job_or_response(
    deps: _CloneRouteDeps,
    job_type: str,
    *,
    target_chat_id: int | None = None,
    target_label: str | None = None,
):
    return create_exclusive_job_or_response(
        deps.admin_try_create_exclusive_job_fn,
        job_type,
        target_chat_id=target_chat_id,
        target_label=target_label,
    )


def _create_clone_record_or_response(
    deps: _CloneRouteDeps,
    *,
    job_id: str,
    create_record_fn: Callable[[Any], Any],
    error_message: str,
    start_failure_message: str | None = None,
):
    failure_message = str(start_failure_message or error_message)
    try:
        with closing(deps.get_conn_fn()) as conn:
            return create_record_fn(conn), None
    except sqlite3.Error:
        _job_creation_failed_response(deps, job_id, message=failure_message)
        return None, logged_json_error(
            deps.logger,
            error_message,
            error_message,
        )
    except Exception as exc:
        _job_creation_failed_response(deps, job_id, message=failure_message)
        return None, json_error(str(exc) or error_message, 400)


def _start_clone_job_response(
    deps: _CloneRouteDeps,
    *,
    job_id: str,
    initial_logs: list[str] | tuple[str, ...] = (),
    start_job_fn: Callable[[str], Any],
    response_extra: dict[str, Any] | None = None,
):
    for message in initial_logs:
        deps.admin_job_append_log_fn(job_id, str(message))
    start_job_fn(job_id)
    return created_job_snapshot_response(
        job_id,
        deps.admin_job_get_snapshot_fn,
        **(response_extra or {}),
    )


def _timeline_migration_readiness(
    clone_run: dict,
    latest_plan: dict | None,
    timeline_preview: dict | None = None,
) -> dict:
    reasons: list[str] = []
    shared = clone_plan_timeline_readiness(latest_plan, preview=timeline_preview)
    target_write_account = str(shared["target_write_account"] or "")
    migration_account = str(shared["migration_account"] or "")
    media_execution_account = str(shared["media_execution_account"] or "")

    if clone_run.get("status") != "done" or not clone_run.get("target_chat_id"):
        reasons.append("目标副本尚未创建完成，不能执行完整时间线迁移")
    if "plan_missing" in shared["reason_codes"]:
        reasons.append("请先执行在线深度预检并生成迁移计划")
    if "plan_missing" not in shared["reason_codes"]:
        reason_messages = {
            "plan_not_done": "迁移计划尚未完成，不能执行完整时间线迁移",
            "plan_blocked": "迁移计划存在阻断项，不能执行完整时间线迁移",
            "target_inaccessible": "目标副本不可访问，不能执行完整时间线迁移",
            "text_strategy_blocked": "迁移计划不允许数据库文本重放",
            "missing_target_write_account": "迁移计划缺少可写目标账号，请重新执行在线深度预检",
            "source_inaccessible": "源群不可访问，不能执行媒体时间线复制",
            "media_strategy_blocked": "迁移计划不允许隐藏来源媒体复制，请重新执行在线深度预检",
            "media_relay_not_ready": "固定中转频道桥接计划未就绪，请重新执行在线深度预检",
            "missing_media_account": "迁移计划缺少同时访问源群与目标副本的媒体迁移账号",
            "no_timeline_remaining": "没有剩余可迁移时间线消息",
        }
        for reason_code in shared["reason_codes"]:
            message = reason_messages.get(str(reason_code))
            if message and message not in reasons:
                reasons.append(message)

    execution_parts = []
    if target_write_account:
        execution_parts.append(f"text:{target_write_account}")
    if media_execution_account:
        execution_parts.append(f"media:{media_execution_account}")
    return {
        "can_migrate_timeline": not reasons,
        "readiness_reasons": reasons,
        "target_write_account": target_write_account,
        "migration_account": migration_account,
        "media_execution_account": media_execution_account,
        "execution_label": "; ".join(execution_parts),
        "media_relay": clone_plan_media_relay(latest_plan or {}),
        "plan_id": str((latest_plan or {}).get("plan_id") or ""),
        "plan_status": str((latest_plan or {}).get("status") or ""),
        "text_strategy": str((latest_plan or {}).get("text_strategy") or ""),
        "media_strategy": str((latest_plan or {}).get("media_strategy") or ""),
        "source_access": str((latest_plan or {}).get("source_access") or ""),
        "target_access": str((latest_plan or {}).get("target_access") or ""),
    }


def _register_clone_page_routes(app) -> None:
    @app.get("/admin/clone")
    @admin_page_login_required
    def admin_clone_page():
        return render_template("admin_clone.html")

    @app.get("/admin/clone/create")
    @admin_page_login_required
    def admin_clone_create_page():
        return render_template("admin_clone_create.html")

    @app.get("/admin/clone/migrate")
    @admin_page_login_required
    def admin_clone_migrate_page():
        return render_template("admin_clone_migrate.html")

    @app.get("/admin/clone/runs/manage")
    @admin_page_login_required
    def admin_clone_runs_manage_page():
        return render_template("admin_clone_runs.html")

    @app.get("/admin/clone/runs/detail")
    @admin_page_login_required
    def admin_clone_run_detail_page():
        return render_template("admin_clone_run_detail.html")


def _register_clone_list_routes(app, deps: _CloneRouteDeps) -> None:
    @app.get("/api/admin/clone/chats")
    @admin_login_required
    def api_admin_clone_chats():
        try:
            sort = request.args.get("sort", "")
            with closing(deps.get_conn_fn()) as conn:
                rows = deps.list_clone_source_chats_fn(conn, sort=sort)
            return jsonify(
                {
                    "ok": True,
                    "items": with_chat_links(
                        rows,
                        deps.build_telegram_chat_link_bundle_fn,
                    ),
                    "count": len(rows),
                }
            )
        except sqlite3.Error:
            return logged_json_error(
                deps.logger,
                "读取克隆源群组列表失败",
                "读取克隆源群组列表失败",
            )
        except Exception:
            return logged_json_error(deps.logger, "系统异常", "系统异常")

    @app.get("/api/admin/clone/runs")
    @admin_login_required
    def api_admin_clone_runs():
        raw_source_chat_id = str(request.args.get("source_chat_id", "") or "").strip()
        source_chat_id = None
        if raw_source_chat_id:
            source_chat_id, error_response = _parse_chat_id(raw_source_chat_id)
            if error_response is not None:
                return error_response

        limit, error_response = _parse_optional_limit(request.args.get("limit"))
        if error_response is not None:
            return error_response
        offset, error_response = _parse_optional_offset(request.args.get("offset"))
        if error_response is not None:
            return error_response
        status = str(request.args.get("status", "") or "").strip()
        query = str(request.args.get("q", "") or "").strip()
        sort = str(request.args.get("sort", "") or "").strip()

        try:
            with closing(deps.get_conn_fn()) as conn:
                rows = deps.list_clone_runs_fn(
                    conn,
                    source_chat_id=source_chat_id,
                    limit=limit,
                    offset=offset,
                    status=status,
                    q=query,
                    sort=sort,
                )
                total = deps.count_clone_runs_fn(
                    conn,
                    source_chat_id=source_chat_id,
                    status=status,
                    q=query,
                )
            return jsonify(
                {
                    "ok": True,
                    "items": _with_clone_run_links(
                        rows,
                        deps.build_telegram_chat_link_bundle_fn,
                    ),
                    "total": total,
                    "limit": limit,
                    "offset": offset,
                }
            )
        except sqlite3.Error:
            return logged_json_error(
                deps.logger,
                "读取克隆运行记录失败",
                "读取克隆运行记录失败",
            )
        except Exception:
            return logged_json_error(deps.logger, "系统异常", "系统异常")


def _register_clone_run_routes(app, deps: _CloneRouteDeps) -> None:
    @app.get("/api/admin/clone/runs/<run_id>/plan")
    @admin_login_required
    def api_admin_clone_run_latest_plan(run_id):
        normalized_run_id, error_response = _parse_run_id(run_id)
        if error_response is not None:
            return error_response

        try:
            with closing(deps.get_conn_fn()) as conn:
                clone_run, error_response = _load_clone_run_or_response(
                    conn,
                    deps.load_clone_run_fn,
                    normalized_run_id,
                )
                if error_response is not None:
                    return error_response
                plan = deps.load_latest_clone_plan_fn(conn, normalized_run_id)
            return jsonify({"ok": True, "plan": plan})
        except sqlite3.Error:
            return logged_json_error(
                deps.logger,
                "读取克隆迁移计划失败",
                "读取克隆迁移计划失败",
            )
        except Exception:
            return logged_json_error(deps.logger, "系统异常", "系统异常")

    @app.get("/api/admin/clone/runs/<run_id>/detail")
    @admin_login_required
    def api_admin_clone_run_detail(run_id):
        normalized_run_id, error_response = _parse_run_id(run_id)
        if error_response is not None:
            return error_response

        try:
            with closing(deps.get_conn_fn()) as conn:
                detail = deps.load_clone_run_detail_fn(conn, normalized_run_id)
                if detail is None:
                    return json_error("克隆运行记录不存在", 404)
            run = _with_clone_run_link(
                detail["run"],
                deps.build_telegram_chat_link_bundle_fn,
            )
            return jsonify(
                {
                    "ok": True,
                    "run": run,
                    "plan": detail.get("plan"),
                    "migration": detail.get("migration"),
                    "timeline_preview": detail.get("timeline_preview"),
                    "mapping_summary": detail.get("mapping_summary"),
                    "recent_mappings": detail.get("recent_mappings") or [],
                    "failure_items": detail.get("failure_items") or [],
                    "delete_confirm": _clone_run_delete_confirm_text(run),
                }
            )
        except sqlite3.Error:
            return logged_json_error(
                deps.logger,
                "读取克隆记录详情失败",
                "读取克隆记录详情失败",
            )
        except Exception:
            return logged_json_error(deps.logger, "系统异常", "系统异常")

    @app.get("/api/admin/clone/runs/<run_id>/messages")
    @admin_login_required
    def api_admin_clone_run_messages(run_id):
        normalized_run_id, error_response = _parse_run_id(run_id)
        if error_response is not None:
            return error_response
        limit, error_response = _parse_optional_limit(
            request.args.get("limit"),
            default=100,
        )
        if error_response is not None:
            return error_response
        offset, error_response = _parse_optional_offset(request.args.get("offset"))
        if error_response is not None:
            return error_response
        status = str(request.args.get("status", "") or "").strip()
        mode = str(request.args.get("mode", "") or "").strip()

        try:
            with closing(deps.get_conn_fn()) as conn:
                clone_run, error_response = _load_clone_run_or_response(
                    conn,
                    deps.load_clone_run_fn,
                    normalized_run_id,
                )
                if error_response is not None:
                    return error_response
                rows = deps.list_clone_message_mappings_fn(
                    conn,
                    run_id=normalized_run_id,
                    status=status,
                    mode=mode,
                    limit=limit,
                    offset=offset,
                )
                total = deps.count_clone_message_mappings_fn(
                    conn,
                    run_id=normalized_run_id,
                    status=status,
                    mode=mode,
                )
            return jsonify(
                {
                    "ok": True,
                    "items": rows,
                    "total": total,
                    "limit": limit,
                    "offset": offset,
                }
            )
        except sqlite3.Error:
            return logged_json_error(
                deps.logger,
                "读取克隆消息映射失败",
                "读取克隆消息映射失败",
            )
        except Exception:
            return logged_json_error(deps.logger, "系统异常", "系统异常")

    @app.delete("/api/admin/clone/runs/<run_id>")
    @admin_login_required
    def api_admin_clone_run_delete(run_id):
        normalized_run_id, error_response = _parse_run_id(run_id)
        if error_response is not None:
            return error_response
        data, error_response = require_json_dict()
        if error_response is not None:
            return error_response

        try:
            with closing(deps.get_conn_fn()) as conn:
                clone_run, error_response = _load_clone_run_or_response(
                    conn,
                    deps.load_clone_run_fn,
                    normalized_run_id,
                )
                if error_response is not None:
                    return error_response
                expected_confirm = _clone_run_delete_confirm_text(clone_run)
                if str(data.get("confirm") or "").strip() != expected_confirm:
                    return json_error("confirm 参数不匹配", 400)
                deleted = deps.delete_clone_run_fn(conn, run_id=normalized_run_id)
            if not deleted:
                return json_error("克隆运行记录不存在", 404)
            return jsonify(
                {
                    "ok": True,
                    "deleted": True,
                    "run_id": normalized_run_id,
                    "telegram_target_deleted": False,
                }
            )
        except sqlite3.Error:
            return logged_json_error(
                deps.logger,
                "删除克隆本地记录失败",
                "删除克隆本地记录失败",
            )
        except Exception:
            return logged_json_error(deps.logger, "系统异常", "系统异常")

    @app.get("/api/admin/clone/runs/<run_id>/migration")
    @admin_login_required
    def api_admin_clone_run_latest_migration(run_id):
        normalized_run_id, error_response = _parse_run_id(run_id)
        if error_response is not None:
            return error_response

        try:
            with closing(deps.get_conn_fn()) as conn:
                clone_run, error_response = _load_clone_run_or_response(
                    conn,
                    deps.load_clone_run_fn,
                    normalized_run_id,
                )
                if error_response is not None:
                    return error_response
                timeline_migration = deps.load_latest_clone_migration_fn(
                    conn,
                    normalized_run_id,
                    mode="timeline_replay",
                )
                latest_plan = deps.load_latest_clone_plan_fn(conn, normalized_run_id)
                source_chat_id = int(clone_run.get("source_chat_id") or 0)
                timeline_preview = deps.build_clone_timeline_replay_preview_fn(
                    conn,
                    run_id=normalized_run_id,
                    source_chat_id=source_chat_id,
                )
            timeline_preview.update(
                _timeline_migration_readiness(
                    clone_run,
                    latest_plan,
                    timeline_preview,
                )
            )
            if timeline_migration is not None:
                timeline_preview["latest_migration_id"] = timeline_migration.get(
                    "migration_id"
                )
                timeline_preview["latest_migration_status"] = timeline_migration.get(
                    "status"
                )
                timeline_preview["latest_migration_phase"] = timeline_migration.get(
                    "phase"
                )
            return jsonify(
                {
                    "ok": True,
                    "migration": timeline_migration,
                    "timeline_migration": timeline_migration,
                    "timeline_preview": timeline_preview,
                }
            )
        except sqlite3.Error:
            return logged_json_error(
                deps.logger,
                "读取克隆迁移记录失败",
                "读取克隆迁移记录失败",
            )
        except Exception:
            return logged_json_error(deps.logger, "系统异常", "系统异常")


def _register_clone_job_routes(app, deps: _CloneRouteDeps) -> None:
    @app.post("/api/admin/clone/runs/<run_id>/deep-preflight")
    @admin_login_required
    def api_admin_clone_run_deep_preflight(run_id):
        normalized_run_id, error_response = _parse_run_id(run_id)
        if error_response is not None:
            return error_response

        try:
            with closing(deps.get_conn_fn()) as conn:
                clone_run, error_response = _load_clone_run_or_response(
                    conn,
                    deps.load_clone_run_fn,
                    normalized_run_id,
                )
                if error_response is not None:
                    return error_response
        except sqlite3.Error:
            return logged_json_error(
                deps.logger,
                "读取克隆运行记录失败",
                "读取克隆运行记录失败",
            )
        except Exception:
            return logged_json_error(deps.logger, "系统异常", "系统异常")

        source_chat_id, source_title, target_title, target_label = _clone_target_summary(
            clone_run
        )
        job_id, error_response = _create_clone_job_or_response(
            deps,
            "clone_deep_preflight",
            target_chat_id=source_chat_id if source_chat_id else None,
            target_label=target_label,
        )
        if error_response is not None:
            return error_response

        plan, error_response = _create_clone_record_or_response(
            deps,
            job_id=job_id,
            error_message="创建克隆迁移计划失败",
            start_failure_message="克隆迁移计划创建失败",
            create_record_fn=lambda conn: deps.create_clone_plan_fn(
                conn,
                plan_id=job_id,
                run_id=normalized_run_id,
                job_id=job_id,
                status="queued",
                plan={
                    "run_id": normalized_run_id,
                    "source_chat_id": source_chat_id,
                    "target_chat_id": clone_run.get("target_chat_id"),
                },
            ),
        )
        if error_response is not None:
            return error_response

        return _start_clone_job_response(
            deps,
            job_id=job_id,
            initial_logs=[
                "已接收克隆深度预检请求",
                f"运行记录：{normalized_run_id}",
                f"预检目标：{target_label}",
            ],
            start_job_fn=lambda current_job_id: deps.admin_start_clone_deep_preflight_job_thread_fn(
                current_job_id,
                run_id=normalized_run_id,
                plan_id=plan["plan_id"],
                cfg=deps.cfg,
                get_conn_fn=deps.get_conn_fn,
                admin_job_set_status_fn=deps.admin_job_set_status_fn,
                admin_job_append_log_fn=deps.admin_job_append_log_fn,
            ),
            response_extra={
                "request": {
                    "run_id": normalized_run_id,
                    "source_chat_id": source_chat_id,
                    "source_title": source_title,
                    "target_title": target_title,
                    "mode": "deep_preflight",
                },
                "plan": plan,
            },
        )

    @app.post("/api/admin/clone/runs/<run_id>/migrate-timeline")
    @admin_login_required
    def api_admin_clone_run_migrate_timeline(run_id):
        normalized_run_id, error_response = _parse_run_id(run_id)
        if error_response is not None:
            return error_response
        options, error_response = _parse_timeline_migration_options()
        if error_response is not None:
            return error_response

        try:
            with closing(deps.get_conn_fn()) as conn:
                clone_run = deps.load_clone_run_fn(conn, normalized_run_id)
                latest_plan = deps.load_latest_clone_plan_fn(conn, normalized_run_id)
                timeline_preview = (
                    deps.build_clone_timeline_replay_preview_fn(
                        conn,
                        run_id=normalized_run_id,
                        source_chat_id=int((clone_run or {}).get("source_chat_id") or 0),
                    )
                    if clone_run is not None
                    else None
                )
        except sqlite3.Error:
            return logged_json_error(
                deps.logger,
                "读取完整时间线迁移上下文失败",
                "读取完整时间线迁移上下文失败",
            )
        except Exception:
            return logged_json_error(deps.logger, "系统异常", "系统异常")

        if clone_run is None:
            return json_error("克隆运行记录不存在", 404)
        readiness = _timeline_migration_readiness(
            clone_run,
            latest_plan,
            timeline_preview,
        )
        if not readiness["can_migrate_timeline"]:
            return json_error(readiness["readiness_reasons"][0], 400)
        execution_label = str(readiness["execution_label"] or "")

        source_chat_id, source_title, target_title, target_label = _clone_target_summary(
            clone_run
        )
        job_id, error_response = _create_clone_job_or_response(
            deps,
            "clone_timeline_migration",
            target_chat_id=source_chat_id if source_chat_id else None,
            target_label=target_label,
        )
        if error_response is not None:
            return error_response

        migration, error_response = _create_clone_record_or_response(
            deps,
            job_id=job_id,
            error_message="创建完整时间线迁移记录失败",
            start_failure_message="完整时间线迁移记录创建失败",
            create_record_fn=lambda conn: deps.create_clone_migration_fn(
                conn,
                migration_id=job_id,
                run_id=normalized_run_id,
                plan_id=latest_plan["plan_id"],
                job_id=job_id,
                mode="timeline_replay",
                status="queued",
                phase="queued",
                target_chat_id=clone_run.get("target_chat_id"),
                target_title=target_title,
                target_write_account=execution_label,
                requested_limit=options["message_limit"],
                send_delay_ms=options["send_delay_ms"],
                text_total=int((timeline_preview or {}).get("text_total") or 0),
                media_total=int((timeline_preview or {}).get("media_total") or 0),
                media_group_total=int(
                    (timeline_preview or {}).get("media_group_total") or 0
                ),
                plan=latest_plan,
            ),
        )
        if error_response is not None:
            return error_response

        return _start_clone_job_response(
            deps,
            job_id=job_id,
            initial_logs=[
                "已接收完整时间线迁移请求",
                f"运行记录：{normalized_run_id}",
                f"迁移目标：{target_label}",
                "迁移策略：按原群顺序混合迁移文本、媒体和相册；媒体隐藏来源，不带原群跳转",
                "迁移参数："
                f"本次上限={options['message_limit'] or '全部'}，"
                f"发送间隔={options['send_delay_ms']}ms",
            ],
            start_job_fn=lambda current_job_id: deps.admin_start_clone_timeline_migration_job_thread_fn(
                current_job_id,
                run_id=normalized_run_id,
                plan_id=latest_plan["plan_id"],
                migration_id=migration["migration_id"],
                message_limit=options["message_limit"],
                send_delay_ms=options["send_delay_ms"],
                cfg=deps.cfg,
                get_conn_fn=deps.get_conn_fn,
                admin_job_set_status_fn=deps.admin_job_set_status_fn,
                admin_job_append_log_fn=deps.admin_job_append_log_fn,
            ),
            response_extra={
                "request": {
                    "run_id": normalized_run_id,
                    "plan_id": latest_plan["plan_id"],
                    "source_chat_id": source_chat_id,
                    "source_title": source_title,
                    "target_title": target_title,
                    "mode": "timeline_replay",
                    "message_limit": options["message_limit"],
                    "send_delay_ms": options["send_delay_ms"],
                },
                "migration": migration,
                "timeline_preview": timeline_preview,
            },
        )

    @app.post("/api/admin/clone/preflight")
    @admin_login_required
    def api_admin_clone_preflight():
        data, error_response = require_json_dict()
        if error_response is not None:
            return error_response

        chat_id, error_response = _parse_chat_id(data.get("chat_id"))
        if error_response is not None:
            return error_response

        report, error_response = _load_clone_preflight_report_or_response(
            deps,
            chat_id,
            db_error_message="执行克隆预检失败",
        )
        if error_response is not None:
            return error_response
        return jsonify({"ok": True, "report": report})

    @app.post("/api/admin/clone/jobs")
    @admin_login_required
    def api_admin_clone_job_create():
        data, error_response = require_json_dict()
        if error_response is not None:
            return error_response

        chat_id, error_response = _parse_chat_id(data.get("chat_id"))
        if error_response is not None:
            return error_response

        report, error_response = _load_clone_preflight_report_or_response(
            deps,
            chat_id,
            db_error_message="执行克隆启动前预检失败",
        )
        if error_response is not None:
            return error_response

        expected_confirm = str(report.get("confirm") or "")
        if str(data.get("confirm") or "").strip() != expected_confirm:
            return json_error("confirm 参数不匹配", 400)

        account = report.get("account") if isinstance(report, dict) else {}
        if not bool((account or {}).get("secondary_session_distinct")):
            return json_error("第二账号未就绪，不能开始结构克隆", 400)

        source = report.get("source") if isinstance(report, dict) else {}
        target = report.get("target") if isinstance(report, dict) else {}
        fallback_title = str((target or {}).get("default_title") or "")
        target_title, error_response = _normalize_requested_target_title(
            data.get("target_title"),
            fallback_title=fallback_title,
        )
        if error_response is not None:
            return error_response

        target_kind, error_response = _normalize_requested_target_kind(
            data.get("target_kind"),
            source_chat_type=(source or {}).get("chat_type"),
        )
        if error_response is not None:
            return error_response

        target_owner_session = str(
            getattr(deps.cfg, "secondary_session_name", "") or ""
        ).strip()
        source_title = str((source or {}).get("chat_title") or chat_id)
        job_id, error_response = _create_clone_job_or_response(
            deps,
            "clone_structure",
            target_chat_id=chat_id,
            target_label=target_title,
        )
        if error_response is not None:
            return error_response

        clone_run, error_response = _create_clone_record_or_response(
            deps,
            job_id=job_id,
            error_message="创建克隆运行记录失败",
            start_failure_message="克隆运行记录创建失败",
            create_record_fn=lambda conn: deps.create_clone_run_fn(
                conn,
                run_id=job_id,
                job_id=job_id,
                source_chat=source,
                target_title=target_title,
                target_kind=target_kind,
                target_owner_session=target_owner_session,
                plan=report,
            ),
        )
        if error_response is not None:
            return error_response

        return _start_clone_job_response(
            deps,
            job_id=job_id,
            initial_logs=[
                "已接收结构克隆请求",
                f"源群组：{source_title} ({chat_id})",
                f"目标：{target_title} / {target_kind}",
            ],
            start_job_fn=lambda current_job_id: deps.admin_start_clone_structure_job_thread_fn(
                current_job_id,
                source_chat_id=chat_id,
                target_title=target_title,
                target_kind=target_kind,
                clone_run_id=clone_run["run_id"],
                cfg=deps.cfg,
                get_conn_fn=deps.get_conn_fn,
                admin_job_set_status_fn=deps.admin_job_set_status_fn,
                admin_job_append_log_fn=deps.admin_job_append_log_fn,
            ),
            response_extra={
                "request": {
                    "chat_id": chat_id,
                    "source_title": source_title,
                    "target_title": target_title,
                    "target_kind": target_kind,
                    "mode": "structure_only",
                },
                "clone_run": clone_run,
            },
        )


def _clone_deps_from_services(services: CloneRouteServices) -> _CloneRouteDeps:
    return _CloneRouteDeps(
        logger=services.logger,
        get_conn_fn=services.get_conn_fn,
        cfg=services.cfg,
        list_clone_source_chats_fn=services.list_clone_source_chats_fn,
        build_clone_preflight_report_fn=services.build_clone_preflight_report_fn,
        create_clone_run_fn=services.create_clone_run_fn,
        load_clone_run_fn=services.load_clone_run_fn,
        list_clone_runs_fn=services.list_clone_runs_fn,
        count_clone_runs_fn=services.count_clone_runs_fn,
        load_clone_run_detail_fn=services.load_clone_run_detail_fn,
        list_clone_message_mappings_fn=services.list_clone_message_mappings_fn,
        count_clone_message_mappings_fn=services.count_clone_message_mappings_fn,
        delete_clone_run_fn=services.delete_clone_run_fn,
        create_clone_plan_fn=services.create_clone_plan_fn,
        load_latest_clone_plan_fn=services.load_latest_clone_plan_fn,
        create_clone_migration_fn=services.create_clone_migration_fn,
        load_latest_clone_migration_fn=services.load_latest_clone_migration_fn,
        build_clone_timeline_replay_preview_fn=(
            services.build_clone_timeline_replay_preview_fn
        ),
        build_telegram_chat_link_bundle_fn=services.build_telegram_chat_link_bundle_fn,
        admin_try_create_exclusive_job_fn=services.admin_try_create_exclusive_job_fn,
        admin_job_get_snapshot_fn=services.admin_job_get_snapshot_fn,
        admin_job_append_log_fn=services.admin_job_append_log_fn,
        admin_job_set_status_fn=services.admin_job_set_status_fn,
        admin_start_clone_structure_job_thread_fn=(
            services.admin_start_clone_structure_job_thread_fn
        ),
        admin_start_clone_deep_preflight_job_thread_fn=(
            services.admin_start_clone_deep_preflight_job_thread_fn
        ),
        admin_start_clone_timeline_migration_job_thread_fn=(
            services.admin_start_clone_timeline_migration_job_thread_fn
        ),
    )


def _clone_deps_from_legacy_kwargs(
    *,
    logger=None,
    get_conn_fn=None,
    cfg=None,
    list_clone_source_chats_fn=None,
    build_clone_preflight_report_fn=None,
    create_clone_run_fn=None,
    load_clone_run_fn=None,
    list_clone_runs_fn=None,
    count_clone_runs_fn=None,
    load_clone_run_detail_fn=None,
    list_clone_message_mappings_fn=None,
    count_clone_message_mappings_fn=None,
    delete_clone_run_fn=None,
    create_clone_plan_fn=None,
    load_latest_clone_plan_fn=None,
    create_clone_migration_fn=None,
    load_latest_clone_migration_fn=None,
    build_clone_timeline_replay_preview_fn=None,
    build_telegram_chat_link_bundle_fn=None,
    admin_try_create_exclusive_job_fn=None,
    admin_job_get_snapshot_fn=None,
    admin_job_append_log_fn=None,
    admin_job_set_status_fn=None,
    admin_start_clone_structure_job_thread_fn=None,
    admin_start_clone_deep_preflight_job_thread_fn=None,
    admin_start_clone_timeline_migration_job_thread_fn=None,
) -> _CloneRouteDeps:
    return _CloneRouteDeps(
        logger=logger,
        get_conn_fn=get_conn_fn,
        cfg=cfg,
        list_clone_source_chats_fn=list_clone_source_chats_fn,
        build_clone_preflight_report_fn=build_clone_preflight_report_fn,
        create_clone_run_fn=create_clone_run_fn,
        load_clone_run_fn=load_clone_run_fn,
        list_clone_runs_fn=list_clone_runs_fn,
        count_clone_runs_fn=count_clone_runs_fn,
        load_clone_run_detail_fn=load_clone_run_detail_fn,
        list_clone_message_mappings_fn=list_clone_message_mappings_fn,
        count_clone_message_mappings_fn=count_clone_message_mappings_fn,
        delete_clone_run_fn=delete_clone_run_fn,
        create_clone_plan_fn=create_clone_plan_fn,
        load_latest_clone_plan_fn=load_latest_clone_plan_fn,
        create_clone_migration_fn=create_clone_migration_fn,
        load_latest_clone_migration_fn=load_latest_clone_migration_fn,
        build_clone_timeline_replay_preview_fn=build_clone_timeline_replay_preview_fn,
        build_telegram_chat_link_bundle_fn=build_telegram_chat_link_bundle_fn,
        admin_try_create_exclusive_job_fn=admin_try_create_exclusive_job_fn,
        admin_job_get_snapshot_fn=admin_job_get_snapshot_fn,
        admin_job_append_log_fn=admin_job_append_log_fn,
        admin_job_set_status_fn=admin_job_set_status_fn,
        admin_start_clone_structure_job_thread_fn=(
            admin_start_clone_structure_job_thread_fn
        ),
        admin_start_clone_deep_preflight_job_thread_fn=(
            admin_start_clone_deep_preflight_job_thread_fn
        ),
        admin_start_clone_timeline_migration_job_thread_fn=(
            admin_start_clone_timeline_migration_job_thread_fn
        ),
    )


def register_clone_routes(
    app,
    *,
    services: CloneRouteServices | None = None,
    logger=None,
    get_conn_fn=None,
    cfg=None,
    list_clone_source_chats_fn=None,
    build_clone_preflight_report_fn=None,
    create_clone_run_fn=None,
    load_clone_run_fn=None,
    list_clone_runs_fn=None,
    count_clone_runs_fn=None,
    load_clone_run_detail_fn=None,
    list_clone_message_mappings_fn=None,
    count_clone_message_mappings_fn=None,
    delete_clone_run_fn=None,
    create_clone_plan_fn=None,
    load_latest_clone_plan_fn=None,
    create_clone_migration_fn=None,
    load_latest_clone_migration_fn=None,
    build_clone_timeline_replay_preview_fn=None,
    build_telegram_chat_link_bundle_fn=None,
    admin_try_create_exclusive_job_fn=None,
    admin_job_get_snapshot_fn=None,
    admin_job_append_log_fn=None,
    admin_job_set_status_fn=None,
    admin_start_clone_structure_job_thread_fn=None,
    admin_start_clone_deep_preflight_job_thread_fn=None,
    admin_start_clone_timeline_migration_job_thread_fn=None,
) -> None:
    deps = (
        _clone_deps_from_services(services)
        if services is not None
        else _clone_deps_from_legacy_kwargs(
            logger=logger,
            get_conn_fn=get_conn_fn,
            cfg=cfg,
            list_clone_source_chats_fn=list_clone_source_chats_fn,
            build_clone_preflight_report_fn=build_clone_preflight_report_fn,
            create_clone_run_fn=create_clone_run_fn,
            load_clone_run_fn=load_clone_run_fn,
            list_clone_runs_fn=list_clone_runs_fn,
            count_clone_runs_fn=count_clone_runs_fn,
            load_clone_run_detail_fn=load_clone_run_detail_fn,
            list_clone_message_mappings_fn=list_clone_message_mappings_fn,
            count_clone_message_mappings_fn=count_clone_message_mappings_fn,
            delete_clone_run_fn=delete_clone_run_fn,
            create_clone_plan_fn=create_clone_plan_fn,
            load_latest_clone_plan_fn=load_latest_clone_plan_fn,
            create_clone_migration_fn=create_clone_migration_fn,
            load_latest_clone_migration_fn=load_latest_clone_migration_fn,
            build_clone_timeline_replay_preview_fn=(
                build_clone_timeline_replay_preview_fn
            ),
            build_telegram_chat_link_bundle_fn=build_telegram_chat_link_bundle_fn,
            admin_try_create_exclusive_job_fn=admin_try_create_exclusive_job_fn,
            admin_job_get_snapshot_fn=admin_job_get_snapshot_fn,
            admin_job_append_log_fn=admin_job_append_log_fn,
            admin_job_set_status_fn=admin_job_set_status_fn,
            admin_start_clone_structure_job_thread_fn=(
                admin_start_clone_structure_job_thread_fn
            ),
            admin_start_clone_deep_preflight_job_thread_fn=(
                admin_start_clone_deep_preflight_job_thread_fn
            ),
            admin_start_clone_timeline_migration_job_thread_fn=(
                admin_start_clone_timeline_migration_job_thread_fn
            ),
        )
    )
    _register_clone_page_routes(app)
    _register_clone_list_routes(app, deps)
    _register_clone_run_routes(app, deps)
    _register_clone_job_routes(app, deps)
