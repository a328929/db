# -*- coding: utf-8 -*-
import sqlite3
from contextlib import closing
from typing import Any, Callable, Optional

from flask import jsonify, request


def register_admin_routes(
    app,
    *,
    logger,
    cfg,
    get_conn_fn: Callable[[], sqlite3.Connection],
    parse_admin_chat_id_fn: Callable[[Optional[str]], Optional[int]],
    build_admin_chats_payload_fn: Callable[[sqlite3.Connection], dict],
    build_admin_stats_payload_fn: Callable[[sqlite3.Connection, Optional[int]], tuple],
    admin_get_chat_brief_fn: Callable[[sqlite3.Connection, int], Optional[dict]],
    admin_job_get_snapshot_fn: Callable[[str], Optional[dict]],
    admin_job_get_logs_fn: Callable[[str, int], Optional[list]],
    admin_has_any_active_job_fn: Callable[[], bool],
    admin_create_chat_job_if_absent_fn: Callable[..., tuple],
    admin_job_create_fn: Callable[..., dict],
    admin_job_append_log_fn: Callable[[str, str], Any],
    admin_start_harvest_job_thread_fn: Callable[..., Any],
    admin_start_update_job_thread_fn: Callable[..., Any],
    admin_start_delete_job_thread_fn: Callable[..., Any],
    admin_start_cleanup_job_thread_fn: Callable[..., Any],
    admin_start_cleanup_empty_job_thread_fn: Callable[..., Any],
    admin_make_job_log_handler_fn,
    admin_job_set_status_fn,
    admin_harvest_target_max_len: int,
    admin_cleanup_keyword_max_len: int,
    has_fts_fn: Callable[[sqlite3.Connection], bool],
) -> None:
    @app.get("/api/admin/chats")
    def api_admin_chats():
        try:
            with closing(get_conn_fn()) as conn:
                payload = build_admin_chats_payload_fn(conn)
            return jsonify(payload)
        except sqlite3.Error:
            logger.exception("读取后台群列表失败")
            return jsonify({"ok": False, "error": "读取后台群列表失败"}), 500
        except Exception:
            logger.exception("系统异常")
            return jsonify({"ok": False, "error": "系统异常"}), 500

    @app.get("/api/admin/stats")
    def api_admin_stats():
        try:
            chat_id = parse_admin_chat_id_fn(request.args.get("chat_id"))
        except (ValueError, TypeError):
            return jsonify({"ok": False, "error": "chat_id 参数非法"}), 400

        try:
            with closing(get_conn_fn()) as conn:
                payload, status_code = build_admin_stats_payload_fn(conn, chat_id)
            return jsonify(payload), status_code
        except sqlite3.Error:
            logger.exception("读取后台统计失败")
            return jsonify({"ok": False, "error": "读取后台统计失败"}), 500
        except Exception:
            logger.exception("系统异常")
            return jsonify({"ok": False, "error": "系统异常"}), 500

    @app.get("/api/admin/jobs/<job_id>")
    def api_admin_job_snapshot(job_id: str):
        snapshot = admin_job_get_snapshot_fn(job_id)
        if snapshot is None:
            return jsonify({"ok": False, "error": "任务不存在"}), 404
        return jsonify({"ok": True, "job": snapshot})

    @app.get("/api/admin/jobs/<job_id>/logs")
    def api_admin_job_logs(job_id: str):
        raw_after_seq = (request.args.get("after_seq") or "").strip()
        try:
            after_seq = int(raw_after_seq) if raw_after_seq else 0
            if after_seq < 0:
                raise ValueError()
        except (ValueError, TypeError):
            return jsonify({"ok": False, "error": "after_seq 参数非法"}), 400

        logs = admin_job_get_logs_fn(job_id, after_seq=after_seq)
        if logs is None:
            return jsonify({"ok": False, "error": "任务不存在"}), 404
        return jsonify({"ok": True, "job_id": job_id, "after_seq": after_seq, "logs": logs})

    @app.post("/api/admin/jobs/harvest")
    def api_admin_job_create_harvest():
        if not request.is_json:
            return jsonify({"ok": False, "error": "请求必须为 JSON"}), 400

        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify({"ok": False, "error": "请求 JSON 格式错误"}), 400

        if admin_has_any_active_job_fn():
            return jsonify({"ok": False, "error": "当前已有进行中的任务，请等待完成后再试"}), 409

        raw_target = data.get("target", "")
        if not isinstance(raw_target, str):
            return jsonify({"ok": False, "error": "target 参数必须为字符串"}), 400

        target = raw_target.strip()
        if not target:
            return jsonify({"ok": False, "error": "target 不能为空"}), 400
        if len(target) > admin_harvest_target_max_len:
            return jsonify({"ok": False, "error": f"target 长度不能超过 {admin_harvest_target_max_len}"}), 400

        job = admin_job_create_fn("harvest", target_chat_id=None, target_label=target)
        job_id = str(job.get("job_id") or "")

        admin_job_append_log_fn(job_id, f"已接收抓取目标：{target}")
        admin_start_harvest_job_thread_fn(
            job_id,
            target,
            cfg=cfg,
            get_conn_fn=get_conn_fn,
            admin_make_job_log_handler_fn=admin_make_job_log_handler_fn,
            admin_job_set_status_fn=admin_job_set_status_fn,
            admin_job_append_log_fn=admin_job_append_log_fn,
        )

        snapshot = admin_job_get_snapshot_fn(job_id)
        if snapshot is None:
            return jsonify({"ok": False, "error": "任务创建失败"}), 500
        return jsonify({"ok": True, "job": snapshot})

    @app.post("/api/admin/jobs/update")
    def api_admin_job_create_update():
        if not request.is_json:
            return jsonify({"ok": False, "error": "请求必须为 JSON"}), 400

        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify({"ok": False, "error": "请求 JSON 格式错误"}), 400

        if admin_has_any_active_job_fn():
            return jsonify({"ok": False, "error": "当前已有进行中的任务，请等待完成后再试"}), 409

        raw_chat_id = data.get("chat_id")
        is_all_scope = isinstance(raw_chat_id, str) and raw_chat_id.strip().lower() == "all"
        chat_id: Optional[int] = None
        if not is_all_scope:
            try:
                chat_id = int(raw_chat_id)
            except (TypeError, ValueError):
                return jsonify({"ok": False, "error": "chat_id 参数非法"}), 400

        incremental = data.get("incremental", True)
        if not isinstance(incremental, bool):
            return jsonify({"ok": False, "error": "incremental 参数必须为布尔值"}), 400
        if incremental is False:
            return jsonify({"ok": False, "error": "当前仅支持增量更新"}), 400

        if is_all_scope:
            job = admin_job_create_fn("update", target_chat_id=None, target_label="全部群聊")
            job_id = str(job.get("job_id") or "")
            admin_job_append_log_fn(job_id, "已接收增量更新请求")
            admin_job_append_log_fn(job_id, "目标范围：全部群聊")
            admin_start_update_job_thread_fn(
                job_id,
                "all",
                "全部群聊",
                incremental,
                cfg=cfg,
                get_conn_fn=get_conn_fn,
                admin_make_job_log_handler_fn=admin_make_job_log_handler_fn,
                admin_job_set_status_fn=admin_job_set_status_fn,
                admin_job_append_log_fn=admin_job_append_log_fn,
            )
        else:
            try:
                with closing(get_conn_fn()) as conn:
                    chat_brief = admin_get_chat_brief_fn(conn, chat_id)
            except sqlite3.Error:
                logger.exception("读取群信息失败")
                return jsonify({"ok": False, "error": "读取群信息失败"}), 500
            except Exception:
                logger.exception("系统异常")
                return jsonify({"ok": False, "error": "系统异常"}), 500

            if chat_brief is None:
                return jsonify({"ok": False, "error": "chat_id 不存在"}), 404

            chat_title = str(chat_brief["chat_title"])
            job, existing_job = admin_create_chat_job_if_absent_fn("update", chat_id=chat_id, target_label=chat_title)
            if existing_job is not None:
                return jsonify({"ok": False, "error": "该目标已有进行中的任务", "existing_job": existing_job}), 409
            job_id = str(job.get("job_id") or "")

            admin_job_append_log_fn(job_id, "已接收增量更新请求")
            admin_job_append_log_fn(job_id, f"目标群组：{chat_title} ({chat_id})")
            admin_start_update_job_thread_fn(
                job_id,
                chat_id,
                chat_title,
                incremental,
                cfg=cfg,
                get_conn_fn=get_conn_fn,
                admin_make_job_log_handler_fn=admin_make_job_log_handler_fn,
                admin_job_set_status_fn=admin_job_set_status_fn,
                admin_job_append_log_fn=admin_job_append_log_fn,
            )

        snapshot = admin_job_get_snapshot_fn(job_id)
        if snapshot is None:
            return jsonify({"ok": False, "error": "任务创建失败"}), 500
        return jsonify({"ok": True, "job": snapshot})

    @app.post("/api/admin/jobs/delete")
    def api_admin_job_create_delete():
        if not request.is_json:
            return jsonify({"ok": False, "error": "请求必须为 JSON"}), 400

        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify({"ok": False, "error": "请求 JSON 格式错误"}), 400

        if admin_has_any_active_job_fn():
            return jsonify({"ok": False, "error": "当前已有进行中的任务，请等待完成后再试"}), 409

        raw_chat_id = data.get("chat_id")
        try:
            chat_id = int(raw_chat_id)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "chat_id 参数非法"}), 400

        try:
            with closing(get_conn_fn()) as conn:
                chat_brief = admin_get_chat_brief_fn(conn, chat_id)
        except sqlite3.Error:
            logger.exception("读取群信息失败")
            return jsonify({"ok": False, "error": "读取群信息失败"}), 500
        except Exception:
            logger.exception("系统异常")
            return jsonify({"ok": False, "error": "系统异常"}), 500

        if chat_brief is None:
            return jsonify({"ok": False, "error": "chat_id 不存在"}), 404

        chat_title = str(chat_brief["chat_title"])
        job, existing_job = admin_create_chat_job_if_absent_fn("delete", chat_id=chat_id, target_label=chat_title)
        if existing_job is not None:
            return jsonify({"ok": False, "error": "该目标已有进行中的任务", "existing_job": existing_job}), 409
        job_id = str(job.get("job_id") or "")

        admin_job_append_log_fn(job_id, "已接收删除请求")
        admin_job_append_log_fn(job_id, f"目标群组：{chat_title} ({chat_id})")
        admin_start_delete_job_thread_fn(
            job_id,
            chat_id,
            chat_title,
            get_conn_fn=get_conn_fn,
            admin_job_set_status_fn=admin_job_set_status_fn,
            admin_job_append_log_fn=admin_job_append_log_fn,
        )

        snapshot = admin_job_get_snapshot_fn(job_id)
        if snapshot is None:
            return jsonify({"ok": False, "error": "任务创建失败"}), 500
        return jsonify({"ok": True, "job": snapshot})

    @app.post("/api/admin/jobs/cleanup")
    def api_admin_job_create_cleanup():
        if not request.is_json:
            return jsonify({"ok": False, "error": "请求必须为 JSON"}), 400

        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify({"ok": False, "error": "请求 JSON 格式错误"}), 400

        if admin_has_any_active_job_fn():
            return jsonify({"ok": False, "error": "当前已有进行中的任务，请等待完成后再试"}), 409

        raw_keyword = data.get("keyword")
        if not isinstance(raw_keyword, str):
            return jsonify({"ok": False, "error": "keyword 参数必须为字符串"}), 400

        keyword = raw_keyword.strip()
        if not keyword:
            return jsonify({"ok": False, "error": "keyword 不能为空"}), 400
        if len(keyword) > admin_cleanup_keyword_max_len:
            return jsonify({"ok": False, "error": f"keyword 长度不能超过 {admin_cleanup_keyword_max_len}"}), 400

        raw_scope = data.get("scope", "")
        scope = str(raw_scope or "").strip().lower()
        if scope not in {"all", "chat"}:
            return jsonify({"ok": False, "error": "scope 参数必须为 all 或 chat"}), 400

        chat_id: Optional[int] = None
        target_label = "全部数据"

        if scope == "chat":
            raw_chat_id = data.get("chat_id")
            try:
                chat_id = int(raw_chat_id)
            except (TypeError, ValueError):
                return jsonify({"ok": False, "error": "chat_id 参数非法"}), 400

            try:
                with closing(get_conn_fn()) as conn:
                    chat_brief = admin_get_chat_brief_fn(conn, chat_id)
            except sqlite3.Error:
                logger.exception("读取群信息失败")
                return jsonify({"ok": False, "error": "读取群信息失败"}), 500
            except Exception:
                logger.exception("系统异常")
                return jsonify({"ok": False, "error": "系统异常"}), 500

            if chat_brief is None:
                return jsonify({"ok": False, "error": "chat_id 不存在"}), 404

            target_label = str(chat_brief["chat_title"])

        job = admin_job_create_fn("cleanup", target_chat_id=chat_id, target_label=target_label)
        job_id = str(job.get("job_id") or "")

        admin_job_append_log_fn(job_id, "已接收垃圾清理请求")
        scope_label = {"all": "全部数据", "chat": "当前群组"}.get(scope, scope)
        admin_job_append_log_fn(job_id, f"作用范围：{scope_label}")
        chat_suffix = '' if chat_id is None else f' ({chat_id})'
        admin_job_append_log_fn(job_id, f"目标：{target_label}{chat_suffix}")
        admin_job_append_log_fn(job_id, f"关键字：{keyword}")
        admin_start_cleanup_job_thread_fn(
            job_id=job_id,
            keyword=keyword,
            scope=scope,
            chat_id=chat_id,
            target_label=target_label,
            get_conn_fn=get_conn_fn,
            admin_job_set_status_fn=admin_job_set_status_fn,
            admin_job_append_log_fn=admin_job_append_log_fn,
            has_fts_fn=has_fts_fn,
        )

        snapshot = admin_job_get_snapshot_fn(job_id)
        if snapshot is None:
            return jsonify({"ok": False, "error": "任务创建失败"}), 500

        return jsonify({
            "ok": True,
            "job": snapshot,
            "request": {
                "scope": scope,
                "chat_id": chat_id,
                "target_label": target_label,
                "keyword": keyword,
            },
        })


    @app.post("/api/admin/jobs/cleanup-empty")
    def api_admin_job_create_cleanup_empty():
        if not request.is_json:
            return jsonify({"ok": False, "error": "请求必须为 JSON"}), 400

        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify({"ok": False, "error": "请求 JSON 格式错误"}), 400

        if admin_has_any_active_job_fn():
            return jsonify({"ok": False, "error": "当前已有进行中的任务，请等待完成后再试"}), 409

        raw_scope = data.get("scope", "")
        scope = str(raw_scope or "").strip().lower()
        if scope not in {"all", "chat"}:
            return jsonify({"ok": False, "error": "scope 参数必须为 all 或 chat"}), 400

        chat_id: Optional[int] = None
        target_label = "全部数据"

        if scope == "chat":
            raw_chat_id = data.get("chat_id")
            try:
                chat_id = int(raw_chat_id)
            except (TypeError, ValueError):
                return jsonify({"ok": False, "error": "chat_id 参数非法"}), 400

            try:
                with closing(get_conn_fn()) as conn:
                    chat_brief = admin_get_chat_brief_fn(conn, chat_id)
            except sqlite3.Error:
                logger.exception("读取群信息失败")
                return jsonify({"ok": False, "error": "读取群信息失败"}), 500
            except Exception:
                logger.exception("系统异常")
                return jsonify({"ok": False, "error": "系统异常"}), 500

            if chat_brief is None:
                return jsonify({"ok": False, "error": "chat_id 不存在"}), 404

            target_label = str(chat_brief["chat_title"])

        job = admin_job_create_fn("cleanup_empty", target_chat_id=chat_id, target_label=target_label)
        job_id = str(job.get("job_id") or "")

        admin_job_append_log_fn(job_id, "已接收无文本媒体清理请求")
        scope_label = {"all": "全部数据", "chat": "当前群组"}.get(scope, scope)
        admin_job_append_log_fn(job_id, f"作用范围：{scope_label}")
        chat_suffix = '' if chat_id is None else f' ({chat_id})'
        admin_job_append_log_fn(job_id, f"目标：{target_label}{chat_suffix}")
        admin_start_cleanup_empty_job_thread_fn(
            job_id=job_id,
            scope=scope,
            chat_id=chat_id,
            target_label=target_label,
            get_conn_fn=get_conn_fn,
            admin_job_set_status_fn=admin_job_set_status_fn,
            admin_job_append_log_fn=admin_job_append_log_fn,
            has_fts_fn=has_fts_fn,
        )

        snapshot = admin_job_get_snapshot_fn(job_id)
        if snapshot is None:
            return jsonify({"ok": False, "error": "任务创建失败"}), 500

        return jsonify({
            "ok": True,
            "job": snapshot,
            "request": {
                "scope": scope,
                "chat_id": chat_id,
                "target_label": target_label,
            },
        })
