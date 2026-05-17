# -*- coding: utf-8 -*-
import sqlite3
from contextlib import closing

from flask import jsonify, render_template

from tg_harvest.web.auth import admin_login_required


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


def register_channel_routes(
    app,
    *,
    logger,
    get_conn_fn,
    cfg,
    list_database_channels_fn,
    list_missing_chat_scan_results_fn,
    list_absent_chat_scan_results_fn,
    list_restricted_chat_scan_results_fn,
    build_telegram_chat_link_bundle_fn,
    admin_try_create_exclusive_job_fn,
    admin_job_get_snapshot_fn,
    admin_job_append_log_fn,
    admin_job_set_status_fn,
    admin_start_missing_chats_scan_job_thread_fn,
    admin_start_absent_chats_scan_job_thread_fn,
    admin_start_restricted_chats_scan_job_thread_fn,
) -> None:
    @app.get("/admin/channels")
    def admin_channels_page():
        return render_template("admin_channels.html")

    @app.get("/api/admin/channels")
    @admin_login_required
    def api_admin_channels():
        try:
            from flask import request

            sort = request.args.get("sort", "")
            with closing(get_conn_fn()) as conn:
                channels = list_database_channels_fn(conn, sort=sort)
            channels = _with_chat_links(channels, build_telegram_chat_link_bundle_fn)
            return jsonify({"ok": True, "channels": channels})
        except sqlite3.Error:
            logger.exception("读取频道管理列表失败")
            return jsonify({"ok": False, "error": "读取频道管理列表失败"}), 500
        except Exception:
            logger.exception("系统异常")
            return jsonify({"ok": False, "error": "系统异常"}), 500

    @app.get("/api/admin/channels/missing")
    @admin_login_required
    def api_admin_missing_channels():
        try:
            with closing(get_conn_fn()) as conn:
                rows = list_missing_chat_scan_results_fn(conn)
            items = _with_chat_links(rows, build_telegram_chat_link_bundle_fn)
            return jsonify({"ok": True, "items": items, "count": len(items)})
        except sqlite3.Error:
            logger.exception("读取未入库群组扫描结果失败")
            return jsonify({"ok": False, "error": "读取未入库群组扫描结果失败"}), 500
        except Exception:
            logger.exception("系统异常")
            return jsonify({"ok": False, "error": "系统异常"}), 500

    @app.get("/api/admin/channels/absent")
    @admin_login_required
    def api_admin_absent_channels():
        try:
            with closing(get_conn_fn()) as conn:
                rows = list_absent_chat_scan_results_fn(conn)
            items = _with_chat_links(rows, build_telegram_chat_link_bundle_fn)
            return jsonify({"ok": True, "items": items, "count": len(items)})
        except sqlite3.Error:
            logger.exception("读取账号外数据库群组扫描结果失败")
            return jsonify({"ok": False, "error": "读取账号外数据库群组扫描结果失败"}), 500
        except Exception:
            logger.exception("系统异常")
            return jsonify({"ok": False, "error": "系统异常"}), 500

    @app.get("/api/admin/channels/restricted")
    @admin_login_required
    def api_admin_restricted_channels():
        try:
            with closing(get_conn_fn()) as conn:
                rows = list_restricted_chat_scan_results_fn(conn)
            items = _with_chat_links(rows, build_telegram_chat_link_bundle_fn)
            return jsonify({"ok": True, "items": items, "count": len(items)})
        except sqlite3.Error:
            logger.exception("读取内容限制群组扫描结果失败")
            return jsonify({"ok": False, "error": "读取内容限制群组扫描结果失败"}), 500
        except Exception:
            logger.exception("系统异常")
            return jsonify({"ok": False, "error": "系统异常"}), 500

    def _create_scan_job_response(
        job_type,
        *,
        target_label,
        received_log,
        start_thread_fn,
    ):
        job, existing_job = admin_try_create_exclusive_job_fn(
            job_type,
            target_chat_id=None,
            target_label=target_label,
        )
        if existing_job is not None:
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

        job_id = str((job or {}).get("job_id") or "")
        if not job_id:
            return jsonify({"ok": False, "error": "任务创建失败"}), 500

        admin_job_append_log_fn(job_id, received_log)
        start_thread_fn(
            job_id,
            cfg=cfg,
            get_conn_fn=get_conn_fn,
            admin_job_set_status_fn=admin_job_set_status_fn,
            admin_job_append_log_fn=admin_job_append_log_fn,
        )
        snapshot = admin_job_get_snapshot_fn(job_id)
        if snapshot is None:
            return jsonify({"ok": False, "error": "任务创建失败"}), 500
        return jsonify({"ok": True, "job": snapshot})

    @app.post("/api/admin/channels/missing/scan")
    @admin_login_required
    def api_admin_missing_channels_scan():
        return _create_scan_job_response(
            "missing_chats_scan",
            target_label="账号未入库群组扫描",
            received_log="已接收账号未入库群组扫描请求",
            start_thread_fn=admin_start_missing_chats_scan_job_thread_fn,
        )

    @app.post("/api/admin/channels/absent/scan")
    @admin_login_required
    def api_admin_absent_channels_scan():
        return _create_scan_job_response(
            "absent_chats_scan",
            target_label="账号外数据库群组扫描",
            received_log="已接收账号外数据库群组扫描请求",
            start_thread_fn=admin_start_absent_chats_scan_job_thread_fn,
        )

    @app.post("/api/admin/channels/restricted/scan")
    @admin_login_required
    def api_admin_restricted_channels_scan():
        return _create_scan_job_response(
            "restricted_chats_scan",
            target_label="内容限制/风险标记扫描",
            received_log="已接收内容限制/风险标记扫描请求",
            start_thread_fn=admin_start_restricted_chats_scan_job_thread_fn,
        )
