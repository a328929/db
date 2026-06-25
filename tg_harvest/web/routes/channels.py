import sqlite3
from contextlib import closing

from flask import jsonify, render_template

from tg_harvest.app.services import ChannelRouteServices
from tg_harvest.web.auth import admin_login_required, admin_page_login_required
from tg_harvest.web.responses import (
    create_exclusive_job_or_response,
    created_job_snapshot_response,
    logged_json_error,
)
from tg_harvest.web.routes.chat_links import with_chat_links


def _load_linked_channel_items(get_conn_fn, list_fn, build_link_bundle_fn, *args, **kwargs):
    with closing(get_conn_fn()) as conn:
        rows = list_fn(conn, *args, **kwargs)
    return with_chat_links(rows, build_link_bundle_fn)


def _channel_items_payload(items):
    return {"ok": True, "items": items, "count": len(items)}


def _resolve_channel_services(
    *,
    services: ChannelRouteServices | None,
    logger=None,
    get_conn_fn=None,
    cfg=None,
    list_database_channels_fn=None,
    list_missing_chat_scan_results_fn=None,
    list_absent_chat_scan_results_fn=None,
    list_restricted_chat_scan_results_fn=None,
    build_telegram_chat_link_bundle_fn=None,
    admin_try_create_exclusive_job_fn=None,
    admin_job_get_snapshot_fn=None,
    admin_job_append_log_fn=None,
    admin_job_set_status_fn=None,
    admin_start_missing_chats_scan_job_thread_fn=None,
    admin_start_absent_chats_scan_job_thread_fn=None,
    admin_start_restricted_chats_scan_job_thread_fn=None,
) -> ChannelRouteServices:
    if services is not None:
        return services
    return ChannelRouteServices(
        logger=logger,
        get_conn_fn=get_conn_fn,
        cfg=cfg,
        list_database_channels_fn=list_database_channels_fn,
        list_missing_chat_scan_results_fn=list_missing_chat_scan_results_fn,
        list_absent_chat_scan_results_fn=list_absent_chat_scan_results_fn,
        list_restricted_chat_scan_results_fn=list_restricted_chat_scan_results_fn,
        build_telegram_chat_link_bundle_fn=build_telegram_chat_link_bundle_fn,
        admin_try_create_exclusive_job_fn=admin_try_create_exclusive_job_fn,
        admin_job_get_snapshot_fn=admin_job_get_snapshot_fn,
        admin_job_append_log_fn=admin_job_append_log_fn,
        admin_job_set_status_fn=admin_job_set_status_fn,
        admin_start_missing_chats_scan_job_thread_fn=(
            admin_start_missing_chats_scan_job_thread_fn
        ),
        admin_start_absent_chats_scan_job_thread_fn=(
            admin_start_absent_chats_scan_job_thread_fn
        ),
        admin_start_restricted_chats_scan_job_thread_fn=(
            admin_start_restricted_chats_scan_job_thread_fn
        ),
    )


def register_channel_routes(
    app,
    *,
    services: ChannelRouteServices | None = None,
    logger=None,
    get_conn_fn=None,
    cfg=None,
    list_database_channels_fn=None,
    list_missing_chat_scan_results_fn=None,
    list_absent_chat_scan_results_fn=None,
    list_restricted_chat_scan_results_fn=None,
    build_telegram_chat_link_bundle_fn=None,
    admin_try_create_exclusive_job_fn=None,
    admin_job_get_snapshot_fn=None,
    admin_job_append_log_fn=None,
    admin_job_set_status_fn=None,
    admin_start_missing_chats_scan_job_thread_fn=None,
    admin_start_absent_chats_scan_job_thread_fn=None,
    admin_start_restricted_chats_scan_job_thread_fn=None,
) -> None:
    services = _resolve_channel_services(
        services=services,
        logger=logger,
        get_conn_fn=get_conn_fn,
        cfg=cfg,
        list_database_channels_fn=list_database_channels_fn,
        list_missing_chat_scan_results_fn=list_missing_chat_scan_results_fn,
        list_absent_chat_scan_results_fn=list_absent_chat_scan_results_fn,
        list_restricted_chat_scan_results_fn=list_restricted_chat_scan_results_fn,
        build_telegram_chat_link_bundle_fn=build_telegram_chat_link_bundle_fn,
        admin_try_create_exclusive_job_fn=admin_try_create_exclusive_job_fn,
        admin_job_get_snapshot_fn=admin_job_get_snapshot_fn,
        admin_job_append_log_fn=admin_job_append_log_fn,
        admin_job_set_status_fn=admin_job_set_status_fn,
        admin_start_missing_chats_scan_job_thread_fn=(
            admin_start_missing_chats_scan_job_thread_fn
        ),
        admin_start_absent_chats_scan_job_thread_fn=(
            admin_start_absent_chats_scan_job_thread_fn
        ),
        admin_start_restricted_chats_scan_job_thread_fn=(
            admin_start_restricted_chats_scan_job_thread_fn
        ),
    )

    @app.get("/admin/channels")
    @admin_page_login_required
    def admin_channels_page():
        return render_template("admin_channels.html")

    @app.get("/api/admin/channels")
    @admin_login_required
    def api_admin_channels():
        try:
            from flask import request

            sort = request.args.get("sort", "")
            channels = _load_linked_channel_items(
                services.get_conn_fn,
                services.list_database_channels_fn,
                services.build_telegram_chat_link_bundle_fn,
                sort=sort,
            )
            return jsonify({"ok": True, "channels": channels})
        except sqlite3.Error:
            return logged_json_error(
                services.logger,
                "读取频道管理列表失败",
                "读取频道管理列表失败",
            )
        except Exception:
            return logged_json_error(services.logger, "系统异常", "系统异常")

    def _scan_result_response(list_fn, log_message: str):
        try:
            items = _load_linked_channel_items(
                services.get_conn_fn,
                list_fn,
                services.build_telegram_chat_link_bundle_fn,
            )
            return jsonify(_channel_items_payload(items))
        except sqlite3.Error:
            return logged_json_error(services.logger, log_message, log_message)
        except Exception:
            return logged_json_error(services.logger, "系统异常", "系统异常")

    @app.get("/api/admin/channels/missing")
    @admin_login_required
    def api_admin_missing_channels():
        return _scan_result_response(
            services.list_missing_chat_scan_results_fn,
            "读取未入库群组扫描结果失败",
        )

    @app.get("/api/admin/channels/absent")
    @admin_login_required
    def api_admin_absent_channels():
        return _scan_result_response(
            services.list_absent_chat_scan_results_fn,
            "读取账号外数据库群组扫描结果失败",
        )

    @app.get("/api/admin/channels/restricted")
    @admin_login_required
    def api_admin_restricted_channels():
        return _scan_result_response(
            services.list_restricted_chat_scan_results_fn,
            "读取内容限制群组扫描结果失败",
        )

    def _create_scan_job_response(
        job_type,
        *,
        target_label,
        received_log,
        start_thread_fn,
    ):
        job_id, error_response = create_exclusive_job_or_response(
            services.admin_try_create_exclusive_job_fn,
            job_type,
            target_chat_id=None,
            target_label=target_label,
        )
        if error_response is not None:
            return error_response

        services.admin_job_append_log_fn(job_id, received_log)
        start_thread_fn(
            job_id,
            cfg=services.cfg,
            get_conn_fn=services.get_conn_fn,
            admin_job_set_status_fn=services.admin_job_set_status_fn,
            admin_job_append_log_fn=services.admin_job_append_log_fn,
        )
        return created_job_snapshot_response(job_id, services.admin_job_get_snapshot_fn)

    @app.post("/api/admin/channels/missing/scan")
    @admin_login_required
    def api_admin_missing_channels_scan():
        return _create_scan_job_response(
            "missing_chats_scan",
            target_label="账号未入库群组扫描",
            received_log="已接收账号未入库群组扫描请求",
            start_thread_fn=services.admin_start_missing_chats_scan_job_thread_fn,
        )

    @app.post("/api/admin/channels/absent/scan")
    @admin_login_required
    def api_admin_absent_channels_scan():
        return _create_scan_job_response(
            "absent_chats_scan",
            target_label="账号外数据库群组扫描",
            received_log="已接收账号外数据库群组扫描请求",
            start_thread_fn=services.admin_start_absent_chats_scan_job_thread_fn,
        )

    @app.post("/api/admin/channels/restricted/scan")
    @admin_login_required
    def api_admin_restricted_channels_scan():
        return _create_scan_job_response(
            "restricted_chats_scan",
            target_label="内容限制/风险标记扫描",
            received_log="已接收内容限制/风险标记扫描请求",
            start_thread_fn=services.admin_start_restricted_chats_scan_job_thread_fn,
        )
