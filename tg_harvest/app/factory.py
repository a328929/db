# -*- coding: utf-8 -*-
import sqlite3
import os
import logging
import threading
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from datetime import timedelta

from flask import Flask

from tg_harvest.config import CFG
from tg_harvest.app.services import AdminRouteServices, RouteRegistryServices
from tg_harvest.search.params import _parse_search_params
from tg_harvest.search.service import _search_payload_service, configure_message_search_maintenance, schedule_message_search_maintenance
from tg_harvest.ingest.parse import setup_logging
from tg_harvest.admin_jobs.runners import _admin_start_harvest_job_thread, _admin_start_update_job_thread, _admin_start_delete_job_thread, _admin_start_cleanup_job_thread, _admin_start_cleanup_empty_job_thread
from tg_harvest.admin_jobs.core import _admin_create_chat_job_if_absent, _admin_has_any_active_job, _admin_job_append_log, _admin_job_create, _admin_job_get_logs, _admin_job_get_snapshot, _admin_try_create_exclusive_job, _admin_job_set_status, _admin_make_job_log_handler, _admin_recover_interrupted_jobs, configure_admin_job_runtime
from tg_harvest.storage.schema import ensure_configured_db, connect_db
from tg_harvest.domain.meta_payload import _build_meta_payload, _chat_sort_key, _chat_title_or_fallback
from tg_harvest.search.result_mapper import _map_search_items
from tg_harvest.storage.access import FROM_SQL, get_conn as runtime_get_conn, has_fts
from tg_harvest.app.routes_registry import register_all_routes

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]


setup_logging()

DB_PATH = Path(CFG.db_name)
PAGE_SIZE = 100
MAX_COUNT = 50000000

ADMIN_HARVEST_TARGET_MAX_LEN = 300
ADMIN_CLEANUP_KEYWORD_MAX_LEN = 120


def get_conn() -> sqlite3.Connection:
    return runtime_get_conn(
        db_path=DB_PATH,
        connect_db_fn=connect_db,
        cache_mb=CFG.sqlite_cache_mb,
        mmap_mb=CFG.sqlite_mmap_mb,
    )


def _build_admin_chats_payload(conn: sqlite3.Connection) -> Dict[str, Any]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                c.chat_id,
                c.chat_title,
                c.message_count
            FROM chats c
            """
        )
        # /api/admin/chats 主字段契约为 chat_id/chat_title/message_count；冗余别名字段已移除（前端兼容在 JS 内处理）。
        chats = [
            {
                "chat_id": int(row["chat_id"]),
                "chat_title": _chat_title_or_fallback(
                    int(row["chat_id"]), row["chat_title"]
                ),
                "message_count": int(row["message_count"] or 0),
            }
            for row in cur.fetchall()
        ]
        chats.sort(
            key=lambda item: _chat_sort_key(str(item.get("chat_title") or ""), int(str(item.get("chat_id") or 0)))
        )
        return {"ok": True, "chats": chats}
    finally:
        cur.close()


def _parse_admin_chat_id(raw_chat_id: Optional[str]) -> Optional[int]:
    value = (raw_chat_id or "").strip()
    if not value or value.lower() == "none":
        return None
    return int(value)


def _build_admin_stats_payload(
    conn: sqlite3.Connection, chat_id: Optional[int]
) -> Tuple[Dict[str, Any], int]:
    cur = conn.cursor()
    try:
        if chat_id is None:
            cur.execute("SELECT COUNT(*) AS chat_count FROM chats")
            chat_count = int(cur.fetchone()["chat_count"] or 0)
            cur.execute("SELECT COALESCE(SUM(message_count), 0) AS message_count FROM chats")
            message_count = int(cur.fetchone()["message_count"] or 0)

            return {
                "ok": True,
                "scope": "all",
                "chat_count": chat_count,
                "message_count": message_count,
            }, 200

        cur.execute(
            """
            SELECT
                c.chat_id,
                c.chat_title,
                c.message_count
            FROM chats c
            WHERE c.chat_id = ?
            """,
            (chat_id,),
        )
        row = cur.fetchone()
        if row is None:
            return {"ok": False, "error": "chat_id 不存在"}, 404

        return {
            "ok": True,
            "scope": "chat",
            "chat_id": int(row["chat_id"]),
            "chat_title": _chat_title_or_fallback(
                int(row["chat_id"]), row["chat_title"]
            ),
            "message_count": int(row["message_count"] or 0),
        }, 200
    finally:
        cur.close()


def _admin_get_chat_brief(
    conn: sqlite3.Connection, chat_id: int
) -> Optional[Dict[str, Any]]:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT chat_id, chat_title FROM chats WHERE chat_id = ? LIMIT 1",
            (chat_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        actual_chat_id = int(row["chat_id"])
        return {
            "chat_id": actual_chat_id,
            "chat_title": _chat_title_or_fallback(actual_chat_id, row["chat_title"]),
        }
    finally:
        cur.close()


def _ensure_db() -> None:
    conn, _ = ensure_configured_db(cfg=CFG)
    conn.close()
    recovered_jobs = _admin_recover_interrupted_jobs()
    if recovered_jobs > 0:
        logger.warning(
            "检测到 %s 个后台任务因进程重启中断，已统一标记为失败",
            recovered_jobs,
        )
    configure_message_search_maintenance(get_conn)
    schedule_message_search_maintenance()


def _ensure_runtime_db(app: Flask) -> None:
    if app.extensions.get("tg_db_ready"):
        return

    lock = app.extensions["tg_db_ready_lock"]
    with lock:
        if app.extensions.get("tg_db_ready"):
            return
        _ensure_db()
        app.extensions["tg_db_ready"] = True


def _build_route_services() -> RouteRegistryServices:
    admin_services = AdminRouteServices(
        logger=logger,
        cfg=CFG,
        get_conn_fn=get_conn,
        parse_admin_chat_id_fn=_parse_admin_chat_id,
        build_admin_chats_payload_fn=_build_admin_chats_payload,
        build_admin_stats_payload_fn=_build_admin_stats_payload,
        admin_get_chat_brief_fn=_admin_get_chat_brief,
        admin_job_get_snapshot_fn=_admin_job_get_snapshot,
        admin_job_get_logs_fn=_admin_job_get_logs,
        admin_has_any_active_job_fn=_admin_has_any_active_job,
        admin_try_create_exclusive_job_fn=_admin_try_create_exclusive_job,
        admin_create_chat_job_if_absent_fn=_admin_create_chat_job_if_absent,
        admin_job_create_fn=_admin_job_create,
        admin_job_append_log_fn=_admin_job_append_log,
        admin_start_harvest_job_thread_fn=_admin_start_harvest_job_thread,
        admin_start_update_job_thread_fn=_admin_start_update_job_thread,
        admin_start_delete_job_thread_fn=_admin_start_delete_job_thread,
        admin_start_cleanup_job_thread_fn=_admin_start_cleanup_job_thread,
        admin_start_cleanup_empty_job_thread_fn=_admin_start_cleanup_empty_job_thread,
        admin_make_job_log_handler_fn=_admin_make_job_log_handler,
        admin_job_set_status_fn=_admin_job_set_status,
        admin_harvest_target_max_len=ADMIN_HARVEST_TARGET_MAX_LEN,
        admin_cleanup_keyword_max_len=ADMIN_CLEANUP_KEYWORD_MAX_LEN,
    )
    return RouteRegistryServices(
        page_size=PAGE_SIZE,
        logger=logger,
        get_conn_fn=get_conn,
        build_meta_payload_fn=_build_meta_payload,
        has_fts_fn=has_fts,
        from_sql=FROM_SQL,
        max_count=MAX_COUNT,
        map_search_items_fn=_map_search_items,
        parse_search_params_fn=_parse_search_params,
        search_payload_service_fn=_search_payload_service,
        admin=admin_services,
    )


def create_app(*, init_db: bool = False) -> Flask:
    configure_admin_job_runtime()
    if init_db:
        _ensure_db()
    app = Flask(
        "tg_harvest",
        template_folder=str(PROJECT_ROOT / "templates"),
        static_folder=str(PROJECT_ROOT / "static"),
    )
    app.secret_key = os.getenv("FLASK_SECRET_KEY", os.urandom(32).hex())
    app.extensions["tg_db_ready"] = bool(init_db)
    app.extensions["tg_db_ready_lock"] = threading.Lock()

    # 映射：将配置文件中的秒数注入为 Flask Session 的持久生命周期
    app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(seconds=CFG.admin_session_expiry)
    app.config["TG_DB_PATH"] = str(DB_PATH)
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    @app.before_request
    def _before_request_ensure_db() -> None:
        _ensure_runtime_db(app)

    @app.after_request
    def _apply_security_headers(response):
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self'; "
            "style-src 'self'; "
            "img-src 'self' data:; "
            "object-src 'none'; "
            "base-uri 'self'; "
            "frame-ancestors 'none'"
        )
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        return response
    
    register_all_routes(app, services=_build_route_services())
    return app


app = create_app(init_db=False)


def run_web_server(*, host: str = "0.0.0.0", port: int = 8890, debug: bool = False) -> None:
    _ensure_db()
    app.run(host=host, port=port, debug=debug)

if __name__ == "__main__":
    run_web_server()
