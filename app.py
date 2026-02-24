# -*- coding: utf-8 -*-
import sqlite3
import os
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask

from tg_harvest.config import CFG
from tg_harvest.search_params import _parse_search_params
from tg_harvest.search_service import _search_payload_service
from tg_harvest.admin_jobs_runners import (
    _admin_start_harvest_job_thread,
    _admin_start_update_job_thread,
    _admin_start_delete_job_thread,
    _admin_start_cleanup_job_thread,
)
from tg_harvest.admin_jobs_core import (
    _admin_create_chat_job_if_absent,
    _admin_get_progress_log_step,
    _admin_has_any_active_job,
    _admin_job_append_log,
    _admin_job_create,
    _admin_job_get_logs,
    _admin_job_get_snapshot,
    _admin_job_set_status,
    _admin_job_update_progress,
    _admin_make_job_log_handler,
)
from tg_harvest.db import connect_db, create_schema, resolve_db_path as resolve_db_path_lib
from tg_harvest.meta_payload import _build_meta_payload, _chat_sort_key, _chat_title_or_fallback
from tg_harvest.search_query_text import tokenize_query, to_fts_match
from tg_harvest.search_result_mapper import _map_search_items
from tg_harvest.db_access_runtime import FROM_SQL, get_conn as runtime_get_conn, has_fts
from tg_harvest.routes_registry import register_all_routes

logger = logging.getLogger(__name__)


def _init_logging() -> None:
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
        logging.getLogger("telethon").setLevel(logging.WARNING)
        logging.getLogger("werkzeug").setLevel(logging.WARNING)


_init_logging()

DB_PATH_STR = resolve_db_path_lib(os.getenv("TG_DB_NAME", "tg_data.db"))
DB_PATH = Path(DB_PATH_STR)
PAGE_SIZE = 100
MAX_COUNT = 50000

ADMIN_HARVEST_TARGET_MAX_LEN = 300
ADMIN_CLEANUP_KEYWORD_MAX_LEN = 120


def _admin_job_get_snapshot_locked(job: Dict[str, Any]) -> Dict[str, Any]:
    progress = dict(job.get("progress") or {})
    return {
        "job_id": str(job.get("job_id", "")),
        "job_type": str(job.get("job_type", "unknown")),
        "status": str(job.get("status", "queued")),
        "target_chat_id": job.get("target_chat_id"),
        "target_label": job.get("target_label"),
        "created_at": str(job.get("created_at", "")),
        "updated_at": str(job.get("updated_at", "")),
        "progress": {
            "current": int(progress.get("current") or 0),
            "total": progress.get("total"),
            "stage": str(progress.get("stage") or "queued"),
        },
        "log_count": len(job.get("logs", [])),
        "last_seq": int(job.get("next_log_seq", 1)) - 1,
    }


def _admin_job_get_snapshot(job_id: str) -> Optional[Dict[str, Any]]:
    with ADMIN_JOBS_LOCK:
        job = ADMIN_JOBS.get(job_id)
        if job is None:
            return None
        return _admin_job_get_snapshot_locked(job)


def _admin_job_get_logs(job_id: str, after_seq: int = 0) -> Optional[List[Dict[str, Any]]]:
    with ADMIN_JOBS_LOCK:
        job = ADMIN_JOBS.get(job_id)
        if job is None:
            return None
        logs = job.get("logs", [])
        return [dict(item) for item in logs if int(item.get("seq", 0)) > after_seq]


def get_conn() -> sqlite3.Connection:
    return runtime_get_conn(db_path=DB_PATH, connect_db_fn=connect_db)


def split_positive_negative_terms(raw_query: str) -> Tuple[List[str], List[str]]:
    includes: List[str] = []
    excludes: List[str] = []
    pending_not = False
    for kind, value in tokenize_query(raw_query):
        if kind in {"TERM", "PHRASE"}:
            (excludes if pending_not else includes).append(value)
            pending_not = False
            continue
        if value == "-":
            pending_not = True
        elif value in {"+", "/"}:
            pending_not = False
    return includes, excludes


def _build_admin_chats_payload(conn: sqlite3.Connection) -> Dict[str, Any]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                c.chat_id,
                c.chat_title,
                COUNT(m.pk) AS message_count
            FROM chats c
            LEFT JOIN messages m ON m.chat_id = c.chat_id
            GROUP BY c.chat_id, c.chat_title
            """
        )
        # /api/admin/chats 主字段契约为 chat_id/chat_title/message_count；冗余别名字段已移除（前端兼容在 JS 内处理）。
        chats = [
            {
                "chat_id": int(row["chat_id"]),
                "chat_title": _chat_title_or_fallback(int(row["chat_id"]), row["chat_title"]),
                "message_count": int(row["message_count"] or 0),
            }
            for row in cur.fetchall()
        ]
        chats.sort(key=lambda item: _chat_sort_key(item["chat_title"], int(item["chat_id"])))
        return {"ok": True, "chats": chats}
    finally:
        cur.close()


def _parse_admin_chat_id(raw_chat_id: Optional[str]) -> Optional[int]:
    value = (raw_chat_id or "").strip()
    if not value or value.lower() == "none":
        return None
    return int(value)


def _build_admin_stats_payload(conn: sqlite3.Connection, chat_id: Optional[int]) -> Tuple[Dict[str, Any], int]:
    cur = conn.cursor()
    try:
        if chat_id is None:
            cur.execute("SELECT COUNT(*) AS chat_count FROM chats")
            chat_count = int(cur.fetchone()["chat_count"] or 0)

            cur.execute("SELECT COUNT(*) AS message_count FROM messages")
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
                COUNT(m.pk) AS message_count
            FROM chats c
            LEFT JOIN messages m ON m.chat_id = c.chat_id
            WHERE c.chat_id = ?
            GROUP BY c.chat_id, c.chat_title
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
            "chat_title": _chat_title_or_fallback(int(row["chat_id"]), row["chat_title"]),
            "message_count": int(row["message_count"] or 0),
        }, 200
    finally:
        cur.close()


def _admin_get_chat_brief(conn: sqlite3.Connection, chat_id: int) -> Optional[Dict[str, Any]]:
    cur = conn.cursor()
    try:
        cur.execute("SELECT chat_id, chat_title FROM chats WHERE chat_id = ? LIMIT 1", (chat_id,))
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
    conn, feats = connect_db(str(DB_PATH))
    try:
        create_schema(conn, feats)
    finally:
        conn.close()


def create_app() -> Flask:
    _ensure_db()
    app = Flask(__name__, template_folder="templates", static_folder="static")
    register_all_routes(
        app,
        page_size=PAGE_SIZE,
        logger=logger,
        get_conn_fn=get_conn,
        build_meta_payload_fn=_build_meta_payload,
        has_fts_fn=has_fts,
        from_sql=FROM_SQL,
        max_count=MAX_COUNT,
        tokenize_query_fn=tokenize_query,
        to_fts_match_fn=to_fts_match,
        map_search_items_fn=_map_search_items,
        parse_search_params_fn=_parse_search_params,
        search_payload_service_fn=_search_payload_service,
        cfg=CFG,
        parse_admin_chat_id_fn=_parse_admin_chat_id,
        build_admin_chats_payload_fn=_build_admin_chats_payload,
        build_admin_stats_payload_fn=_build_admin_stats_payload,
        admin_get_chat_brief_fn=_admin_get_chat_brief,
        admin_job_get_snapshot_fn=_admin_job_get_snapshot,
        admin_job_get_logs_fn=_admin_job_get_logs,
        admin_has_any_active_job_fn=_admin_has_any_active_job,
        admin_create_chat_job_if_absent_fn=_admin_create_chat_job_if_absent,
        admin_job_create_fn=_admin_job_create,
        admin_job_append_log_fn=_admin_job_append_log,
        admin_start_harvest_job_thread_fn=_admin_start_harvest_job_thread,
        admin_start_update_job_thread_fn=_admin_start_update_job_thread,
        admin_start_delete_job_thread_fn=_admin_start_delete_job_thread,
        admin_start_cleanup_job_thread_fn=_admin_start_cleanup_job_thread,
        admin_make_job_log_handler_fn=_admin_make_job_log_handler,
        admin_job_set_status_fn=_admin_job_set_status,
        admin_harvest_target_max_len=ADMIN_HARVEST_TARGET_MAX_LEN,
        admin_cleanup_keyword_max_len=ADMIN_CLEANUP_KEYWORD_MAX_LEN,
    )
    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8890, debug=False)
