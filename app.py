# -*- coding: utf-8 -*-
import asyncio
import sqlite3
import os
import logging
from contextlib import closing
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, jsonify, request
from pypinyin import lazy_pinyin

from tg_harvest.config import CFG
from tg_harvest.search_params import SearchParams, _parse_search_params
from tg_harvest.search_service import _search_payload_service
from tg_harvest.routes_search import register_search_routes
from tg_harvest.routes_meta import register_meta_routes
from tg_harvest.routes_pages import register_page_routes
from tg_harvest.routes_admin import register_admin_routes
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
from tg_harvest.normalize import normalize_search_term

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

CURLY_QUOTES_MAP = str.maketrans({"“": '"', "”": '"', "‘": "'", "’": "'"})
TYPE_FALLBACK_TITLE = {
    "PHOTO": "[无文案图片]",
    "VIDEO": "[无文案视频]",
    "GIF": "[无文案视频]",
    "VIDEO_NOTE": "[无文案视频]",
    "AUDIO": "[无文案音频]",
    "VOICE": "[无文案音频]",
    "FILE": "[无文案文件]",
    "TEXT": "[无文本内容]",
}

FROM_SQL = """
    FROM messages m
    LEFT JOIN chats c ON c.chat_id = m.chat_id
    LEFT JOIN message_media mm ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id
"""

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
    conn, _ = connect_db(str(DB_PATH))
    return conn


def norm_for_search(term: str) -> str:
    return normalize_search_term(term)


def tokenize_query(query: str) -> List[Tuple[str, str]]:
    q = (query or "").translate(CURLY_QUOTES_MAP)
    tokens: List[Tuple[str, str]] = []
    i, n = 0, len(q)
    while i < n:
        ch = q[i]
        if ch.isspace():
            i += 1
            continue
        if ch in "+-/":
            tokens.append(("OP", ch))
            i += 1
            continue
        if ch == '"':
            i += 1
            buf = []
            while i < n:
                c = q[i]
                if c == "\\" and i + 1 < n:
                    buf.append(q[i + 1])
                    i += 2
                    continue
                if c == '"':
                    i += 1
                    break
                buf.append(c)
                i += 1
            term = norm_for_search("".join(buf))
            if term:
                tokens.append(("PHRASE", term))
            continue
        buf = []
        while i < n and (not q[i].isspace()) and q[i] not in '+-/"':
            buf.append(q[i])
            i += 1
        term = norm_for_search("".join(buf))
        if term:
            tokens.append(("TERM", term))
    return tokens


def to_fts_match(raw_query: str) -> str:
    tokens = tokenize_query(raw_query)
    if not tokens:
        return ""

    parts: List[str] = []
    deferred_not_terms: List[str] = []
    prev_was_term = False
    pending_not = False
    positive_terms = 0

    for kind, value in tokens:
        if kind in {"TERM", "PHRASE"}:
            prev_was_term, pending_not, positive_terms = _handle_fts_term_or_phrase(
                value,
                parts,
                deferred_not_terms,
                prev_was_term,
                pending_not,
                positive_terms,
            )
            continue

        prev_was_term, pending_not = _handle_fts_op_token(value, parts, prev_was_term, pending_not)

    return _finalize_fts_match(parts, deferred_not_terms, positive_terms)


def _handle_fts_term_or_phrase(
    term_value: str,
    parts: List[str],
    deferred_not_terms: List[str],
    prev_was_term: bool,
    pending_not: bool,
    positive_terms: int,
) -> Tuple[bool, bool, int]:
    quoted = f'"{term_value.replace(chr(34), "")}"'
    if pending_not:
        if prev_was_term:
            parts.append("NOT")
            parts.append(quoted)
            return True, False, positive_terms
        # 前置负词（如 -bar foo）先挂起，后续有正向词时再拼接 NOT。
        deferred_not_terms.append(quoted)
        return False, False, positive_terms

    if prev_was_term:
        parts.append("AND")
    parts.append(quoted)
    return True, False, positive_terms + 1


def _handle_fts_op_token(op_value: str, parts: List[str], prev_was_term: bool, pending_not: bool) -> Tuple[bool, bool]:
    if op_value == "+" and parts and parts[-1] not in {"AND", "OR", "NOT"}:
        parts.append("AND")
        return False, pending_not
    if op_value == "/" and parts and parts[-1] not in {"AND", "OR", "NOT"}:
        parts.append("OR")
        return False, pending_not
    if op_value == "-":
        return prev_was_term, True
    return prev_was_term, pending_not


def _finalize_fts_match(parts: List[str], deferred_not_terms: List[str], positive_terms: int) -> str:
    # 纯负词查询（如 -bar）不走 FTS，交给 LIKE fallback。
    if positive_terms == 0:
        return ""

    for term in deferred_not_terms:
        parts.append("NOT")
        parts.append(term)

    while parts and parts[-1] in {"AND", "OR", "NOT"}:
        parts.pop()
    return " ".join(parts)


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


def has_fts(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='messages_fts' LIMIT 1")
        return cur.fetchone() is not None
    finally:
        cur.close()


def _build_search_display_fields(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "content": row["content"] or "",
        "file_name": row["file_name"] or "",
        "title": build_result_title(row),
    }


def _map_search_row(row: sqlite3.Row) -> Dict[str, Any]:
    file_size = int(row["file_size"]) if row["file_size"] is not None else None
    item = {
        "pk": int(row["pk"]),
        "chat_id": int(row["chat_id"]),
        "chat_title": row["chat_title"] or "",
        "message_id": int(row["message_id"]),
        "msg_date_text": row["msg_date_text"] or "",
        "msg_type": row["msg_type"] or "TEXT",
        "link": row["link"] or "",
        "file_size": file_size,
    }
    item.update(_build_search_display_fields(row))
    return item


def _map_search_items(rows: List[sqlite3.Row]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for row in rows:
        items.append(_map_search_row(row))
    return items


def _build_meta_payload(conn: sqlite3.Connection) -> Dict[str, Any]:
    cur = conn.cursor()
    try:
        cur.execute("SELECT chat_id, chat_title FROM chats")
        chats = [{"chat_id": int(r["chat_id"]), "chat_title": _chat_title_or_fallback(int(r["chat_id"]), r["chat_title"])} for r in cur.fetchall()]
        chats.sort(key=lambda item: _chat_sort_key(item["chat_title"], int(item["chat_id"])))
        return {"ok": True, "chats": chats, "page_size": PAGE_SIZE}
    finally:
        cur.close()


def _chat_title_or_fallback(chat_id: int, chat_title: Optional[str]) -> str:
    title = (chat_title or "").strip()
    return title if title else f"Chat {chat_id}"


def _is_cjk_char(ch: str) -> bool:
    if not ch:
        return False
    codepoint = ord(ch)
    return (
        0x4E00 <= codepoint <= 0x9FFF
        or 0x3400 <= codepoint <= 0x4DBF
        or 0x20000 <= codepoint <= 0x2A6DF
        or 0x2A700 <= codepoint <= 0x2B73F
        or 0x2B740 <= codepoint <= 0x2B81F
        or 0x2B820 <= codepoint <= 0x2CEAF
        or 0xF900 <= codepoint <= 0xFAFF
    )


def _chat_sort_key(chat_title: str, chat_id: int) -> Tuple[int, str, str, int]:
    normalized_title = (chat_title or "").strip() or f"Chat {chat_id}"
    first_char = normalized_title[0]

    if first_char.isdigit():
        category = 0
        lexical_key = normalized_title.casefold()
    elif _is_cjk_char(first_char):
        category = 1
        lexical_key = "".join(lazy_pinyin(normalized_title)).casefold()
    elif first_char.isascii() and first_char.isalpha():
        category = 2
        lexical_key = normalized_title.casefold()
    else:
        category = 3
        lexical_key = normalized_title.casefold()

    return category, lexical_key, normalized_title.casefold(), chat_id


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


def _register_routes(app: Flask) -> None:

    register_page_routes(
        app,
        page_size=PAGE_SIZE,
    )

    register_meta_routes(
        app,
        logger=logger,
        get_conn_fn=get_conn,
        build_meta_payload_fn=_build_meta_payload,
    )

    register_search_routes(
        app,
        logger=logger,
        get_conn_fn=get_conn,
        has_fts_fn=has_fts,
        from_sql=FROM_SQL,
        page_size=PAGE_SIZE,
        max_count=MAX_COUNT,
        tokenize_query_fn=tokenize_query,
        to_fts_match_fn=to_fts_match,
        map_search_items_fn=_map_search_items,
        parse_search_params_fn=_parse_search_params,
        search_payload_service_fn=_search_payload_service,
    )

    register_admin_routes(
        app,
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
        has_fts_fn=has_fts,
    )



def _ensure_db() -> None:
    conn, feats = connect_db(str(DB_PATH))
    try:
        create_schema(conn, feats)
    finally:
        conn.close()


def create_app() -> Flask:
    _ensure_db()
    app = Flask(__name__, template_folder="templates", static_folder="static")
    _register_routes(app)
    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8890, debug=False)
