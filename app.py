# -*- coding: utf-8 -*-
import math
import asyncio
import sqlite3
import os
import logging
import threading
import uuid
import importlib
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, jsonify, render_template, request

from tg_harvest.config import CFG
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

ADMIN_JOBS: Dict[str, Dict[str, Any]] = {}
ADMIN_JOBS_LOCK = threading.Lock()
ADMIN_JOB_LOG_MAX_LINES = 200
ADMIN_JOB_MAX_COUNT = 100
ADMIN_HARVEST_TARGET_MAX_LEN = 300
ADMIN_JOB_ALLOWED_STATUSES = {"queued", "running", "done", "error"}
ADMIN_PROGRESS_LOG_STEP_FALLBACK = 1000


def _admin_get_progress_log_step() -> int:
    try:
        config_module = importlib.import_module("tg_harvest.config")
        cfg = getattr(config_module, "CFG", None)
        step = int(getattr(cfg, "log_every", ADMIN_PROGRESS_LOG_STEP_FALLBACK))
        if step > 0:
            return step
    except Exception:
        pass
    return ADMIN_PROGRESS_LOG_STEP_FALLBACK


def _admin_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _admin_job_trim_locked() -> None:
    # 仅裁剪终态任务（done/error），避免误删 queued/running 导致轮询中途 404。
    terminal_statuses = {"done", "error"}
    while len(ADMIN_JOBS) > ADMIN_JOB_MAX_COUNT:
        removable_job_id = None
        for job_id, job in ADMIN_JOBS.items():
            status = str((job or {}).get("status") or "").lower()
            if status in terminal_statuses:
                removable_job_id = job_id
                break

        if removable_job_id is None:
            # 当前超上限但无可删终态任务：保护 active 任务，暂不裁剪。
            return
        ADMIN_JOBS.pop(removable_job_id, None)


def _admin_job_append_log_locked(job: Dict[str, Any], message: str) -> Dict[str, Any]:
    next_seq = int(job.get("next_log_seq", 1))
    log_item = {
        "seq": next_seq,
        "ts": _admin_now_iso(),
        "message": str(message),
    }
    logs = job.setdefault("logs", [])
    logs.append(log_item)
    if len(logs) > ADMIN_JOB_LOG_MAX_LINES:
        del logs[: len(logs) - ADMIN_JOB_LOG_MAX_LINES]

    job["next_log_seq"] = next_seq + 1
    job["updated_at"] = log_item["ts"]
    return dict(log_item)


def _admin_job_create_locked(job_type: str, target_chat_id: Optional[int] = None, target_label: Optional[str] = None) -> Dict[str, Any]:
    created_at = _admin_now_iso()
    job_id = uuid.uuid4().hex
    job: Dict[str, Any] = {
        "job_id": job_id,
        "job_type": str(job_type or "unknown"),
        "status": "queued",
        "target_chat_id": target_chat_id,
        "target_label": (target_label or "").strip() or None,
        "created_at": created_at,
        "updated_at": created_at,
        "logs": [],
        "next_log_seq": 1,
        "progress": {
            "current": 0,
            "total": None,
            "stage": "queued",
            "last_logged_current": 0,
        },
    }
    ADMIN_JOBS[job_id] = job
    _admin_job_trim_locked()
    _admin_job_append_log_locked(job, "任务已创建（占位）")
    return _admin_job_get_snapshot_locked(job)


def _admin_job_create(job_type: str, target_chat_id: Optional[int] = None, target_label: Optional[str] = None) -> Dict[str, Any]:
    with ADMIN_JOBS_LOCK:
        return _admin_job_create_locked(job_type=job_type, target_chat_id=target_chat_id, target_label=target_label)


def _admin_find_active_chat_job_locked(chat_id: int) -> Optional[Dict[str, Any]]:
    active_statuses = {"queued", "running"}
    guarded_job_types = {"update", "delete"}
    for job in ADMIN_JOBS.values():
        if not isinstance(job, dict):
            continue
        if job.get("target_chat_id") != chat_id:
            continue
        job_type = str(job.get("job_type") or "").lower()
        if job_type not in guarded_job_types:
            continue
        status = str(job.get("status") or "").lower()
        if status not in active_statuses:
            continue
        return {
            "job_id": str(job.get("job_id") or ""),
            "job_type": job_type,
            "status": status,
        }
    return None


# 防止同一 chat_id 同时存在并发 update/delete 活跃任务（queued/running）。
def _admin_create_chat_job_if_absent(job_type: str, chat_id: int, target_label: Optional[str] = None) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    with ADMIN_JOBS_LOCK:
        existing_job = _admin_find_active_chat_job_locked(chat_id)
        if existing_job is not None:
            return None, existing_job
        created_job = _admin_job_create_locked(job_type=job_type, target_chat_id=chat_id, target_label=target_label)
        return created_job, None


def _admin_has_any_active_job() -> bool:
    with ADMIN_JOBS_LOCK:
        for job in ADMIN_JOBS.values():
            if isinstance(job, dict) and str(job.get("status") or "").lower() in {"queued", "running"}:
                return True
    return False


def _admin_find_active_chat_job(chat_id: int) -> Optional[Dict[str, Any]]:
    with ADMIN_JOBS_LOCK:
        return _admin_find_active_chat_job_locked(chat_id)


def _admin_job_append_log(job_id: str, message: str) -> Optional[Dict[str, Any]]:
    with ADMIN_JOBS_LOCK:
        job = ADMIN_JOBS.get(job_id)
        if job is None:
            return None
        return _admin_job_append_log_locked(job, message)


def _admin_job_set_status(job_id: str, status: str) -> bool:
    with ADMIN_JOBS_LOCK:
        job = ADMIN_JOBS.get(job_id)
        if job is None:
            return False
        # 后端状态契约收口，避免非法状态破坏前端轮询白名单逻辑。
        normalized_status = str(status or "queued").strip().lower()
        if normalized_status not in ADMIN_JOB_ALLOWED_STATUSES:
            normalized_status = "error"
        job["status"] = normalized_status
        job["updated_at"] = _admin_now_iso()
        return True


def _admin_job_update_progress(
    job_id: str,
    current: int,
    total: Optional[int] = None,
    stage: Optional[str] = None,
    log_step: int = 1000,
    force_log: bool = False,
) -> bool:
    should_log = False
    log_message: Optional[str] = None
    with ADMIN_JOBS_LOCK:
        job = ADMIN_JOBS.get(job_id)
        if job is None:
            return False

        safe_current = max(int(current), 0)
        safe_total = int(total) if isinstance(total, int) and total >= 0 else None

        progress = job.setdefault(
            "progress",
            {
                "current": 0,
                "total": None,
                "stage": "queued",
                "last_logged_current": 0,
            },
        )
        progress["current"] = safe_current
        if total is not None:
            progress["total"] = safe_total
        if stage is not None:
            progress["stage"] = str(stage)
        job["updated_at"] = _admin_now_iso()

        progress_total = progress.get("total")
        last_logged_current = int(progress.get("last_logged_current") or 0)
        progress_stage = str(progress.get("stage") or "running")
        if isinstance(log_step, int) and log_step > 0:
            on_step = safe_current > 0 and safe_current % log_step == 0
        else:
            on_step = False
        is_final = isinstance(progress_total, int) and safe_current >= progress_total
        should_log = force_log or on_step or is_final

        if should_log and safe_current != last_logged_current:
            if isinstance(progress_total, int):
                log_message = f"正在抓取消息（占位）：第 {safe_current}/{progress_total} 条"
            else:
                log_message = f"正在抓取消息（占位）：第 {safe_current} 条"
            progress["last_logged_current"] = safe_current
        else:
            should_log = False

    if should_log and log_message:
        stage_prefix = f"[{progress_stage}] " if progress_stage else ""
        _admin_job_append_log(job_id, f"{stage_prefix}{log_message}")
    return True


def _admin_harvest_job_runner(job_id: str, target: str) -> None:
    from telethon.sync import TelegramClient
    from tg_harvest.harvest_runner import _process_entity
    from tg_harvest.harvest_parse import resolve_target_entity

    class JobLogHandler(logging.Handler):
        def __init__(self, target_job_id: str) -> None:
            super().__init__()
            self._target_job_id = target_job_id
            self._thread_id = threading.get_ident()

        def emit(self, record: logging.LogRecord) -> None:
            if threading.get_ident() != self._thread_id:
                return
            message = record.getMessage()
            _admin_job_append_log(self._target_job_id, message)

    root_logger = logging.getLogger()
    job_log_handler = JobLogHandler(job_id)
    root_logger.addHandler(job_log_handler)
    try:
        _admin_job_set_status(job_id, "running")
        _admin_job_append_log(job_id, f"开始抓取目标：{target}")
        asyncio.set_event_loop(asyncio.new_event_loop())
        client = TelegramClient(CFG.session_name, CFG.api_id, CFG.api_hash)
        client.connect()
        try:
            if not client.is_user_authorized():
                _admin_job_append_log(job_id, "Telegram 未登录！请先在终端运行 python jb.py 完成登录授权。")
                _admin_job_set_status(job_id, "error")
                return

            entity = resolve_target_entity(client, target)
            if entity is None:
                _admin_job_append_log(job_id, "未找到该群组/频道，请检查名称或链接")
                _admin_job_set_status(job_id, "error")
                return

            entity_title = getattr(entity, "title", None) or getattr(entity, "username", None) or str(target)
            _admin_job_append_log(job_id, f"成功解析目标：{entity_title}")
            conn = get_conn()
            try:
                _process_entity(conn, client, entity, idx=1, total=1)
            finally:
                conn.close()
        finally:
            client.disconnect()
        _admin_job_set_status(job_id, "done")
    except Exception as exc:
        _admin_job_append_log(job_id, f"抓取失败：{exc}")
        _admin_job_set_status(job_id, "error")
    finally:
        root_logger.removeHandler(job_log_handler)


def _admin_update_job_runner(job_id: str, chat_id: int, chat_title: str, incremental: bool) -> None:
    from telethon.sync import TelegramClient
    from tg_harvest.harvest_runner import _process_entity

    class JobLogHandler(logging.Handler):
        def __init__(self, target_job_id: str) -> None:
            super().__init__()
            self._target_job_id = target_job_id
            self._thread_id = threading.get_ident()

        def emit(self, record: logging.LogRecord) -> None:
            if threading.get_ident() != self._thread_id:
                return
            message = record.getMessage()
            _admin_job_append_log(self._target_job_id, message)

    root_logger = logging.getLogger()
    job_log_handler = JobLogHandler(job_id)
    root_logger.addHandler(job_log_handler)
    try:
        _admin_job_set_status(job_id, "running")
        mode_label = "增量" if incremental else "全量"
        _admin_job_append_log(job_id, f"开始{mode_label}更新：{chat_title} ({chat_id})")
        asyncio.set_event_loop(asyncio.new_event_loop())
        client = TelegramClient(CFG.session_name, CFG.api_id, CFG.api_hash)
        client.connect()
        try:
            if not client.is_user_authorized():
                _admin_job_append_log(job_id, "Telegram 未登录！请先在终端运行 python jb.py 完成登录授权。")
                _admin_job_set_status(job_id, "error")
                return

            entity = client.get_entity(chat_id)
            entity_title = getattr(entity, "title", None) or getattr(entity, "username", None) or str(chat_id)
            _admin_job_append_log(job_id, f"成功连接并获取实体：{entity_title}")
            conn = get_conn()
            try:
                _process_entity(conn, client, entity, idx=1, total=1)
            finally:
                conn.close()
        finally:
            client.disconnect()
        _admin_job_set_status(job_id, "done")
    except Exception as exc:
        _admin_job_append_log(job_id, f"更新失败：{exc}")
        _admin_job_set_status(job_id, "error")
    finally:
        root_logger.removeHandler(job_log_handler)


def _admin_start_update_job_thread(job_id: str, chat_id: int, chat_title: str, incremental: bool) -> threading.Thread:
    worker = threading.Thread(
        target=_admin_update_job_runner,
        args=(job_id, chat_id, chat_title, incremental),
        daemon=True,
    )
    worker.start()
    return worker


def _admin_delete_job_runner(job_id: str, chat_id: int, chat_title: str) -> None:
    conn: Optional[sqlite3.Connection] = None
    try:
        _admin_job_set_status(job_id, "running")
        _admin_job_append_log(job_id, f"开始删除目标：{chat_title} ({chat_id})")

        conn = get_conn()
        cur = conn.cursor()
        try:
            _admin_job_append_log(job_id, "统计待删除消息数量")
            cur.execute("SELECT COUNT(*) AS cnt FROM messages WHERE chat_id = ?", (chat_id,))
            count_row = cur.fetchone()
            message_count = int((count_row["cnt"] if count_row and "cnt" in count_row.keys() else 0) or 0)
            _admin_job_append_log(job_id, f"待删除消息数量：{message_count}")

            cur.execute("DELETE FROM dedupe_actions WHERE chat_id = ?", (chat_id,))
            cur.execute("DELETE FROM dedupe_runs WHERE chat_id = ?", (chat_id,))
            cur.execute("DELETE FROM media_groups WHERE chat_id = ?", (chat_id,))
            cur.execute("DELETE FROM message_media WHERE chat_id = ?", (chat_id,))
            _admin_job_append_log(job_id, "清理关联表数据完成")

            _admin_job_append_log(job_id, "删除 messages 表数据")
            cur.execute("DELETE FROM messages WHERE chat_id = ?", (chat_id,))
            deleted_messages = int(cur.rowcount or 0)
            _admin_job_append_log(job_id, f"messages 删除行数：{deleted_messages}")

            _admin_job_append_log(job_id, "删除 chats 表记录")
            cur.execute("DELETE FROM chats WHERE chat_id = ?", (chat_id,))
            deleted_chats = int(cur.rowcount or 0)
            _admin_job_append_log(job_id, f"chats 删除行数：{deleted_chats}")

            if deleted_chats != 1:
                raise RuntimeError(f"chats 删除异常，预期 1 行，实际 {deleted_chats} 行")

            conn.commit()
            _admin_job_append_log(job_id, "事务已提交")
            _admin_job_append_log(job_id, f"删除完成：消息 {deleted_messages} 条，chat 记录删除 {deleted_chats} 条")
            _admin_job_set_status(job_id, "done")
        finally:
            cur.close()
    except Exception as exc:
        if conn is not None:
            try:
                conn.rollback()
                _admin_job_append_log(job_id, "删除失败，事务已回滚")
            except Exception as rollback_exc:
                _admin_job_append_log(job_id, f"删除失败，回滚异常：{rollback_exc}")

        _admin_job_append_log(job_id, f"删除失败：{exc}")
        _admin_job_set_status(job_id, "error")
    finally:
        if conn is not None:
            conn.close()


def _admin_start_delete_job_thread(job_id: str, chat_id: int, chat_title: str) -> threading.Thread:
    worker = threading.Thread(
        target=_admin_delete_job_runner,
        args=(job_id, chat_id, chat_title),
        daemon=True,
    )
    worker.start()
    return worker


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


@dataclass
class SearchParams:
    raw_query: str
    search_type: str
    sort_by_req: str
    order_req: str
    page: int
    chat_id: Optional[int]


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
        if ch in "+-|":
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
        while i < n and (not q[i].isspace()) and q[i] not in '+-|"':
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
    if op_value == "|" and parts and parts[-1] not in {"AND", "OR", "NOT"}:
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
        elif value in {"+", "|"}:
            pending_not = False
    return includes, excludes


def has_fts(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='messages_fts' LIMIT 1")
        return cur.fetchone() is not None
    finally:
        cur.close()


def make_type_clause(search_type: str) -> Tuple[str, List[Any]]:
    st = (search_type or "all").lower()
    if st == "text":
        return "m.msg_type = 'TEXT'", []
    if st == "image":
        return "m.msg_type = 'PHOTO'", []
    if st == "video":
        return "m.msg_type IN ('VIDEO', 'GIF', 'VIDEO_NOTE')", []
    if st == "audio":
        return "m.msg_type IN ('AUDIO', 'VOICE')", []
    return "", []


def choose_sort(search_type: str, sort_by: str, order: str) -> Tuple[str, str, str]:
    st = (search_type or "all").lower()
    sb = (sort_by or "time").lower()
    od = "ASC" if str(order).lower() == "asc" else "DESC"
    if st in {"all", "text"} and sb == "size":
        sb = "time"
    if sb == "size":
        return "COALESCE(mm.file_size, 0)", "size", od
    return "m.msg_date_ts", "time", od


def build_result_title(row: sqlite3.Row) -> str:
    content = (row["content"] or "").strip()
    if content:
        return content
    file_name = (row["file_name"] or "").strip()
    if file_name:
        return file_name
    return TYPE_FALLBACK_TITLE.get((row["msg_type"] or "TEXT").upper(), "[无文本内容]")


def _parse_search_params(data: Dict[str, Any]) -> SearchParams:
    raw_query = str(data.get("query", "") or "")
    search_type = str(data.get("search_type", "all") or "all").lower()
    sort_by_req = str(data.get("sort_by", "time") or "time").lower()
    order_req = str(data.get("order", "desc") or "desc").lower()

    page = max(int(data.get("page", 1) or 1), 1)
    chat_id_raw = data.get("chat_id", "all")
    chat_id = None if str(chat_id_raw).lower() == "all" else int(chat_id_raw)

    return SearchParams(
        raw_query=raw_query,
        search_type=search_type,
        sort_by_req=sort_by_req,
        order_req=order_req,
        page=page,
        chat_id=chat_id,
    )


def _build_search_filters(params: SearchParams, fts_enabled: bool) -> Tuple[str, List[Any], str]:
    where_parts: List[str] = ["1=1"]
    sql_params: List[Any] = []

    match_query = _append_text_search_filters(where_parts, sql_params, params.raw_query, fts_enabled)
    _append_scope_filters(where_parts, sql_params, params)

    return " AND ".join(where_parts), sql_params, match_query


def _append_text_search_filters(
    where_parts: List[str],
    sql_params: List[Any],
    raw_query: str,
    fts_enabled: bool,
) -> str:
    match_query = to_fts_match(raw_query)
    if _append_fts_match_filter(where_parts, sql_params, match_query, fts_enabled):
        return match_query

    _append_like_fallback_filters(where_parts, sql_params, raw_query)
    return match_query


def _append_fts_match_filter(where_parts: List[str], sql_params: List[Any], match_query: str, fts_enabled: bool) -> bool:
    if not (match_query and fts_enabled):
        return False
    where_parts.append("m.pk IN (SELECT rowid FROM messages_fts WHERE messages_fts MATCH ?)")
    sql_params.append(match_query)
    return True


def _append_like_fallback_filters(where_parts: List[str], sql_params: List[Any], raw_query: str) -> None:
    like_clause, like_params = _build_like_logic_clause(raw_query)
    if not like_clause:
        return
    where_parts.append(like_clause)
    sql_params.extend(like_params)


def _build_like_logic_clause(raw_query: str) -> Tuple[str, List[Any]]:
    tokens = tokenize_query(raw_query)
    if not tokens:
        return "", []

    stream: List[Tuple[str, str]] = []
    prev_operand = False

    for kind, value in tokens:
        if kind in {"TERM", "PHRASE"}:
            if prev_operand:
                stream.append(("OP", "AND"))
            stream.append(("TERM", value))
            prev_operand = True
            continue

        if value == "-":
            if prev_operand:
                stream.append(("OP", "AND"))
            stream.append(("OP", "NOT"))
            prev_operand = False
            continue

        if value == "+":
            if prev_operand:
                stream.append(("OP", "AND"))
            prev_operand = False
            continue

        if value == "|":
            if prev_operand:
                stream.append(("OP", "OR"))
            prev_operand = False

    while stream and stream[-1][0] == "OP":
        stream.pop()

    if not stream:
        return "", []

    output: List[Tuple[str, str]] = []
    op_stack: List[str] = []
    precedence = {"OR": 1, "AND": 2, "NOT": 3}
    right_assoc = {"NOT"}

    for token_kind, token_value in stream:
        if token_kind == "TERM":
            output.append((token_kind, token_value))
            continue

        while op_stack:
            top = op_stack[-1]
            if (top not in right_assoc and precedence[top] >= precedence[token_value]) or (
                top in right_assoc and precedence[top] > precedence[token_value]
            ):
                output.append(("OP", op_stack.pop()))
                continue
            break
        op_stack.append(token_value)

    while op_stack:
        output.append(("OP", op_stack.pop()))

    expr_stack: List[str] = []
    expr_params: List[Any] = []
    content_expr = "LOWER(COALESCE(NULLIF(m.content_norm, ''), m.content, ''))"

    for token_kind, token_value in output:
        if token_kind == "TERM":
            expr_stack.append(f"({content_expr} LIKE ?)")
            expr_params.append(f"%{token_value.lower()}%")
            continue

        if token_value == "NOT":
            if not expr_stack:
                return "", []
            operand = expr_stack.pop()
            expr_stack.append(f"(NOT {operand})")
            continue

        if len(expr_stack) < 2:
            return "", []
        right = expr_stack.pop()
        left = expr_stack.pop()
        expr_stack.append(f"({left} {token_value} {right})")

    if len(expr_stack) != 1:
        return "", []

    return expr_stack[0], expr_params


def _append_scope_filters(where_parts: List[str], sql_params: List[Any], params: SearchParams) -> None:
    if params.chat_id is not None:
        where_parts.append("m.chat_id = ?")
        sql_params.append(params.chat_id)

    type_clause, type_params = make_type_clause(params.search_type)
    if type_clause:
        where_parts.append(type_clause)
        sql_params.extend(type_params)


def _build_search_sql(where_sql: str, search_type: str, sort_by_req: str, order_req: str) -> Tuple[str, str, str, str, str]:
    order_expr, effective_sort, effective_order = choose_sort(search_type, sort_by_req, order_req)
    count_sql = f"SELECT COUNT(*) AS c FROM (SELECT m.pk {FROM_SQL} WHERE {where_sql} LIMIT ?)"
    query_sql = f"""
        SELECT m.pk,m.chat_id,c.chat_title,m.message_id,m.msg_date_text,m.msg_date_ts,m.msg_type,m.link,m.content,m.grouped_id,
               mm.file_name,mm.file_size,mm.mime_type,mm.media_kind
        {FROM_SQL}
        WHERE {where_sql}
        ORDER BY {order_expr} {effective_order}, m.msg_date_ts {effective_order}, m.pk {effective_order}
        LIMIT ? OFFSET ?
    """
    return count_sql, query_sql, order_expr, effective_sort, effective_order


def _execute_count_query(cur: sqlite3.Cursor, count_sql: str, sql_params: List[Any]) -> Tuple[int, bool, int]:
    cur.execute(count_sql, sql_params + [MAX_COUNT + 1])
    counted = int(cur.fetchone()["c"] or 0)
    total_is_capped = counted > MAX_COUNT
    total = min(counted, MAX_COUNT)
    total_pages = math.ceil(total / PAGE_SIZE) if total > 0 else 0
    return total, total_is_capped, total_pages


def _execute_rows_query(
    cur: sqlite3.Cursor,
    query_sql: str,
    sql_params: List[Any],
    page: int,
    total_pages: int,
) -> Tuple[List[sqlite3.Row], int]:
    effective_page = page
    if total_pages > 0 and effective_page > total_pages:
        effective_page = total_pages

    offset = (effective_page - 1) * PAGE_SIZE if total_pages > 0 else 0
    cur.execute(query_sql, sql_params + [PAGE_SIZE, offset])
    rows = cur.fetchall()
    return rows, effective_page


def _run_search_query(
    conn: sqlite3.Connection,
    count_sql: str,
    query_sql: str,
    sql_params: List[Any],
    page: int,
) -> Tuple[List[sqlite3.Row], int, int, bool, int]:
    cur = conn.cursor()
    try:
        total, total_is_capped, total_pages = _execute_count_query(cur, count_sql, sql_params)
        rows, effective_page = _execute_rows_query(cur, query_sql, sql_params, page, total_pages)
        return rows, total, total_pages, total_is_capped, effective_page
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
        cur.execute("SELECT chat_id, chat_title FROM chats ORDER BY LOWER(chat_title) ASC, chat_id ASC")
        chats = [{"chat_id": int(r["chat_id"]), "chat_title": (r["chat_title"] or f"Chat {r['chat_id']}").strip()} for r in cur.fetchall()]
        return {"ok": True, "chats": chats, "page_size": PAGE_SIZE}
    finally:
        cur.close()


def _chat_title_or_fallback(chat_id: int, chat_title: Optional[str]) -> str:
    title = (chat_title or "").strip()
    return title if title else f"Chat {chat_id}"


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
            ORDER BY
                LOWER(COALESCE(NULLIF(TRIM(c.chat_title), ''), printf('Chat %d', c.chat_id))) ASC,
                c.chat_id ASC
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


def _search_payload(params: SearchParams) -> Dict[str, Any]:
    with closing(get_conn()) as conn:
        fts_enabled = has_fts(conn)
        where_sql, sql_params, match_query = _build_search_filters(params, fts_enabled)
        count_sql, query_sql, _, effective_sort, effective_order = _build_search_sql(
            where_sql,
            params.search_type,
            params.sort_by_req,
            params.order_req,
        )
        rows, total, total_pages, total_is_capped, effective_page = _run_search_query(
            conn,
            count_sql,
            query_sql,
            sql_params,
            params.page,
        )

    items = _map_search_items(rows)
    return {
        "ok": True,
        "query": params.raw_query,
        "fts_query": match_query,
        "page": effective_page,
        "page_size": PAGE_SIZE,
        "total": total,
        "total_pages": total_pages,
        "total_is_capped": total_is_capped,
        "effective_sort": effective_sort,
        "effective_order": effective_order.lower(),
        "items": items,
    }


def _search_payload_like_fallback(params: SearchParams) -> Optional[Dict[str, Any]]:
    like_clause, like_params = _build_like_logic_clause(params.raw_query)
    if not like_clause:
        return None

    where_parts: List[str] = ["1=1", like_clause]
    sql_params: List[Any] = list(like_params)
    _append_scope_filters(where_parts, sql_params, params)
    where_sql = " AND ".join(where_parts)

    with closing(get_conn()) as conn:
        count_sql, query_sql, _, effective_sort, effective_order = _build_search_sql(
            where_sql,
            params.search_type,
            params.sort_by_req,
            params.order_req,
        )
        rows, total, total_pages, total_is_capped, effective_page = _run_search_query(
            conn,
            count_sql,
            query_sql,
            sql_params,
            params.page,
        )

    if total <= 0:
        return None

    return {
        "ok": True,
        "query": params.raw_query,
        "fts_query": to_fts_match(params.raw_query),
        "page": effective_page,
        "page_size": PAGE_SIZE,
        "total": total,
        "total_pages": total_pages,
        "total_is_capped": total_is_capped,
        "effective_sort": effective_sort,
        "effective_order": effective_order.lower(),
        "items": _map_search_items(rows),
    }


def _register_routes(app: Flask) -> None:
    @app.get("/")
    def index():
        return render_template("index.html", page_size=PAGE_SIZE)

    @app.get("/admin/manage")
    def admin_manage_page():
        return render_template("admin_manage.html")

    @app.get("/api/meta")
    def api_meta():
        try:
            with closing(get_conn()) as conn:
                payload = _build_meta_payload(conn)
            return jsonify(payload)
        except sqlite3.Error:
            logger.exception("读取群列表失败")
            return jsonify({"ok": False, "error": "读取群列表失败"}), 500
        except Exception:
            logger.exception("系统异常")
            return jsonify({"ok": False, "error": "系统异常"}), 500

    @app.post("/api/search")
    def api_search():
        data = request.get_json(silent=True) or {}
        try:
            params = _parse_search_params(data)
            payload = _search_payload(params)
            if int(payload.get("total") or 0) == 0:
                fallback_payload = _search_payload_like_fallback(params)
                if fallback_payload is not None:
                    return jsonify(fallback_payload)
            return jsonify(payload)
        except (ValueError, TypeError):
            return jsonify({"ok": False, "error": "参数格式错误"}), 400
        except sqlite3.Error:
            logger.exception("查询失败")
            return jsonify({"ok": False, "error": "查询失败"}), 500
        except Exception:
            logger.exception("系统异常")
            return jsonify({"ok": False, "error": "系统异常"}), 500

    @app.get("/api/admin/chats")
    def api_admin_chats():
        try:
            with closing(get_conn()) as conn:
                payload = _build_admin_chats_payload(conn)
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
            chat_id = _parse_admin_chat_id(request.args.get("chat_id"))
        except (ValueError, TypeError):
            return jsonify({"ok": False, "error": "chat_id 参数非法"}), 400

        try:
            with closing(get_conn()) as conn:
                payload, status_code = _build_admin_stats_payload(conn, chat_id)
            return jsonify(payload), status_code
        except sqlite3.Error:
            logger.exception("读取后台统计失败")
            return jsonify({"ok": False, "error": "读取后台统计失败"}), 500
        except Exception:
            logger.exception("系统异常")
            return jsonify({"ok": False, "error": "系统异常"}), 500

    @app.get("/api/admin/jobs/<job_id>")
    def api_admin_job_snapshot(job_id: str):
        snapshot = _admin_job_get_snapshot(job_id)
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

        logs = _admin_job_get_logs(job_id, after_seq=after_seq)
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

        if _admin_has_any_active_job():
            return jsonify({"ok": False, "error": "当前已有进行中的任务，请等待完成后再试"}), 409

        raw_target = data.get("target", "")
        if not isinstance(raw_target, str):
            return jsonify({"ok": False, "error": "target 参数必须为字符串"}), 400

        target = raw_target.strip()
        if not target:
            return jsonify({"ok": False, "error": "target 不能为空"}), 400
        if len(target) > ADMIN_HARVEST_TARGET_MAX_LEN:
            return jsonify({"ok": False, "error": f"target 长度不能超过 {ADMIN_HARVEST_TARGET_MAX_LEN}"}), 400

        job = _admin_job_create("harvest", target_chat_id=None, target_label=target)
        job_id = str(job.get("job_id") or "")

        _admin_job_append_log(job_id, f"已接收抓取目标：{target}")
        worker = threading.Thread(target=_admin_harvest_job_runner, args=(job_id, target), daemon=True)
        worker.start()

        snapshot = _admin_job_get_snapshot(job_id)
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

        if _admin_has_any_active_job():
            return jsonify({"ok": False, "error": "当前已有进行中的任务，请等待完成后再试"}), 409

        raw_chat_id = data.get("chat_id")
        try:
            chat_id = int(raw_chat_id)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "chat_id 参数非法"}), 400

        incremental = data.get("incremental", True)
        if not isinstance(incremental, bool):
            return jsonify({"ok": False, "error": "incremental 参数必须为布尔值"}), 400
        if incremental is False:
            return jsonify({"ok": False, "error": "当前仅支持增量更新"}), 400

        try:
            with closing(get_conn()) as conn:
                chat_brief = _admin_get_chat_brief(conn, chat_id)
        except sqlite3.Error:
            logger.exception("读取群信息失败")
            return jsonify({"ok": False, "error": "读取群信息失败"}), 500
        except Exception:
            logger.exception("系统异常")
            return jsonify({"ok": False, "error": "系统异常"}), 500

        if chat_brief is None:
            return jsonify({"ok": False, "error": "chat_id 不存在"}), 404

        chat_title = str(chat_brief["chat_title"])
        job, existing_job = _admin_create_chat_job_if_absent("update", chat_id=chat_id, target_label=chat_title)
        if existing_job is not None:
            return jsonify({"ok": False, "error": "该目标已有进行中的任务", "existing_job": existing_job}), 409
        job_id = str(job.get("job_id") or "")

        _admin_job_append_log(job_id, "已接收增量更新请求")
        _admin_job_append_log(job_id, f"目标群组：{chat_title} ({chat_id})")
        _admin_start_update_job_thread(job_id, chat_id, chat_title, incremental)

        snapshot = _admin_job_get_snapshot(job_id)
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

        if _admin_has_any_active_job():
            return jsonify({"ok": False, "error": "当前已有进行中的任务，请等待完成后再试"}), 409

        raw_chat_id = data.get("chat_id")
        try:
            chat_id = int(raw_chat_id)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "error": "chat_id 参数非法"}), 400

        try:
            with closing(get_conn()) as conn:
                chat_brief = _admin_get_chat_brief(conn, chat_id)
        except sqlite3.Error:
            logger.exception("读取群信息失败")
            return jsonify({"ok": False, "error": "读取群信息失败"}), 500
        except Exception:
            logger.exception("系统异常")
            return jsonify({"ok": False, "error": "系统异常"}), 500

        if chat_brief is None:
            return jsonify({"ok": False, "error": "chat_id 不存在"}), 404

        chat_title = str(chat_brief["chat_title"])
        job, existing_job = _admin_create_chat_job_if_absent("delete", chat_id=chat_id, target_label=chat_title)
        if existing_job is not None:
            return jsonify({"ok": False, "error": "该目标已有进行中的任务", "existing_job": existing_job}), 409
        job_id = str(job.get("job_id") or "")

        _admin_job_append_log(job_id, "已接收删除请求")
        _admin_job_append_log(job_id, f"目标群组：{chat_title} ({chat_id})")
        _admin_start_delete_job_thread(job_id, chat_id, chat_title)

        snapshot = _admin_job_get_snapshot(job_id)
        if snapshot is None:
            return jsonify({"ok": False, "error": "任务创建失败"}), 500
        return jsonify({"ok": True, "job": snapshot})


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
