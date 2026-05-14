# -*- coding: utf-8 -*-
import sqlite3
from typing import Any, Iterable, List

from tg_harvest.domain.chat_inventory import ChatInventoryRow
from tg_harvest.storage.connection import synchronized_write


CHANNEL_SORT_DEFAULT = "message_count_desc"
CHANNEL_SORT_OPTIONS = {
    "message_count_asc": (
        "COALESCE(message_count, 0) ASC, chat_title COLLATE NOCASE ASC, chat_id ASC"
    ),
    "message_count_desc": (
        "COALESCE(message_count, 0) DESC, chat_title COLLATE NOCASE ASC, chat_id ASC"
    ),
    "updated_desc": (
        "CASE WHEN last_message_ts IS NULL THEN 1 ELSE 0 END ASC, "
        "last_message_ts DESC, chat_title COLLATE NOCASE ASC, chat_id ASC"
    ),
    "updated_asc": (
        "CASE WHEN last_message_ts IS NULL THEN 1 ELSE 0 END ASC, "
        "last_message_ts ASC, chat_title COLLATE NOCASE ASC, chat_id ASC"
    ),
}


def normalize_channel_sort(raw_sort: Any) -> str:
    value = str(raw_sort or CHANNEL_SORT_DEFAULT).strip().lower()
    if value in CHANNEL_SORT_OPTIONS:
        return value
    return CHANNEL_SORT_DEFAULT


def _chat_title_or_fallback(chat_id: int, chat_title: Any) -> str:
    title = str(chat_title or "").strip()
    return title if title else f"Chat {chat_id}"


def list_database_channels(conn: sqlite3.Connection, *, sort: Any) -> List[dict]:
    normalized_sort = normalize_channel_sort(sort)
    order_sql = CHANNEL_SORT_OPTIONS[normalized_sort]
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT
                c.chat_id,
                c.chat_title,
                c.chat_username,
                c.chat_type,
                c.message_count,
                c.last_seen_at,
                (
                    SELECT m.msg_date_text
                    FROM messages m
                    WHERE m.chat_id = c.chat_id
                    ORDER BY m.msg_date_ts DESC, m.message_id DESC
                    LIMIT 1
                ) AS last_message_at,
                (
                    SELECT m.msg_date_ts
                    FROM messages m
                    WHERE m.chat_id = c.chat_id
                    ORDER BY m.msg_date_ts DESC, m.message_id DESC
                    LIMIT 1
                ) AS last_message_ts
            FROM chats c
            ORDER BY {order_sql}
            """
        )
        channels = []
        for row in cur.fetchall():
            chat_id = int(row["chat_id"])
            channels.append(
                {
                    "chat_id": chat_id,
                    "chat_title": _chat_title_or_fallback(chat_id, row["chat_title"]),
                    "chat_username": str(row["chat_username"] or ""),
                    "chat_type": str(row["chat_type"] or ""),
                    "message_count": int(row["message_count"] or 0),
                    "last_seen_at": str(row["last_seen_at"] or ""),
                    "last_message_at": str(row["last_message_at"] or ""),
                    "last_message_ts": (
                        int(row["last_message_ts"])
                        if row["last_message_ts"] is not None
                        else None
                    ),
                }
            )
        return channels
    finally:
        cur.close()


@synchronized_write
def replace_missing_chat_scan_results(
    conn: sqlite3.Connection,
    rows: Iterable[ChatInventoryRow],
    *,
    scan_job_id: str,
    scanned_at: str,
) -> int:
    normalized_rows = list(rows)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("DELETE FROM admin_missing_chats")
        cur.executemany(
            """
            INSERT INTO admin_missing_chats(
                chat_id,
                chat_title,
                chat_username,
                chat_type,
                is_public,
                scan_job_id,
                scanned_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                chat_title = excluded.chat_title,
                chat_username = excluded.chat_username,
                chat_type = excluded.chat_type,
                is_public = excluded.is_public,
                scan_job_id = excluded.scan_job_id,
                scanned_at = excluded.scanned_at
            """,
            [
                (
                    int(row.chat_id),
                    str(row.chat_title or "").strip() or f"Chat {int(row.chat_id)}",
                    str(getattr(row, "chat_username", "") or "").strip().lstrip("@"),
                    str(getattr(row, "chat_type", "") or ""),
                    1 if int(getattr(row, "is_public", 0) or 0) == 1 else 0,
                    str(scan_job_id or ""),
                    str(scanned_at or ""),
                )
                for row in normalized_rows
            ],
        )
        conn.commit()
        return len(normalized_rows)
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        cur.close()


def list_missing_chat_scan_results(conn: sqlite3.Connection) -> List[dict]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                chat_id,
                chat_title,
                chat_username,
                chat_type,
                is_public,
                scan_job_id,
                scanned_at
            FROM admin_missing_chats
            ORDER BY chat_title COLLATE NOCASE ASC, chat_id ASC
            """
        )
        rows = []
        for row in cur.fetchall():
            chat_id = int(row["chat_id"])
            rows.append(
                {
                    "chat_id": chat_id,
                    "chat_title": _chat_title_or_fallback(chat_id, row["chat_title"]),
                    "chat_username": str(row["chat_username"] or ""),
                    "chat_type": str(row["chat_type"] or ""),
                    "is_public": int(row["is_public"] or 0),
                    "scan_job_id": str(row["scan_job_id"] or ""),
                    "scanned_at": str(row["scanned_at"] or ""),
                }
            )
        return rows
    finally:
        cur.close()


def _scan_row_value(row: Any, key: str, default: Any = "") -> Any:
    if isinstance(row, dict):
        value = row.get(key, default)
    else:
        value = getattr(row, key, default)
    return default if value is None else value


@synchronized_write
def replace_absent_chat_scan_results(
    conn: sqlite3.Connection,
    rows: Iterable[dict],
    *,
    scan_job_id: str,
    scanned_at: str,
) -> int:
    normalized_rows = list(rows)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("DELETE FROM admin_absent_chats")
        cur.executemany(
            """
            INSERT INTO admin_absent_chats(
                chat_id,
                chat_title,
                chat_username,
                chat_type,
                message_count,
                last_seen_at,
                scan_job_id,
                scanned_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                chat_title = excluded.chat_title,
                chat_username = excluded.chat_username,
                chat_type = excluded.chat_type,
                message_count = excluded.message_count,
                last_seen_at = excluded.last_seen_at,
                scan_job_id = excluded.scan_job_id,
                scanned_at = excluded.scanned_at
            """,
            [
                (
                    int(_scan_row_value(row, "chat_id", 0) or 0),
                    _chat_title_or_fallback(
                        int(_scan_row_value(row, "chat_id", 0) or 0),
                        _scan_row_value(row, "chat_title", ""),
                    ),
                    str(_scan_row_value(row, "chat_username", "")).strip().lstrip("@"),
                    str(_scan_row_value(row, "chat_type", "")),
                    int(_scan_row_value(row, "message_count", 0) or 0),
                    str(_scan_row_value(row, "last_seen_at", "")),
                    str(scan_job_id or ""),
                    str(scanned_at or ""),
                )
                for row in normalized_rows
            ],
        )
        conn.commit()
        return len(normalized_rows)
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        cur.close()


def list_absent_chat_scan_results(conn: sqlite3.Connection) -> List[dict]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                a.chat_id,
                a.chat_title,
                a.chat_username,
                a.chat_type,
                a.message_count,
                a.last_seen_at,
                a.scan_job_id,
                a.scanned_at
            FROM admin_absent_chats a
            WHERE EXISTS (
                SELECT 1 FROM chats c WHERE c.chat_id = a.chat_id
            )
            ORDER BY
                COALESCE(a.message_count, 0) DESC,
                COALESCE(a.last_seen_at, '') DESC,
                a.chat_title COLLATE NOCASE ASC,
                a.chat_id ASC
            """
        )
        rows = []
        for row in cur.fetchall():
            chat_id = int(row["chat_id"])
            rows.append(
                {
                    "chat_id": chat_id,
                    "chat_title": _chat_title_or_fallback(chat_id, row["chat_title"]),
                    "chat_username": str(row["chat_username"] or ""),
                    "chat_type": str(row["chat_type"] or ""),
                    "message_count": int(row["message_count"] or 0),
                    "last_seen_at": str(row["last_seen_at"] or ""),
                    "scan_job_id": str(row["scan_job_id"] or ""),
                    "scanned_at": str(row["scanned_at"] or ""),
                }
            )
        return rows
    finally:
        cur.close()
