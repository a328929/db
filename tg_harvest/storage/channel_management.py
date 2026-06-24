import sqlite3
from collections.abc import Iterable
from contextlib import suppress
from typing import Any

from tg_harvest.domain.chat_inventory import (
    ChatInventoryRow,
    RestrictedChatInventoryRow,
    _optional_int,
)
from tg_harvest.domain.chat_titles import (
    chat_title_or_fallback as _chat_title_or_fallback,
)
from tg_harvest.storage.connection import synchronized_write

CHANNEL_SORT_DEFAULT = "message_count_desc"
CHANNEL_SORT_OPTIONS = {
    "message_count_asc": (
        "c.message_count ASC, c.chat_title COLLATE NOCASE ASC, c.chat_id ASC"
    ),
    "message_count_desc": (
        "c.message_count DESC, c.chat_title COLLATE NOCASE ASC, c.chat_id ASC"
    ),
    "updated_desc": (
        "CASE WHEN last_message_ts IS NULL THEN 1 ELSE 0 END ASC, "
        "last_message_ts DESC, c.chat_title COLLATE NOCASE ASC, c.chat_id ASC"
    ),
    "updated_asc": (
        "CASE WHEN last_message_ts IS NULL THEN 1 ELSE 0 END ASC, "
        "last_message_ts ASC, c.chat_title COLLATE NOCASE ASC, c.chat_id ASC"
    ),
}


def normalize_channel_sort(raw_sort: Any) -> str:
    value = str(raw_sort or CHANNEL_SORT_DEFAULT).strip().lower()
    if value in CHANNEL_SORT_OPTIONS:
        return value
    return CHANNEL_SORT_DEFAULT


def list_database_channels(conn: sqlite3.Connection, *, sort: Any) -> list[dict]:
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
                lm.msg_date_text AS last_message_at,
                lm.msg_date_ts AS last_message_ts
            FROM chats c
            LEFT JOIN messages lm
              ON lm.chat_id = c.chat_id
             AND lm.message_id = (
                    SELECT m.message_id
                    FROM messages m
                    WHERE m.chat_id = c.chat_id
                    ORDER BY m.msg_date_ts DESC, m.message_id DESC
                    LIMIT 1
                )
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
                last_message_at,
                last_message_ts,
                scan_job_id,
                scanned_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                chat_title = excluded.chat_title,
                chat_username = excluded.chat_username,
                chat_type = excluded.chat_type,
                is_public = excluded.is_public,
                last_message_at = excluded.last_message_at,
                last_message_ts = excluded.last_message_ts,
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
                    str(getattr(row, "last_message_at", "") or ""),
                    _optional_int(getattr(row, "last_message_ts", None)),
                    str(scan_job_id or ""),
                    str(scanned_at or ""),
                )
                for row in normalized_rows
            ],
        )
        conn.commit()
        return len(normalized_rows)
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        cur.close()


def list_missing_chat_scan_results(conn: sqlite3.Connection) -> list[dict]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                a.chat_id,
                a.chat_title,
                a.chat_username,
                a.chat_type,
                a.is_public,
                COALESCE(
                    NULLIF(a.last_message_at, ''),
                    lm.msg_date_text,
                    ''
                ) AS last_message_at,
                COALESCE(
                    a.last_message_ts,
                    lm.msg_date_ts
                ) AS last_message_ts,
                a.scan_job_id,
                a.scanned_at
            FROM admin_missing_chats a
            LEFT JOIN messages lm
              ON lm.chat_id = a.chat_id
             AND lm.message_id = (
                    SELECT m.message_id
                    FROM messages m
                    WHERE m.chat_id = a.chat_id
                    ORDER BY m.msg_date_ts DESC, m.message_id DESC
                    LIMIT 1
                )
            WHERE NOT EXISTS (
                SELECT 1 FROM chats c WHERE c.chat_id = a.chat_id
            )
            ORDER BY a.chat_title COLLATE NOCASE ASC, a.chat_id ASC
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
                    "last_message_at": str(row["last_message_at"] or ""),
                    "last_message_ts": _optional_int(row["last_message_ts"]),
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
                last_message_at,
                last_message_ts,
                scan_reason,
                scan_job_id,
                scanned_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                chat_title = excluded.chat_title,
                chat_username = excluded.chat_username,
                chat_type = excluded.chat_type,
                message_count = excluded.message_count,
                last_seen_at = excluded.last_seen_at,
                last_message_at = excluded.last_message_at,
                last_message_ts = excluded.last_message_ts,
                scan_reason = excluded.scan_reason,
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
                    str(_scan_row_value(row, "last_message_at", "")),
                    _optional_int(_scan_row_value(row, "last_message_ts", None)),
                    str(_scan_row_value(row, "scan_reason", "")).strip()
                    or "账号未加入",
                    str(scan_job_id or ""),
                    str(scanned_at or ""),
                )
                for row in normalized_rows
            ],
        )
        conn.commit()
        return len(normalized_rows)
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        cur.close()


def list_absent_chat_scan_results(conn: sqlite3.Connection) -> list[dict]:
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
                COALESCE(
                    NULLIF(a.last_message_at, ''),
                    lm.msg_date_text,
                    NULLIF(a.last_seen_at, ''),
                    ''
                ) AS last_message_at,
                COALESCE(
                    a.last_message_ts,
                    lm.msg_date_ts
                ) AS last_message_ts,
                a.scan_reason,
                a.scan_job_id,
                a.scanned_at
            FROM admin_absent_chats a
            LEFT JOIN messages lm
              ON lm.chat_id = a.chat_id
             AND lm.message_id = (
                    SELECT m.message_id
                    FROM messages m
                    WHERE m.chat_id = a.chat_id
                    ORDER BY m.msg_date_ts DESC, m.message_id DESC
                    LIMIT 1
                )
            WHERE EXISTS (
                SELECT 1 FROM chats c WHERE c.chat_id = a.chat_id
            )
            ORDER BY
                COALESCE(a.message_count, 0) DESC,
                COALESCE(last_message_ts, 0) DESC,
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
                    "last_message_at": str(row["last_message_at"] or ""),
                    "last_message_ts": _optional_int(row["last_message_ts"]),
                    "scan_reason": str(row["scan_reason"] or ""),
                    "scan_job_id": str(row["scan_job_id"] or ""),
                    "scanned_at": str(row["scanned_at"] or ""),
                }
            )
        return rows
    finally:
        cur.close()


@synchronized_write
def replace_restricted_chat_scan_results(
    conn: sqlite3.Connection,
    rows: Iterable[RestrictedChatInventoryRow],
    *,
    scan_job_id: str,
    scanned_at: str,
) -> int:
    normalized_rows = list(rows)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("DELETE FROM admin_restricted_chats")
        cur.executemany(
            """
            INSERT INTO admin_restricted_chats(
                chat_id,
                chat_title,
                chat_username,
                chat_type,
                is_public,
                restriction_platforms,
                restriction_reasons,
                restriction_text,
                risk_flags,
                last_message_at,
                last_message_ts,
                scan_job_id,
                scanned_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                chat_title = excluded.chat_title,
                chat_username = excluded.chat_username,
                chat_type = excluded.chat_type,
                is_public = excluded.is_public,
                restriction_platforms = excluded.restriction_platforms,
                restriction_reasons = excluded.restriction_reasons,
                restriction_text = excluded.restriction_text,
                risk_flags = excluded.risk_flags,
                last_message_at = excluded.last_message_at,
                last_message_ts = excluded.last_message_ts,
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
                    str(getattr(row, "restriction_platforms", "") or "").strip(),
                    str(getattr(row, "restriction_reasons", "") or "").strip(),
                    str(getattr(row, "restriction_text", "") or "").strip(),
                    str(getattr(row, "risk_flags", "") or "").strip(),
                    str(getattr(row, "last_message_at", "") or ""),
                    _optional_int(getattr(row, "last_message_ts", None)),
                    str(scan_job_id or ""),
                    str(scanned_at or ""),
                )
                for row in normalized_rows
            ],
        )
        conn.commit()
        return len(normalized_rows)
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        cur.close()


def list_restricted_chat_scan_results(conn: sqlite3.Connection) -> list[dict]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                a.chat_id,
                a.chat_title,
                a.chat_username,
                a.chat_type,
                a.is_public,
                a.restriction_platforms,
                a.restriction_reasons,
                a.restriction_text,
                a.risk_flags,
                COALESCE(
                    NULLIF(a.last_message_at, ''),
                    lm.msg_date_text,
                    ''
                ) AS last_message_at,
                COALESCE(
                    a.last_message_ts,
                    lm.msg_date_ts
                ) AS last_message_ts,
                a.scan_job_id,
                a.scanned_at
            FROM admin_restricted_chats a
            LEFT JOIN messages lm
              ON lm.chat_id = a.chat_id
             AND lm.message_id = (
                    SELECT m.message_id
                    FROM messages m
                    WHERE m.chat_id = a.chat_id
                    ORDER BY m.msg_date_ts DESC, m.message_id DESC
                    LIMIT 1
                )
            ORDER BY a.chat_title COLLATE NOCASE ASC, a.chat_id ASC
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
                    "restriction_platforms": str(row["restriction_platforms"] or ""),
                    "restriction_reasons": str(row["restriction_reasons"] or ""),
                    "restriction_text": str(row["restriction_text"] or ""),
                    "risk_flags": str(row["risk_flags"] or ""),
                    "last_message_at": str(row["last_message_at"] or ""),
                    "last_message_ts": _optional_int(row["last_message_ts"]),
                    "scan_job_id": str(row["scan_job_id"] or ""),
                    "scanned_at": str(row["scanned_at"] or ""),
                }
            )
        return rows
    finally:
        cur.close()
