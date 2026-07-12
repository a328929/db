import sqlite3
from collections.abc import Iterable
from contextlib import suppress
from typing import Any

from tg_harvest.domain.chat_inventory import (
    ChatInventoryRow,
    RestrictedChatInventoryRow,
    _optional_int,
    chat_identity_candidates,
    load_known_chat_identities,
)
from tg_harvest.domain.chat_titles import (
    chat_title_or_fallback as _chat_title_or_fallback,
)
from tg_harvest.domain.coerce import clean_username, enabled_int
from tg_harvest.storage.connection import synchronized_write
from tg_harvest.storage.row_access import (
    row_int as _row_int,
)

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


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table' AND name = ?
            LIMIT 1
            """,
            (str(table_name or "").strip(),),
        )
        return cur.fetchone() is not None
    finally:
        cur.close()


def list_database_channels(conn: sqlite3.Connection, *, sort: Any) -> list[dict]:
    normalized_sort = normalize_channel_sort(sort)
    order_sql = CHANNEL_SORT_OPTIONS[normalized_sort]
    has_sync_state = _table_exists(conn, "sync_chat_state")
    sync_select_sql = (
        """
                s.membership_scope AS sync_membership_scope,
                s.status AS sync_status,
                s.source_accounts AS sync_source_accounts,
                s.last_source_account AS sync_last_source_account,
                s.next_probe_at AS sync_next_probe_at,
                s.next_update_at AS sync_next_update_at,
                s.priority_score AS sync_priority_score,
                s.quarantine_reason AS sync_quarantine_reason,
                s.last_probe_status AS sync_last_probe_status,
        """
        if has_sync_state
        else """
                '' AS sync_membership_scope,
                '' AS sync_status,
                '' AS sync_source_accounts,
                '' AS sync_last_source_account,
                '' AS sync_next_probe_at,
                '' AS sync_next_update_at,
                0 AS sync_priority_score,
                '' AS sync_quarantine_reason,
                '' AS sync_last_probe_status,
        """
    )
    sync_join_sql = (
        "LEFT JOIN sync_chat_state s ON s.chat_id = c.chat_id"
        if has_sync_state
        else ""
    )
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
{sync_select_sql}
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
            {sync_join_sql}
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
                    "message_count": _row_int(row, "message_count"),
                    "last_seen_at": str(row["last_seen_at"] or ""),
                    "last_message_at": str(row["last_message_at"] or ""),
                    "last_message_ts": _optional_int(row["last_message_ts"]),
                    "sync_membership_scope": str(row["sync_membership_scope"] or ""),
                    "sync_status": str(row["sync_status"] or ""),
                    "sync_source_accounts": str(row["sync_source_accounts"] or ""),
                    "sync_last_source_account": str(
                        row["sync_last_source_account"] or ""
                    ),
                    "sync_next_probe_at": str(row["sync_next_probe_at"] or ""),
                    "sync_next_update_at": str(row["sync_next_update_at"] or ""),
                    "sync_priority_score": float(row["sync_priority_score"] or 0.0),
                    "sync_quarantine_reason": str(
                        row["sync_quarantine_reason"] or ""
                    ),
                    "sync_last_probe_status": str(row["sync_last_probe_status"] or ""),
                }
            )
        return channels
    finally:
        cur.close()


def _has_current_chat_identity(
    known_chat_identities: set[tuple[str, int]],
    *,
    chat_id: Any,
    chat_type: Any,
) -> bool:
    return not known_chat_identities.isdisjoint(
        chat_identity_candidates(chat_id, chat_type)
    )


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
                unavailable_reason,
                last_message_at,
                last_message_ts,
                scan_job_id,
                scanned_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                chat_title = excluded.chat_title,
                chat_username = excluded.chat_username,
                chat_type = excluded.chat_type,
                is_public = excluded.is_public,
                unavailable_reason = excluded.unavailable_reason,
                last_message_at = excluded.last_message_at,
                last_message_ts = excluded.last_message_ts,
                scan_job_id = excluded.scan_job_id,
                scanned_at = excluded.scanned_at
            """,
            [
                (
                    int(row.chat_id),
                    str(row.chat_title or "").strip() or f"Chat {int(row.chat_id)}",
                    clean_username(getattr(row, "chat_username", "")),
                    str(getattr(row, "chat_type", "") or ""),
                    enabled_int(getattr(row, "is_public", None)),
                    str(getattr(row, "unavailable_reason", "") or "").strip(),
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
        known_chat_identities = load_known_chat_identities(conn)
        cur.execute(
            """
            SELECT
                a.chat_id,
                a.chat_title,
                a.chat_username,
                a.chat_type,
                a.is_public,
                a.unavailable_reason,
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
            ORDER BY a.chat_title COLLATE NOCASE ASC, a.chat_id ASC
            """
        )
        rows = []
        for row in cur.fetchall():
            chat_id = int(row["chat_id"])
            if _has_current_chat_identity(
                known_chat_identities,
                chat_id=chat_id,
                chat_type=row["chat_type"],
            ):
                continue
            rows.append(
                {
                    "chat_id": chat_id,
                    "chat_title": _chat_title_or_fallback(chat_id, row["chat_title"]),
                    "chat_username": str(row["chat_username"] or ""),
                    "chat_type": str(row["chat_type"] or ""),
                    "is_public": _row_int(row, "is_public"),
                    "unavailable_reason": str(row["unavailable_reason"] or ""),
                    "last_message_at": str(row["last_message_at"] or ""),
                    "last_message_ts": _optional_int(row["last_message_ts"]),
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
                membership_scope,
                last_message_at,
                last_message_ts,
                scan_job_id,
                scanned_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                chat_title = excluded.chat_title,
                chat_username = excluded.chat_username,
                chat_type = excluded.chat_type,
                is_public = excluded.is_public,
                restriction_platforms = excluded.restriction_platforms,
                restriction_reasons = excluded.restriction_reasons,
                restriction_text = excluded.restriction_text,
                risk_flags = excluded.risk_flags,
                membership_scope = excluded.membership_scope,
                last_message_at = excluded.last_message_at,
                last_message_ts = excluded.last_message_ts,
                scan_job_id = excluded.scan_job_id,
                scanned_at = excluded.scanned_at
            """,
            [
                (
                    int(row.chat_id),
                    str(row.chat_title or "").strip() or f"Chat {int(row.chat_id)}",
                    clean_username(getattr(row, "chat_username", "")),
                    str(getattr(row, "chat_type", "") or ""),
                    enabled_int(getattr(row, "is_public", None)),
                    str(getattr(row, "restriction_platforms", "") or "").strip(),
                    str(getattr(row, "restriction_reasons", "") or "").strip(),
                    str(getattr(row, "restriction_text", "") or "").strip(),
                    str(getattr(row, "risk_flags", "") or "").strip(),
                    str(getattr(row, "membership_scope", "") or "joined").strip()
                    or "joined",
                    str(getattr(row, "last_message_at", "") or ""),
                    _optional_int(getattr(row, "last_message_ts", None)),
                    str(getattr(row, "scan_job_id", "") or scan_job_id or ""),
                    str(getattr(row, "scanned_at", "") or scanned_at or ""),
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
                a.membership_scope,
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
            ORDER BY
                CASE a.membership_scope
                    WHEN 'joined' THEN 0
                    WHEN 'public_unjoined' THEN 1
                    ELSE 2
                END ASC,
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
                    "is_public": _row_int(row, "is_public"),
                    "restriction_platforms": str(row["restriction_platforms"] or ""),
                    "restriction_reasons": str(row["restriction_reasons"] or ""),
                    "restriction_text": str(row["restriction_text"] or ""),
                    "risk_flags": str(row["risk_flags"] or ""),
                    "membership_scope": str(row["membership_scope"] or "joined"),
                    "last_message_at": str(row["last_message_at"] or ""),
                    "last_message_ts": _optional_int(row["last_message_ts"]),
                    "scan_job_id": str(row["scan_job_id"] or ""),
                    "scanned_at": str(row["scanned_at"] or ""),
                }
            )
        return rows
    finally:
        cur.close()
