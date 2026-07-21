import sqlite3
from collections.abc import Iterable
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any

from tg_harvest.domain.chat_inventory import (
    ChatInventoryRow,
    RestrictedChatInventoryRow,
    _optional_int,
    chat_identity_candidates,
    classify_chat_access_failure_text,
    load_known_chat_identities,
    normalize_chat_type_category,
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


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _append_token(value: Any, token: str) -> str:
    items = [part.strip() for part in str(value or "").split("、") if part.strip()]
    if token and token not in items:
        items.append(token)
    return "、".join(items)


@synchronized_write
def record_chat_access_risk(
    conn: sqlite3.Connection,
    *,
    chat_id: int,
    chat_title: str,
    chat_username: str | None,
    chat_type: str,
    risk_type: str,
    risk_message: str,
    source_job_id: str = "",
    source_account: str = "",
    observed_at: str = "",
) -> None:
    timestamp = str(observed_at or _utc_now_iso())
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            """
            INSERT INTO admin_chat_access_risks(
                chat_id,
                chat_title,
                chat_username,
                chat_type,
                risk_type,
                risk_message,
                failure_count,
                first_failed_at,
                last_failed_at,
                last_success_at,
                source_job_id,
                source_account,
                is_active,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, '', ?, ?, 1, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                chat_title = excluded.chat_title,
                chat_username = CASE
                    WHEN excluded.chat_username <> '' THEN excluded.chat_username
                    ELSE admin_chat_access_risks.chat_username
                END,
                chat_type = CASE
                    WHEN excluded.chat_type <> '' THEN excluded.chat_type
                    ELSE admin_chat_access_risks.chat_type
                END,
                risk_type = excluded.risk_type,
                risk_message = excluded.risk_message,
                failure_count = admin_chat_access_risks.failure_count + 1,
                last_failed_at = excluded.last_failed_at,
                source_job_id = excluded.source_job_id,
                source_account = excluded.source_account,
                is_active = 1,
                updated_at = excluded.updated_at
            """,
            (
                int(chat_id),
                str(chat_title or "").strip() or f"Chat {int(chat_id)}",
                clean_username(chat_username),
                str(chat_type or ""),
                str(risk_type or "access_unavailable"),
                str(risk_message or "群组当前不可访问")[:500],
                timestamp,
                timestamp,
                str(source_job_id or ""),
                str(source_account or ""),
                timestamp,
            ),
        )
        conn.commit()
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        cur.close()


@synchronized_write
def resolve_chat_access_risk(
    conn: sqlite3.Connection,
    *,
    chat_id: int,
    source_job_id: str = "",
    resolved_at: str = "",
) -> bool:
    has_access_risks = _table_exists(conn, "admin_chat_access_risks")
    has_sync_state = _table_exists(conn, "sync_chat_state")
    if not has_access_risks and not has_sync_state:
        return False
    timestamp = str(resolved_at or _utc_now_iso())
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        changed = False
        if has_access_risks:
            cur.execute(
                """
                UPDATE admin_chat_access_risks
                SET
                    is_active = 0,
                    last_success_at = ?,
                    source_job_id = CASE WHEN ? <> '' THEN ? ELSE source_job_id END,
                    updated_at = ?
                WHERE chat_id = ? AND is_active = 1
                """,
                (timestamp, source_job_id, source_job_id, timestamp, int(chat_id)),
            )
            changed = int(cur.rowcount or 0) > 0
        if has_sync_state:
            cur.execute(
                """
                UPDATE sync_chat_state
                SET
                    last_success_at = ?,
                    last_failure_message = '',
                    failure_count = 0,
                    updated_at = ?
                WHERE chat_id = ? AND last_failure_message <> ''
                """,
                (timestamp, timestamp, int(chat_id)),
            )
            changed = changed or int(cur.rowcount or 0) > 0
        conn.commit()
        return changed
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        cur.close()


def list_active_chat_access_risk_ids(conn: sqlite3.Connection) -> set[int]:
    has_access_risks = _table_exists(conn, "admin_chat_access_risks")
    has_sync_state = _table_exists(conn, "sync_chat_state")
    if not has_access_risks and not has_sync_state:
        return set()
    cur = conn.cursor()
    try:
        chat_ids: set[int] = set()
        if has_access_risks:
            cur.execute(
                "SELECT chat_id FROM admin_chat_access_risks WHERE is_active = 1"
            )
            chat_ids.update(int(row["chat_id"]) for row in cur.fetchall())
        if has_sync_state:
            cur.execute(
                """
                SELECT chat_id, last_failure_message
                FROM sync_chat_state
                WHERE is_active = 1 AND last_failure_message <> ''
                """
            )
            chat_ids.update(
                int(row["chat_id"])
                for row in cur.fetchall()
                if classify_chat_access_failure_text(row["last_failure_message"])
            )
        return chat_ids
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


def _load_database_chat_summaries(
    cur: sqlite3.Cursor,
) -> tuple[dict[int, dict], dict[tuple[str, int], list[dict]]]:
    """Index local chat summaries by every supported Telegram ID shape.

    Scanner rows normally use the positive IDs stored by the harvester, but
    older scan records can contain a signed or ``-100`` entity ID.  Joining on
    the raw integer would then report an existing chat as having no messages
    and would pass the wrong ID to the management actions.
    """
    cur.execute(
        """
        SELECT
            c.chat_id,
            c.chat_type,
            c.message_count,
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
        """
    )
    summaries_by_chat_id: dict[int, dict] = {}
    summaries_by_identity: dict[tuple[str, int], list[dict]] = {}
    for row in cur.fetchall():
        summary = {
            "chat_id": int(row["chat_id"]),
            "chat_type": str(row["chat_type"] or ""),
            "message_count": _row_int(row, "message_count"),
            "last_message_at": str(row["last_message_at"] or ""),
            "last_message_ts": _optional_int(row["last_message_ts"]),
        }
        summaries_by_chat_id[summary["chat_id"]] = summary
        for identity in chat_identity_candidates(
            summary["chat_id"], summary["chat_type"]
        ):
            summaries_by_identity.setdefault(identity, []).append(summary)
    return summaries_by_chat_id, summaries_by_identity


def _find_database_chat_summary(
    summaries_by_chat_id: dict[int, dict],
    summaries_by_identity: dict[tuple[str, int], list[dict]],
    *,
    chat_id: Any,
    chat_type: Any,
) -> tuple[dict | None, bool]:
    try:
        scanned_chat_id = int(chat_id)
    except (TypeError, ValueError):
        return None, False
    candidates = chat_identity_candidates(chat_id, chat_type)
    if not candidates:
        return None, False

    exact = summaries_by_chat_id.get(scanned_chat_id)
    if exact is not None and not candidates.isdisjoint(
        chat_identity_candidates(exact["chat_id"], exact["chat_type"])
    ):
        return exact, False

    normalized_type = normalize_chat_type_category(chat_type)
    ordered = sorted(
        candidates,
        key=lambda identity: (
            0 if normalized_type and identity[0] == normalized_type else 1,
            identity[0],
            identity[1],
        ),
    )
    matches: dict[int, dict] = {}
    for identity in ordered:
        for summary in summaries_by_identity.get(identity, []):
            matches[int(summary["chat_id"])] = summary
    if len(matches) == 1:
        return next(iter(matches.values())), False
    return None, len(matches) > 1


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


def _merge_active_access_risks(
    conn: sqlite3.Connection,
    cur: sqlite3.Cursor,
    *,
    summaries_by_chat_id: dict[int, dict],
    summaries_by_identity: dict[tuple[str, int], list[dict]],
    rows: list[dict],
) -> list[dict]:
    rows_by_chat_id = {int(item["chat_id"]): item for item in rows}
    access_records: list[dict[str, Any]] = []
    persisted_chat_ids: set[int] = set()
    if _table_exists(conn, "admin_chat_access_risks"):
        cur.execute(
            """
            SELECT
                chat_id,
                chat_title,
                chat_username,
                chat_type,
                risk_type,
                risk_message,
                failure_count,
                first_failed_at,
                last_failed_at,
                source_job_id,
                source_account,
                'access' AS risk_source
            FROM admin_chat_access_risks
            WHERE is_active = 1
            ORDER BY last_failed_at DESC, chat_id ASC
            """
        )
        access_records.extend(dict(row) for row in cur.fetchall())
        persisted_chat_ids = {int(row["chat_id"]) for row in access_records}

    if _table_exists(conn, "sync_chat_state"):
        cur.execute(
            """
            SELECT
                s.chat_id,
                s.chat_title,
                s.chat_username,
                c.chat_type,
                s.last_failure_message AS risk_message,
                s.failure_count,
                s.last_failure_at AS first_failed_at,
                s.last_failure_at AS last_failed_at,
                '' AS source_job_id,
                s.last_source_account AS source_account,
                'scheduler' AS risk_source
            FROM sync_chat_state s
            LEFT JOIN chats c ON c.chat_id = s.chat_id
            WHERE s.is_active = 1
              AND s.last_failure_message <> ''
            ORDER BY s.last_failure_at DESC, s.chat_id ASC
            """
        )
        for row in cur.fetchall():
            chat_id = int(row["chat_id"])
            risk_type = classify_chat_access_failure_text(row["risk_message"])
            if not risk_type or chat_id in persisted_chat_ids:
                continue
            record = dict(row)
            record["risk_type"] = risk_type
            access_records.append(record)

    for access_row in access_records:
        stored_chat_id = int(access_row["chat_id"])
        database_summary, database_match_ambiguous = _find_database_chat_summary(
            summaries_by_chat_id,
            summaries_by_identity,
            chat_id=stored_chat_id,
            chat_type=access_row["chat_type"],
        )
        chat_id = (
            int(database_summary["chat_id"])
            if database_summary is not None
            else stored_chat_id
        )
        risk_type = str(access_row["risk_type"] or "access_unavailable")
        risk_source = str(access_row["risk_source"] or "access")
        access_fields = {
            "access_failure_type": risk_type,
            "access_failure_message": str(access_row["risk_message"] or ""),
            "access_failure_count": int(access_row["failure_count"] or 0),
            "access_first_failed_at": str(access_row["first_failed_at"] or ""),
            "access_last_failed_at": str(access_row["last_failed_at"] or ""),
            "access_source_job_id": str(access_row["source_job_id"] or ""),
            "access_source_account": str(access_row["source_account"] or ""),
        }
        existing = rows_by_chat_id.get(chat_id)
        if existing is not None:
            existing["risk_flags"] = _append_token(
                existing.get("risk_flags"), risk_type
            )
            sources = [
                value
                for value in str(existing.get("risk_source") or "").split(",")
                if value
            ]
            if risk_source not in sources:
                sources.append(risk_source)
            existing["risk_source"] = ",".join(sources)
            existing.update(access_fields)
            continue

        in_database = database_summary is not None
        item = {
            "chat_id": chat_id,
            "chat_title": _chat_title_or_fallback(chat_id, access_row["chat_title"]),
            "chat_username": str(access_row["chat_username"] or ""),
            "chat_type": str(access_row["chat_type"] or ""),
            "is_public": int(bool(access_row["chat_username"])),
            "restriction_platforms": "",
            "restriction_reasons": "",
            "restriction_text": "",
            "risk_flags": risk_type,
            "membership_scope": "access_unknown",
            "in_database": int(in_database),
            "database_match_ambiguous": int(database_match_ambiguous),
            "message_count": (
                int(database_summary["message_count"])
                if database_summary is not None
                else 0
            ),
            "last_message_at": (
                str(database_summary["last_message_at"] or "")
                if database_summary is not None
                else ""
            ),
            "last_message_ts": (
                database_summary["last_message_ts"]
                if database_summary is not None
                else None
            ),
            "scan_job_id": "",
            "scanned_at": str(access_row["last_failed_at"] or ""),
            "risk_source": risk_source,
            **access_fields,
        }
        rows.append(item)
        rows_by_chat_id[chat_id] = item

    scope_order = {"joined": 0, "public_unjoined": 1, "access_unknown": 2}
    rows.sort(
        key=lambda item: (
            scope_order.get(str(item.get("membership_scope") or ""), 3),
            str(item.get("chat_title") or "").casefold(),
            int(item.get("chat_id") or 0),
        )
    )
    return rows


def list_restricted_chat_scan_results(conn: sqlite3.Connection) -> list[dict]:
    cur = conn.cursor()
    try:
        summaries_by_chat_id, summaries_by_identity = (
            _load_database_chat_summaries(cur)
        )
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
                a.last_message_at,
                a.last_message_ts,
                a.scan_job_id,
                a.scanned_at
            FROM admin_restricted_chats a
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
            scanned_chat_id = int(row["chat_id"])
            database_summary, database_match_ambiguous = (
                _find_database_chat_summary(
                    summaries_by_chat_id,
                    summaries_by_identity,
                    chat_id=scanned_chat_id,
                    chat_type=row["chat_type"],
                )
            )
            in_database = database_summary is not None
            # Prefer the canonical ID and summary whenever the scan row maps
            # to exactly one local chat.  This keeps delete/probe actions on
            # the same key used by messages, chats, and sync state.
            chat_id = (
                int(database_summary["chat_id"])
                if database_summary is not None
                else scanned_chat_id
            )
            database_message_count = (
                int(database_summary["message_count"])
                if database_summary is not None
                else 0
            )
            last_message_at = str(row["last_message_at"] or "")
            if not last_message_at and database_summary is not None:
                last_message_at = str(database_summary["last_message_at"] or "")
            last_message_ts = _optional_int(row["last_message_ts"])
            if last_message_ts is None and database_summary is not None:
                last_message_ts = database_summary["last_message_ts"]
            scan_risk_flags = str(row["risk_flags"] or "")
            access_failure_type = next(
                (
                    risk_type
                    for risk_type in (
                        "access_unavailable",
                        "access_denied",
                        "account_banned",
                        "entity_unavailable",
                    )
                    if risk_type in scan_risk_flags.split("、")
                ),
                "",
            )
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
                    "risk_flags": scan_risk_flags,
                    "membership_scope": str(row["membership_scope"] or "joined"),
                    "in_database": int(in_database),
                    "database_match_ambiguous": int(database_match_ambiguous),
                    "message_count": database_message_count if in_database else 0,
                    "last_message_at": last_message_at,
                    "last_message_ts": last_message_ts,
                    "scan_job_id": str(row["scan_job_id"] or ""),
                    "scanned_at": str(row["scanned_at"] or ""),
                    "risk_source": "telegram",
                    "access_failure_type": access_failure_type,
                    "access_failure_message": (
                        str(row["restriction_text"] or "")
                        if access_failure_type
                        else ""
                    ),
                    "access_failure_count": 0,
                    "access_first_failed_at": "",
                    "access_last_failed_at": "",
                    "access_source_job_id": "",
                    "access_source_account": "",
                }
            )
        return _merge_active_access_risks(
            conn,
            cur,
            summaries_by_chat_id=summaries_by_chat_id,
            summaries_by_identity=summaries_by_identity,
            rows=rows,
        )
    finally:
        cur.close()
