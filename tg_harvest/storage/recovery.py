import sqlite3
from collections.abc import Iterable
from contextlib import suppress
from typing import Any

from tg_harvest.domain.chat_inventory import (
    SessionChatRecoveryRow,
    _optional_int,
)
from tg_harvest.domain.chat_titles import (
    chat_title_or_fallback as _chat_title_or_fallback,
)
from tg_harvest.domain.coerce import clean_username, enabled_int, safe_int
from tg_harvest.ingest.store import UPSERT_CHAT_SQL
from tg_harvest.storage.connection import synchronized_write
from tg_harvest.storage.row_access import (
    row_int as _row_int,
)
from tg_harvest.storage.row_access import (
    scan_row_int as _scan_row_int,
)
from tg_harvest.storage.row_access import (
    scan_row_value as _scan_row_value,
)
from tg_harvest.storage.schema import _refresh_chat_message_counts


def _recovery_row_params(row: Any, *, scan_job_id: str, scanned_at: str) -> tuple:
    chat_id = _scan_row_int(row, "chat_id", 0)
    return (
        chat_id,
        _chat_title_or_fallback(chat_id, _scan_row_value(row, "chat_title", "")),
        clean_username(_scan_row_value(row, "chat_username", "")),
        str(_scan_row_value(row, "chat_type", "") or "SessionEntity"),
        enabled_int(_scan_row_value(row, "is_public", 0)),
        str(_scan_row_value(row, "source_session", "")).strip(),
        _optional_int(_scan_row_value(row, "source_entity_id", None)),
        _optional_int(_scan_row_value(row, "source_access_hash", None)),
        str(_scan_row_value(row, "availability_reason", "")).strip(),
        str(_scan_row_value(row, "session_entity_date", "")).strip(),
        _optional_int(_scan_row_value(row, "session_entity_ts", None)),
        str(scan_job_id or ""),
        str(scanned_at or ""),
    )


@synchronized_write
def replace_recovery_chat_scan_results(
    conn: sqlite3.Connection,
    rows: Iterable[SessionChatRecoveryRow],
    *,
    scan_job_id: str,
    scanned_at: str,
) -> int:
    normalized_rows = list(rows)
    normalized_chat_ids = sorted(
        {
            _scan_row_int(row, "chat_id", 0)
            for row in normalized_rows
            if _scan_row_int(row, "chat_id", 0) != 0
        }
    )
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.executemany(
            """
            INSERT INTO admin_recovery_chats(
                chat_id,
                chat_title,
                chat_username,
                chat_type,
                is_public,
                source_session,
                source_entity_id,
                source_access_hash,
                availability_reason,
                session_entity_date,
                session_entity_ts,
                scan_job_id,
                scanned_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                chat_title = excluded.chat_title,
                chat_username = excluded.chat_username,
                chat_type = excluded.chat_type,
                is_public = excluded.is_public,
                source_session = excluded.source_session,
                source_entity_id = excluded.source_entity_id,
                source_access_hash = excluded.source_access_hash,
                availability_reason = excluded.availability_reason,
                session_entity_date = excluded.session_entity_date,
                session_entity_ts = excluded.session_entity_ts,
                scan_job_id = excluded.scan_job_id,
                scanned_at = excluded.scanned_at
            """,
            [
                _recovery_row_params(
                    row,
                    scan_job_id=scan_job_id,
                    scanned_at=scanned_at,
                )
                for row in normalized_rows
            ],
        )
        if normalized_chat_ids:
            cur.execute(
                """
                CREATE TEMP TABLE IF NOT EXISTS temp_recovery_scan_chat_ids (
                    chat_id INTEGER PRIMARY KEY
                )
                """
            )
            cur.execute("DELETE FROM temp_recovery_scan_chat_ids")
            cur.executemany(
                "INSERT OR IGNORE INTO temp_recovery_scan_chat_ids(chat_id) VALUES (?)",
                [(chat_id,) for chat_id in normalized_chat_ids],
            )
            cur.execute(
                """
                DELETE FROM admin_recovery_chats
                WHERE chat_id NOT IN (
                    SELECT chat_id FROM temp_recovery_scan_chat_ids
                )
                """
            )
            cur.execute("DELETE FROM temp_recovery_scan_chat_ids")
        else:
            cur.execute("DELETE FROM admin_recovery_chats")
        conn.commit()
        return len(normalized_rows)
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        cur.close()


def list_recovery_chat_candidates(conn: sqlite3.Connection) -> list[dict]:
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
                a.source_session,
                a.source_entity_id,
                a.source_access_hash,
                a.availability_reason,
                a.session_entity_date,
                a.session_entity_ts,
                a.recovered_at,
                a.recovered_job_id,
                a.scan_job_id,
                a.scanned_at,
                CASE WHEN c.chat_id IS NULL THEN 0 ELSE 1 END AS in_database,
                COALESCE(c.message_count, 0) AS message_count,
                lm.msg_date_text AS last_message_at,
                lm.msg_date_ts AS last_message_ts,
                c.last_seen_at AS database_last_seen_at
            FROM admin_recovery_chats a
            LEFT JOIN chats c ON c.chat_id = a.chat_id
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
                CASE WHEN c.chat_id IS NULL THEN 0 ELSE 1 END ASC,
                CASE
                    WHEN a.recovered_at IS NULL OR a.recovered_at = '' THEN 0
                    ELSE 1
                END ASC,
                a.session_entity_ts DESC,
                a.chat_title COLLATE NOCASE ASC,
                a.chat_id ASC
            """
        )
        rows = []
        for row in cur.fetchall():
            chat_id = _row_int(row, "chat_id")
            rows.append(
                {
                    "chat_id": chat_id,
                    "chat_title": _chat_title_or_fallback(chat_id, row["chat_title"]),
                    "chat_username": str(row["chat_username"] or ""),
                    "chat_type": str(row["chat_type"] or ""),
                    "is_public": _row_int(row, "is_public"),
                    "source_session": str(row["source_session"] or ""),
                    "source_entity_id": _optional_int(row["source_entity_id"]),
                    "source_access_hash": _optional_int(row["source_access_hash"]),
                    "availability_reason": str(row["availability_reason"] or ""),
                    "session_entity_date": str(row["session_entity_date"] or ""),
                    "session_entity_ts": _optional_int(row["session_entity_ts"]),
                    "recovered_at": str(row["recovered_at"] or ""),
                    "recovered_job_id": str(row["recovered_job_id"] or ""),
                    "scan_job_id": str(row["scan_job_id"] or ""),
                    "scanned_at": str(row["scanned_at"] or ""),
                    "in_database": _row_int(row, "in_database"),
                    "message_count": _row_int(row, "message_count"),
                    "last_message_at": str(row["last_message_at"] or ""),
                    "last_message_ts": _optional_int(row["last_message_ts"]),
                    "database_last_seen_at": str(row["database_last_seen_at"] or ""),
                }
            )
        return rows
    finally:
        cur.close()


def build_recovery_overview(conn: sqlite3.Connection) -> dict:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                COUNT(*) AS total_count,
                SUM(CASE WHEN c.chat_id IS NULL THEN 1 ELSE 0 END) AS pending_count,
                SUM(CASE WHEN c.chat_id IS NOT NULL THEN 1 ELSE 0 END) AS in_database_count,
                SUM(
                    CASE
                        WHEN a.recovered_at IS NULL OR a.recovered_at = '' THEN 0
                        ELSE 1
                    END
                ) AS recovered_count,
                MAX(a.scanned_at) AS last_scanned_at
            FROM admin_recovery_chats a
            LEFT JOIN chats c ON c.chat_id = a.chat_id
            """
        )
        row = cur.fetchone()
        return {
            "total_count": _row_int(row, "total_count"),
            "pending_count": _row_int(row, "pending_count"),
            "in_database_count": _row_int(row, "in_database_count"),
            "recovered_count": _row_int(row, "recovered_count"),
            "last_scanned_at": str(row["last_scanned_at"] or "") if row else "",
        }
    finally:
        cur.close()


def _candidate_upsert_chat_row(row: sqlite3.Row) -> tuple:
    chat_id = _row_int(row, "chat_id")
    chat_username = clean_username(row["chat_username"])
    return (
        chat_id,
        _chat_title_or_fallback(chat_id, row["chat_title"]),
        chat_username,
        enabled_int(_row_int(row, "is_public")),
        str(row["chat_type"] or "SessionEntity"),
    )


@synchronized_write
def recover_chats_from_candidates(
    conn: sqlite3.Connection,
    *,
    chat_ids: Iterable[int] | None,
    job_id: str,
    recovered_at: str,
) -> dict:
    normalized_chat_ids = (
        None
        if chat_ids is None
        else sorted({safe_int(chat_id) for chat_id in chat_ids if safe_int(chat_id) != 0})
    )
    cur = conn.cursor()
    try:
        where_sql = "1 = 1"
        if normalized_chat_ids is not None:
            if not normalized_chat_ids:
                return {"candidate_count": 0, "recovered_count": 0, "skipped_count": 0}
            cur.execute("DROP TABLE IF EXISTS temp_recovery_chat_ids")
            cur.execute(
                "CREATE TEMP TABLE temp_recovery_chat_ids "
                "(chat_id INTEGER PRIMARY KEY)"
            )
            cur.executemany(
                "INSERT INTO temp_recovery_chat_ids(chat_id) VALUES (?)",
                [(chat_id,) for chat_id in normalized_chat_ids],
            )
            where_sql = (
                "EXISTS ("
                "SELECT 1 FROM temp_recovery_chat_ids selected "
                "WHERE selected.chat_id = a.chat_id"
                ")"
            )

        cur.execute(
            f"""
            SELECT
                a.chat_id,
                a.chat_title,
                a.chat_username,
                a.chat_type,
                a.is_public,
                c.chat_id AS existing_chat_id
            FROM admin_recovery_chats a
            LEFT JOIN chats c ON c.chat_id = a.chat_id
            WHERE {where_sql}
            ORDER BY a.chat_title COLLATE NOCASE ASC, a.chat_id ASC
            """
        )
        rows = cur.fetchall()
        recover_rows = [row for row in rows if row["existing_chat_id"] is None]

        if recover_rows:
            cur.executemany(
                UPSERT_CHAT_SQL,
                [_candidate_upsert_chat_row(row) for row in recover_rows],
            )
            _refresh_chat_message_counts(
                cur,
                [_row_int(row, "chat_id") for row in recover_rows],
            )

        update_where_sql = "1 = 1"
        if normalized_chat_ids is not None:
            update_where_sql = (
                "EXISTS ("
                "SELECT 1 FROM temp_recovery_chat_ids selected "
                "WHERE selected.chat_id = admin_recovery_chats.chat_id"
                ")"
            )
        cur.execute(
            f"""
            UPDATE admin_recovery_chats
            SET recovered_at = ?,
                recovered_job_id = ?
            WHERE {update_where_sql}
              AND chat_id IN (SELECT chat_id FROM chats)
            """,
            [str(recovered_at or ""), str(job_id or "")],
        )
        if normalized_chat_ids is not None:
            cur.execute("DROP TABLE IF EXISTS temp_recovery_chat_ids")
        conn.commit()
        return {
            "candidate_count": len(rows),
            "recovered_count": len(recover_rows),
            "skipped_count": len(rows) - len(recover_rows),
        }
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        with suppress(Exception):
            cur.execute("DROP TABLE IF EXISTS temp_recovery_chat_ids")
        cur.close()
