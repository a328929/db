import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

from tg_harvest.domain.chat_titles import chat_sort_key, chat_title_or_fallback
from tg_harvest.domain.coerce import safe_int
from tg_harvest.storage.row_access import row_int as _row_int

_ADMIN_SYNC_WINDOWS: tuple[dict[str, Any], ...] = (
    {"key": "live", "label": "时时", "seconds": 0, "is_live": True},
    {"key": "10m", "label": "最近10分钟", "seconds": 10 * 60},
    {"key": "30m", "label": "最近30分钟", "seconds": 30 * 60},
    {"key": "1h", "label": "最近1小时", "seconds": 60 * 60},
    {"key": "2h", "label": "最近2小时", "seconds": 2 * 60 * 60},
    {"key": "5h", "label": "最近5小时", "seconds": 5 * 60 * 60},
    {"key": "10h", "label": "最近10小时", "seconds": 10 * 60 * 60},
    {"key": "1d", "label": "最近1天", "seconds": 24 * 60 * 60},
    {"key": "2d", "label": "最近2天", "seconds": 2 * 24 * 60 * 60},
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


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


def _empty_admin_sync_window_payload(window: dict[str, Any]) -> dict[str, Any]:
    return {
        "window_key": str(window["key"]),
        "label": str(window["label"]),
        "seconds": int(window["seconds"]),
        "is_live": bool(window.get("is_live", False)),
        "message_count": 0,
        "chat_count": 0,
        "oldest_created_at": "",
        "latest_created_at": "",
    }


def _truncate_text(value: Any, *, max_len: int) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= max_len:
        return text
    return f"{text[:max_len]}..."


def build_admin_sync_live_messages_payload(
    conn: sqlite3.Connection,
    *,
    limit: int = 50,
) -> dict[str, Any]:
    effective_limit = max(1, min(200, int(limit)))
    generated_at = _format_utc_text(_utc_now())

    if not _table_exists(conn, "messages"):
        return {
            "ok": True,
            "generated_at": generated_at,
            "limit": effective_limit,
            "items": [],
        }

    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                m.pk,
                m.chat_id,
                c.chat_title,
                c.chat_username,
                c.chat_type,
                m.message_id,
                m.msg_date_text,
                m.msg_type,
                COALESCE(NULLIF(TRIM(m.content), ''), NULLIF(TRIM(m.content_norm), ''), '') AS content,
                m.created_at
            FROM messages m
            JOIN chats c
              ON c.chat_id = m.chat_id
            ORDER BY
                m.created_at DESC,
                m.chat_id DESC,
                m.message_id DESC,
                m.pk DESC
            LIMIT ?
            """,
            (effective_limit,),
        )
        items = []
        for row in cur.fetchall():
            chat_id = int(row["chat_id"])
            message_id = int(row["message_id"])
            content = str(row["content"] or "").strip()
            items.append(
                {
                    "pk": int(row["pk"]),
                    "chat_id": chat_id,
                    "chat_title": chat_title_or_fallback(chat_id, row["chat_title"]),
                    "chat_username": str(row["chat_username"] or ""),
                    "chat_type": str(row["chat_type"] or ""),
                    "message_id": message_id,
                    "msg_type": str(row["msg_type"] or "TEXT"),
                    "msg_date_text": str(row["msg_date_text"] or ""),
                    "created_at": str(row["created_at"] or ""),
                    "content_preview": _truncate_text(content, max_len=140),
                }
            )
    finally:
        cur.close()

    return {
        "ok": True,
        "generated_at": generated_at,
        "limit": effective_limit,
        "items": items,
    }


def build_admin_sync_stats_payload(conn: sqlite3.Connection) -> dict[str, Any]:
    windows = [dict(item) for item in _ADMIN_SYNC_WINDOWS]
    base_now = _utc_now()
    generated_at = _format_utc_text(base_now)

    if not _table_exists(conn, "messages"):
        window_rows = [_empty_admin_sync_window_payload(window) for window in windows]
        latest_window = window_rows[-1] if window_rows else {}
        return {
            "ok": True,
            "generated_at": generated_at,
            "metric_note": "按 messages.created_at 统计首次入库消息，同一消息重复更新不会重复计数。",
            "latest_message_created_at": "",
            "summary": {
                "largest_window_key": latest_window.get("window_key", ""),
                "largest_window_label": latest_window.get("label", ""),
                "largest_window_message_count": 0,
                "largest_window_chat_count": 0,
            },
            "windows": window_rows,
            "default_window_key": "live",
        }

    cur = conn.cursor()
    try:
        values_sql = ",".join(["(?, ?, ?, ?, ?)"] * len(windows))
        params: list[Any] = []
        aggregate_windows = [
            window for window in windows if not bool(window.get("is_live", False))
        ]
        values_sql = ",".join(["(?, ?, ?, ?, ?)"] * len(aggregate_windows))
        params: list[Any] = []
        for sort_order, window in enumerate(aggregate_windows, start=1):
            cutoff_text = _format_utc_text(
                base_now - timedelta(seconds=int(window["seconds"]))
            )
            params.extend(
                [
                    int(sort_order),
                    str(window["key"]),
                    str(window["label"]),
                    int(window["seconds"]),
                    cutoff_text,
                ]
            )

        aggregate_rows_by_key: dict[str, dict[str, Any]] = {}
        if aggregate_windows:
            cur.execute(
                f"""
                WITH windows(
                    sort_order,
                    window_key,
                    window_label,
                    window_seconds,
                    cutoff_at
                ) AS (
                    VALUES {values_sql}
                )
                SELECT
                    w.sort_order,
                    w.window_key,
                    w.window_label,
                    w.window_seconds,
                    COUNT(m.rowid) AS message_count,
                    COUNT(DISTINCT m.chat_id) AS chat_count,
                    COALESCE(MIN(m.created_at), '') AS oldest_created_at,
                    COALESCE(MAX(m.created_at), '') AS latest_created_at
                FROM windows w
                LEFT JOIN messages m
                  ON m.created_at >= w.cutoff_at
                GROUP BY
                    w.sort_order,
                    w.window_key,
                    w.window_label,
                    w.window_seconds
                ORDER BY w.sort_order ASC
                """,
                params,
            )
            aggregate_rows_by_key = {
                str(row["window_key"]): {
                    "window_key": str(row["window_key"]),
                    "label": str(row["window_label"]),
                    "seconds": _row_int(row, "window_seconds"),
                    "is_live": False,
                    "message_count": _row_int(row, "message_count"),
                    "chat_count": _row_int(row, "chat_count"),
                    "oldest_created_at": str(row["oldest_created_at"] or ""),
                    "latest_created_at": str(row["latest_created_at"] or ""),
                }
                for row in cur.fetchall()
            }

        cur.execute(
            "SELECT COALESCE(MAX(created_at), '') AS latest_created_at FROM messages"
        )
        latest_row = cur.fetchone()
        latest_created_at = (
            str(latest_row["latest_created_at"] or "") if latest_row else ""
        )
        cur.execute("SELECT COUNT(*) AS message_count FROM messages")
        total_message_row = cur.fetchone()
        total_message_count = _row_int(total_message_row, "message_count")
        cur.execute("SELECT COUNT(DISTINCT chat_id) AS chat_count FROM messages")
        total_chat_row = cur.fetchone()
        total_chat_count = _row_int(total_chat_row, "chat_count")
    finally:
        cur.close()

    window_rows = []
    for window in windows:
        if bool(window.get("is_live", False)):
            window_rows.append(
                {
                    "window_key": str(window["key"]),
                    "label": str(window["label"]),
                    "seconds": int(window["seconds"]),
                    "is_live": True,
                    "message_count": int(total_message_count),
                    "chat_count": int(total_chat_count),
                    "oldest_created_at": "",
                    "latest_created_at": latest_created_at,
                }
            )
            continue
        window_rows.append(
            aggregate_rows_by_key.get(
                str(window["key"]),
                _empty_admin_sync_window_payload(window),
            )
        )

    latest_window = window_rows[-1] if window_rows else {}
    return {
        "ok": True,
        "generated_at": generated_at,
        "metric_note": "按 messages.created_at 统计首次入库消息，同一消息重复更新不会重复计数。",
        "latest_message_created_at": latest_created_at,
        "summary": {
            "largest_window_key": latest_window.get("window_key", ""),
            "largest_window_label": latest_window.get("label", ""),
            "largest_window_message_count": int(
                latest_window.get("message_count") or 0
            ),
            "largest_window_chat_count": int(latest_window.get("chat_count") or 0),
        },
        "windows": window_rows,
        "default_window_key": "live",
    }


def build_admin_chats_payload(conn: sqlite3.Connection) -> dict[str, Any]:
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
        chats = []
        for row in cur.fetchall():
            chat_id = int(row["chat_id"])
            chats.append(
                {
                    "chat_id": chat_id,
                    "chat_title": chat_title_or_fallback(chat_id, row["chat_title"]),
                    "message_count": _row_int(row, "message_count"),
                }
            )
        chats.sort(
            key=lambda item: chat_sort_key(
                str(item.get("chat_title") or ""),
                safe_int(item.get("chat_id")),
            )
        )
        return {"ok": True, "chats": chats}
    finally:
        cur.close()


def parse_admin_chat_id(raw_chat_id: str | None) -> int | None:
    value = (raw_chat_id or "").strip()
    if not value or value.lower() == "none":
        return None
    return int(value)


def build_admin_stats_payload(
    conn: sqlite3.Connection, chat_id: int | None
) -> tuple[dict[str, Any], int]:
    cur = conn.cursor()
    try:
        if chat_id is None:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS chat_count,
                    COALESCE(SUM(message_count), 0) AS message_count
                FROM chats
                """
            )
            row = cur.fetchone()
            chat_count = _row_int(row, "chat_count")
            message_count = _row_int(row, "message_count")

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

        actual_chat_id = int(row["chat_id"])
        return {
            "ok": True,
            "scope": "chat",
            "chat_id": actual_chat_id,
            "chat_title": chat_title_or_fallback(actual_chat_id, row["chat_title"]),
            "message_count": _row_int(row, "message_count"),
        }, 200
    finally:
        cur.close()


def get_admin_chat_brief(
    conn: sqlite3.Connection, chat_id: int
) -> dict[str, Any] | None:
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
            "chat_title": chat_title_or_fallback(actual_chat_id, row["chat_title"]),
        }
    finally:
        cur.close()
