import sqlite3
from typing import Any

from tg_harvest.domain.chat_titles import chat_sort_key
from tg_harvest.storage.clone_common import (
    _chat_title_or_fallback,
    _default_clone_title,
    _optional_int,
    _percent,
)
from tg_harvest.storage.row_access import row_int as _row_int


def _fetch_clone_source_chat(conn: sqlite3.Connection, chat_id: int) -> dict | None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                c.chat_id,
                c.chat_title,
                c.chat_username,
                c.chat_type,
                c.message_count,
                c.first_seen_at,
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
            WHERE c.chat_id = ?
            LIMIT 1
            """,
            (int(chat_id),),
        )
        row = cur.fetchone()
        if row is None:
            return None
        normalized_chat_id = _row_int(row, "chat_id")
        return {
            "chat_id": normalized_chat_id,
            "chat_title": _chat_title_or_fallback(
                normalized_chat_id,
                row["chat_title"],
            ),
            "chat_username": str(row["chat_username"] or ""),
            "chat_type": str(row["chat_type"] or ""),
            "message_count": _row_int(row, "message_count"),
            "first_seen_at": str(row["first_seen_at"] or ""),
            "last_seen_at": str(row["last_seen_at"] or ""),
            "last_message_at": str(row["last_message_at"] or ""),
            "last_message_ts": _optional_int(row["last_message_ts"]),
        }
    finally:
        cur.close()


def list_clone_source_chats(
    conn: sqlite3.Connection,
    *,
    sort: Any = "message_count_desc",
) -> list[dict]:
    normalized_sort = str(sort or "message_count_desc").strip().lower()
    order_sql = {
        "title_asc": "c.chat_id ASC",
        "message_count_asc": (
            "c.message_count ASC, c.chat_title COLLATE NOCASE ASC, c.chat_id ASC"
        ),
        "updated_desc": (
            "CASE WHEN last_message_ts IS NULL THEN 1 ELSE 0 END ASC, "
            "last_message_ts DESC, c.chat_title COLLATE NOCASE ASC, c.chat_id ASC"
        ),
        "updated_asc": (
            "CASE WHEN last_message_ts IS NULL THEN 1 ELSE 0 END ASC, "
            "last_message_ts ASC, c.chat_title COLLATE NOCASE ASC, c.chat_id ASC"
        ),
    }.get(
        normalized_sort,
        "c.message_count DESC, c.chat_title COLLATE NOCASE ASC, c.chat_id ASC",
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
                lm.msg_date_text AS last_message_at,
                lm.msg_date_ts AS last_message_ts,
                COALESCE(mm.media_rows, 0) AS media_rows
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
            LEFT JOIN (
                SELECT chat_id, COUNT(*) AS media_rows
                FROM message_media
                GROUP BY chat_id
            ) mm ON mm.chat_id = c.chat_id
            ORDER BY {order_sql}
            """
        )
        items: list[dict] = []
        for row in cur.fetchall():
            chat_id = _row_int(row, "chat_id")
            items.append(
                {
                    "chat_id": chat_id,
                    "chat_title": _chat_title_or_fallback(chat_id, row["chat_title"]),
                    "chat_username": str(row["chat_username"] or ""),
                    "chat_type": str(row["chat_type"] or ""),
                    "message_count": _row_int(row, "message_count"),
                    "media_rows": _row_int(row, "media_rows"),
                    "last_seen_at": str(row["last_seen_at"] or ""),
                    "last_message_at": str(row["last_message_at"] or ""),
                    "last_message_ts": _optional_int(row["last_message_ts"]),
                }
            )
        if normalized_sort == "title_asc":
            items.sort(
                key=lambda item: chat_sort_key(
                    item.get("chat_title"),
                    int(item.get("chat_id") or 0),
                )
            )
        return items
    finally:
        cur.close()


def _message_metrics(conn: sqlite3.Connection, chat_id: int) -> dict[str, int]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                COUNT(*) AS total_messages,
                SUM(
                    CASE WHEN COALESCE(
                        NULLIF(TRIM(content_norm), ''),
                        NULLIF(TRIM(content), ''),
                        ''
                    ) <> '' THEN 1 ELSE 0 END
                ) AS text_messages,
                SUM(CASE WHEN has_media = 1 THEN 1 ELSE 0 END) AS media_messages,
                SUM(CASE WHEN grouped_id IS NOT NULL THEN 1 ELSE 0 END)
                    AS grouped_messages,
                SUM(
                    CASE WHEN COALESCE(
                        NULLIF(TRIM(content_norm), ''),
                        NULLIF(TRIM(content), ''),
                        ''
                    ) = '' THEN 1 ELSE 0 END
                ) AS empty_text_messages
            FROM messages
            WHERE chat_id = ?
            """,
            (int(chat_id),),
        )
        row = cur.fetchone()
        if row is None:
            return {
                "total_messages": 0,
                "text_messages": 0,
                "media_messages": 0,
                "grouped_messages": 0,
                "empty_text_messages": 0,
            }
        return {
            "total_messages": _row_int(row, "total_messages"),
            "text_messages": _row_int(row, "text_messages"),
            "media_messages": _row_int(row, "media_messages"),
            "grouped_messages": _row_int(row, "grouped_messages"),
            "empty_text_messages": _row_int(row, "empty_text_messages"),
        }
    finally:
        cur.close()


def _media_metrics(conn: sqlite3.Connection, chat_id: int) -> dict[str, int]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                COUNT(*) AS media_metadata_rows,
                SUM(CASE WHEN COALESCE(NULLIF(TRIM(file_name), ''), '') <> ''
                    THEN 1 ELSE 0 END) AS named_media_rows,
                SUM(CASE WHEN COALESCE(NULLIF(TRIM(media_fingerprint), ''), '') <> ''
                    THEN 1 ELSE 0 END) AS fingerprinted_media_rows
            FROM message_media
            WHERE chat_id = ?
            """,
            (int(chat_id),),
        )
        row = cur.fetchone()
        return {
            "media_metadata_rows": _row_int(row, "media_metadata_rows"),
            "named_media_rows": _row_int(row, "named_media_rows"),
            "fingerprinted_media_rows": _row_int(row, "fingerprinted_media_rows"),
        }
    finally:
        cur.close()


def _media_group_metrics(conn: sqlite3.Connection, chat_id: int) -> dict[str, int]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                grouped.grouped_id,
                grouped.message_count,
                grouped.media_meta_count,
                COALESCE(mg.item_count, 0) AS recorded_item_count,
                COALESCE(mg.active_items, 0) AS recorded_active_items
            FROM (
                SELECT
                    m.grouped_id,
                    COUNT(*) AS message_count,
                    SUM(CASE WHEN mm.message_id IS NOT NULL THEN 1 ELSE 0 END)
                        AS media_meta_count
                FROM messages m
                LEFT JOIN message_media mm
                  ON mm.chat_id = m.chat_id
                 AND mm.message_id = m.message_id
                WHERE m.chat_id = ?
                  AND m.grouped_id IS NOT NULL
                GROUP BY m.grouped_id
            ) grouped
            LEFT JOIN media_groups mg
              ON mg.chat_id = ?
             AND mg.grouped_id = grouped.grouped_id
            """,
            (int(chat_id), int(chat_id)),
        )
        total_groups = 0
        suspect_groups = 0
        complete_groups = 0
        single_item_groups = 0
        metadata_incomplete_groups = 0
        recorded_larger_groups = 0

        for row in cur.fetchall():
            total_groups += 1
            message_count = _row_int(row, "message_count")
            media_meta_count = _row_int(row, "media_meta_count")
            recorded_item_count = _row_int(row, "recorded_item_count")
            recorded_active_items = _row_int(row, "recorded_active_items")
            is_single_item = message_count <= 1
            metadata_incomplete = media_meta_count < message_count
            recorded_larger = (
                recorded_item_count > message_count
                or recorded_active_items > message_count
            )
            if is_single_item:
                single_item_groups += 1
            if metadata_incomplete:
                metadata_incomplete_groups += 1
            if recorded_larger:
                recorded_larger_groups += 1
            if is_single_item or metadata_incomplete or recorded_larger:
                suspect_groups += 1
            else:
                complete_groups += 1

        return {
            "media_group_count": total_groups,
            "complete_media_group_count": complete_groups,
            "suspect_media_group_count": suspect_groups,
            "single_item_media_group_count": single_item_groups,
            "metadata_incomplete_media_group_count": metadata_incomplete_groups,
            "recorded_larger_media_group_count": recorded_larger_groups,
        }
    finally:
        cur.close()


def _account_assessment(cfg: Any) -> dict[str, Any]:
    primary_session_name = str(getattr(cfg, "session_name", "") or "").strip()
    secondary_session_name = str(
        getattr(cfg, "secondary_session_name", "") or ""
    ).strip()
    secondary_distinct = bool(
        secondary_session_name and secondary_session_name != primary_session_name
    )
    return {
        "primary_session_configured": bool(primary_session_name),
        "secondary_session_configured": bool(secondary_session_name),
        "secondary_session_distinct": secondary_distinct,
        "target_owner_account": "secondary" if secondary_distinct else "",
        "network_access_checked": False,
        "network_access_note": "第一版预检只评估本地数据库和配置；源群读取/转发能力将在后续媒体迁移阶段检测。",
    }


def _build_capabilities(
    *,
    total_messages: int,
    text_messages: int,
    media_messages: int,
    secondary_ready: bool,
) -> list[dict[str, str]]:
    return [
        {
            "key": "structure_clone",
            "label": "结构克隆",
            "status": "ready" if secondary_ready else "blocked",
            "detail": (
                "第二账号已配置，可创建频道/超级群结构副本。"
                if secondary_ready
                else "需要配置与主账号不同的 TG_SECONDARY_SESSION_NAME。"
            ),
        },
        {
            "key": "text_rebuild",
            "label": "文本重建",
            "status": "ready" if text_messages > 0 else "empty",
            "detail": f"数据库中可用于后续重发的文本消息约 {text_messages} 条。",
        },
        {
            "key": "media_recovery",
            "label": "媒体恢复",
            "status": "requires_source",
            "detail": (
                f"数据库中有媒体消息约 {media_messages} 条；第一版不迁移媒体，后续必须依赖源群转发或本地媒体保险库。"
            ),
        },
        {
            "key": "history_clone",
            "label": "历史内容克隆",
            "status": "deferred" if total_messages > 0 else "empty",
            "detail": "第一版仅执行结构克隆，历史迁移将在文本/媒体迁移阶段启用。",
        },
    ]


def _build_warnings(
    *,
    chat: dict,
    message_metrics: dict[str, int],
    media_metrics: dict[str, int],
    group_metrics: dict[str, int],
    secondary_ready: bool,
) -> list[str]:
    warnings: list[str] = []
    if not secondary_ready:
        warnings.append("未配置独立第二账号，无法由第二账号创建克隆副本。")
    if message_metrics["total_messages"] <= 0:
        warnings.append("该群组数据库中没有消息，第一版只能创建空结构副本。")
    if media_metrics["media_metadata_rows"] < message_metrics["media_messages"]:
        warnings.append("部分媒体消息缺少 message_media 元信息，后续媒体迁移会降级。")
    if group_metrics["suspect_media_group_count"] > 0:
        warnings.append(
            "检测到疑似残缺媒体组；后续媒体迁移需要按组降级处理。"
        )
    if not str(chat.get("chat_username") or "").strip():
        warnings.append("源群组没有公开 username，后续源群访问更依赖账号本地实体缓存。")
    warnings.append("第一版不复制历史消息、成员、原发布时间、反应、阅读数、评论和媒体文件本体。")
    return warnings


def _recommendation(
    *,
    total_messages: int,
    media_messages: int,
    media_metadata_rows: int,
    suspect_media_group_count: int,
    media_group_count: int,
    secondary_ready: bool,
) -> dict[str, Any]:
    if not secondary_ready:
        return {
            "level": "D",
            "mode": "blocked",
            "summary": "先配置独立第二账号后再执行结构克隆。",
        }
    if total_messages <= 0:
        return {
            "level": "C",
            "mode": "structure_only",
            "summary": "可创建结构副本；数据库中没有可迁移历史消息。",
        }

    metadata_coverage = _percent(media_metadata_rows, media_messages)
    suspect_ratio = _percent(suspect_media_group_count, media_group_count)
    if media_messages > 0 and (metadata_coverage < 50 or suspect_ratio >= 50):
        return {
            "level": "C",
            "mode": "structure_then_text",
            "summary": "建议先做结构克隆；媒体组残缺或媒体元信息不足，内容迁移需后续谨慎分批。",
        }
    if media_messages > 0:
        return {
            "level": "B",
            "mode": "structure_then_text_then_source_media",
            "summary": "建议先结构克隆，后续文本重建；媒体需要源群转发或本地媒体缓存。",
        }
    return {
        "level": "B",
        "mode": "structure_then_text",
        "summary": "该群以文本为主，适合先结构克隆，再进入文本重建阶段。",
    }


def build_clone_preflight_report(
    conn: sqlite3.Connection,
    *,
    chat_id: int,
    cfg: Any,
) -> dict[str, Any]:
    normalized_chat_id = int(chat_id)
    chat = _fetch_clone_source_chat(conn, normalized_chat_id)
    if chat is None:
        raise ValueError("chat_id 不存在")

    message_metrics = _message_metrics(conn, normalized_chat_id)
    media_metrics = _media_metrics(conn, normalized_chat_id)
    group_metrics = _media_group_metrics(conn, normalized_chat_id)
    account = _account_assessment(cfg)
    secondary_ready = bool(account["secondary_session_distinct"])
    media_messages = message_metrics["media_messages"]
    media_metadata_rows = media_metrics["media_metadata_rows"]
    media_group_count = group_metrics["media_group_count"]
    suspect_media_group_count = group_metrics["suspect_media_group_count"]

    metrics = {
        **message_metrics,
        **media_metrics,
        **group_metrics,
        "media_metadata_coverage_percent": _percent(
            media_metadata_rows,
            media_messages,
        ),
        "suspect_media_group_ratio_percent": _percent(
            suspect_media_group_count,
            media_group_count,
        ),
        "text_rebuild_coverage_percent": _percent(
            message_metrics["text_messages"],
            message_metrics["total_messages"],
        ),
    }
    recommendation = _recommendation(
        total_messages=message_metrics["total_messages"],
        media_messages=media_messages,
        media_metadata_rows=media_metadata_rows,
        suspect_media_group_count=suspect_media_group_count,
        media_group_count=media_group_count,
        secondary_ready=secondary_ready,
    )
    return {
        "source": chat,
        "target": {
            "default_title": _default_clone_title(str(chat["chat_title"])),
            "supported_kinds": ["channel", "megagroup"],
        },
        "metrics": metrics,
        "account": account,
        "capabilities": _build_capabilities(
            total_messages=message_metrics["total_messages"],
            text_messages=message_metrics["text_messages"],
            media_messages=media_messages,
            secondary_ready=secondary_ready,
        ),
        "warnings": _build_warnings(
            chat=chat,
            message_metrics=message_metrics,
            media_metrics=media_metrics,
            group_metrics=group_metrics,
            secondary_ready=secondary_ready,
        ),
        "recommendation": recommendation,
        "confirm": f"CLONE:STRUCTURE:{normalized_chat_id}",
    }


def load_clone_source_chat(conn: sqlite3.Connection, chat_id: int) -> dict:
    chat = _fetch_clone_source_chat(conn, int(chat_id))
    if chat is None:
        raise ValueError("chat_id 不存在")
    return chat
