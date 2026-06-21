import json
import sqlite3
from datetime import UTC, datetime
from typing import Any

from tg_harvest.domain.chat_titles import chat_sort_key
from tg_harvest.domain.clone_plan import CLONE_TEXT_REPLAY_CHUNK_MAX_LEN


def _chat_title_or_fallback(chat_id: int, chat_title: Any) -> str:
    title = str(chat_title or "").strip()
    return title if title else f"Chat {chat_id}"


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _percent(part: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(max(0.0, min(100.0, float(part) * 100.0 / float(total))), 1)


def _default_clone_title(chat_title: str) -> str:
    base = str(chat_title or "").strip() or "未命名群组"
    suffix = " 副本"
    max_len = 128
    if len(base) + len(suffix) <= max_len:
        return base + suffix
    return base[: max_len - len(suffix)].rstrip() + suffix


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


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
        normalized_chat_id = int(row["chat_id"])
        return {
            "chat_id": normalized_chat_id,
            "chat_title": _chat_title_or_fallback(
                normalized_chat_id,
                row["chat_title"],
            ),
            "chat_username": str(row["chat_username"] or ""),
            "chat_type": str(row["chat_type"] or ""),
            "message_count": int(row["message_count"] or 0),
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
            chat_id = int(row["chat_id"])
            items.append(
                {
                    "chat_id": chat_id,
                    "chat_title": _chat_title_or_fallback(chat_id, row["chat_title"]),
                    "chat_username": str(row["chat_username"] or ""),
                    "chat_type": str(row["chat_type"] or ""),
                    "message_count": int(row["message_count"] or 0),
                    "media_rows": int(row["media_rows"] or 0),
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
            "total_messages": int(row["total_messages"] or 0),
            "text_messages": int(row["text_messages"] or 0),
            "media_messages": int(row["media_messages"] or 0),
            "grouped_messages": int(row["grouped_messages"] or 0),
            "empty_text_messages": int(row["empty_text_messages"] or 0),
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
            "media_metadata_rows": int(row["media_metadata_rows"] or 0),
            "named_media_rows": int(row["named_media_rows"] or 0),
            "fingerprinted_media_rows": int(row["fingerprinted_media_rows"] or 0),
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
            message_count = int(row["message_count"] or 0)
            media_meta_count = int(row["media_meta_count"] or 0)
            recorded_item_count = int(row["recorded_item_count"] or 0)
            recorded_active_items = int(row["recorded_active_items"] or 0)
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


def _normalize_plan_json(plan: Any) -> str:
    if plan is None:
        return ""
    if isinstance(plan, str):
        return plan
    return json.dumps(plan, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _json_text(value: Any, *, default: str) -> str:
    if value is None:
        return default
    if isinstance(value, str):
        normalized = value.strip()
        return normalized if normalized else default
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _json_value(value: Any, *, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return default
    return value


def _clone_run_from_row(row: sqlite3.Row | None) -> dict | None:
    if row is None:
        return None
    return {
        "run_id": str(row["run_id"] or ""),
        "job_id": str(row["job_id"] or ""),
        "source_chat_id": int(row["source_chat_id"] or 0),
        "source_title": str(row["source_title"] or ""),
        "source_chat_username": str(row["source_chat_username"] or ""),
        "source_chat_type": str(row["source_chat_type"] or ""),
        "source_message_count": int(row["source_message_count"] or 0),
        "source_last_message_at": str(row["source_last_message_at"] or ""),
        "source_last_message_ts": _optional_int(row["source_last_message_ts"]),
        "target_chat_id": _optional_int(row["target_chat_id"]),
        "target_access_hash": str(row["target_access_hash"] or ""),
        "target_title": str(row["target_title"] or ""),
        "target_kind": str(row["target_kind"] or ""),
        "target_username": str(row["target_username"] or ""),
        "target_owner_session": str(row["target_owner_session"] or ""),
        "phase": str(row["phase"] or ""),
        "status": str(row["status"] or ""),
        "plan_json": str(row["plan_json"] or ""),
        "error_message": str(row["error_message"] or ""),
        "target_created_at": str(row["target_created_at"] or ""),
        "completed_at": str(row["completed_at"] or ""),
        "created_at": str(row["created_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
    }


def _clone_plan_from_row(row: sqlite3.Row | None) -> dict | None:
    if row is None:
        return None
    return {
        "plan_id": str(row["plan_id"] or ""),
        "run_id": str(row["run_id"] or ""),
        "job_id": str(row["job_id"] or ""),
        "status": str(row["status"] or ""),
        "source_access": str(row["source_access"] or ""),
        "target_access": str(row["target_access"] or ""),
        "primary_session_status": str(row["primary_session_status"] or ""),
        "secondary_session_status": str(row["secondary_session_status"] or ""),
        "migration_account": str(row["migration_account"] or ""),
        "text_strategy": str(row["text_strategy"] or ""),
        "media_strategy": str(row["media_strategy"] or ""),
        "media_group_strategy": str(row["media_group_strategy"] or ""),
        "avatar_strategy": str(row["avatar_strategy"] or ""),
        "blocking_issues": _json_value(row["blocking_issues_json"], default=[]),
        "warnings": _json_value(row["warnings_json"], default=[]),
        "capabilities": _json_value(row["capabilities_json"], default={}),
        "plan": _json_value(row["plan_json"], default={}),
        "blocking_issues_json": str(row["blocking_issues_json"] or ""),
        "warnings_json": str(row["warnings_json"] or ""),
        "capabilities_json": str(row["capabilities_json"] or ""),
        "plan_json": str(row["plan_json"] or ""),
        "error_message": str(row["error_message"] or ""),
        "created_at": str(row["created_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
        "completed_at": str(row["completed_at"] or ""),
    }


def _clone_migration_from_row(row: sqlite3.Row | None) -> dict | None:
    if row is None:
        return None
    return {
        "migration_id": str(row["migration_id"] or ""),
        "run_id": str(row["run_id"] or ""),
        "plan_id": str(row["plan_id"] or ""),
        "job_id": str(row["job_id"] or ""),
        "mode": str(row["mode"] or ""),
        "status": str(row["status"] or ""),
        "phase": str(row["phase"] or ""),
        "target_chat_id": _optional_int(row["target_chat_id"]),
        "target_title": str(row["target_title"] or ""),
        "target_write_account": str(row["target_write_account"] or ""),
        "requested_limit": int(row["requested_limit"] or 0),
        "send_delay_ms": int(row["send_delay_ms"] or 0),
        "text_total": int(row["text_total"] or 0),
        "text_sent": int(row["text_sent"] or 0),
        "text_skipped": int(row["text_skipped"] or 0),
        "text_failed": int(row["text_failed"] or 0),
        "media_total": int(row["media_total"] or 0),
        "media_sent": int(row["media_sent"] or 0),
        "media_skipped": int(row["media_skipped"] or 0),
        "media_failed": int(row["media_failed"] or 0),
        "media_group_total": int(row["media_group_total"] or 0),
        "media_group_sent": int(row["media_group_sent"] or 0),
        "media_group_skipped": int(row["media_group_skipped"] or 0),
        "media_group_failed": int(row["media_group_failed"] or 0),
        "plan": _json_value(row["plan_json"], default={}),
        "plan_json": str(row["plan_json"] or ""),
        "error_message": str(row["error_message"] or ""),
        "created_at": str(row["created_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
        "completed_at": str(row["completed_at"] or ""),
    }


def _clone_message_mapping_from_row(row: sqlite3.Row | None) -> dict | None:
    if row is None:
        return None
    return {
        "id": int(row["id"] or 0),
        "migration_id": str(row["migration_id"] or ""),
        "run_id": str(row["run_id"] or ""),
        "plan_id": str(row["plan_id"] or ""),
        "source_chat_id": int(row["source_chat_id"] or 0),
        "source_message_id": int(row["source_message_id"] or 0),
        "source_msg_date_ts": _optional_int(row["source_msg_date_ts"]),
        "source_msg_date_text": str(row["source_msg_date_text"] or ""),
        "target_chat_id": int(row["target_chat_id"] or 0),
        "target_message_id": _optional_int(row["target_message_id"]),
        "chunk_index": int(row["chunk_index"] or 0),
        "chunk_count": int(row["chunk_count"] or 1),
        "mode": str(row["mode"] or ""),
        "status": str(row["status"] or ""),
        "error_message": str(row["error_message"] or ""),
        "sent_at": str(row["sent_at"] or ""),
        "created_at": str(row["created_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
    }


def _clone_message_mapping_summary_from_row(row: sqlite3.Row | None) -> dict:
    if row is None:
        return {
            "total": 0,
            "done": 0,
            "error": 0,
            "text_total": 0,
            "text_done": 0,
            "text_error": 0,
            "media_total": 0,
            "media_done": 0,
            "media_error": 0,
            "media_group_total": 0,
            "media_group_done": 0,
            "media_group_error": 0,
            "latest_sent_at": "",
            "latest_updated_at": "",
        }
    return {
        "total": int(row["total"] or 0),
        "done": int(row["done"] or 0),
        "error": int(row["error"] or 0),
        "text_total": int(row["text_total"] or 0),
        "text_done": int(row["text_done"] or 0),
        "text_error": int(row["text_error"] or 0),
        "media_total": int(row["media_total"] or 0),
        "media_done": int(row["media_done"] or 0),
        "media_error": int(row["media_error"] or 0),
        "media_group_total": int(row["media_group_total"] or 0),
        "media_group_done": int(row["media_group_done"] or 0),
        "media_group_error": int(row["media_group_error"] or 0),
        "latest_sent_at": str(row["latest_sent_at"] or ""),
        "latest_updated_at": str(row["latest_updated_at"] or ""),
    }


def create_clone_run(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    job_id: str,
    source_chat: dict[str, Any],
    target_title: str,
    target_kind: str,
    target_owner_session: str,
    plan: Any = None,
) -> dict:
    normalized_run_id = _clean_text(run_id)
    normalized_job_id = _clean_text(job_id)
    if not normalized_run_id:
        raise ValueError("run_id 不能为空")
    if not normalized_job_id:
        raise ValueError("job_id 不能为空")

    source_chat_id = int(source_chat.get("chat_id") or 0)
    if source_chat_id == 0:
        raise ValueError("source_chat.chat_id 参数非法")

    now = _now_iso()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO admin_clone_runs(
                run_id,
                job_id,
                source_chat_id,
                source_title,
                source_chat_username,
                source_chat_type,
                source_message_count,
                source_last_message_at,
                source_last_message_ts,
                target_title,
                target_kind,
                target_owner_session,
                phase,
                status,
                plan_json,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized_run_id,
                normalized_job_id,
                source_chat_id,
                _chat_title_or_fallback(source_chat_id, source_chat.get("chat_title")),
                _clean_text(source_chat.get("chat_username")),
                _clean_text(source_chat.get("chat_type")),
                int(source_chat.get("message_count") or 0),
                _clean_text(source_chat.get("last_message_at")),
                _optional_int(source_chat.get("last_message_ts")),
                _clean_text(target_title) or _default_clone_title(
                    _clean_text(source_chat.get("chat_title"))
                ),
                _clean_text(target_kind) or "channel",
                _clean_text(target_owner_session),
                "queued",
                "queued",
                _normalize_plan_json(plan),
                now,
                now,
            ),
        )
        conn.commit()
        created = load_clone_run(conn, normalized_run_id)
        if created is None:
            raise RuntimeError("clone run 创建后读取失败")
        return created
    finally:
        cur.close()


def create_clone_plan(
    conn: sqlite3.Connection,
    *,
    plan_id: str,
    run_id: str,
    job_id: str = "",
    status: str = "queued",
    source_access: str = "unknown",
    target_access: str = "unknown",
    primary_session_status: str = "unknown",
    secondary_session_status: str = "unknown",
    migration_account: str = "",
    text_strategy: str = "",
    media_strategy: str = "",
    media_group_strategy: str = "",
    avatar_strategy: str = "",
    blocking_issues: Any = None,
    warnings: Any = None,
    capabilities: Any = None,
    plan: Any = None,
    error_message: Any = None,
    completed_at: Any = None,
) -> dict:
    normalized_plan_id = _clean_text(plan_id)
    normalized_run_id = _clean_text(run_id)
    if not normalized_plan_id:
        raise ValueError("plan_id 不能为空")
    if not normalized_run_id:
        raise ValueError("run_id 不能为空")

    now = _now_iso()
    final_completed_at = _clean_text(completed_at) if completed_at is not None else None
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO admin_clone_plans(
                plan_id,
                run_id,
                job_id,
                status,
                source_access,
                target_access,
                primary_session_status,
                secondary_session_status,
                migration_account,
                text_strategy,
                media_strategy,
                media_group_strategy,
                avatar_strategy,
                blocking_issues_json,
                warnings_json,
                capabilities_json,
                plan_json,
                error_message,
                created_at,
                updated_at,
                completed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized_plan_id,
                normalized_run_id,
                _clean_text(job_id),
                _clean_text(status) or "queued",
                _clean_text(source_access) or "unknown",
                _clean_text(target_access) or "unknown",
                _clean_text(primary_session_status) or "unknown",
                _clean_text(secondary_session_status) or "unknown",
                _clean_text(migration_account),
                _clean_text(text_strategy),
                _clean_text(media_strategy),
                _clean_text(media_group_strategy),
                _clean_text(avatar_strategy),
                _json_text(blocking_issues, default="[]"),
                _json_text(warnings, default="[]"),
                _json_text(capabilities, default="{}"),
                _json_text(plan, default="{}"),
                _clean_text(error_message) if error_message is not None else "",
                now,
                now,
                final_completed_at,
            ),
        )
        conn.commit()
        created = load_clone_plan(conn, normalized_plan_id)
        if created is None:
            raise RuntimeError("clone plan 创建后读取失败")
        return created
    finally:
        cur.close()


def load_clone_run(conn: sqlite3.Connection, run_id: str) -> dict | None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT *
            FROM admin_clone_runs
            WHERE run_id = ?
            LIMIT 1
            """,
            (_clean_text(run_id),),
        )
        return _clone_run_from_row(cur.fetchone())
    finally:
        cur.close()


def update_clone_plan(
    conn: sqlite3.Connection,
    *,
    plan_id: str,
    status: str | None = None,
    source_access: str | None = None,
    target_access: str | None = None,
    primary_session_status: str | None = None,
    secondary_session_status: str | None = None,
    migration_account: str | None = None,
    text_strategy: str | None = None,
    media_strategy: str | None = None,
    media_group_strategy: str | None = None,
    avatar_strategy: str | None = None,
    blocking_issues: Any = None,
    warnings: Any = None,
    capabilities: Any = None,
    plan: Any = None,
    error_message: Any = None,
    completed_at: Any = None,
) -> dict | None:
    fields: list[str] = ["updated_at = ?"]
    values: list[Any] = [_now_iso()]

    optional_fields = {
        "status": _clean_text(status) if status is not None else None,
        "source_access": _clean_text(source_access) if source_access is not None else None,
        "target_access": _clean_text(target_access) if target_access is not None else None,
        "primary_session_status": _clean_text(primary_session_status)
        if primary_session_status is not None
        else None,
        "secondary_session_status": _clean_text(secondary_session_status)
        if secondary_session_status is not None
        else None,
        "migration_account": _clean_text(migration_account)
        if migration_account is not None
        else None,
        "text_strategy": _clean_text(text_strategy) if text_strategy is not None else None,
        "media_strategy": _clean_text(media_strategy) if media_strategy is not None else None,
        "media_group_strategy": _clean_text(media_group_strategy)
        if media_group_strategy is not None
        else None,
        "avatar_strategy": _clean_text(avatar_strategy)
        if avatar_strategy is not None
        else None,
        "blocking_issues_json": _json_text(blocking_issues, default="[]")
        if blocking_issues is not None
        else None,
        "warnings_json": _json_text(warnings, default="[]") if warnings is not None else None,
        "capabilities_json": _json_text(capabilities, default="{}")
        if capabilities is not None
        else None,
        "plan_json": _json_text(plan, default="{}") if plan is not None else None,
        "error_message": _clean_text(error_message) if error_message is not None else None,
        "completed_at": _clean_text(completed_at) if completed_at is not None else None,
    }
    for column_name, value in optional_fields.items():
        if value is None:
            continue
        fields.append(f"{column_name} = ?")
        values.append(value)

    values.append(_clean_text(plan_id))
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            UPDATE admin_clone_plans
            SET {", ".join(fields)}
            WHERE plan_id = ?
            """,
            values,
        )
        if cur.rowcount <= 0:
            conn.commit()
            return None
        conn.commit()
        return load_clone_plan(conn, _clean_text(plan_id))
    finally:
        cur.close()


def load_clone_plan(conn: sqlite3.Connection, plan_id: str) -> dict | None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT *
            FROM admin_clone_plans
            WHERE plan_id = ?
            LIMIT 1
            """,
            (_clean_text(plan_id),),
        )
        return _clone_plan_from_row(cur.fetchone())
    finally:
        cur.close()


def load_latest_clone_plan(conn: sqlite3.Connection, run_id: str) -> dict | None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT *
            FROM admin_clone_plans
            WHERE run_id = ?
            ORDER BY updated_at DESC, created_at DESC, plan_id DESC
            LIMIT 1
            """,
            (_clean_text(run_id),),
        )
        return _clone_plan_from_row(cur.fetchone())
    finally:
        cur.close()


def list_clone_plans(
    conn: sqlite3.Connection,
    *,
    run_id: Any = None,
    limit: Any = 20,
) -> list[dict]:
    try:
        normalized_limit = int(limit)
    except (TypeError, ValueError):
        normalized_limit = 20
    normalized_limit = max(1, min(100, normalized_limit))

    params: list[Any] = []
    where_sql = ""
    if run_id not in (None, ""):
        where_sql = "WHERE run_id = ?"
        params.append(_clean_text(run_id))
    params.append(normalized_limit)

    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT *
            FROM admin_clone_plans
            {where_sql}
            ORDER BY updated_at DESC, created_at DESC, plan_id DESC
            LIMIT ?
            """,
            params,
        )
        return [
            plan
            for plan in (_clone_plan_from_row(row) for row in cur.fetchall())
            if plan is not None
        ]
    finally:
        cur.close()


def create_clone_migration(
    conn: sqlite3.Connection,
    *,
    migration_id: str,
    run_id: str,
    plan_id: str = "",
    job_id: str = "",
    mode: str = "text_replay",
    status: str = "queued",
    phase: str = "queued",
    target_chat_id: Any = None,
    target_title: Any = None,
    target_write_account: Any = None,
    requested_limit: Any = 0,
    send_delay_ms: Any = 0,
    text_total: Any = 0,
    text_sent: Any = 0,
    text_skipped: Any = 0,
    text_failed: Any = 0,
    media_total: Any = 0,
    media_sent: Any = 0,
    media_skipped: Any = 0,
    media_failed: Any = 0,
    media_group_total: Any = 0,
    media_group_sent: Any = 0,
    media_group_skipped: Any = 0,
    media_group_failed: Any = 0,
    plan: Any = None,
    error_message: Any = None,
    completed_at: Any = None,
) -> dict:
    normalized_migration_id = _clean_text(migration_id)
    normalized_run_id = _clean_text(run_id)
    if not normalized_migration_id:
        raise ValueError("migration_id 不能为空")
    if not normalized_run_id:
        raise ValueError("run_id 不能为空")

    now = _now_iso()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO admin_clone_migrations(
                migration_id,
                run_id,
                plan_id,
                job_id,
                mode,
                status,
                phase,
                target_chat_id,
                target_title,
                target_write_account,
                requested_limit,
                send_delay_ms,
                text_total,
                text_sent,
                text_skipped,
                text_failed,
                media_total,
                media_sent,
                media_skipped,
                media_failed,
                media_group_total,
                media_group_sent,
                media_group_skipped,
                media_group_failed,
                plan_json,
                error_message,
                created_at,
                updated_at,
                completed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized_migration_id,
                normalized_run_id,
                _clean_text(plan_id),
                _clean_text(job_id),
                _clean_text(mode) or "text_replay",
                _clean_text(status) or "queued",
                _clean_text(phase) or "queued",
                _optional_int(target_chat_id),
                _clean_text(target_title),
                _clean_text(target_write_account),
                int(requested_limit or 0),
                int(send_delay_ms or 0),
                int(text_total or 0),
                int(text_sent or 0),
                int(text_skipped or 0),
                int(text_failed or 0),
                int(media_total or 0),
                int(media_sent or 0),
                int(media_skipped or 0),
                int(media_failed or 0),
                int(media_group_total or 0),
                int(media_group_sent or 0),
                int(media_group_skipped or 0),
                int(media_group_failed or 0),
                _json_text(plan, default="{}"),
                _clean_text(error_message) if error_message is not None else "",
                now,
                now,
                _clean_text(completed_at) if completed_at is not None else None,
            ),
        )
        conn.commit()
        created = load_clone_migration(conn, normalized_migration_id)
        if created is None:
            raise RuntimeError("clone migration 创建后读取失败")
        return created
    finally:
        cur.close()


def update_clone_migration(
    conn: sqlite3.Connection,
    *,
    migration_id: str,
    status: str | None = None,
    phase: str | None = None,
    target_chat_id: Any = None,
    target_title: Any = None,
    target_write_account: Any = None,
    requested_limit: Any = None,
    send_delay_ms: Any = None,
    text_total: Any = None,
    text_sent: Any = None,
    text_skipped: Any = None,
    text_failed: Any = None,
    media_total: Any = None,
    media_sent: Any = None,
    media_skipped: Any = None,
    media_failed: Any = None,
    media_group_total: Any = None,
    media_group_sent: Any = None,
    media_group_skipped: Any = None,
    media_group_failed: Any = None,
    plan: Any = None,
    error_message: Any = None,
    completed_at: Any = None,
) -> dict | None:
    fields: list[str] = ["updated_at = ?"]
    values: list[Any] = [_now_iso()]
    optional_fields = {
        "status": _clean_text(status) if status is not None else None,
        "phase": _clean_text(phase) if phase is not None else None,
        "target_chat_id": _optional_int(target_chat_id)
        if target_chat_id is not None
        else None,
        "target_title": _clean_text(target_title) if target_title is not None else None,
        "target_write_account": _clean_text(target_write_account)
        if target_write_account is not None
        else None,
        "requested_limit": int(requested_limit)
        if requested_limit is not None
        else None,
        "send_delay_ms": int(send_delay_ms) if send_delay_ms is not None else None,
        "text_total": int(text_total) if text_total is not None else None,
        "text_sent": int(text_sent) if text_sent is not None else None,
        "text_skipped": int(text_skipped) if text_skipped is not None else None,
        "text_failed": int(text_failed) if text_failed is not None else None,
        "media_total": int(media_total) if media_total is not None else None,
        "media_sent": int(media_sent) if media_sent is not None else None,
        "media_skipped": int(media_skipped) if media_skipped is not None else None,
        "media_failed": int(media_failed) if media_failed is not None else None,
        "media_group_total": int(media_group_total)
        if media_group_total is not None
        else None,
        "media_group_sent": int(media_group_sent)
        if media_group_sent is not None
        else None,
        "media_group_skipped": int(media_group_skipped)
        if media_group_skipped is not None
        else None,
        "media_group_failed": int(media_group_failed)
        if media_group_failed is not None
        else None,
        "plan_json": _json_text(plan, default="{}") if plan is not None else None,
        "error_message": _clean_text(error_message)
        if error_message is not None
        else None,
        "completed_at": _clean_text(completed_at) if completed_at is not None else None,
    }
    for column_name, value in optional_fields.items():
        if value is None:
            continue
        fields.append(f"{column_name} = ?")
        values.append(value)

    values.append(_clean_text(migration_id))
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            UPDATE admin_clone_migrations
            SET {", ".join(fields)}
            WHERE migration_id = ?
            """,
            values,
        )
        if cur.rowcount <= 0:
            conn.commit()
            return None
        conn.commit()
        return load_clone_migration(conn, _clean_text(migration_id))
    finally:
        cur.close()


def load_clone_migration(conn: sqlite3.Connection, migration_id: str) -> dict | None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT *
            FROM admin_clone_migrations
            WHERE migration_id = ?
            LIMIT 1
            """,
            (_clean_text(migration_id),),
        )
        return _clone_migration_from_row(cur.fetchone())
    finally:
        cur.close()


def load_latest_clone_migration(
    conn: sqlite3.Connection,
    run_id: str,
    mode: str | None = None,
) -> dict | None:
    mode_filter = _clean_text(mode)
    mode_clause = "AND mode = ?" if mode_filter else ""
    params: list[Any] = [_clean_text(run_id)]
    if mode_filter:
        params.append(mode_filter)
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT *
            FROM admin_clone_migrations
            WHERE run_id = ?
              {mode_clause}
            ORDER BY updated_at DESC, created_at DESC, migration_id DESC
            LIMIT 1
            """,
            params,
        )
        return _clone_migration_from_row(cur.fetchone())
    finally:
        cur.close()


def record_clone_message_mapping(
    conn: sqlite3.Connection,
    *,
    migration_id: str,
    run_id: str,
    plan_id: str = "",
    source_chat_id: int,
    source_message_id: int,
    source_msg_date_ts: Any = None,
    source_msg_date_text: Any = None,
    target_chat_id: int,
    target_message_id: Any = None,
    chunk_index: int = 0,
    chunk_count: int = 1,
    mode: str = "text_replay",
    status: str = "done",
    error_message: Any = None,
    sent_at: Any = None,
) -> dict:
    now = _now_iso()
    normalized_run_id = _clean_text(run_id)
    normalized_mode = _clean_text(mode) or "text_replay"
    normalized_sent_at = _clean_text(sent_at) if sent_at is not None else now
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO admin_clone_message_map(
                migration_id,
                run_id,
                plan_id,
                source_chat_id,
                source_message_id,
                source_msg_date_ts,
                source_msg_date_text,
                target_chat_id,
                target_message_id,
                chunk_index,
                chunk_count,
                mode,
                status,
                error_message,
                sent_at,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(
                run_id,
                source_chat_id,
                source_message_id,
                chunk_index,
                mode
            ) DO UPDATE SET
                migration_id = excluded.migration_id,
                plan_id = excluded.plan_id,
                source_msg_date_ts = excluded.source_msg_date_ts,
                source_msg_date_text = excluded.source_msg_date_text,
                target_chat_id = excluded.target_chat_id,
                target_message_id = excluded.target_message_id,
                chunk_count = excluded.chunk_count,
                status = excluded.status,
                error_message = excluded.error_message,
                sent_at = excluded.sent_at,
                updated_at = excluded.updated_at
            """,
            (
                _clean_text(migration_id),
                normalized_run_id,
                _clean_text(plan_id),
                int(source_chat_id),
                int(source_message_id),
                _optional_int(source_msg_date_ts),
                _clean_text(source_msg_date_text),
                int(target_chat_id),
                _optional_int(target_message_id),
                int(chunk_index),
                int(chunk_count),
                normalized_mode,
                _clean_text(status) or "done",
                _clean_text(error_message) if error_message is not None else "",
                normalized_sent_at,
                now,
                now,
            ),
        )
        conn.commit()
        mapping = load_clone_message_mapping(
            conn,
            run_id=normalized_run_id,
            source_chat_id=int(source_chat_id),
            source_message_id=int(source_message_id),
            chunk_index=int(chunk_index),
            mode=normalized_mode,
        )
        if mapping is None:
            raise RuntimeError("clone message mapping 写入后读取失败")
        return mapping
    finally:
        cur.close()


def load_clone_message_mapping(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    source_chat_id: int,
    source_message_id: int,
    chunk_index: int = 0,
    mode: str = "text_replay",
) -> dict | None:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT *
            FROM admin_clone_message_map
            WHERE run_id = ?
              AND source_chat_id = ?
              AND source_message_id = ?
              AND chunk_index = ?
              AND mode = ?
            LIMIT 1
            """,
            (
                _clean_text(run_id),
                int(source_chat_id),
                int(source_message_id),
                int(chunk_index),
                _clean_text(mode) or "text_replay",
            ),
        )
        return _clone_message_mapping_from_row(cur.fetchone())
    finally:
        cur.close()


def count_clone_text_replay_candidates(conn: sqlite3.Connection, chat_id: int) -> int:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM messages
            WHERE chat_id = ?
              AND COALESCE(has_media, 0) = 0
              AND grouped_id IS NULL
              AND COALESCE(
                    NULLIF(TRIM(content), ''),
                    NULLIF(TRIM(content_norm), ''),
                    ''
                  ) <> ''
            """,
            (int(chat_id),),
        )
        row = cur.fetchone()
        return int(row["c"] or 0)
    finally:
        cur.close()


def count_clone_media_replay_skips(conn: sqlite3.Connection, chat_id: int) -> int:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM messages
            WHERE chat_id = ?
              AND COALESCE(has_media, 0) = 1
            """,
            (int(chat_id),),
        )
        row = cur.fetchone()
        return int(row["c"] or 0)
    finally:
        cur.close()


def build_clone_text_replay_preview(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    source_chat_id: int,
) -> dict:
    normalized_run_id = _clean_text(run_id)
    normalized_source_chat_id = int(source_chat_id)
    chunk_size = CLONE_TEXT_REPLAY_CHUNK_MAX_LEN
    cur = conn.cursor()
    try:
        cur.execute(
            """
            WITH raw_candidates AS (
                SELECT
                    message_id,
                    COALESCE(
                        NULLIF(TRIM(content), ''),
                        NULLIF(TRIM(content_norm), ''),
                        ''
                    ) AS text
                FROM messages
                WHERE chat_id = ?
                  AND COALESCE(has_media, 0) = 0
                  AND grouped_id IS NULL
                  AND COALESCE(
                        NULLIF(TRIM(content), ''),
                        NULLIF(TRIM(content_norm), ''),
                        ''
                      ) <> ''
            ),
            candidates AS (
                SELECT
                    message_id,
                    CAST(((LENGTH(text) + ? - 1) / ?) AS INTEGER) AS chunk_count
                FROM raw_candidates
            ),
            mapped AS (
                SELECT
                    source_message_id,
                    MAX(chunk_count) AS mapped_chunk_count,
                    SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) AS done_chunks,
                    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_chunks
                FROM admin_clone_message_map
                WHERE run_id = ?
                  AND source_chat_id = ?
                  AND mode = 'text_replay'
                GROUP BY source_message_id
            )
            SELECT
                COUNT(*) AS text_total,
                COALESCE(SUM(c.chunk_count), 0) AS text_chunks_total,
                COALESCE(
                    SUM(MIN(COALESCE(m.done_chunks, 0), c.chunk_count)),
                    0
                ) AS text_chunks_done,
                COALESCE(SUM(COALESCE(m.error_chunks, 0)), 0) AS text_chunks_error,
                COALESCE(
                    SUM(
                        CASE
                            WHEN COALESCE(m.done_chunks, 0) >= c.chunk_count
                            THEN 1 ELSE 0
                        END
                    ),
                    0
                ) AS text_completed,
                COALESCE(
                    SUM(
                        CASE
                            WHEN COALESCE(m.done_chunks, 0) > 0
                             AND COALESCE(m.done_chunks, 0) < c.chunk_count
                            THEN 1 ELSE 0
                        END
                    ),
                    0
                ) AS text_partial,
                COALESCE(
                    SUM(
                        CASE
                            WHEN COALESCE(m.error_chunks, 0) > 0
                            THEN 1 ELSE 0
                        END
                    ),
                    0
                ) AS text_error
            FROM candidates c
            LEFT JOIN mapped m ON m.source_message_id = c.message_id
            """,
            (
                normalized_source_chat_id,
                chunk_size,
                chunk_size,
                normalized_run_id,
                normalized_source_chat_id,
            ),
        )
        row = cur.fetchone()
        text_total = int(row["text_total"] or 0) if row is not None else 0
        text_completed = int(row["text_completed"] or 0) if row is not None else 0

        cur.execute(
            """
            SELECT
                COUNT(*) AS total_messages,
                SUM(CASE WHEN COALESCE(has_media, 0) = 1 THEN 1 ELSE 0 END)
                    AS media_skipped,
                SUM(CASE WHEN grouped_id IS NOT NULL THEN 1 ELSE 0 END)
                    AS grouped_skipped,
                SUM(
                    CASE
                        WHEN COALESCE(has_media, 0) = 0
                         AND grouped_id IS NULL
                         AND COALESCE(
                                NULLIF(TRIM(content), ''),
                                NULLIF(TRIM(content_norm), ''),
                                ''
                             ) = ''
                        THEN 1 ELSE 0
                    END
                ) AS empty_text_skipped
            FROM messages
            WHERE chat_id = ?
            """,
            (normalized_source_chat_id,),
        )
        skip_row = cur.fetchone()
        return {
            "run_id": normalized_run_id,
            "source_chat_id": normalized_source_chat_id,
            "chunk_size": chunk_size,
            "text_total": text_total,
            "text_completed": text_completed,
            "text_remaining": max(0, text_total - text_completed),
            "text_partial": int(row["text_partial"] or 0) if row is not None else 0,
            "text_error": int(row["text_error"] or 0) if row is not None else 0,
            "text_chunks_total": int(row["text_chunks_total"] or 0)
            if row is not None
            else 0,
            "text_chunks_done": int(row["text_chunks_done"] or 0)
            if row is not None
            else 0,
            "text_chunks_error": int(row["text_chunks_error"] or 0)
            if row is not None
            else 0,
            "total_messages": int(skip_row["total_messages"] or 0)
            if skip_row is not None
            else 0,
            "media_skipped": int(skip_row["media_skipped"] or 0)
            if skip_row is not None
            else 0,
            "grouped_skipped": int(skip_row["grouped_skipped"] or 0)
            if skip_row is not None
            else 0,
            "empty_text_skipped": int(skip_row["empty_text_skipped"] or 0)
            if skip_row is not None
            else 0,
        }
    finally:
        cur.close()


def list_clone_text_replay_batch(
    conn: sqlite3.Connection,
    *,
    chat_id: int,
    after_ts: Any = None,
    after_message_id: Any = None,
    limit: Any = 200,
) -> list[dict]:
    try:
        normalized_limit = int(limit)
    except (TypeError, ValueError):
        normalized_limit = 200
    normalized_limit = max(1, min(1000, normalized_limit))
    normalized_after_ts = _optional_int(after_ts)
    normalized_after_message_id = _optional_int(after_message_id)

    where_cursor = ""
    params: list[Any] = [int(chat_id)]
    if normalized_after_ts is not None and normalized_after_message_id is not None:
        where_cursor = """
          AND (
                COALESCE(msg_date_ts, 0) > ?
             OR (COALESCE(msg_date_ts, 0) = ? AND message_id > ?)
          )
        """
        params.extend(
            [
                normalized_after_ts,
                normalized_after_ts,
                normalized_after_message_id,
            ]
        )
    params.append(normalized_limit)

    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT
                chat_id,
                message_id,
                msg_date_text,
                msg_date_ts,
                COALESCE(msg_date_ts, 0) AS sort_ts,
                COALESCE(
                    NULLIF(TRIM(content), ''),
                    NULLIF(TRIM(content_norm), ''),
                    ''
                ) AS text
            FROM messages
            WHERE chat_id = ?
              AND COALESCE(has_media, 0) = 0
              AND grouped_id IS NULL
              AND COALESCE(
                    NULLIF(TRIM(content), ''),
                    NULLIF(TRIM(content_norm), ''),
                    ''
                  ) <> ''
              {where_cursor}
            ORDER BY COALESCE(msg_date_ts, 0) ASC, message_id ASC
            LIMIT ?
            """,
            params,
        )
        return [
            {
                "chat_id": int(row["chat_id"] or 0),
                "message_id": int(row["message_id"] or 0),
                "msg_date_text": str(row["msg_date_text"] or ""),
                "msg_date_ts": int(row["msg_date_ts"] or 0),
                "sort_ts": int(row["sort_ts"] or 0),
                "text": str(row["text"] or ""),
            }
            for row in cur.fetchall()
        ]
    finally:
        cur.close()


def build_clone_media_copy_preview(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    source_chat_id: int,
) -> dict:
    normalized_run_id = _clean_text(run_id)
    normalized_source_chat_id = int(source_chat_id)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            WITH group_stats AS (
                SELECT
                    m.chat_id,
                    m.grouped_id,
                    COUNT(*) AS current_item_count,
                    MIN(m.message_id) AS first_message_id,
                    MAX(m.message_id) AS last_message_id,
                    MAX(m.message_id) - MIN(m.message_id) + 1 AS message_id_span
                FROM messages m
                WHERE m.chat_id = ?
                  AND m.grouped_id IS NOT NULL
                  AND COALESCE(m.has_media, 0) = 1
                GROUP BY m.chat_id, m.grouped_id
            ),
            media_messages AS (
                SELECT
                    m.chat_id,
                    m.message_id,
                    m.grouped_id,
                    CASE
                        WHEN m.grouped_id IS NULL THEN 'solo'
                        WHEN mg.grouped_id IS NULL THEN 'missing_group_meta'
                        WHEN COALESCE(mg.item_count, 0) <= 0
                         OR COALESCE(mg.active_items, 0) <> COALESCE(mg.item_count, 0)
                         OR COALESCE(gs.current_item_count, 0) <> COALESCE(mg.item_count, 0)
                        THEN 'incomplete_group'
                        WHEN COALESCE(gs.current_item_count, 0) < 2
                         OR COALESCE(gs.message_id_span, 0) <> COALESCE(gs.current_item_count, 0)
                        THEN 'suspected_incomplete_group'
                        WHEN COALESCE(mg.item_count, 0) > 0
                         AND COALESCE(mg.active_items, 0) = COALESCE(mg.item_count, 0)
                        THEN 'complete_group'
                        ELSE 'incomplete_group'
                    END AS media_bucket
                FROM messages m
                LEFT JOIN media_groups mg
                  ON mg.chat_id = m.chat_id
                 AND mg.grouped_id = m.grouped_id
                LEFT JOIN group_stats gs
                  ON gs.chat_id = m.chat_id
                 AND gs.grouped_id = m.grouped_id
                WHERE m.chat_id = ?
                  AND COALESCE(m.has_media, 0) = 1
            ),
            mapped AS (
                SELECT
                    source_message_id,
                    mode,
                    MAX(CASE WHEN status = 'done' THEN 1 ELSE 0 END) AS done
                FROM admin_clone_message_map
                WHERE run_id = ?
                  AND source_chat_id = ?
                  AND mode IN ('media_copy', 'media_group_copy')
                GROUP BY source_message_id, mode
            )
            SELECT
                COUNT(*) AS media_total,
                SUM(CASE WHEN media_bucket = 'solo' THEN 1 ELSE 0 END)
                    AS solo_media_total,
                SUM(CASE WHEN media_bucket = 'complete_group' THEN 1 ELSE 0 END)
                    AS complete_group_items,
                COUNT(DISTINCT CASE WHEN media_bucket = 'complete_group'
                    THEN grouped_id END) AS complete_group_total,
                SUM(CASE WHEN grouped_id IS NOT NULL THEN 1 ELSE 0 END)
                    AS media_group_candidate_items,
                COUNT(DISTINCT grouped_id) AS media_group_candidate_total,
                SUM(CASE WHEN media_bucket = 'incomplete_group' THEN 1 ELSE 0 END)
                    AS incomplete_group_items,
                COUNT(DISTINCT CASE WHEN media_bucket = 'incomplete_group'
                    THEN grouped_id END) AS incomplete_group_total,
                SUM(CASE WHEN media_bucket = 'suspected_incomplete_group' THEN 1 ELSE 0 END)
                    AS suspected_incomplete_group_items,
                COUNT(DISTINCT CASE WHEN media_bucket = 'suspected_incomplete_group'
                    THEN grouped_id END) AS suspected_incomplete_group_total,
                SUM(CASE WHEN media_bucket = 'missing_group_meta' THEN 1 ELSE 0 END)
                    AS missing_group_meta_items,
                COUNT(DISTINCT CASE WHEN media_bucket = 'missing_group_meta'
                    THEN grouped_id END) AS missing_group_meta_total,
                SUM(CASE WHEN media_bucket = 'solo'
                    AND COALESCE(ms.done, 0) = 1 THEN 1 ELSE 0 END)
                    AS solo_media_done,
                SUM(CASE WHEN media_bucket = 'complete_group'
                    AND COALESCE(mg.done, 0) = 1 THEN 1 ELSE 0 END)
                    AS complete_group_items_done,
                SUM(CASE WHEN grouped_id IS NOT NULL
                    AND COALESCE(mg.done, 0) = 1 THEN 1 ELSE 0 END)
                    AS grouped_items_done
            FROM media_messages mm
            LEFT JOIN mapped ms
              ON ms.source_message_id = mm.message_id
             AND ms.mode = 'media_copy'
            LEFT JOIN mapped mg
              ON mg.source_message_id = mm.message_id
             AND mg.mode = 'media_group_copy'
            """,
            (
                normalized_source_chat_id,
                normalized_source_chat_id,
                normalized_run_id,
                normalized_source_chat_id,
            ),
        )
        row = cur.fetchone()
        solo_total = int(row["solo_media_total"] or 0) if row is not None else 0
        complete_group_items = (
            int(row["complete_group_items"] or 0) if row is not None else 0
        )
        solo_done = int(row["solo_media_done"] or 0) if row is not None else 0
        group_done = (
            int(row["complete_group_items_done"] or 0) if row is not None else 0
        )
        media_total = int(row["media_total"] or 0) if row is not None else 0
        grouped_items_done = int(row["grouped_items_done"] or 0) if row is not None else 0
        media_group_candidate_total = (
            int(row["media_group_candidate_total"] or 0) if row is not None else 0
        )
        media_group_candidate_items = (
            int(row["media_group_candidate_items"] or 0) if row is not None else 0
        )
        incomplete_group_total = (
            int(row["incomplete_group_total"] or 0) if row is not None else 0
        )
        incomplete_group_items = (
            int(row["incomplete_group_items"] or 0) if row is not None else 0
        )
        suspected_incomplete_group_total = (
            int(row["suspected_incomplete_group_total"] or 0)
            if row is not None
            else 0
        )
        suspected_incomplete_group_items = (
            int(row["suspected_incomplete_group_items"] or 0)
            if row is not None
            else 0
        )
        missing_group_meta_total = (
            int(row["missing_group_meta_total"] or 0) if row is not None else 0
        )
        missing_group_meta_items = (
            int(row["missing_group_meta_items"] or 0) if row is not None else 0
        )
        executable_total = media_total
        completed = min(solo_done + grouped_items_done, executable_total)
        remaining = max(0, executable_total - completed)
        db_self_check_risk_group_total = (
            incomplete_group_total
            + suspected_incomplete_group_total
            + missing_group_meta_total
        )
        db_self_check_risk_group_items = (
            incomplete_group_items
            + suspected_incomplete_group_items
            + missing_group_meta_items
        )
        return {
            "run_id": normalized_run_id,
            "source_chat_id": normalized_source_chat_id,
            "mode": "media_copy_without_attribution",
            "forward_privacy": "without_source_attribution",
            "media_total": media_total,
            "media_candidate_total": executable_total,
            "media_executable_total": executable_total,
            "media_completed": completed,
            "media_candidate_remaining": remaining,
            "media_remaining": remaining,
            "solo_media_total": solo_total,
            "solo_media_done": solo_done,
            "complete_group_total": int(row["complete_group_total"] or 0)
            if row is not None
            else 0,
            "complete_group_items": complete_group_items,
            "complete_group_items_done": group_done,
            "media_group_candidate_total": media_group_candidate_total,
            "media_group_candidate_items": media_group_candidate_items,
            "media_group_items_done": grouped_items_done,
            "db_self_check_risk_group_total": db_self_check_risk_group_total,
            "db_self_check_risk_group_items": db_self_check_risk_group_items,
            "incomplete_group_total": incomplete_group_total,
            "incomplete_group_items": incomplete_group_items,
            "suspected_incomplete_group_total": suspected_incomplete_group_total,
            "suspected_incomplete_group_items": suspected_incomplete_group_items,
            "missing_group_meta_total": missing_group_meta_total,
            "missing_group_meta_items": missing_group_meta_items,
        }
    finally:
        cur.close()


def list_clone_solo_media_copy_batch(
    conn: sqlite3.Connection,
    *,
    chat_id: int,
    after_ts: Any = None,
    after_message_id: Any = None,
    limit: Any = 100,
) -> list[dict]:
    try:
        normalized_limit = int(limit)
    except (TypeError, ValueError):
        normalized_limit = 100
    normalized_limit = max(1, min(500, normalized_limit))
    normalized_after_ts = _optional_int(after_ts)
    normalized_after_message_id = _optional_int(after_message_id)

    where_cursor = ""
    params: list[Any] = [int(chat_id)]
    if normalized_after_ts is not None and normalized_after_message_id is not None:
        where_cursor = """
          AND (
                COALESCE(m.msg_date_ts, 0) > ?
             OR (COALESCE(m.msg_date_ts, 0) = ? AND m.message_id > ?)
          )
        """
        params.extend(
            [
                normalized_after_ts,
                normalized_after_ts,
                normalized_after_message_id,
            ]
        )
    params.append(normalized_limit)

    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT
                m.chat_id,
                m.message_id,
                m.msg_date_text,
                m.msg_date_ts,
                COALESCE(m.msg_date_ts, 0) AS sort_ts,
                COALESCE(NULLIF(TRIM(m.content), ''), NULLIF(TRIM(m.content_norm), ''), '')
                    AS caption,
                mm.media_kind,
                mm.file_name,
                mm.media_fingerprint
            FROM messages m
            LEFT JOIN message_media mm
              ON mm.chat_id = m.chat_id
             AND mm.message_id = m.message_id
            WHERE m.chat_id = ?
              AND COALESCE(m.has_media, 0) = 1
              AND m.grouped_id IS NULL
              {where_cursor}
            ORDER BY COALESCE(m.msg_date_ts, 0) ASC, m.message_id ASC
            LIMIT ?
            """,
            params,
        )
        return [
            {
                "chat_id": int(row["chat_id"] or 0),
                "message_id": int(row["message_id"] or 0),
                "msg_date_text": str(row["msg_date_text"] or ""),
                "msg_date_ts": _optional_int(row["msg_date_ts"]),
                "sort_ts": int(row["sort_ts"] or 0),
                "caption": str(row["caption"] or ""),
                "media_kind": str(row["media_kind"] or ""),
                "file_name": str(row["file_name"] or ""),
                "media_fingerprint": str(row["media_fingerprint"] or ""),
            }
            for row in cur.fetchall()
        ]
    finally:
        cur.close()


def list_clone_media_group_candidate_batch(
    conn: sqlite3.Connection,
    *,
    chat_id: int,
    after_ts: Any = None,
    after_grouped_id: Any = None,
    limit: Any = 50,
) -> list[dict]:
    try:
        normalized_limit = int(limit)
    except (TypeError, ValueError):
        normalized_limit = 50
    normalized_limit = max(1, min(200, normalized_limit))
    normalized_after_ts = _optional_int(after_ts)
    normalized_after_grouped_id = _optional_int(after_grouped_id)

    where_cursor = ""
    params: list[Any] = [int(chat_id)]
    if normalized_after_ts is not None and normalized_after_grouped_id is not None:
        where_cursor = """
          AND (
                gc.group_sort_ts > ?
             OR (gc.group_sort_ts = ? AND gc.grouped_id > ?)
          )
        """
        params.extend(
            [
                normalized_after_ts,
                normalized_after_ts,
                normalized_after_grouped_id,
            ]
        )
    params.append(normalized_limit)

    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            WITH grouped_candidates AS (
                SELECT
                    m.chat_id,
                    m.grouped_id,
                    MIN(COALESCE(m.msg_date_ts, 0)) AS group_sort_ts,
                    MIN(m.message_id) AS first_message_id,
                    MAX(m.message_id) AS last_message_id,
                    COUNT(*) AS current_item_count
                FROM messages m
                WHERE m.chat_id = ?
                  AND m.grouped_id IS NOT NULL
                  AND COALESCE(m.has_media, 0) = 1
                GROUP BY m.chat_id, m.grouped_id
            )
            SELECT
                gc.chat_id,
                gc.grouped_id,
                gc.group_sort_ts AS sort_ts,
                COALESCE(mg.item_count, 0) AS item_count,
                COALESCE(mg.active_items, 0) AS active_items,
                gc.current_item_count,
                gc.first_message_id,
                gc.last_message_id
            FROM grouped_candidates gc
            LEFT JOIN media_groups mg
              ON mg.chat_id = gc.chat_id
             AND mg.grouped_id = gc.grouped_id
            WHERE 1 = 1
              {where_cursor}
            ORDER BY gc.group_sort_ts ASC, gc.grouped_id ASC
            LIMIT ?
            """,
            params,
        )
        return [
            {
                "chat_id": int(row["chat_id"] or 0),
                "grouped_id": int(row["grouped_id"] or 0),
                "sort_ts": int(row["sort_ts"] or 0),
                "item_count": int(row["item_count"] or 0),
                "active_items": int(row["active_items"] or 0),
                "current_item_count": int(row["current_item_count"] or 0),
                "first_message_id": int(row["first_message_id"] or 0),
                "last_message_id": int(row["last_message_id"] or 0),
            }
            for row in cur.fetchall()
        ]
    finally:
        cur.close()


def list_clone_media_group_messages(
    conn: sqlite3.Connection,
    *,
    chat_id: int,
    grouped_id: int,
) -> list[dict]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                chat_id,
                message_id,
                msg_date_text,
                msg_date_ts,
                COALESCE(msg_date_ts, 0) AS sort_ts,
                COALESCE(NULLIF(TRIM(content), ''), NULLIF(TRIM(content_norm), ''), '')
                    AS caption
            FROM messages
            WHERE chat_id = ?
              AND grouped_id = ?
              AND COALESCE(has_media, 0) = 1
            ORDER BY message_id ASC
            """,
            (int(chat_id), int(grouped_id)),
        )
        return [
            {
                "chat_id": int(row["chat_id"] or 0),
                "message_id": int(row["message_id"] or 0),
                "msg_date_text": str(row["msg_date_text"] or ""),
                "msg_date_ts": _optional_int(row["msg_date_ts"]),
                "sort_ts": int(row["sort_ts"] or 0),
                "caption": str(row["caption"] or ""),
            }
            for row in cur.fetchall()
        ]
    finally:
        cur.close()


def build_clone_timeline_replay_preview(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    source_chat_id: int,
) -> dict:
    text_preview = build_clone_text_replay_preview(
        conn,
        run_id=run_id,
        source_chat_id=source_chat_id,
    )
    media_preview = build_clone_media_copy_preview(
        conn,
        run_id=run_id,
        source_chat_id=source_chat_id,
    )
    cur = conn.cursor()
    try:
        cur.execute(
            """
            WITH timeline_items AS (
                SELECT message_id AS sort_message_id
                FROM messages
                WHERE chat_id = ?
                  AND COALESCE(has_media, 0) = 0
                  AND grouped_id IS NULL
                  AND COALESCE(
                        NULLIF(TRIM(content), ''),
                        NULLIF(TRIM(content_norm), ''),
                        ''
                      ) <> ''
                UNION ALL
                SELECT message_id AS sort_message_id
                FROM messages
                WHERE chat_id = ?
                  AND COALESCE(has_media, 0) = 1
                  AND grouped_id IS NULL
                UNION ALL
                SELECT MIN(message_id) AS sort_message_id
                FROM messages
                WHERE chat_id = ?
                  AND COALESCE(has_media, 0) = 1
                  AND grouped_id IS NOT NULL
                GROUP BY grouped_id
            )
            SELECT COUNT(*) AS c
            FROM timeline_items
            """,
            (int(source_chat_id), int(source_chat_id), int(source_chat_id)),
        )
        row = cur.fetchone()
        timeline_items_total = int(row["c"] or 0) if row is not None else 0
    finally:
        cur.close()

    text_remaining = int(text_preview.get("text_remaining") or 0)
    media_remaining = int(media_preview.get("media_remaining") or 0)
    text_total = int(text_preview.get("text_total") or 0)
    media_total = int(media_preview.get("media_total") or 0)
    media_group_total = int(media_preview.get("media_group_candidate_total") or 0)
    return {
        "run_id": _clean_text(run_id),
        "source_chat_id": int(source_chat_id),
        "mode": "timeline_replay",
        "timeline_items_total": timeline_items_total,
        "timeline_source_messages_total": text_total + media_total,
        "timeline_remaining": text_remaining + media_remaining,
        "text_total": text_total,
        "text_completed": int(text_preview.get("text_completed") or 0),
        "text_remaining": text_remaining,
        "media_total": media_total,
        "media_completed": int(media_preview.get("media_completed") or 0),
        "media_remaining": media_remaining,
        "media_group_total": media_group_total,
        "media_group_candidate_items": int(
            media_preview.get("media_group_candidate_items") or 0
        ),
        "db_self_check_risk_group_total": int(
            media_preview.get("db_self_check_risk_group_total") or 0
        ),
        "db_self_check_risk_group_items": int(
            media_preview.get("db_self_check_risk_group_items") or 0
        ),
        "text_preview": text_preview,
        "media_preview": media_preview,
    }


def list_clone_timeline_replay_batch(
    conn: sqlite3.Connection,
    *,
    run_id: str = "",
    chat_id: int,
    after_ts: Any = None,
    after_message_id: Any = None,
    limit: Any = 100,
) -> list[dict]:
    try:
        normalized_limit = int(limit)
    except (TypeError, ValueError):
        normalized_limit = 100
    normalized_limit = max(1, min(500, normalized_limit))
    normalized_run_id = _clean_text(run_id)
    normalized_after_ts = _optional_int(after_ts)
    normalized_after_message_id = _optional_int(after_message_id)

    where_cursor = ""
    params: list[Any] = [
        CLONE_TEXT_REPLAY_CHUNK_MAX_LEN,
        CLONE_TEXT_REPLAY_CHUNK_MAX_LEN,
        int(chat_id),
        int(chat_id),
        int(chat_id),
        normalized_run_id,
        int(chat_id),
        normalized_run_id,
        int(chat_id),
        normalized_run_id,
        int(chat_id),
        normalized_run_id,
    ]
    if normalized_after_ts is not None and normalized_after_message_id is not None:
        where_cursor = """
          AND (
                ti.sort_ts > ?
             OR (ti.sort_ts = ? AND ti.sort_message_id > ?)
          )
        """
        params.extend(
            [
                normalized_after_ts,
                normalized_after_ts,
                normalized_after_message_id,
            ]
        )
    params.append(normalized_limit)

    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            WITH timeline_items AS (
                SELECT
                    'text' AS item_type,
                    chat_id,
                    message_id AS source_message_id,
                    NULL AS grouped_id,
                    COALESCE(msg_date_ts, 0) AS sort_ts,
                    message_id AS sort_message_id,
                    msg_date_text,
                    msg_date_ts,
                    COALESCE(
                        NULLIF(TRIM(content), ''),
                        NULLIF(TRIM(content_norm), ''),
                        ''
                    ) AS text,
                    1 AS item_count,
                    CAST((
                        (
                            LENGTH(COALESCE(
                                NULLIF(TRIM(content), ''),
                                NULLIF(TRIM(content_norm), ''),
                                ''
                            )) + ? - 1
                        ) / ?
                    ) AS INTEGER) AS expected_done_count
                FROM messages
                WHERE chat_id = ?
                  AND COALESCE(has_media, 0) = 0
                  AND grouped_id IS NULL
                  AND COALESCE(
                        NULLIF(TRIM(content), ''),
                        NULLIF(TRIM(content_norm), ''),
                        ''
                      ) <> ''

                UNION ALL

                SELECT
                    'solo_media' AS item_type,
                    m.chat_id,
                    m.message_id AS source_message_id,
                    NULL AS grouped_id,
                    COALESCE(m.msg_date_ts, 0) AS sort_ts,
                    m.message_id AS sort_message_id,
                    m.msg_date_text,
                    m.msg_date_ts,
                    COALESCE(NULLIF(TRIM(m.content), ''), NULLIF(TRIM(m.content_norm), ''), '')
                        AS text,
                    1 AS item_count,
                    1 AS expected_done_count
                FROM messages m
                WHERE m.chat_id = ?
                  AND COALESCE(m.has_media, 0) = 1
                  AND m.grouped_id IS NULL

                UNION ALL

                SELECT
                    'media_group' AS item_type,
                    m.chat_id,
                    MIN(m.message_id) AS source_message_id,
                    m.grouped_id,
                    MIN(COALESCE(m.msg_date_ts, 0)) AS sort_ts,
                    MIN(m.message_id) AS sort_message_id,
                    MIN(m.msg_date_text) AS msg_date_text,
                    MIN(m.msg_date_ts) AS msg_date_ts,
                    '' AS text,
                    COUNT(*) AS item_count,
                    COUNT(*) AS expected_done_count
                FROM messages m
                WHERE m.chat_id = ?
                  AND COALESCE(m.has_media, 0) = 1
                  AND m.grouped_id IS NOT NULL
                GROUP BY m.chat_id, m.grouped_id
            ),
            text_done AS (
                SELECT
                    source_message_id,
                    COUNT(DISTINCT chunk_index) AS done_count
                FROM admin_clone_message_map
                WHERE run_id = ?
                  AND source_chat_id = ?
                  AND mode = 'text_replay'
                  AND status = 'done'
                GROUP BY source_message_id
            ),
            solo_done AS (
                SELECT
                    source_message_id,
                    1 AS done_count
                FROM admin_clone_message_map
                WHERE run_id = ?
                  AND source_chat_id = ?
                  AND mode = 'media_copy'
                  AND status = 'done'
                GROUP BY source_message_id
            ),
            group_done AS (
                SELECT
                    m.grouped_id,
                    COUNT(DISTINCT m.message_id) AS done_count
                FROM messages m
                JOIN admin_clone_message_map cmm
                  ON cmm.run_id = ?
                 AND cmm.source_chat_id = m.chat_id
                 AND cmm.source_message_id = m.message_id
                 AND cmm.mode = 'media_group_copy'
                 AND cmm.status = 'done'
                WHERE m.chat_id = ?
                  AND COALESCE(m.has_media, 0) = 1
                  AND m.grouped_id IS NOT NULL
                GROUP BY m.grouped_id
            )
            SELECT *
            FROM timeline_items ti
            LEFT JOIN text_done td
              ON td.source_message_id = ti.source_message_id
             AND ti.item_type = 'text'
            LEFT JOIN solo_done sd
              ON sd.source_message_id = ti.source_message_id
             AND ti.item_type = 'solo_media'
            LEFT JOIN group_done gd
              ON gd.grouped_id = ti.grouped_id
             AND ti.item_type = 'media_group'
            WHERE (
                    ? = ''
                 OR CASE
                        WHEN ti.item_type = 'text'
                        THEN COALESCE(td.done_count, 0) >= ti.expected_done_count
                        WHEN ti.item_type = 'solo_media'
                        THEN COALESCE(sd.done_count, 0) >= 1
                        WHEN ti.item_type = 'media_group'
                        THEN COALESCE(gd.done_count, 0) >= ti.expected_done_count
                        ELSE 0
                    END = 0
            )
            {where_cursor}
            ORDER BY ti.sort_ts ASC, ti.sort_message_id ASC
            LIMIT ?
            """,
            params,
        )
        return [
            {
                "item_type": str(row["item_type"] or ""),
                "chat_id": int(row["chat_id"] or 0),
                "source_message_id": int(row["source_message_id"] or 0),
                "grouped_id": _optional_int(row["grouped_id"]),
                "sort_ts": int(row["sort_ts"] or 0),
                "sort_message_id": int(row["sort_message_id"] or 0),
                "msg_date_text": str(row["msg_date_text"] or ""),
                "msg_date_ts": _optional_int(row["msg_date_ts"]),
                "text": str(row["text"] or ""),
                "item_count": int(row["item_count"] or 0),
            }
            for row in cur.fetchall()
        ]
    finally:
        cur.close()


def update_clone_run(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    status: str | None = None,
    phase: str | None = None,
    target_chat_id: Any = None,
    target_access_hash: Any = None,
    target_title: Any = None,
    target_kind: Any = None,
    target_username: Any = None,
    target_owner_session: Any = None,
    error_message: Any = None,
    target_created_at: Any = None,
    completed_at: Any = None,
) -> dict | None:
    fields: list[str] = ["updated_at = ?"]
    values: list[Any] = [_now_iso()]

    optional_fields = {
        "status": _clean_text(status) if status is not None else None,
        "phase": _clean_text(phase) if phase is not None else None,
        "target_chat_id": _optional_int(target_chat_id)
        if target_chat_id is not None
        else None,
        "target_access_hash": _clean_text(target_access_hash)
        if target_access_hash is not None
        else None,
        "target_title": _clean_text(target_title) if target_title is not None else None,
        "target_kind": _clean_text(target_kind) if target_kind is not None else None,
        "target_username": _clean_text(target_username)
        if target_username is not None
        else None,
        "target_owner_session": _clean_text(target_owner_session)
        if target_owner_session is not None
        else None,
        "error_message": _clean_text(error_message)
        if error_message is not None
        else None,
        "target_created_at": _clean_text(target_created_at)
        if target_created_at is not None
        else None,
        "completed_at": _clean_text(completed_at) if completed_at is not None else None,
    }
    for column_name, value in optional_fields.items():
        if value is None:
            continue
        fields.append(f"{column_name} = ?")
        values.append(value)

    values.append(_clean_text(run_id))
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            UPDATE admin_clone_runs
            SET {", ".join(fields)}
            WHERE run_id = ?
            """,
            values,
        )
        if cur.rowcount <= 0:
            conn.commit()
            return None
        conn.commit()
        return load_clone_run(conn, _clean_text(run_id))
    finally:
        cur.close()


def list_clone_runs(
    conn: sqlite3.Connection,
    *,
    source_chat_id: Any = None,
    limit: Any = 20,
    offset: Any = 0,
    status: Any = "",
    q: Any = "",
    sort: Any = "updated_desc",
) -> list[dict]:
    try:
        normalized_limit = int(limit)
    except (TypeError, ValueError):
        normalized_limit = 20
    normalized_limit = max(1, min(100, normalized_limit))
    try:
        normalized_offset = int(offset)
    except (TypeError, ValueError):
        normalized_offset = 0
    normalized_offset = max(0, normalized_offset)
    normalized_status = _clean_text(status).lower()
    normalized_query = _clean_text(q)
    normalized_sort = _clean_text(sort).lower()

    where_sql_parts = []
    params: list[Any] = []
    if source_chat_id not in (None, ""):
        normalized_source_chat_id = int(source_chat_id)
        where_sql_parts.append("source_chat_id = ?")
        params.append(normalized_source_chat_id)
    if normalized_status:
        if normalized_status in {"done", "running", "queued", "error"}:
            where_sql_parts.append("status = ?")
            params.append(normalized_status)
        else:
            where_sql_parts.append("status LIKE ?")
            params.append(f"%{normalized_status}%")
    if normalized_query:
        where_sql_parts.append(
            "("
            "source_title LIKE ? OR "
            "target_title LIKE ? OR "
            "source_chat_username LIKE ? OR "
            "target_username LIKE ? OR "
            "CAST(source_chat_id AS TEXT) LIKE ? OR "
            "CAST(target_chat_id AS TEXT) LIKE ? OR "
            "run_id LIKE ? OR "
            "job_id LIKE ?"
            ")"
        )
        query_like = f"%{normalized_query}%"
        params.extend([query_like] * 8)

    where_sql = ""
    if where_sql_parts:
        where_sql = "WHERE " + " AND ".join(where_sql_parts)
    order_sql = {
        "created_asc": "created_at ASC, updated_at ASC, run_id ASC",
        "target_asc": "target_title COLLATE NOCASE ASC, updated_at DESC, run_id DESC",
        "source_asc": "source_title COLLATE NOCASE ASC, updated_at DESC, run_id DESC",
        "status_asc": "status ASC, updated_at DESC, run_id DESC",
    }.get(
        normalized_sort,
        "updated_at DESC, created_at DESC, run_id DESC",
    )
    params.extend([normalized_limit, normalized_offset])

    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT *
            FROM admin_clone_runs
            {where_sql}
            ORDER BY {order_sql}
            LIMIT ? OFFSET ?
            """,
            params,
        )
        return [
            run
            for run in (_clone_run_from_row(row) for row in cur.fetchall())
            if run is not None
        ]
    finally:
        cur.close()


def count_clone_runs(
    conn: sqlite3.Connection,
    *,
    source_chat_id: Any = None,
    status: Any = "",
    q: Any = "",
) -> int:
    normalized_status = _clean_text(status).lower()
    normalized_query = _clean_text(q)
    where_sql_parts = []
    params: list[Any] = []
    if source_chat_id not in (None, ""):
        normalized_source_chat_id = int(source_chat_id)
        where_sql_parts.append("source_chat_id = ?")
        params.append(normalized_source_chat_id)
    if normalized_status:
        if normalized_status in {"done", "running", "queued", "error"}:
            where_sql_parts.append("status = ?")
            params.append(normalized_status)
        else:
            where_sql_parts.append("status LIKE ?")
            params.append(f"%{normalized_status}%")
    if normalized_query:
        where_sql_parts.append(
            "("
            "source_title LIKE ? OR "
            "target_title LIKE ? OR "
            "source_chat_username LIKE ? OR "
            "target_username LIKE ? OR "
            "CAST(source_chat_id AS TEXT) LIKE ? OR "
            "CAST(target_chat_id AS TEXT) LIKE ? OR "
            "run_id LIKE ? OR "
            "job_id LIKE ?"
            ")"
        )
        query_like = f"%{normalized_query}%"
        params.extend([query_like] * 8)
    where_sql = ""
    if where_sql_parts:
        where_sql = "WHERE " + " AND ".join(where_sql_parts)
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM admin_clone_runs
            {where_sql}
            """,
            params,
        )
        row = cur.fetchone()
        return int(row["c"] or 0) if row is not None else 0
    finally:
        cur.close()


def load_clone_run_detail(
    conn: sqlite3.Connection,
    run_id: str,
) -> dict | None:
    normalized_run_id = _clean_text(run_id)
    if not normalized_run_id:
        return None
    run = load_clone_run(conn, normalized_run_id)
    if run is None:
        return None
    plan = load_latest_clone_plan(conn, normalized_run_id)
    migration = load_latest_clone_migration(conn, normalized_run_id)
    preview = build_clone_timeline_replay_preview(
        conn,
        run_id=normalized_run_id,
        source_chat_id=int(run["source_chat_id"]),
    )
    summary = load_clone_message_mapping_summary(conn, normalized_run_id)
    recent_mappings = list_clone_message_mappings(
        conn,
        run_id=normalized_run_id,
        limit=100,
    )
    failures = list_clone_message_mappings(
        conn,
        run_id=normalized_run_id,
        status="error",
        limit=100,
    )
    return {
        "run": run,
        "plan": plan,
        "migration": migration,
        "timeline_preview": preview,
        "mapping_summary": summary,
        "recent_mappings": recent_mappings,
        "failure_items": failures,
    }


def load_clone_message_mapping_summary(
    conn: sqlite3.Connection,
    run_id: str,
) -> dict:
    normalized_run_id = _clean_text(run_id)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) AS done,
                SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error,
                SUM(CASE WHEN mode = 'text_replay' THEN 1 ELSE 0 END) AS text_total,
                SUM(CASE WHEN mode = 'text_replay' AND status = 'done' THEN 1 ELSE 0 END) AS text_done,
                SUM(CASE WHEN mode = 'text_replay' AND status = 'error' THEN 1 ELSE 0 END) AS text_error,
                SUM(CASE WHEN mode = 'media_copy' THEN 1 ELSE 0 END) AS media_total,
                SUM(CASE WHEN mode = 'media_copy' AND status = 'done' THEN 1 ELSE 0 END) AS media_done,
                SUM(CASE WHEN mode = 'media_copy' AND status = 'error' THEN 1 ELSE 0 END) AS media_error,
                SUM(CASE WHEN mode = 'media_group_copy' THEN 1 ELSE 0 END) AS media_group_total,
                SUM(CASE WHEN mode = 'media_group_copy' AND status = 'done' THEN 1 ELSE 0 END) AS media_group_done,
                SUM(CASE WHEN mode = 'media_group_copy' AND status = 'error' THEN 1 ELSE 0 END) AS media_group_error,
                MAX(sent_at) AS latest_sent_at,
                MAX(updated_at) AS latest_updated_at
            FROM admin_clone_message_map
            WHERE run_id = ?
            """,
            (normalized_run_id,),
        )
        return _clone_message_mapping_summary_from_row(cur.fetchone())
    finally:
        cur.close()


def list_clone_message_mappings(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    status: Any = "",
    mode: Any = "",
    limit: Any = 100,
    offset: Any = 0,
) -> list[dict]:
    try:
        normalized_limit = int(limit)
    except (TypeError, ValueError):
        normalized_limit = 100
    normalized_limit = max(1, min(1000, normalized_limit))
    try:
        normalized_offset = int(offset)
    except (TypeError, ValueError):
        normalized_offset = 0
    normalized_offset = max(0, normalized_offset)
    normalized_run_id = _clean_text(run_id)
    normalized_status = _clean_text(status)
    normalized_mode = _clean_text(mode)
    where_sql_parts = ["run_id = ?"]
    params: list[Any] = [normalized_run_id]
    if normalized_status:
        where_sql_parts.append("status = ?")
        params.append(normalized_status)
    if normalized_mode:
        where_sql_parts.append("mode = ?")
        params.append(normalized_mode)
    params.extend([normalized_limit, normalized_offset])
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT *
            FROM admin_clone_message_map
            WHERE {" AND ".join(where_sql_parts)}
            ORDER BY updated_at DESC, created_at DESC, id DESC
            LIMIT ? OFFSET ?
            """,
            params,
        )
        return [
            mapping
            for mapping in (
                _clone_message_mapping_from_row(row) for row in cur.fetchall()
            )
            if mapping is not None
        ]
    finally:
        cur.close()


def count_clone_message_mappings(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    status: Any = "",
    mode: Any = "",
) -> int:
    normalized_run_id = _clean_text(run_id)
    normalized_status = _clean_text(status)
    normalized_mode = _clean_text(mode)
    where_sql_parts = ["run_id = ?"]
    params: list[Any] = [normalized_run_id]
    if normalized_status:
        where_sql_parts.append("status = ?")
        params.append(normalized_status)
    if normalized_mode:
        where_sql_parts.append("mode = ?")
        params.append(normalized_mode)

    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM admin_clone_message_map
            WHERE {" AND ".join(where_sql_parts)}
            """,
            params,
        )
        row = cur.fetchone()
        return int(row["c"] or 0) if row is not None else 0
    finally:
        cur.close()


def delete_clone_run(
    conn: sqlite3.Connection,
    *,
    run_id: str,
) -> bool:
    normalized_run_id = _clean_text(run_id)
    if not normalized_run_id:
        return False
    cur = conn.cursor()
    try:
        cur.execute(
            "DELETE FROM admin_clone_message_map WHERE run_id = ?",
            (normalized_run_id,),
        )
        cur.execute(
            "DELETE FROM admin_clone_migrations WHERE run_id = ?",
            (normalized_run_id,),
        )
        cur.execute(
            "DELETE FROM admin_clone_plans WHERE run_id = ?",
            (normalized_run_id,),
        )
        cur.execute(
            "DELETE FROM admin_clone_runs WHERE run_id = ?",
            (normalized_run_id,),
        )
        deleted = cur.rowcount > 0
        conn.commit()
        return deleted
    finally:
        cur.close()
