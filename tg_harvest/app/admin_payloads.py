import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

from tg_harvest.domain.chat_titles import chat_sort_key, chat_title_or_fallback
from tg_harvest.domain.coerce import safe_int
from tg_harvest.storage import sync_scheduler
from tg_harvest.storage.db_health import build_database_health_payload
from tg_harvest.storage.row_access import row_int as _row_int

_ADMIN_SYNC_WINDOWS: tuple[dict[str, Any], ...] = (
    {"key": "live", "label": "实时", "seconds": 0, "is_live": True},
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
    return datetime.now(UTC)


def _format_utc_text(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def _parse_utc_text(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=UTC
        )
    except ValueError:
        return None


def _age_seconds_from_text(base_now: datetime, value: Any) -> int | None:
    parsed = _parse_utc_text(value)
    if parsed is None:
        return None
    return max(0, int((base_now - parsed).total_seconds()))


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


def _build_sync_window_rows(
    *,
    windows: list[dict[str, Any]],
    counts_by_key: dict[str, dict[str, int]],
    timestamps_by_key: dict[str, dict[str, str]],
    live_message_count: int,
    live_chat_count: int,
    latest_created_at: str,
) -> list[dict[str, Any]]:
    window_rows: list[dict[str, Any]] = []
    for window in windows:
        window_key = str(window["key"])
        if bool(window.get("is_live", False)):
            window_rows.append(
                {
                    "window_key": window_key,
                    "label": str(window["label"]),
                    "seconds": int(window["seconds"]),
                    "is_live": True,
                    "message_count": int(live_message_count),
                    "chat_count": int(live_chat_count),
                    "oldest_created_at": "",
                    "latest_created_at": latest_created_at,
                }
            )
            continue

        count_item = counts_by_key.get(window_key, {})
        timestamp_item = timestamps_by_key.get(window_key, {})
        window_rows.append(
            {
                "window_key": window_key,
                "label": str(window["label"]),
                "seconds": int(window["seconds"]),
                "is_live": False,
                "message_count": int(count_item.get("message_count") or 0),
                "chat_count": int(count_item.get("chat_count") or 0),
                "oldest_created_at": str(
                    timestamp_item.get("oldest_created_at") or ""
                ),
                "latest_created_at": str(
                    timestamp_item.get("latest_created_at") or ""
                ),
            }
        )
    return window_rows


def _evaluate_sync_health(
    *,
    base_now: datetime,
    windows_by_key: dict[str, dict[str, Any]],
    latest_created_at: str,
    health_snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    snapshot = dict(health_snapshot or {})
    reasons: list[dict[str, str]] = []
    actions: list[str] = []
    status = "healthy"

    ten_min_window = dict(windows_by_key.get("10m") or {})
    ten_min_count = int(ten_min_window.get("message_count") or 0)
    latest_message_age_seconds = _age_seconds_from_text(base_now, latest_created_at)
    listener_enabled = bool(snapshot.get("listener_enabled"))
    public_probe_enabled = bool(snapshot.get("public_probe_enabled"))
    active_listener_count = int(snapshot.get("active_listener_count") or 0)
    configured_listener_count = int(snapshot.get("configured_listener_count") or 0)
    worker_thread_alive = bool(snapshot.get("worker_thread_alive"))
    refresh_thread_alive = bool(snapshot.get("refresh_thread_alive"))
    public_probe_thread_alive = bool(snapshot.get("public_probe_thread_alive"))
    queue_size = int(snapshot.get("queue_size") or 0)
    scheduler_enabled = bool(snapshot.get("scheduler_enabled"))
    due_update_count = int(snapshot.get("due_update_count") or 0)
    in_flight_update_count = int(snapshot.get("in_flight_update_count") or 0)
    last_update_success_age_seconds = snapshot.get("last_update_success_age_seconds")
    last_update_failure_age_seconds = snapshot.get("last_update_failure_age_seconds")
    last_update_failure_message = str(
        snapshot.get("last_update_failure_message") or ""
    ).strip()
    last_probe_status = str(snapshot.get("last_probe_status") or "").strip()
    last_probe_result_age_seconds = snapshot.get("last_probe_result_age_seconds")
    tracked_chat_count = int(snapshot.get("tracked_chat_count") or 0)

    if listener_enabled and configured_listener_count > 0 and active_listener_count <= 0:
        status = "critical"
        reasons.append(
            {
                "code": "listener_disconnected",
                "severity": "critical",
                "message": "监听线程未连接到任何账号，实时同步可能已中断。",
            }
        )
        actions.append("检查 Telegram 账号会话是否失效，并执行一次即时诊断。")

    if listener_enabled and not worker_thread_alive:
        status = "critical"
        reasons.append(
            {
                "code": "worker_not_alive",
                "severity": "critical",
                "message": "监听更新处理线程未运行，事件可能无法入库。",
            }
        )
        actions.append("重启服务或检查后台运行时线程状态。")

    if listener_enabled and not refresh_thread_alive:
        status = "critical"
        reasons.append(
            {
                "code": "refresh_not_alive",
                "severity": "critical",
                "message": "监听缓存刷新线程未运行，群组追踪状态可能已过期。",
            }
        )

    if public_probe_enabled and not public_probe_thread_alive:
        status = "critical" if status == "healthy" else status
        reasons.append(
            {
                "code": "probe_not_alive",
                "severity": "critical",
                "message": "低频轮巡探测线程未运行，补救同步能力已失效。",
            }
        )

    if queue_size >= 100:
        status = "critical"
        reasons.append(
            {
                "code": "queue_backlog_high",
                "severity": "critical",
                "message": f"监听更新队列积压 {queue_size} 项，入库处理明显滞后。",
            }
        )
        actions.append("检查数据库写入性能和 Telegram 接口可用性。")
    elif queue_size >= 20 and status == "healthy":
        status = "warning"
        reasons.append(
            {
                "code": "queue_backlog_warn",
                "severity": "warning",
                "message": f"监听更新队列积压 {queue_size} 项，入库可能出现延迟。",
            }
        )

    if scheduler_enabled and due_update_count >= 20 and status == "healthy":
        status = "warning"
        reasons.append(
            {
                "code": "scheduler_due_backlog",
                "severity": "warning",
                "message": f"智能调度已有 {due_update_count} 个到期任务等待执行。",
            }
        )

    if scheduler_enabled and in_flight_update_count > 0 and status == "healthy":
        reasons.append(
            {
                "code": "scheduler_in_flight",
                "severity": "info",
                "message": f"智能调度当前有 {in_flight_update_count} 个任务正在执行。",
            }
        )

    if (
        ten_min_count <= 0
        and tracked_chat_count > 0
        and latest_message_age_seconds is not None
        and latest_message_age_seconds >= 10 * 60
    ):
        severity = "warning"
        message = "最近 10 分钟无新入库消息。"
        if (
            active_listener_count <= 0
            or bool(last_update_failure_message)
            or (last_probe_status and last_probe_status in {"failed", "flood_wait", "no_account"})
        ):
            severity = "critical"
            message = "最近 10 分钟无新入库消息，且监听/探测状态异常，疑似同步链路故障。"
        if severity == "critical":
            status = "critical"
        elif status == "healthy":
            status = "warning"
        reasons.append(
            {
                "code": "no_recent_ingest",
                "severity": severity,
                "message": message,
            }
        )
        actions.append("执行一次即时诊断，并核查目标群组近 10 分钟是否确有新消息。")

    if (
        last_update_success_age_seconds is not None
        and last_update_success_age_seconds >= 10 * 60
        and tracked_chat_count > 0
        and status == "healthy"
    ):
        status = "warning"
        reasons.append(
            {
                "code": "update_success_stale",
                "severity": "warning",
                "message": "监听链路超过 10 分钟没有成功完成单群更新。",
            }
        )

    if bool(last_update_failure_message) and (
        last_update_failure_age_seconds is None
        or int(last_update_failure_age_seconds) <= 10 * 60
    ):
        if status == "healthy":
            status = "warning"
        reasons.append(
            {
                "code": "recent_update_failure",
                "severity": "warning" if status != "critical" else "critical",
                "message": "最近一次增量更新失败：" + last_update_failure_message,
            }
        )

    if last_probe_status in {"failed", "flood_wait", "no_account"} and (
        last_probe_result_age_seconds is None
        or int(last_probe_result_age_seconds) <= 10 * 60
    ):
        if status == "healthy":
            status = "warning"
        reasons.append(
            {
                "code": "recent_probe_issue",
                "severity": "warning" if status != "critical" else "critical",
                "message": "最近一次低频轮巡探测状态异常：" + last_probe_status,
            }
        )

    if not actions:
        actions.append("当前未发现明确异常，可继续观察或手动刷新。")

    return {
        "status": status,
        "checked_at": _format_utc_text(base_now),
        "latest_message_age_seconds": latest_message_age_seconds,
        "reasons": reasons,
        "actions": actions,
        "listener": snapshot,
    }


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
            FROM (
                SELECT
                    pk,
                    chat_id,
                    message_id,
                    msg_date_text,
                    msg_type,
                    content,
                    content_norm,
                    created_at
                FROM messages
                ORDER BY
                    created_at DESC,
                    chat_id DESC,
                    message_id DESC,
                    pk DESC
                LIMIT ?
            ) m
            JOIN chats c
              ON c.chat_id = m.chat_id
            ORDER BY
                m.created_at DESC,
                m.chat_id DESC,
                m.message_id DESC,
                m.pk DESC
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


def build_admin_sync_stats_payload(
    conn: sqlite3.Connection,
    *,
    health_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    windows = [dict(item) for item in _ADMIN_SYNC_WINDOWS]
    base_now = _utc_now()
    generated_at = _format_utc_text(base_now)
    aggregate_windows = [
        window for window in windows if not bool(window.get("is_live", False))
    ]
    cutoff_texts_by_key = {
        str(window["key"]): _format_utc_text(
            base_now - timedelta(seconds=int(window["seconds"]))
        )
        for window in aggregate_windows
    }

    if not _table_exists(conn, "messages"):
        window_rows = [_empty_admin_sync_window_payload(window) for window in windows]
        latest_window = window_rows[-1] if window_rows else {}
        scheduler_payload = sync_scheduler.build_scheduler_summary(
            conn,
            health_snapshot=health_snapshot,
        )
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
            "scheduler": scheduler_payload,
            "windows": window_rows,
            "default_window_key": "live",
            "health": _evaluate_sync_health(
                base_now=base_now,
                windows_by_key={
                    str(item["window_key"]): dict(item) for item in window_rows
                },
                latest_created_at="",
                health_snapshot=health_snapshot,
            ),
        }

    cur = conn.cursor()
    try:
        latest_created_at = ""
        live_message_count = 0
        live_chat_count = 0
        counts_by_key: dict[str, dict[str, int]] = {}
        timestamps_by_key: dict[str, dict[str, str]] = {}

        if aggregate_windows:
            largest_window = aggregate_windows[-1]
            largest_cutoff_text = cutoff_texts_by_key[str(largest_window["key"])]
            aggregate_metrics_sql: list[str] = ["SELECT"]
            for index, window in enumerate(aggregate_windows):
                window_key = str(window["key"])
                aggregate_metrics_sql.append(
                    "    SUM(CASE WHEN created_at >= ? THEN 1 ELSE 0 END) AS"
                    f" message_count_{index},"
                )
                aggregate_metrics_sql.append(
                    "    COALESCE(MIN(CASE WHEN created_at >= ? THEN created_at END), '') AS"
                    f" oldest_created_at_{index},"
                )
                aggregate_metrics_sql.append(
                    "    COALESCE(MAX(CASE WHEN created_at >= ? THEN created_at END), '') AS"
                    + (
                        f" latest_created_at_{index},"
                        if index < len(aggregate_windows) - 1
                        else f" latest_created_at_{index}"
                    )
                )
                cutoff_text = cutoff_texts_by_key[window_key]
                counts_by_key[window_key] = {
                    "message_count": 0,
                    "chat_count": 0,
                }
            aggregate_metrics_sql.extend(["FROM messages", "WHERE created_at >= ?"])
            aggregate_metrics_params: list[Any] = []
            for window in aggregate_windows:
                cutoff_text = cutoff_texts_by_key[str(window["key"])]
                aggregate_metrics_params.extend(
                    [cutoff_text, cutoff_text, cutoff_text]
                )
            aggregate_metrics_params.append(largest_cutoff_text)
            cur.execute(
                "\n".join(aggregate_metrics_sql),
                aggregate_metrics_params,
            )
            aggregate_metrics_row = cur.fetchone()

            aggregate_chat_sql: list[str] = ["SELECT"]
            for index, window in enumerate(aggregate_windows):
                cutoff_text = cutoff_texts_by_key[str(window["key"])]
                aggregate_chat_sql.append(
                    "    SUM(CASE WHEN last_message_created_at >= ? THEN 1 ELSE 0 END) AS"
                    + (
                        f" chat_count_{index},"
                        if index < len(aggregate_windows) - 1
                        else f" chat_count_{index}"
                    )
                )
            aggregate_chat_sql.append("FROM chats")
            aggregate_chat_params = [
                cutoff_texts_by_key[str(window["key"])]
                for window in aggregate_windows
            ]
            cur.execute(
                "\n".join(aggregate_chat_sql),
                aggregate_chat_params,
            )
            aggregate_chat_row = cur.fetchone()

            for index, window in enumerate(aggregate_windows):
                window_key = str(window["key"])
                counts_by_key[window_key] = {
                    "message_count": _row_int(
                        aggregate_metrics_row, f"message_count_{index}"
                    ),
                    "chat_count": _row_int(aggregate_chat_row, f"chat_count_{index}"),
                }
                timestamps_by_key[window_key] = {
                    "oldest_created_at": str(
                        aggregate_metrics_row[f"oldest_created_at_{index}"] or ""
                    )
                    if aggregate_metrics_row
                    else "",
                    "latest_created_at": str(
                        aggregate_metrics_row[f"latest_created_at_{index}"] or ""
                    )
                    if aggregate_metrics_row
                    else "",
                }

        cur.execute(
            """
            SELECT
                COALESCE(MAX(last_message_created_at), '') AS latest_created_at,
                COALESCE(SUM(message_count), 0) AS message_count,
                SUM(CASE WHEN message_count > 0 THEN 1 ELSE 0 END) AS chat_count
            FROM chats
            """
        )
        latest_row = cur.fetchone()
        latest_created_at = (
            str(latest_row["latest_created_at"] or "") if latest_row else ""
        )
        live_message_count = _row_int(latest_row, "message_count")
        live_chat_count = _row_int(latest_row, "chat_count")
    finally:
        cur.close()

    window_rows = _build_sync_window_rows(
        windows=windows,
        counts_by_key=counts_by_key,
        timestamps_by_key=timestamps_by_key,
        live_message_count=live_message_count,
        live_chat_count=live_chat_count,
        latest_created_at=latest_created_at,
    )

    latest_window = window_rows[-1] if window_rows else {}
    scheduler_payload = sync_scheduler.build_scheduler_summary(
        conn,
        health_snapshot=health_snapshot,
    )
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
        "scheduler": scheduler_payload,
        "windows": window_rows,
        "default_window_key": "live",
        "health": _evaluate_sync_health(
            base_now=base_now,
            windows_by_key={str(item["window_key"]): dict(item) for item in window_rows},
            latest_created_at=latest_created_at,
            health_snapshot=health_snapshot,
        ),
    }


def build_admin_storage_health_payload(
    conn: sqlite3.Connection,
    *,
    cfg: Any | None = None,
) -> dict[str, Any]:
    """Return the lightweight, read-only database capacity health snapshot."""
    return build_database_health_payload(conn, cfg=cfg)


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
