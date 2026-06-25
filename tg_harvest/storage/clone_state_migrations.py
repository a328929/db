import sqlite3
from typing import Any

from tg_harvest.storage.clone_common import (
    _clean_text,
    _json_text,
    _now_iso,
    _optional_int,
)
from tg_harvest.storage.clone_state_common import (
    _append_optional_fields,
    _clone_migration_from_row,
    _commit_and_load_required,
    _execute_update_and_reload,
    _query_one,
)


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
        return _commit_and_load_required(
            conn,
            load_fn=lambda: load_clone_migration(conn, normalized_migration_id),
            missing_message="clone migration 创建后读取失败",
        )
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
    _append_optional_fields(
        fields,
        values,
        {
            "status": _clean_text(status) if status is not None else None,
            "phase": _clean_text(phase) if phase is not None else None,
            "target_chat_id": _optional_int(target_chat_id)
            if target_chat_id is not None
            else None,
            "target_title": _clean_text(target_title)
            if target_title is not None
            else None,
            "target_write_account": _clean_text(target_write_account)
            if target_write_account is not None
            else None,
            "requested_limit": int(requested_limit)
            if requested_limit is not None
            else None,
            "send_delay_ms": int(send_delay_ms)
            if send_delay_ms is not None
            else None,
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
            "completed_at": _clean_text(completed_at)
            if completed_at is not None
            else None,
        },
    )

    normalized_migration_id = _clean_text(migration_id)
    values.append(normalized_migration_id)
    return _execute_update_and_reload(
        conn,
        f"""
        UPDATE admin_clone_migrations
        SET {", ".join(fields)}
        WHERE migration_id = ?
        """,
        values,
        load_fn=lambda: load_clone_migration(conn, normalized_migration_id),
    )


def load_clone_migration(conn: sqlite3.Connection, migration_id: str) -> dict | None:
    return _query_one(
        conn,
        """
        SELECT *
        FROM admin_clone_migrations
        WHERE migration_id = ?
        LIMIT 1
        """,
        (_clean_text(migration_id),),
        _clone_migration_from_row,
    )


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
    return _query_one(
        conn,
        f"""
        SELECT *
        FROM admin_clone_migrations
        WHERE run_id = ?
          {mode_clause}
        ORDER BY updated_at DESC, created_at DESC, migration_id DESC
        LIMIT 1
        """,
        params,
        _clone_migration_from_row,
    )
