import sqlite3
from typing import Any

from tg_harvest.storage.clone_common import (
    _chat_title_or_fallback,
    _clean_text,
    _default_clone_title,
    _normalize_bounded_int,
    _normalize_plan_json,
    _now_iso,
    _optional_int,
)
from tg_harvest.storage.clone_state_common import (
    _append_optional_fields,
    _build_clone_run_filters,
    _clone_run_from_row,
    _commit_and_load_required,
    _execute_update_and_reload,
    _normalize_offset,
    _query_all,
    _query_count,
    _query_one,
)


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
        return _commit_and_load_required(
            conn,
            load_fn=lambda: load_clone_run(conn, normalized_run_id),
            missing_message="clone run 创建后读取失败",
        )
    finally:
        cur.close()


def load_clone_run(conn: sqlite3.Connection, run_id: str) -> dict | None:
    return _query_one(
        conn,
        """
        SELECT *
        FROM admin_clone_runs
        WHERE run_id = ?
        LIMIT 1
        """,
        (_clean_text(run_id),),
        _clone_run_from_row,
    )


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
    _append_optional_fields(
        fields,
        values,
        {
            "status": _clean_text(status) if status is not None else None,
            "phase": _clean_text(phase) if phase is not None else None,
            "target_chat_id": _optional_int(target_chat_id)
            if target_chat_id is not None
            else None,
            "target_access_hash": _clean_text(target_access_hash)
            if target_access_hash is not None
            else None,
            "target_title": _clean_text(target_title)
            if target_title is not None
            else None,
            "target_kind": _clean_text(target_kind)
            if target_kind is not None
            else None,
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
            "completed_at": _clean_text(completed_at)
            if completed_at is not None
            else None,
        },
    )

    normalized_run_id = _clean_text(run_id)
    values.append(normalized_run_id)
    return _execute_update_and_reload(
        conn,
        f"""
        UPDATE admin_clone_runs
        SET {", ".join(fields)}
        WHERE run_id = ?
        """,
        values,
        load_fn=lambda: load_clone_run(conn, normalized_run_id),
    )


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
    normalized_limit = _normalize_bounded_int(
        limit,
        default=20,
        minimum=1,
        maximum=100,
    )
    normalized_offset = _normalize_offset(offset)
    normalized_sort = _clean_text(sort).lower()
    where_sql, params = _build_clone_run_filters(
        source_chat_id=source_chat_id,
        status=status,
        q=q,
    )
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

    return _query_all(
        conn,
        f"""
        SELECT *
        FROM admin_clone_runs
        {where_sql}
        ORDER BY {order_sql}
        LIMIT ? OFFSET ?
        """,
        params,
        _clone_run_from_row,
    )


def count_clone_runs(
    conn: sqlite3.Connection,
    *,
    source_chat_id: Any = None,
    status: Any = "",
    q: Any = "",
) -> int:
    where_sql, params = _build_clone_run_filters(
        source_chat_id=source_chat_id,
        status=status,
        q=q,
    )
    return _query_count(
        conn,
        f"""
        SELECT COUNT(*) AS c
        FROM admin_clone_runs
        {where_sql}
        """,
        params,
    )


def load_clone_run_detail(
    conn: sqlite3.Connection,
    run_id: str,
    *,
    load_latest_clone_plan_fn,
    load_latest_clone_migration_fn,
    load_clone_message_mapping_summary_fn,
    list_clone_message_mappings_fn,
) -> dict | None:
    normalized_run_id = _clean_text(run_id)
    if not normalized_run_id:
        return None
    run = load_clone_run(conn, normalized_run_id)
    if run is None:
        return None
    plan = load_latest_clone_plan_fn(conn, normalized_run_id)
    migration = load_latest_clone_migration_fn(conn, normalized_run_id)
    summary = load_clone_message_mapping_summary_fn(conn, normalized_run_id)
    recent_mappings = list_clone_message_mappings_fn(
        conn,
        run_id=normalized_run_id,
        limit=100,
    )
    failures = list_clone_message_mappings_fn(
        conn,
        run_id=normalized_run_id,
        status="error",
        limit=100,
    )
    return {
        "run": run,
        "plan": plan,
        "migration": migration,
        "mapping_summary": summary,
        "recent_mappings": recent_mappings,
        "failure_items": failures,
    }
