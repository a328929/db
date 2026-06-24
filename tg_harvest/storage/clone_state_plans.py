import sqlite3
from typing import Any

from tg_harvest.storage.clone_common import (
    _clean_text,
    _json_text,
    _normalize_bounded_int,
    _now_iso,
)
from tg_harvest.storage.clone_state_common import (
    _append_optional_fields,
    _clone_plan_from_row,
    _commit_and_load_required,
    _execute_update_and_reload,
    _query_all,
    _query_one,
)


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
        return _commit_and_load_required(
            conn,
            load_fn=lambda: load_clone_plan(conn, normalized_plan_id),
            missing_message="clone plan 创建后读取失败",
        )
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
    _append_optional_fields(
        fields,
        values,
        {
            "status": _clean_text(status) if status is not None else None,
            "source_access": _clean_text(source_access)
            if source_access is not None
            else None,
            "target_access": _clean_text(target_access)
            if target_access is not None
            else None,
            "primary_session_status": _clean_text(primary_session_status)
            if primary_session_status is not None
            else None,
            "secondary_session_status": _clean_text(secondary_session_status)
            if secondary_session_status is not None
            else None,
            "migration_account": _clean_text(migration_account)
            if migration_account is not None
            else None,
            "text_strategy": _clean_text(text_strategy)
            if text_strategy is not None
            else None,
            "media_strategy": _clean_text(media_strategy)
            if media_strategy is not None
            else None,
            "media_group_strategy": _clean_text(media_group_strategy)
            if media_group_strategy is not None
            else None,
            "avatar_strategy": _clean_text(avatar_strategy)
            if avatar_strategy is not None
            else None,
            "blocking_issues_json": _json_text(blocking_issues, default="[]")
            if blocking_issues is not None
            else None,
            "warnings_json": _json_text(warnings, default="[]")
            if warnings is not None
            else None,
            "capabilities_json": _json_text(capabilities, default="{}")
            if capabilities is not None
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

    normalized_plan_id = _clean_text(plan_id)
    values.append(normalized_plan_id)
    return _execute_update_and_reload(
        conn,
        f"""
        UPDATE admin_clone_plans
        SET {", ".join(fields)}
        WHERE plan_id = ?
        """,
        values,
        load_fn=lambda: load_clone_plan(conn, normalized_plan_id),
    )


def load_clone_plan(conn: sqlite3.Connection, plan_id: str) -> dict | None:
    return _query_one(
        conn,
        """
        SELECT *
        FROM admin_clone_plans
        WHERE plan_id = ?
        LIMIT 1
        """,
        (_clean_text(plan_id),),
        _clone_plan_from_row,
    )


def load_latest_clone_plan(conn: sqlite3.Connection, run_id: str) -> dict | None:
    return _query_one(
        conn,
        """
        SELECT *
        FROM admin_clone_plans
        WHERE run_id = ?
        ORDER BY updated_at DESC, created_at DESC, plan_id DESC
        LIMIT 1
        """,
        (_clean_text(run_id),),
        _clone_plan_from_row,
    )


def list_clone_plans(
    conn: sqlite3.Connection,
    *,
    run_id: Any = None,
    limit: Any = 20,
) -> list[dict]:
    normalized_limit = _normalize_bounded_int(
        limit,
        default=20,
        minimum=1,
        maximum=100,
    )

    params: list[Any] = []
    where_sql = ""
    if run_id not in (None, ""):
        where_sql = "WHERE run_id = ?"
        params.append(_clean_text(run_id))
    params.append(normalized_limit)

    return _query_all(
        conn,
        f"""
        SELECT *
        FROM admin_clone_plans
        {where_sql}
        ORDER BY updated_at DESC, created_at DESC, plan_id DESC
        LIMIT ?
        """,
        params,
        _clone_plan_from_row,
    )
