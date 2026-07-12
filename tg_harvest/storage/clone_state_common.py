import sqlite3
from typing import Any

from tg_harvest.storage.clone_common import (
    _clean_text,
    _json_value,
    _optional_int,
    _safe_int,
)
from tg_harvest.storage.row_access import row_int as _row_int


def _append_optional_fields(
    fields: list[str],
    values: list[Any],
    optional_fields: dict[str, Any],
) -> None:
    for column_name, value in optional_fields.items():
        if value is None:
            continue
        fields.append(f"{column_name} = ?")
        values.append(value)


def _build_clone_run_filters(
    *,
    source_chat_id: Any = None,
    status: Any = "",
    q: Any = "",
) -> tuple[str, list[Any]]:
    normalized_status = _clean_text(status).lower()
    normalized_query = _clean_text(q)
    where_sql_parts: list[str] = []
    params: list[Any] = []
    if source_chat_id not in (None, ""):
        where_sql_parts.append("source_chat_id = ?")
        params.append(int(source_chat_id))
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
        params.extend([f"%{normalized_query}%"] * 8)
    where_sql = ""
    if where_sql_parts:
        where_sql = "WHERE " + " AND ".join(where_sql_parts)
    return where_sql, params


def _build_clone_message_mapping_filters(
    *,
    run_id: str,
    status: Any = "",
    mode: Any = "",
) -> tuple[str, list[Any]]:
    where_sql_parts = ["run_id = ?"]
    params: list[Any] = [_clean_text(run_id)]
    normalized_status = _clean_text(status)
    normalized_mode = _clean_text(mode)
    if normalized_status:
        where_sql_parts.append("status = ?")
        params.append(normalized_status)
    if normalized_mode:
        where_sql_parts.append("mode = ?")
        params.append(normalized_mode)
    return " AND ".join(where_sql_parts), params


def _normalize_offset(value: Any) -> int:
    return max(0, _safe_int(value))


def _query_one(
    conn: sqlite3.Connection,
    sql: str,
    params: tuple[Any, ...] | list[Any],
    row_mapper,
):
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        return row_mapper(cur.fetchone())
    finally:
        cur.close()


def _query_all(
    conn: sqlite3.Connection,
    sql: str,
    params: tuple[Any, ...] | list[Any],
    row_mapper,
) -> list[Any]:
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        return [
            item
            for item in (row_mapper(row) for row in cur.fetchall())
            if item is not None
        ]
    finally:
        cur.close()


def _query_count(
    conn: sqlite3.Connection,
    sql: str,
    params: tuple[Any, ...] | list[Any],
    *,
    field_name: str = "c",
) -> int:
    row = _query_one(conn, sql, params, lambda item: item)
    return _row_int(row, field_name)


def _commit_and_reload(conn: sqlite3.Connection, *, load_fn):
    conn.commit()
    return load_fn()


def _commit_and_load_required(
    conn: sqlite3.Connection,
    *,
    load_fn,
    missing_message: str,
):
    loaded = _commit_and_reload(conn, load_fn=load_fn)
    if loaded is None:
        raise RuntimeError(missing_message)
    return loaded


def _execute_update_and_reload(
    conn: sqlite3.Connection,
    sql: str,
    params: list[Any],
    *,
    load_fn,
):
    cur = conn.cursor()
    try:
        cur.execute(sql, params)
        if cur.rowcount <= 0:
            conn.commit()
            return None
        return _commit_and_reload(conn, load_fn=load_fn)
    finally:
        cur.close()


def _clone_run_from_row(row: sqlite3.Row | None) -> dict | None:
    if row is None:
        return None
    return {
        "run_id": str(row["run_id"] or ""),
        "job_id": str(row["job_id"] or ""),
        "source_chat_id": _row_int(row, "source_chat_id"),
        "source_title": str(row["source_title"] or ""),
        "source_chat_username": str(row["source_chat_username"] or ""),
        "source_chat_type": str(row["source_chat_type"] or ""),
        "source_message_count": _row_int(row, "source_message_count"),
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
        "requested_limit": _row_int(row, "requested_limit"),
        "send_delay_ms": _row_int(row, "send_delay_ms"),
        "text_total": _row_int(row, "text_total"),
        "text_sent": _row_int(row, "text_sent"),
        "text_skipped": _row_int(row, "text_skipped"),
        "text_failed": _row_int(row, "text_failed"),
        "media_total": _row_int(row, "media_total"),
        "media_sent": _row_int(row, "media_sent"),
        "media_skipped": _row_int(row, "media_skipped"),
        "media_failed": _row_int(row, "media_failed"),
        "media_group_total": _row_int(row, "media_group_total"),
        "media_group_sent": _row_int(row, "media_group_sent"),
        "media_group_skipped": _row_int(row, "media_group_skipped"),
        "media_group_failed": _row_int(row, "media_group_failed"),
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
        "id": _row_int(row, "id"),
        "migration_id": str(row["migration_id"] or ""),
        "run_id": str(row["run_id"] or ""),
        "plan_id": str(row["plan_id"] or ""),
        "source_chat_id": _row_int(row, "source_chat_id"),
        "source_message_id": _row_int(row, "source_message_id"),
        "source_msg_date_ts": _optional_int(row["source_msg_date_ts"]),
        "source_msg_date_text": str(row["source_msg_date_text"] or ""),
        "target_chat_id": _row_int(row, "target_chat_id"),
        "target_message_id": _optional_int(row["target_message_id"]),
        "delivery_random_id": (
            value if (value := _row_int(row, "delivery_random_id")) > 0 else None
        ),
        "delivery_account": str(row["delivery_account"] or ""),
        "chunk_index": _row_int(row, "chunk_index"),
        "chunk_count": _row_int(row, "chunk_count", 1),
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
        "total": _row_int(row, "total"),
        "done": _row_int(row, "done"),
        "error": _row_int(row, "error"),
        "text_total": _row_int(row, "text_total"),
        "text_done": _row_int(row, "text_done"),
        "text_error": _row_int(row, "text_error"),
        "media_total": _row_int(row, "media_total"),
        "media_done": _row_int(row, "media_done"),
        "media_error": _row_int(row, "media_error"),
        "media_group_total": _row_int(row, "media_group_total"),
        "media_group_done": _row_int(row, "media_group_done"),
        "media_group_error": _row_int(row, "media_group_error"),
        "latest_sent_at": str(row["latest_sent_at"] or ""),
        "latest_updated_at": str(row["latest_updated_at"] or ""),
    }
