import sqlite3
from typing import Any

from tg_harvest.storage.clone_common import _clean_text, _now_iso, _optional_int
from tg_harvest.storage.clone_state_common import (
    _build_clone_message_mapping_filters,
    _clone_message_mapping_from_row,
    _clone_message_mapping_summary_from_row,
    _commit_and_load_required,
    _normalize_offset,
    _query_all,
    _query_count,
    _query_one,
)


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
        return _commit_and_load_required(
            conn,
            load_fn=lambda: load_clone_message_mapping(
                conn,
                run_id=normalized_run_id,
                source_chat_id=int(source_chat_id),
                source_message_id=int(source_message_id),
                chunk_index=int(chunk_index),
                mode=normalized_mode,
            ),
            missing_message="clone message mapping 写入后读取失败",
        )
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
    return _query_one(
        conn,
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
        _clone_message_mapping_from_row,
    )


def load_clone_message_mapping_summary(
    conn: sqlite3.Connection,
    run_id: str,
) -> dict:
    normalized_run_id = _clean_text(run_id)
    return _query_one(
        conn,
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
        _clone_message_mapping_summary_from_row,
    )


def list_clone_message_mappings(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    status: Any = "",
    mode: Any = "",
    limit: Any = 100,
    offset: Any = 0,
) -> list[dict]:
    from tg_harvest.storage.clone_common import _normalize_bounded_int

    normalized_limit = _normalize_bounded_int(
        limit,
        default=100,
        minimum=1,
        maximum=1000,
    )
    normalized_offset = _normalize_offset(offset)
    where_sql, params = _build_clone_message_mapping_filters(
        run_id=run_id,
        status=status,
        mode=mode,
    )
    params.extend([normalized_limit, normalized_offset])
    return _query_all(
        conn,
        f"""
        SELECT *
        FROM admin_clone_message_map
        WHERE {where_sql}
        ORDER BY updated_at DESC, created_at DESC, id DESC
        LIMIT ? OFFSET ?
        """,
        params,
        _clone_message_mapping_from_row,
    )


def count_clone_message_mappings(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    status: Any = "",
    mode: Any = "",
) -> int:
    where_sql, params = _build_clone_message_mapping_filters(
        run_id=run_id,
        status=status,
        mode=mode,
    )
    return _query_count(
        conn,
        f"""
        SELECT COUNT(*) AS c
        FROM admin_clone_message_map
        WHERE {where_sql}
        """,
        params,
    )
