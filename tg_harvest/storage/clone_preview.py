import sqlite3
from typing import Any

from tg_harvest.domain.clone_plan import CLONE_TEXT_REPLAY_CHUNK_MAX_LEN
from tg_harvest.storage.clone_common import (
    _clean_text,
    _normalize_bounded_int,
    _optional_int,
    _safe_int,
)
from tg_harvest.storage.row_access import row_int as _row_int


def build_clone_source_snapshot(
    conn: sqlite3.Connection,
    *,
    source_chat_id: int,
) -> dict[str, int]:
    """Describe the locally durable source boundary used by a clone plan."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            WITH recoverable_messages AS (
                SELECT message_id, COALESCE(msg_date_ts, 0) AS msg_date_ts
                FROM messages
                WHERE chat_id = ?
                UNION ALL
                SELECT
                    a.message_id,
                    COALESCE(a.msg_date_ts, 0) AS msg_date_ts
                FROM clone_media_group_anchors AS a
                WHERE a.chat_id = ?
                  AND NOT EXISTS (
                        SELECT 1
                        FROM messages AS m
                        WHERE m.chat_id = a.chat_id
                          AND m.message_id = a.message_id
                  )
            )
            SELECT
                COUNT(*) AS message_count,
                COALESCE(MAX(message_id), 0) AS latest_message_id,
                COALESCE(MAX(msg_date_ts), 0) AS latest_message_ts
            FROM recoverable_messages
            """,
            (int(source_chat_id), int(source_chat_id)),
        )
        row = cur.fetchone()
        return {
            "message_count": _row_int(row, "message_count"),
            "latest_message_id": _row_int(row, "latest_message_id"),
            "latest_message_ts": _row_int(row, "latest_message_ts"),
        }
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
        return _row_int(row, "c")
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
        return _row_int(row, "c")
    finally:
        cur.close()


def build_clone_text_replay_preview(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    source_chat_id: int,
    max_source_message_id: Any = None,
) -> dict:
    normalized_run_id = _clean_text(run_id)
    normalized_source_chat_id = int(source_chat_id)
    normalized_max_message_id = _optional_int(max_source_message_id)
    max_filter = "AND message_id <= ?" if normalized_max_message_id else ""
    chunk_size = CLONE_TEXT_REPLAY_CHUNK_MAX_LEN
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
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
                  {max_filter}
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
            [
                normalized_source_chat_id,
                *([normalized_max_message_id] if normalized_max_message_id else []),
                chunk_size,
                chunk_size,
                normalized_run_id,
                normalized_source_chat_id,
            ],
        )
        row = cur.fetchone()
        text_total = _row_int(row, "text_total")
        text_completed = _row_int(row, "text_completed")

        cur.execute(
            f"""
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
              {max_filter}
            """,
            [
                normalized_source_chat_id,
                *([normalized_max_message_id] if normalized_max_message_id else []),
            ],
        )
        skip_row = cur.fetchone()
        return {
            "run_id": normalized_run_id,
            "source_chat_id": normalized_source_chat_id,
            "source_snapshot_message_id": normalized_max_message_id or 0,
            "chunk_size": chunk_size,
            "text_total": text_total,
            "text_completed": text_completed,
            "text_remaining": max(0, text_total - text_completed),
            "text_partial": _row_int(row, "text_partial"),
            "text_error": _row_int(row, "text_error"),
            "text_chunks_total": _row_int(row, "text_chunks_total"),
            "text_chunks_done": _row_int(row, "text_chunks_done"),
            "text_chunks_error": _row_int(row, "text_chunks_error"),
            "total_messages": _row_int(skip_row, "total_messages"),
            "media_skipped": _row_int(skip_row, "media_skipped"),
            "grouped_skipped": _row_int(skip_row, "grouped_skipped"),
            "empty_text_skipped": _row_int(skip_row, "empty_text_skipped"),
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
    normalized_limit = _normalize_bounded_int(
        limit,
        default=200,
        minimum=1,
        maximum=1000,
    )
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
                "chat_id": _row_int(row, "chat_id"),
                "message_id": _row_int(row, "message_id"),
                "msg_date_text": str(row["msg_date_text"] or ""),
                "msg_date_ts": _row_int(row, "msg_date_ts"),
                "sort_ts": _row_int(row, "sort_ts"),
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
    max_source_message_id: Any = None,
) -> dict:
    normalized_run_id = _clean_text(run_id)
    normalized_source_chat_id = int(source_chat_id)
    normalized_max_message_id = _optional_int(max_source_message_id)
    max_filter = "AND m.message_id <= ?" if normalized_max_message_id else ""
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            WITH source_media AS (
                SELECT m.chat_id, m.message_id, m.grouped_id
                FROM messages AS m
                WHERE m.chat_id = ?
                  {max_filter}
                  AND COALESCE(m.has_media, 0) = 1
                UNION ALL
                SELECT a.chat_id, a.message_id, a.grouped_id
                FROM clone_media_group_anchors AS a
                WHERE a.chat_id = ?
                  {"AND a.message_id <= ?" if normalized_max_message_id else ""}
                  AND NOT EXISTS (
                        SELECT 1
                        FROM messages AS existing
                        WHERE existing.chat_id = a.chat_id
                          AND existing.message_id = a.message_id
                  )
            ),
            group_stats AS (
                SELECT
                    m.chat_id,
                    m.grouped_id,
                    COUNT(*) AS current_item_count,
                    MIN(m.message_id) AS first_message_id,
                    MAX(m.message_id) AS last_message_id,
                    MAX(m.message_id) - MIN(m.message_id) + 1 AS message_id_span
                FROM source_media m
                WHERE m.grouped_id IS NOT NULL
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
                FROM source_media m
                LEFT JOIN media_groups mg
                  ON mg.chat_id = m.chat_id
                 AND mg.grouped_id = m.grouped_id
                LEFT JOIN group_stats gs
                  ON gs.chat_id = m.chat_id
                 AND gs.grouped_id = m.grouped_id
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
            [
                normalized_source_chat_id,
                *([normalized_max_message_id] if normalized_max_message_id else []),
                normalized_source_chat_id,
                *([normalized_max_message_id] if normalized_max_message_id else []),
                normalized_run_id,
                normalized_source_chat_id,
            ],
        )
        row = cur.fetchone()
        solo_total = _row_int(row, "solo_media_total")
        complete_group_items = _row_int(row, "complete_group_items")
        solo_done = _row_int(row, "solo_media_done")
        group_done = _row_int(row, "complete_group_items_done")
        media_total = _row_int(row, "media_total")
        grouped_items_done = _row_int(row, "grouped_items_done")
        media_group_candidate_total = _row_int(row, "media_group_candidate_total")
        media_group_candidate_items = _row_int(row, "media_group_candidate_items")
        incomplete_group_total = _row_int(row, "incomplete_group_total")
        incomplete_group_items = _row_int(row, "incomplete_group_items")
        suspected_incomplete_group_total = _row_int(
            row, "suspected_incomplete_group_total"
        )
        suspected_incomplete_group_items = _row_int(
            row, "suspected_incomplete_group_items"
        )
        missing_group_meta_total = _row_int(row, "missing_group_meta_total")
        missing_group_meta_items = _row_int(row, "missing_group_meta_items")
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
            "source_snapshot_message_id": normalized_max_message_id or 0,
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
            "complete_group_total": _row_int(row, "complete_group_total"),
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
    normalized_limit = _normalize_bounded_int(
        limit,
        default=100,
        minimum=1,
        maximum=500,
    )
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
                "chat_id": _row_int(row, "chat_id"),
                "message_id": _row_int(row, "message_id"),
                "msg_date_text": str(row["msg_date_text"] or ""),
                "msg_date_ts": _optional_int(row["msg_date_ts"]),
                "sort_ts": _row_int(row, "sort_ts"),
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
    normalized_limit = _normalize_bounded_int(
        limit,
        default=50,
        minimum=1,
        maximum=200,
    )
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
                "chat_id": _row_int(row, "chat_id"),
                "grouped_id": _row_int(row, "grouped_id"),
                "sort_ts": _row_int(row, "sort_ts"),
                "item_count": _row_int(row, "item_count"),
                "active_items": _row_int(row, "active_items"),
                "current_item_count": _row_int(row, "current_item_count"),
                "first_message_id": _row_int(row, "first_message_id"),
                "last_message_id": _row_int(row, "last_message_id"),
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
    max_source_message_id: Any = None,
) -> list[dict]:
    normalized_max_message_id = _optional_int(max_source_message_id)
    max_filter = "AND message_id <= ?" if normalized_max_message_id else ""
    params: list[Any] = [int(chat_id), int(grouped_id)]
    if normalized_max_message_id:
        params.append(normalized_max_message_id)
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            WITH recoverable_group_messages AS (
                SELECT
                    chat_id,
                    message_id,
                    msg_date_text,
                    msg_date_ts,
                    grouped_id,
                    COALESCE(
                        NULLIF(TRIM(content), ''),
                        NULLIF(TRIM(content_norm), ''),
                        ''
                    ) AS caption
                FROM messages
                WHERE chat_id = ?
                  AND grouped_id = ?
                  AND COALESCE(has_media, 0) = 1
                  {max_filter}
                UNION ALL
                SELECT
                    a.chat_id,
                    a.message_id,
                    a.msg_date_text,
                    a.msg_date_ts,
                    a.grouped_id,
                    '' AS caption
                FROM clone_media_group_anchors AS a
                WHERE a.chat_id = ?
                  AND a.grouped_id = ?
                  {"AND a.message_id <= ?" if normalized_max_message_id else ""}
                  AND NOT EXISTS (
                        SELECT 1
                        FROM messages AS m
                        WHERE m.chat_id = a.chat_id
                          AND m.message_id = a.message_id
                  )
            )
            SELECT
                chat_id,
                message_id,
                msg_date_text,
                msg_date_ts,
                COALESCE(msg_date_ts, 0) AS sort_ts,
                caption
            FROM recoverable_group_messages
            ORDER BY message_id ASC
            """,
            [
                *params,
                int(chat_id),
                int(grouped_id),
                *([normalized_max_message_id] if normalized_max_message_id else []),
            ],
        )
        return [
            {
                "chat_id": _row_int(row, "chat_id"),
                "message_id": _row_int(row, "message_id"),
                "msg_date_text": str(row["msg_date_text"] or ""),
                "msg_date_ts": _optional_int(row["msg_date_ts"]),
                "sort_ts": _row_int(row, "sort_ts"),
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
    max_source_message_id: Any = None,
) -> dict:
    normalized_max_message_id = _optional_int(max_source_message_id)
    text_max_filter = "AND message_id <= ?" if normalized_max_message_id else ""
    text_preview = build_clone_text_replay_preview(
        conn,
        run_id=run_id,
        source_chat_id=source_chat_id,
        max_source_message_id=normalized_max_message_id,
    )
    media_preview = build_clone_media_copy_preview(
        conn,
        run_id=run_id,
        source_chat_id=source_chat_id,
        max_source_message_id=normalized_max_message_id,
    )
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            WITH timeline_items AS (
                SELECT message_id AS sort_message_id
                FROM messages
                WHERE chat_id = ?
                  {text_max_filter}
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
                  {text_max_filter}
                  AND COALESCE(has_media, 0) = 1
                  AND grouped_id IS NULL
                UNION ALL
                SELECT MIN(group_message_id) AS sort_message_id
                FROM (
                    SELECT
                        m.grouped_id,
                        m.message_id AS group_message_id
                    FROM messages AS m
                    WHERE m.chat_id = ?
                      {"AND m.message_id <= ?" if normalized_max_message_id else ""}
                      AND COALESCE(m.has_media, 0) = 1
                      AND m.grouped_id IS NOT NULL
                    UNION ALL
                    SELECT
                        a.grouped_id,
                        a.message_id AS group_message_id
                    FROM clone_media_group_anchors AS a
                    WHERE a.chat_id = ?
                      {"AND a.message_id <= ?" if normalized_max_message_id else ""}
                      AND NOT EXISTS (
                            SELECT 1
                            FROM messages AS existing
                            WHERE existing.chat_id = a.chat_id
                              AND existing.message_id = a.message_id
                      )
                ) AS recoverable_groups
                GROUP BY grouped_id
            )
            SELECT COUNT(*) AS c
            FROM timeline_items
            """,
            [
                int(source_chat_id),
                *([normalized_max_message_id] if normalized_max_message_id else []),
                int(source_chat_id),
                *([normalized_max_message_id] if normalized_max_message_id else []),
                int(source_chat_id),
                *([normalized_max_message_id] if normalized_max_message_id else []),
                int(source_chat_id),
                *([normalized_max_message_id] if normalized_max_message_id else []),
            ],
        )
        row = cur.fetchone()
        timeline_items_total = _row_int(row, "c")
    finally:
        cur.close()

    text_remaining = _safe_int(text_preview.get("text_remaining"))
    media_remaining = _safe_int(media_preview.get("media_remaining"))
    text_total = _safe_int(text_preview.get("text_total"))
    media_total = _safe_int(media_preview.get("media_total"))
    media_group_total = _safe_int(media_preview.get("media_group_candidate_total"))
    return {
        "run_id": _clean_text(run_id),
        "source_chat_id": int(source_chat_id),
        "source_snapshot_message_id": normalized_max_message_id or 0,
        "mode": "timeline_replay",
        "timeline_items_total": timeline_items_total,
        "timeline_source_messages_total": text_total + media_total,
        "timeline_remaining": text_remaining + media_remaining,
        "text_total": text_total,
        "text_completed": _safe_int(text_preview.get("text_completed")),
        "text_remaining": text_remaining,
        "media_total": media_total,
        "media_completed": _safe_int(media_preview.get("media_completed")),
        "media_remaining": media_remaining,
        "media_group_total": media_group_total,
        "media_group_candidate_items": _safe_int(
            media_preview.get("media_group_candidate_items")
        ),
        "db_self_check_risk_group_total": _safe_int(
            media_preview.get("db_self_check_risk_group_total")
        ),
        "db_self_check_risk_group_items": _safe_int(
            media_preview.get("db_self_check_risk_group_items")
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
    max_source_message_id: Any = None,
    limit: Any = 100,
) -> list[dict]:
    normalized_limit = _normalize_bounded_int(
        limit,
        default=100,
        minimum=1,
        maximum=500,
    )
    normalized_run_id = _clean_text(run_id)
    normalized_after_ts = _optional_int(after_ts)
    normalized_after_message_id = _optional_int(after_message_id)
    normalized_max_message_id = _optional_int(max_source_message_id)
    text_max_filter = "AND message_id <= ?" if normalized_max_message_id else ""
    media_max_filter = "AND m.message_id <= ?" if normalized_max_message_id else ""

    where_cursor = ""
    params: list[Any] = [int(chat_id)]
    if normalized_max_message_id:
        params.append(normalized_max_message_id)
    params.append(int(chat_id))
    if normalized_max_message_id:
        params.append(normalized_max_message_id)
    params.extend(
        [
            CLONE_TEXT_REPLAY_CHUNK_MAX_LEN,
            CLONE_TEXT_REPLAY_CHUNK_MAX_LEN,
            int(chat_id),
        ]
    )
    if normalized_max_message_id:
        params.append(normalized_max_message_id)
    params.append(int(chat_id))
    if normalized_max_message_id:
        params.append(normalized_max_message_id)
    params.extend(
        [
            normalized_run_id,
            int(chat_id),
            normalized_run_id,
            int(chat_id),
            normalized_run_id,
            int(chat_id),
            normalized_run_id,
        ]
    )
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
            WITH group_sources AS (
                SELECT
                    m.chat_id,
                    m.message_id,
                    m.grouped_id,
                    m.msg_date_text,
                    m.msg_date_ts
                FROM messages AS m
                WHERE m.chat_id = ?
                  {media_max_filter}
                  AND COALESCE(m.has_media, 0) = 1
                  AND m.grouped_id IS NOT NULL
                UNION ALL
                SELECT
                    a.chat_id,
                    a.message_id,
                    a.grouped_id,
                    a.msg_date_text,
                    a.msg_date_ts
                FROM clone_media_group_anchors AS a
                WHERE a.chat_id = ?
                  {"AND a.message_id <= ?" if normalized_max_message_id else ""}
                  AND NOT EXISTS (
                        SELECT 1
                        FROM messages AS existing
                        WHERE existing.chat_id = a.chat_id
                          AND existing.message_id = a.message_id
                  )
            ),
            timeline_items AS (
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
                  {text_max_filter}
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
                  {media_max_filter}
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
                FROM group_sources m
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
                FROM group_sources m
                JOIN admin_clone_message_map cmm
                  ON cmm.run_id = ?
                 AND cmm.source_chat_id = m.chat_id
                 AND cmm.source_message_id = m.message_id
                 AND cmm.mode = 'media_group_copy'
                 AND cmm.status = 'done'
                WHERE m.chat_id = ?
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
                "chat_id": _row_int(row, "chat_id"),
                "source_message_id": _row_int(row, "source_message_id"),
                "grouped_id": _optional_int(row["grouped_id"]),
                "sort_ts": _row_int(row, "sort_ts"),
                "sort_message_id": _row_int(row, "sort_message_id"),
                "msg_date_text": str(row["msg_date_text"] or ""),
                "msg_date_ts": _optional_int(row["msg_date_ts"]),
                "text": str(row["text"] or ""),
                "item_count": _row_int(row, "item_count"),
            }
            for row in cur.fetchall()
        ]
    finally:
        cur.close()
