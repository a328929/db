from __future__ import annotations

import math
import sqlite3
from typing import Any

from tg_harvest.search.data_version import (
    format_data_version,
    read_database_fingerprint,
)
from tg_harvest.search.params import SearchParams, split_query_media_duration

_FROM_SQL = """
FROM messages m
LEFT JOIN chats c ON c.chat_id = m.chat_id
LEFT JOIN message_media mm
  ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id
"""


def _filters(params: SearchParams) -> tuple[list[str], list[Any]]:
    clauses: list[str] = []
    values: list[Any] = []
    if params.chat_id is not None:
        clauses.append("m.chat_id = ?")
        values.append(int(params.chat_id))
    if params.start_ts is not None:
        clauses.append("m.msg_date_ts >= ?")
        values.append(int(params.start_ts))
    if params.end_ts_exclusive is not None:
        clauses.append("m.msg_date_ts < ?")
        values.append(int(params.end_ts_exclusive))

    search_type = str(params.search_type or "all").lower()
    type_values = {
        "text": ("TEXT",),
        "image": ("PHOTO",),
        "video": ("VIDEO", "GIF", "VIDEO_NOTE"),
        "audio": ("AUDIO", "VOICE"),
    }.get(search_type)
    if type_values:
        clauses.append(f"m.msg_type IN ({','.join('?' for _ in type_values)})")
        values.extend(type_values)

    _text, parsed_duration = split_query_media_duration(params.raw_query)
    duration = params.duration_sec if params.duration_sec is not None else parsed_duration
    if duration is not None:
        clauses.append("m.msg_type IN ('VIDEO', 'GIF', 'VIDEO_NOTE')")
        clauses.append("COALESCE(mm.duration_sec, 0) = ?")
        values.append(int(duration))
    return clauses, values


def _sort(params: SearchParams) -> tuple[str, str, str]:
    search_type = str(params.search_type or "all").lower()
    requested = str(params.sort_by_req or "time").lower()
    direction = "ASC" if str(params.order_req or "desc").lower() == "asc" else "DESC"
    if requested == "relevance":
        requested = "time"
    if search_type in {"all", "text"} and requested in {"size", "duration"}:
        requested = "time"
    if search_type == "image" and requested == "duration":
        requested = "time"
    if requested == "size":
        return "COALESCE(mm.file_size, 0)", "size", direction
    if requested == "duration":
        return "COALESCE(mm.duration_sec, 0)", "duration", direction
    return "m.msg_date_ts", "time", direction


def _media_sorted_ids(
    cur: sqlite3.Cursor,
    params: SearchParams,
    *,
    where_sql: str,
    values: list[Any],
    sort: str,
    direction: str,
    offset: int,
    limit: int,
) -> list[int]:
    column = "file_size" if sort == "size" else "duration_sec"
    suffix = "" if params.chat_id is not None else "_global"
    media_from = f"""
        FROM message_media mm INDEXED BY idx_media_sort_{sort}{suffix}
        JOIN messages m ON m.chat_id = mm.chat_id AND m.message_id = mm.message_id
    """
    tie_order = f"m.chat_id {direction}, m.message_id {direction}, m.pk {direction}"
    ids: list[int] = []
    # Visit disjoint value ranges in order. The common descending page is
    # satisfied entirely by the media index, without scanning missing metadata.
    for sign in ((">", "=", "<") if direction == "DESC" else ("<", "=", ">")):
        if sign == "=":
            from_sql = """FROM messages m LEFT JOIN message_media mm
                ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id"""
            condition = f"COALESCE(mm.{column}, 0) = 0"
            order = tie_order
        else:
            from_sql = media_from
            condition = f"mm.{column} {sign} 0"
            order = f"mm.{column} {direction}, {tie_order}"
        remaining = offset + limit - len(ids)
        cur.execute(
            f"SELECT m.pk {from_sql} WHERE ({where_sql}) AND {condition} "
            f"ORDER BY {order} LIMIT ?",
            (*values, remaining),
        )
        ids.extend(int(row[0]) for row in cur.fetchall())
        if len(ids) >= offset + limit:
            break
    return ids[offset : offset + limit]


def sqlite_browse_payload_service(
    conn: sqlite3.Connection,
    params: SearchParams,
    *,
    page_size: int,
    max_count: int,
    map_search_items_fn,
    max_browsable_results: int = 100000,
    **_unused: Any,
) -> dict[str, Any]:
    clauses, values = _filters(params)
    where_sql = " AND ".join(clauses) if clauses else "1=1"
    sort_field, effective_sort, direction = _sort(params)
    from_sql = _FROM_SQL
    order_sql = f"{sort_field} {direction}, m.chat_id {direction}, m.message_id {direction}, m.pk {direction}"
    effective_max = max(1, min(int(max_count), int(max_browsable_results)))
    page = max(1, int(params.page))
    offset = (page - 1) * int(page_size)
    if offset >= effective_max:
        raise ValueError(
            f"浏览最多支持前 {math.ceil(effective_max / page_size)} 页"
        )

    cur = conn.cursor()
    try:
        total = -1
        total_is_capped = False
        if params.count_only or not params.skip_count:
            _text, parsed_duration = split_query_media_duration(params.raw_query)
            duration = (
                params.duration_sec
                if params.duration_sec is not None
                else parsed_duration
            )
            count_from_sql = from_sql if duration is not None else "FROM messages m"
            cur.execute(
                f"SELECT COUNT(*) FROM (SELECT 1 {count_from_sql} WHERE {where_sql} LIMIT ?)",
                (*values, effective_max + 1),
            )
            observed = int(cur.fetchone()[0] or 0)
            total_is_capped = observed > effective_max
            total = min(observed, effective_max)

        rows: list[sqlite3.Row] = []
        if not params.count_only:
            ids = None
            result_where, result_values = where_sql, list(values)
            result_offset = offset
            if effective_sort in {"size", "duration"}:
                ids = _media_sorted_ids(
                    cur, params, where_sql=where_sql, values=values,
                    sort=effective_sort, direction=direction, offset=offset,
                    limit=min(int(page_size), effective_max - offset),
                )
                result_where = f"m.pk IN ({','.join('?' for _ in ids)})" if ids else "0"
                result_values = ids
                result_offset = 0
            cur.execute(
                f"""
                SELECT m.pk, m.chat_id, c.chat_title, c.chat_username,
                       m.message_id, m.msg_date_text, m.msg_date_ts, m.msg_type,
                       m.content, m.grouped_id, m.is_promo, mm.file_name,
                       mm.file_size, mm.mime_type, mm.media_kind, mm.duration_sec
                {from_sql}
                WHERE {result_where}
                ORDER BY {order_sql}
                LIMIT ? OFFSET ?
                """,
                (*result_values, min(int(page_size), effective_max - offset), result_offset),
            )
            rows = cur.fetchall()
            if ids is not None:
                by_pk = {int(row["pk"]): row for row in rows}
                rows = [by_pk[pk] for pk in ids if pk in by_pk]
            if params.skip_count and page == 1 and not rows:
                total = 0

    finally:
        cur.close()

    payload = {
        "ok": True,
        "query": params.raw_query,
        "search_backend": "sqlite_browse",
        "page": page,
        "page_size": int(page_size),
        "data_version": format_data_version(read_database_fingerprint(conn)),
        "total": total,
        "total_pages": math.ceil(total / page_size) if total > 0 else 0,
        "total_is_capped": total_is_capped,
        "effective_sort": effective_sort,
        "effective_order": direction.lower(),
        "items": map_search_items_fn(rows),
    }
    if params.count_only:
        payload["chat_facets"] = []
    return payload
