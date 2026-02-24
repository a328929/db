# -*- coding: utf-8 -*-
import math
import sqlite3
from typing import Any, Callable, Dict, List, Optional, Tuple

from tg_harvest.search_params import SearchParams
from tg_harvest.search_sql_builder import _build_search_query_spec


def _execute_count_query(
    cur: sqlite3.Cursor,
    count_sql: str,
    sql_params: List[Any],
    count_limit: int,
    max_count: int,
    page_size: int,
) -> Tuple[int, bool, int]:
    cur.execute(count_sql, sql_params + [count_limit])
    counted = int(cur.fetchone()["c"] or 0)
    total_is_capped = counted > max_count
    total = min(counted, max_count)
    total_pages = math.ceil(total / page_size) if total > 0 else 0
    return total, total_is_capped, total_pages


def _execute_rows_query(
    cur: sqlite3.Cursor,
    query_sql: str,
    sql_params: List[Any],
    page: int,
    total_pages: int,
    page_size: int,
) -> Tuple[List[sqlite3.Row], int]:
    effective_page = page
    if total_pages > 0 and effective_page > total_pages:
        effective_page = total_pages

    offset = (effective_page - 1) * page_size if total_pages > 0 else 0
    cur.execute(query_sql, sql_params + [page_size, offset])
    rows = cur.fetchall()
    return rows, effective_page


def _run_search_query(
    conn: sqlite3.Connection,
    count_sql: str,
    query_sql: str,
    sql_params: List[Any],
    page: int,
    count_limit: int,
    max_count: int,
    page_size: int,
) -> Tuple[List[sqlite3.Row], int, int, bool, int]:
    cur = conn.cursor()
    try:
        total, total_is_capped, total_pages = _execute_count_query(
            cur,
            count_sql,
            sql_params,
            count_limit,
            max_count,
            page_size,
        )
        rows, effective_page = _execute_rows_query(cur, query_sql, sql_params, page, total_pages, page_size)
        return rows, total, total_pages, total_is_capped, effective_page
    finally:
        cur.close()


def _build_payload_from_spec(
    conn: sqlite3.Connection,
    params: SearchParams,
    spec: Dict[str, Any],
    *,
    page_size: int,
    max_count: int,
    map_search_items_fn: Callable[[List[sqlite3.Row]], List[Dict[str, Any]]],
) -> Dict[str, Any]:
    rows, total, total_pages, total_is_capped, effective_page = _run_search_query(
        conn,
        str(spec["count_sql"]),
        str(spec["query_sql"]),
        list(spec["sql_params"]),
        params.page,
        count_limit=int(spec["count_limit"]),
        max_count=max_count,
        page_size=page_size,
    )
    return {
        "ok": True,
        "query": params.raw_query,
        "fts_query": str(spec["match_query"]),
        "page": effective_page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
        "total_is_capped": total_is_capped,
        "effective_sort": str(spec["effective_sort"]),
        "effective_order": str(spec["effective_order"]).lower(),
        "items": map_search_items_fn(rows),
    }


def _search_payload_service(
    conn: sqlite3.Connection,
    params: SearchParams,
    *,
    fts_enabled: bool,
    from_sql: str,
    page_size: int,
    max_count: int,
    tokenize_query_fn: Callable[[str], List[Tuple[str, str]]],
    to_fts_match_fn: Callable[[str], str],
    map_search_items_fn: Callable[[List[sqlite3.Row]], List[Dict[str, Any]]],
) -> Dict[str, Any]:
    primary_spec = _build_search_query_spec(
        params,
        from_sql=from_sql,
        fts_enabled=fts_enabled,
        max_count=max_count,
        tokenize_query_fn=tokenize_query_fn,
        to_fts_match_fn=to_fts_match_fn,
    )
    payload = _build_payload_from_spec(
        conn,
        params,
        primary_spec,
        page_size=page_size,
        max_count=max_count,
        map_search_items_fn=map_search_items_fn,
    )
    if int(payload.get("total") or 0) != 0:
        return payload

    fallback_spec = _build_search_query_spec(
        params,
        from_sql=from_sql,
        fts_enabled=False,
        max_count=max_count,
        tokenize_query_fn=tokenize_query_fn,
        to_fts_match_fn=to_fts_match_fn,
        force_like=True,
    )
    if not bool(fallback_spec.get("has_text_filter")):
        return payload

    fallback_payload = _build_payload_from_spec(
        conn,
        params,
        fallback_spec,
        page_size=page_size,
        max_count=max_count,
        map_search_items_fn=map_search_items_fn,
    )
    if int(fallback_payload.get("total") or 0) <= 0:
        return payload

    fallback_payload["fts_query"] = to_fts_match_fn(params.raw_query)
    return fallback_payload
