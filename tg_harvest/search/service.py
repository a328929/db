import math
import sqlite3
from collections.abc import Callable
from typing import Any

from tg_harvest.search import cache as _search_cache
from tg_harvest.search import maintenance as _search_maintenance
from tg_harvest.search.params import SearchParams
from tg_harvest.search.sql_builder import _build_search_query_spec, _make_type_clause
from tg_harvest.storage import search_terms as _search_terms

_CHAT_FACET_LIMIT = 12


def _execute_count_query(
    cur: sqlite3.Cursor,
    count_sql: str,
    sql_params: list[Any],
    count_limit: int,
    max_count: int,
    page_size: int,
) -> tuple[int, bool, int]:
    cur.execute(count_sql, sql_params + [count_limit])
    counted = int(cur.fetchone()["c"] or 0)
    total_is_capped = counted > max_count
    total = min(counted, max_count)
    total_pages = math.ceil(total / page_size) if total > 0 else 0
    return total, total_is_capped, total_pages


def _execute_rows_query(
    cur: sqlite3.Cursor,
    query_sql: str,
    query_sql_skip: str | None,
    sql_params: list[Any],
    page: int,
    total_pages: int,
    page_size: int,
    *,
    use_skip_optimized_query: bool = False,
) -> tuple[list[sqlite3.Row], int]:
    effective_page = page
    if total_pages > 0 and effective_page > total_pages:
        effective_page = total_pages

    offset = (effective_page - 1) * page_size if total_pages > 0 else 0
    sql_to_run = query_sql_skip if use_skip_optimized_query and query_sql_skip else query_sql
    cur.execute(sql_to_run, sql_params + [page_size, offset])
    rows = cur.fetchall()
    return rows, effective_page


def _has_pending_message_search_term_rebuilds(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='message_search_terms_rebuild_queue' LIMIT 1"
        )
        if cur.fetchone() is None:
            return False
        cur.execute("SELECT 1 FROM message_search_terms_rebuild_queue LIMIT 1")
        return cur.fetchone() is not None
    finally:
        cur.close()


def _should_try_like_fallback(
    conn: sqlite3.Connection, primary_spec: dict[str, Any]
) -> bool:
    if not bool(primary_spec.get("has_text_filter")):
        return False
    if not bool(primary_spec.get("uses_text_index")):
        return False

    # Trigram FTS is maintained synchronously by SQLite triggers. The separate
    # 1/2-char CJK helper index is rebuilt asynchronously, so LIKE fallback is
    # only useful while that queue has pending rows.
    if bool(primary_spec.get("uses_auxiliary_terms")):
        return (
            not _search_terms.message_search_terms_are_current(conn)
            or _has_pending_message_search_term_rebuilds(conn)
        )
    return False


def _payload_has_results(payload: dict[str, Any]) -> bool:
    total_val = int(payload.get("total") or 0)
    if total_val > 0:
        return True
    return total_val == -1 and len(payload.get("items") or []) > 0


def _resolve_precise_count(
    conn: sqlite3.Connection,
    cur: sqlite3.Cursor,
    *,
    count_sql: str,
    sql_params: list[Any],
    params: SearchParams,
    count_limit: int,
    max_count: int,
    page_size: int,
) -> tuple[int, bool, int]:
    cached_count = _try_fast_count(
        conn,
        params,
        page_size=page_size,
        max_count=max_count,
    )
    if cached_count is None:
        cache_key = _search_cache._make_count_cache_key(
            conn,
            count_sql=count_sql,
            sql_params=sql_params,
            count_limit=count_limit,
            page_size=page_size,
        )
        cached_count = _search_cache._get_cached_count(cache_key)
        if cached_count is None:
            cached_count = _execute_count_query(
                cur,
                count_sql,
                sql_params,
                count_limit,
                max_count,
                page_size,
            )
            _search_cache._put_cached_count(cache_key, cached_count)
    return cached_count


def _fast_count_from_chat_summary(
    conn: sqlite3.Connection, chat_id: int | None
) -> int | None:
    cur = conn.cursor()
    try:
        if chat_id is None:
            cur.execute("SELECT COALESCE(SUM(message_count), 0) AS c FROM chats")
        else:
            cur.execute(
                "SELECT COALESCE(message_count, 0) AS c FROM chats WHERE chat_id = ?",
                (chat_id,),
            )
        row = cur.fetchone()
        if row is None:
            return 0
        return int(row["c"] or 0)
    finally:
        cur.close()


def _fast_count_by_type(
    conn: sqlite3.Connection, chat_id: int | None, search_type: str
) -> int | None:
    type_clause, type_params = _make_type_clause(search_type)
    if not type_clause:
        return None
    type_clause = type_clause.replace("m.", "")

    cur = conn.cursor()
    try:
        where_parts = [type_clause]
        sql_params: list[Any] = list(type_params)
        if chat_id is not None:
            where_parts.append("chat_id = ?")
            sql_params.append(chat_id)
        where_sql = " AND ".join(where_parts)
        cur.execute(f"SELECT COUNT(*) AS c FROM messages WHERE {where_sql}", sql_params)
        row = cur.fetchone()
        if row is None:
            return 0
        return int(row["c"] or 0)
    finally:
        cur.close()


def _try_fast_count(
    conn: sqlite3.Connection,
    params: SearchParams,
    *,
    page_size: int,
    max_count: int,
) -> tuple[int, bool, int] | None:
    if (params.raw_query or "").strip():
        return None
    if params.duration_sec is not None:
        return None
    if params.start_ts is not None or params.end_ts_exclusive is not None:
        return None

    total: int | None
    if (params.search_type or "all").lower() == "all":
        total = _fast_count_from_chat_summary(conn, params.chat_id)
    else:
        total = _fast_count_by_type(conn, params.chat_id, params.search_type)

    if total is None:
        return None

    total_is_capped = total > max_count
    effective_total = min(total, max_count)
    total_pages = math.ceil(effective_total / page_size) if effective_total > 0 else 0
    return effective_total, total_is_capped, total_pages


def _run_search_query(
    conn: sqlite3.Connection,
    count_sql: str,
    query_sql: str,
    query_sql_skip: str | None,
    prefer_skip_query: bool,
    sql_params: list[Any],
    params: SearchParams,
    count_limit: int,
    max_count: int,
    page_size: int,
) -> tuple[list[sqlite3.Row], int, int, bool, int]:
    cur = conn.cursor()
    try:
        if params.skip_count and not params.count_only:
            total, total_is_capped, total_pages = -1, False, 0
        else:
            total, total_is_capped, total_pages = _resolve_precise_count(
                conn,
                cur,
                count_sql=count_sql,
                sql_params=sql_params,
                params=params,
                count_limit=count_limit,
                max_count=max_count,
                page_size=page_size,
            )

        if params.count_only:
            rows: list[sqlite3.Row] = []
            effective_page = params.page
        else:
            # When skipping count, we assume we always have enough pages to fetch the current one
            temp_total_pages = total_pages if not params.skip_count else params.page
            rows, effective_page = _execute_rows_query(
                cur,
                query_sql,
                query_sql_skip,
                sql_params,
                params.page,
                temp_total_pages,
                page_size,
                use_skip_optimized_query=params.skip_count and prefer_skip_query,
            )
            if params.skip_count and params.page == 1 and not rows:
                total, total_is_capped, total_pages = 0, False, 0
            if (
                params.skip_count
                and params.page > 1
                and not rows
            ):
                total, total_is_capped, total_pages = _resolve_precise_count(
                    conn,
                    cur,
                    count_sql=count_sql,
                    sql_params=sql_params,
                    params=params,
                    count_limit=count_limit,
                    max_count=max_count,
                    page_size=page_size,
                )
                if total_pages > 0:
                    rows, effective_page = _execute_rows_query(
                        cur,
                        query_sql,
                        query_sql_skip,
                        sql_params,
                        params.page,
                        total_pages,
                        page_size,
                        use_skip_optimized_query=False,
                    )

        return rows, total, total_pages, total_is_capped, effective_page
    finally:
        cur.close()


def _execute_chat_facet_query(
    cur: sqlite3.Cursor,
    chat_facet_sql: str,
    sql_params: list[Any],
    *,
    scan_limit: int,
    limit: int = _CHAT_FACET_LIMIT,
) -> list[dict[str, Any]]:
    cur.execute(
        chat_facet_sql,
        sql_params + [max(1, int(scan_limit)), max(1, int(limit))],
    )
    facets: list[dict[str, Any]] = []
    for row in cur.fetchall():
        chat_id = int(row["chat_id"])
        title = str(row["chat_title"] or "").strip() or f"Chat {chat_id}"
        facets.append(
            {
                "chat_id": chat_id,
                "chat_title": title,
                "count": int(row["match_count"] or 0),
            }
        )
    return facets


def _build_payload_from_spec(
    conn: sqlite3.Connection,
    params: SearchParams,
    spec: dict[str, Any],
    *,
    page_size: int,
    max_count: int,
    map_search_items_fn: Callable[[list[sqlite3.Row]], list[dict[str, Any]]],
    include_chat_facets: bool = False,
) -> dict[str, Any]:
    data_fingerprint = _search_cache._read_database_fingerprint(conn)
    rows, total, total_pages, total_is_capped, effective_page = _run_search_query(
        conn,
        str(spec["count_sql"]),
        str(spec["query_sql"]),
        None if spec.get("query_sql_skip") is None else str(spec["query_sql_skip"]),
        bool(spec.get("prefer_skip_query")),
        list(spec["sql_params"]),
        params,
        count_limit=int(spec["count_limit"]),
        max_count=max_count,
        page_size=page_size,
    )
    chat_facets: list[dict[str, Any]] = []
    if include_chat_facets and params.chat_id is None and spec.get("chat_facet_sql"):
        cur = conn.cursor()
        try:
            chat_facets = _execute_chat_facet_query(
                cur,
                str(spec["chat_facet_sql"]),
                list(spec["sql_params"]),
                scan_limit=int(spec.get("chat_facet_scan_limit") or spec["count_limit"]),
            )
        finally:
            cur.close()

    payload = {
        "ok": True,
        "query": str(spec.get("raw_query") or params.raw_query),
        "fts_query": str(spec["match_query"]),
        "page": effective_page,
        "page_size": page_size,
        "data_version": _search_cache._format_data_version(data_fingerprint),
        "total": total,
        "total_pages": total_pages,
        "total_is_capped": total_is_capped,
        "effective_sort": str(spec["effective_sort"]),
        "effective_order": str(spec["effective_order"]).lower(),
        "items": map_search_items_fn(rows),
    }
    if include_chat_facets:
        payload["chat_facets"] = chat_facets
    return payload


def _search_payload_service(
    conn: sqlite3.Connection,
    params: SearchParams,
    *,
    fts_enabled: bool,
    from_sql: str,
    page_size: int,
    max_count: int,
    map_search_items_fn: Callable[[list[sqlite3.Row]], list[dict[str, Any]]],
) -> dict[str, Any]:
    _search_maintenance.schedule_message_search_maintenance()

    primary_spec = _build_search_query_spec(
        params,
        from_sql=from_sql,
        fts_enabled=fts_enabled,
        max_count=max_count,
    )
    payload = _build_payload_from_spec(
        conn,
        params,
        primary_spec,
        page_size=page_size,
        max_count=max_count,
        map_search_items_fn=map_search_items_fn,
        include_chat_facets=bool(params.count_only),
    )

    should_try_fallback = _should_try_like_fallback(conn, primary_spec)
    if _payload_has_results(payload) and not should_try_fallback:
        return payload

    if not should_try_fallback:
        return payload

    fallback_spec = _build_search_query_spec(
        params,
        from_sql=from_sql,
        fts_enabled=False,
        max_count=max_count,
        force_like=True,
    )
    fallback_payload = _build_payload_from_spec(
        conn,
        params,
        fallback_spec,
        page_size=page_size,
        max_count=max_count,
        map_search_items_fn=map_search_items_fn,
        include_chat_facets=bool(params.count_only),
    )

    if not _payload_has_results(fallback_payload):
        return payload

    fallback_payload["fts_query"] = str(primary_spec.get("match_query") or "")
    return fallback_payload
