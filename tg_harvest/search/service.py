# -*- coding: utf-8 -*-
import hashlib
import math
import logging
import os
import sqlite3
import threading
import time
from contextlib import closing
from typing import Any, Callable, Dict, List, Tuple

from tg_harvest.search.params import SearchParams
from tg_harvest.storage.schema import backfill_message_search_terms_upgrade_batch
from tg_harvest.storage.schema import drain_message_search_terms_rebuild_queue
from tg_harvest.storage.schema import message_search_terms_are_current
from tg_harvest.search.sql_builder import _build_search_query_spec
from tg_harvest.search.sql_builder import _make_type_clause

_COUNT_CACHE_LOCK = threading.Lock()
_COUNT_CACHE: Dict[Tuple[Any, ...], Tuple[int, bool, int]] = {}
_COUNT_CACHE_MAX_ENTRIES = 256
_SEARCH_TERM_MAINTENANCE_LOCK = threading.Lock()
_SEARCH_TERM_MAINTENANCE_EVENT = threading.Event()
_SEARCH_TERM_MAINTENANCE_THREAD: threading.Thread | None = None
_SEARCH_TERM_MAINTENANCE_GET_CONN_FN: Callable[[], sqlite3.Connection] | None = None

_SEARCH_TERM_MAINTENANCE_BATCH_SIZE = 500
_SEARCH_TERM_UPGRADE_BATCH_SIZE = 5000
_SEARCH_TERM_MAINTENANCE_IDLE_SEC = 2.0
_SEARCH_TERM_MAINTENANCE_INTER_BATCH_SEC = 0.05


def configure_message_search_maintenance(
    get_conn_fn: Callable[[], sqlite3.Connection],
) -> None:
    global _SEARCH_TERM_MAINTENANCE_GET_CONN_FN, _SEARCH_TERM_MAINTENANCE_THREAD

    with _SEARCH_TERM_MAINTENANCE_LOCK:
        _SEARCH_TERM_MAINTENANCE_GET_CONN_FN = get_conn_fn
        thread = _SEARCH_TERM_MAINTENANCE_THREAD
        if thread is not None and thread.is_alive():
            _SEARCH_TERM_MAINTENANCE_EVENT.set()
            return

        thread = threading.Thread(
            target=_message_search_maintenance_worker,
            name="message-search-maintenance",
            daemon=True,
        )
        _SEARCH_TERM_MAINTENANCE_THREAD = thread
        thread.start()
        _SEARCH_TERM_MAINTENANCE_EVENT.set()


def schedule_message_search_maintenance() -> None:
    _SEARCH_TERM_MAINTENANCE_EVENT.set()


def _message_search_maintenance_worker() -> None:
    while True:
        _SEARCH_TERM_MAINTENANCE_EVENT.wait(timeout=_SEARCH_TERM_MAINTENANCE_IDLE_SEC)
        _SEARCH_TERM_MAINTENANCE_EVENT.clear()

        with _SEARCH_TERM_MAINTENANCE_LOCK:
            get_conn_fn = _SEARCH_TERM_MAINTENANCE_GET_CONN_FN

        if get_conn_fn is None:
            continue

        while True:
            try:
                from tg_harvest.admin_jobs.core import _admin_has_any_active_job

                if _admin_has_any_active_job():
                    break
                with closing(get_conn_fn()) as conn:
                    drained = drain_message_search_terms_rebuild_queue(
                        conn,
                        batch_size=_SEARCH_TERM_MAINTENANCE_BATCH_SIZE,
                    )
                    upgraded = 0
                    if drained <= 0:
                        upgraded = backfill_message_search_terms_upgrade_batch(
                            conn,
                            batch_size=_SEARCH_TERM_UPGRADE_BATCH_SIZE,
                        )
            except Exception:
                logging.exception("后台维护中文短词搜索索引失败")
                break

            if drained <= 0 and upgraded <= 0:
                break
            time.sleep(_SEARCH_TERM_MAINTENANCE_INTER_BATCH_SEC)


def _read_data_version(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA data_version")
        row = cur.fetchone()
        if row is None:
            return 0
        return int(row[0] if not isinstance(row, sqlite3.Row) else row[0])
    finally:
        cur.close()


def _read_database_fingerprint(conn: sqlite3.Connection) -> Tuple[Any, ...]:
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA database_list")
        rows = cur.fetchall()
    finally:
        cur.close()

    main_path = ""
    for row in rows:
        name = row["name"] if isinstance(row, sqlite3.Row) else row[1]
        if str(name) != "main":
            continue
        raw_file = row["file"] if isinstance(row, sqlite3.Row) else row[2]
        main_path = str(raw_file or "")
        break

    if not main_path:
        return ("memory", _read_data_version(conn))

    main_path = os.path.abspath(main_path)
    stats: List[Tuple[str, int | None, int | None]] = []
    for path in (main_path, f"{main_path}-wal"):
        is_wal = path == f"{main_path}-wal"
        try:
            st = os.stat(path)
        except OSError:
            stats.append((path, 0, None) if is_wal else (path, None, None))
            continue
        size = int(st.st_size)
        if is_wal and size == 0:
            stats.append((path, 0, None))
            continue
        stats.append((path, size, int(st.st_mtime_ns)))
    return ("file", tuple(stats))


def _format_data_version(fingerprint: Tuple[Any, ...]) -> str:
    raw = repr(fingerprint).encode("utf-8", "surrogatepass")
    return hashlib.blake2b(raw, digest_size=12).hexdigest()


def _make_count_cache_key(
    conn: sqlite3.Connection,
    *,
    count_sql: str,
    sql_params: List[Any],
    count_limit: int,
    page_size: int,
) -> Tuple[Any, ...]:
    return (
        _read_database_fingerprint(conn),
        count_sql,
        tuple(sql_params),
        int(count_limit),
        int(page_size),
    )


def _get_cached_count(
    cache_key: Tuple[Any, ...],
) -> Tuple[int, bool, int] | None:
    with _COUNT_CACHE_LOCK:
        return _COUNT_CACHE.get(cache_key)


def _put_cached_count(
    cache_key: Tuple[Any, ...], value: Tuple[int, bool, int]
) -> None:
    with _COUNT_CACHE_LOCK:
        _COUNT_CACHE[cache_key] = value
        if len(_COUNT_CACHE) > _COUNT_CACHE_MAX_ENTRIES:
            oldest_key = next(iter(_COUNT_CACHE))
            _COUNT_CACHE.pop(oldest_key, None)


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
    query_sql_skip: str | None,
    sql_params: List[Any],
    page: int,
    total_pages: int,
    page_size: int,
    *,
    use_skip_optimized_query: bool = False,
) -> Tuple[List[sqlite3.Row], int]:
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
    conn: sqlite3.Connection, primary_spec: Dict[str, Any]
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
            not message_search_terms_are_current(conn)
            or _has_pending_message_search_term_rebuilds(conn)
        )
    return False


def _payload_has_results(payload: Dict[str, Any]) -> bool:
    total_val = int(payload.get("total") or 0)
    if total_val > 0:
        return True
    return total_val == -1 and len(payload.get("items") or []) > 0


def _resolve_precise_count(
    conn: sqlite3.Connection,
    cur: sqlite3.Cursor,
    *,
    count_sql: str,
    sql_params: List[Any],
    params: SearchParams,
    count_limit: int,
    max_count: int,
    page_size: int,
) -> Tuple[int, bool, int]:
    cached_count = _try_fast_count(
        conn,
        params,
        page_size=page_size,
        max_count=max_count,
    )
    if cached_count is None:
        cache_key = _make_count_cache_key(
            conn,
            count_sql=count_sql,
            sql_params=sql_params,
            count_limit=count_limit,
            page_size=page_size,
        )
        cached_count = _get_cached_count(cache_key)
        if cached_count is None:
            cached_count = _execute_count_query(
                cur,
                count_sql,
                sql_params,
                count_limit,
                max_count,
                page_size,
            )
            _put_cached_count(cache_key, cached_count)
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
        sql_params: List[Any] = list(type_params)
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
) -> Tuple[int, bool, int] | None:
    if (params.raw_query or "").strip():
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
    sql_params: List[Any],
    params: SearchParams,
    count_limit: int,
    max_count: int,
    page_size: int,
) -> Tuple[List[sqlite3.Row], int, int, bool, int]:
    cur = conn.cursor()
    try:
        if params.skip_count:
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
            rows: List[sqlite3.Row] = []
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


def _build_payload_from_spec(
    conn: sqlite3.Connection,
    params: SearchParams,
    spec: Dict[str, Any],
    *,
    page_size: int,
    max_count: int,
    map_search_items_fn: Callable[[List[sqlite3.Row]], List[Dict[str, Any]]],
) -> Dict[str, Any]:
    data_fingerprint = _read_database_fingerprint(conn)
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
    return {
        "ok": True,
        "query": str(spec.get("raw_query") or params.raw_query),
        "fts_query": str(spec["match_query"]),
        "page": effective_page,
        "page_size": page_size,
        "data_version": _format_data_version(data_fingerprint),
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
    map_search_items_fn: Callable[[List[sqlite3.Row]], List[Dict[str, Any]]],
) -> Dict[str, Any]:
    schedule_message_search_maintenance()

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
    )

    if not _payload_has_results(fallback_payload):
        return payload

    fallback_payload["fts_query"] = str(primary_spec.get("match_query") or "")
    return fallback_payload
