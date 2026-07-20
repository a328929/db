from __future__ import annotations

import math
import re
import sqlite3
from typing import Any

from tg_harvest.search.data_version import (
    format_data_version,
    read_database_fingerprint,
)
from tg_harvest.search.expression import SearchExprNode, parse_query
from tg_harvest.search.manticore_client import ManticoreClient
from tg_harvest.search.params import SearchParams, split_query_media_duration

_CJK_RE = re.compile(
    r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\U00020000-\U0002ceaf]"
)

def _escape_match_text(value: str) -> str:
    return str(value or "").replace("\\", "\\\\").replace('"', '\\"')


def _compile_match_term(node: SearchExprNode) -> str:
    value = str(node.value or "")
    escaped = _escape_match_text(value)
    if node.kind == "PHRASE" or _CJK_RE.search(value) or any(ch.isspace() for ch in value):
        return f'"{escaped}"'
    escaped = escaped.replace("*", "\\*").replace("?", "\\?").replace("%", "\\%")
    if len(value) < 2:
        return escaped
    return f"*{escaped}*"


def compile_manticore_match(expr: SearchExprNode | None) -> str:
    if expr is None:
        return ""
    if expr.kind in {"TERM", "PHRASE"}:
        return _compile_match_term(expr)
    if expr.kind == "NOT":
        if expr.left is None:
            raise ValueError("NOT 操作缺少目标")
        return f"!({compile_manticore_match(expr.left)})"
    if expr.kind in {"AND", "OR"}:
        if expr.left is None or expr.right is None:
            raise ValueError("二元操作缺少操作数")
        left = compile_manticore_match(expr.left)
        right = compile_manticore_match(expr.right)
        operator = " | " if expr.kind == "OR" else " "
        return f"({left}{operator}{right})"
    raise ValueError("未知搜索表达式节点")


def _sql_string(value: str) -> str:
    escaped = str(value or "").replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _search_text_and_duration(params: SearchParams) -> tuple[str, int | None]:
    parsed_text, parsed_duration = split_query_media_duration(params.raw_query)
    text = str(params.text_query or parsed_text or "").strip()
    duration = (
        int(params.duration_sec)
        if params.duration_sec is not None
        else parsed_duration
    )
    return text, duration


def _build_where_sql(params: SearchParams, match_query: str) -> str:
    clauses = [f"MATCH({_sql_string(match_query)})"] if match_query else ["1=1"]
    if params.chat_id is not None:
        clauses.append(f"chat_id = {int(params.chat_id)}")
    if params.start_ts is not None:
        clauses.append(f"msg_date_ts >= {int(params.start_ts)}")
    if params.end_ts_exclusive is not None:
        clauses.append(f"msg_date_ts < {int(params.end_ts_exclusive)}")

    search_type = str(params.search_type or "all").lower()
    if search_type == "text":
        clauses.append("type_code = 1")
    elif search_type == "image":
        clauses.append("type_code = 2")
    elif search_type == "video":
        clauses.append("type_code = 3")
    elif search_type == "audio":
        clauses.append("type_code = 4")

    _text, duration = _search_text_and_duration(params)
    if duration is not None:
        clauses.append("type_code = 3")
        clauses.append(f"duration_sec = {int(duration)}")
    return " AND ".join(clauses)


def _sort_spec(params: SearchParams) -> tuple[str, str, str]:
    search_type = str(params.search_type or "all").lower()
    requested = str(params.sort_by_req or "time").lower()
    order = "ASC" if str(params.order_req or "desc").lower() == "asc" else "DESC"
    if requested == "relevance":
        return "WEIGHT()", "relevance", order
    if search_type in {"all", "text"} and requested in {"size", "duration"}:
        requested = "time"
    if search_type == "image" and requested == "duration":
        requested = "time"
    if requested == "size":
        return "file_size", "size", order
    if requested == "duration":
        return "duration_sec", "duration", order
    return "msg_date_ts", "time", order


def _extract_hits(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], int, bool]:
    hits_wrapper = payload.get("hits")
    if not isinstance(hits_wrapper, dict):
        return [], 0, False
    raw_hits = hits_wrapper.get("hits")
    hits = [item for item in raw_hits if isinstance(item, dict)] if isinstance(raw_hits, list) else []
    total = int(hits_wrapper.get("total") or 0)
    relation = str(hits_wrapper.get("total_relation") or "eq").lower()
    return hits, total, relation != "eq"


def _query_ids(
    client: ManticoreClient,
    *,
    where_sql: str,
    order_sql: str,
    limit: int,
    offset: int,
    max_matches: int,
    ranker: str,
) -> tuple[list[int], int, bool]:
    payload = client.execute_select(
        f"SELECT id FROM {client.table} WHERE {where_sql} "
        f"ORDER BY {order_sql} LIMIT {max(0, int(offset))}, {max(1, int(limit))} "
        f"OPTION ranker={ranker}, max_matches={max(1, int(max_matches))}"
    )
    hits, total, capped = _extract_hits(payload)
    ids = [int(hit.get("_id") or 0) for hit in hits]
    return [pk for pk in ids if pk > 0], total, capped


def _query_count(
    client: ManticoreClient, *, where_sql: str, max_matches: int
) -> tuple[int, bool]:
    payload = client.execute_select(
        f"SELECT id FROM {client.table} WHERE {where_sql} LIMIT 0 "
        f"OPTION ranker=none, cutoff={max(1, int(max_matches))},"
        f"max_matches={max(1, int(max_matches))}"
    )
    _hits, total, capped = _extract_hits(payload)
    return min(total, max_matches), capped or total >= max_matches


def _hydrate_rows(conn: sqlite3.Connection, ids: list[int]) -> list[dict[str, Any]]:
    if not ids:
        return []
    placeholders = ",".join("?" for _ in ids)
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT
                m.pk,
                m.chat_id,
                c.chat_title,
                c.chat_username,
                m.message_id,
                m.msg_date_text,
                m.msg_date_ts,
                m.msg_type,
                m.content,
                m.grouped_id,
                m.is_promo,
                mm.file_name,
                mm.file_size,
                mm.mime_type,
                mm.media_kind,
                mm.duration_sec
            FROM messages m
            LEFT JOIN chats c ON c.chat_id = m.chat_id
            LEFT JOIN message_media mm
              ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id
            WHERE m.pk IN ({placeholders})
            """,
            ids,
        )
        by_pk = {int(row["pk"]): dict(row) for row in cur.fetchall()}
        return [by_pk[pk] for pk in ids if pk in by_pk]
    finally:
        cur.close()


def manticore_search_payload_service(
    conn: sqlite3.Connection,
    params: SearchParams,
    *,
    client: ManticoreClient,
    page_size: int,
    max_count: int,
    max_matches: int,
    map_search_items_fn,
    **_unused: Any,
) -> dict[str, Any]:
    text_query, _duration = _search_text_and_duration(params)
    expression = parse_query(text_query)
    match_query = compile_manticore_match(expression)
    where_sql = _build_where_sql(params, match_query)
    order_field, effective_sort, effective_order = _sort_spec(params)
    if effective_sort == "relevance":
        order_sql = (
            f"WEIGHT() {effective_order}, msg_date_ts DESC, "
            "message_id DESC, id DESC"
        )
    elif effective_sort in {"size", "duration"}:
        order_sql = (
            f"{order_field} {effective_order}, chat_id {effective_order}, "
            f"message_id {effective_order}"
        )
    else:
        order_sql = (
            f"msg_date_ts {effective_order}, message_id {effective_order}, "
            f"id {effective_order}"
        )

    effective_max = max(1, min(int(max_count), int(max_matches)))
    page = max(1, int(params.page))
    offset = (page - 1) * int(page_size)
    if offset >= effective_max:
        raise ValueError(
            f"Manticore 搜索最多支持前 {math.ceil(effective_max / page_size)} 页"
        )

    ids: list[int] = []
    total = -1
    total_is_capped = False
    effective_page = page
    if params.count_only:
        total, total_is_capped = _query_count(
            client, where_sql=where_sql, max_matches=effective_max
        )
    else:
        ids, observed_total, observed_capped = _query_ids(
            client,
            where_sql=where_sql,
            order_sql=order_sql,
            limit=page_size,
            offset=offset,
            max_matches=effective_max,
            ranker="bm25" if effective_sort == "relevance" else "none",
        )
        # Manticore includes the match total in the page query response.  Do
        # not issue a second count-only request just because the caller asked
        # to defer counting; that would rescan the same expression.  The
        # relation flag still preserves the capped/at-least semantics.
        total = min(observed_total, effective_max)
        total_is_capped = observed_capped or observed_total >= effective_max
        if params.skip_count and page > 1 and not ids:
            total, total_is_capped = _query_count(
                client, where_sql=where_sql, max_matches=effective_max
            )
            total_pages = math.ceil(total / page_size) if total > 0 else 0
            if total_pages > 0:
                effective_page = min(page, total_pages)
                ids, _observed_total, _observed_capped = _query_ids(
                    client,
                    where_sql=where_sql,
                    order_sql=order_sql,
                    limit=page_size,
                    offset=(effective_page - 1) * page_size,
                    max_matches=effective_max,
                    ranker="bm25" if effective_sort == "relevance" else "none",
                )

    total_pages = (
        math.ceil(total / page_size) if total is not None and total > 0 else 0
    )
    rows = _hydrate_rows(conn, ids)
    payload = {
        "ok": True,
        "query": params.raw_query,
        "compiled_query": match_query,
        "search_backend": "manticore",
        "page": effective_page,
        "page_size": page_size,
        "data_version": format_data_version(read_database_fingerprint(conn)),
        "total": total,
        "total_pages": total_pages,
        "total_is_capped": total_is_capped,
        "effective_sort": effective_sort,
        "effective_order": effective_order.lower(),
        "items": map_search_items_fn(rows),
    }
    if params.count_only:
        payload["chat_facets"] = []
    return payload
