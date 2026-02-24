# -*- coding: utf-8 -*-
from typing import Any, Callable, Dict, List, Tuple

from tg_harvest.search_params import SearchParams


def _build_search_query_spec(
    params: SearchParams,
    *,
    from_sql: str,
    fts_enabled: bool,
    max_count: int,
    tokenize_query_fn: Callable[[str], List[Tuple[str, str]]],
    to_fts_match_fn: Callable[[str], str],
    force_like: bool = False,
) -> Dict[str, Any]:
    where_parts: List[str] = ["1=1"]
    sql_params: List[Any] = []

    has_text_filter = False
    match_query = to_fts_match_fn(params.raw_query)

    if force_like:
        like_clause, like_params = _build_like_logic_clause(params.raw_query, tokenize_query_fn)
        if like_clause:
            where_parts.append(like_clause)
            sql_params.extend(like_params)
            has_text_filter = True
    else:
        if match_query and fts_enabled:
            where_parts.append("m.pk IN (SELECT rowid FROM messages_fts WHERE messages_fts MATCH ?)")
            sql_params.append(match_query)
            has_text_filter = True
        else:
            like_clause, like_params = _build_like_logic_clause(params.raw_query, tokenize_query_fn)
            if like_clause:
                where_parts.append(like_clause)
                sql_params.extend(like_params)
                has_text_filter = True

    _append_scope_filters(where_parts, sql_params, params)
    where_sql = " AND ".join(where_parts)

    order_expr, effective_sort, effective_order = _choose_sort(params.search_type, params.sort_by_req, params.order_req)
    count_sql = f"SELECT COUNT(*) AS c FROM (SELECT m.pk {from_sql} WHERE {where_sql} LIMIT ?)"
    query_sql = f"""
        SELECT m.pk,m.chat_id,c.chat_title,m.message_id,m.msg_date_text,m.msg_date_ts,m.msg_type,m.link,m.content,m.grouped_id,
               mm.file_name,mm.file_size,mm.mime_type,mm.media_kind
        {from_sql}
        WHERE {where_sql}
        ORDER BY {order_expr} {effective_order}, m.msg_date_ts {effective_order}, m.pk {effective_order}
        LIMIT ? OFFSET ?
    """

    return {
        "where_sql": where_sql,
        "sql_params": sql_params,
        "match_query": match_query,
        "count_sql": count_sql,
        "query_sql": query_sql,
        "effective_sort": effective_sort,
        "effective_order": effective_order,
        "count_limit": max_count + 1,
        "has_text_filter": has_text_filter,
    }


def _append_scope_filters(where_parts: List[str], sql_params: List[Any], params: SearchParams) -> None:
    if params.chat_id is not None:
        where_parts.append("m.chat_id = ?")
        sql_params.append(params.chat_id)

    type_clause, type_params = _make_type_clause(params.search_type)
    if type_clause:
        where_parts.append(type_clause)
        sql_params.extend(type_params)


def _make_type_clause(search_type: str) -> Tuple[str, List[Any]]:
    st = (search_type or "all").lower()
    if st == "text":
        return "m.msg_type = 'TEXT'", []
    if st == "image":
        return "m.msg_type = 'PHOTO'", []
    if st == "video":
        return "m.msg_type IN ('VIDEO', 'GIF', 'VIDEO_NOTE')", []
    if st == "audio":
        return "m.msg_type IN ('AUDIO', 'VOICE')", []
    return "", []


def _choose_sort(search_type: str, sort_by: str, order: str) -> Tuple[str, str, str]:
    st = (search_type or "all").lower()
    sb = (sort_by or "time").lower()
    od = "ASC" if str(order).lower() == "asc" else "DESC"
    if st in {"all", "text"} and sb == "size":
        sb = "time"
    if sb == "size":
        return "COALESCE(mm.file_size, 0)", "size", od
    return "m.msg_date_ts", "time", od


def _build_like_logic_clause(raw_query: str, tokenize_query_fn: Callable[[str], List[Tuple[str, str]]]) -> Tuple[str, List[Any]]:
    tokens = tokenize_query_fn(raw_query)
    if not tokens:
        return "", []

    stream: List[Tuple[str, str]] = []
    prev_operand = False

    for kind, value in tokens:
        if kind in {"TERM", "PHRASE"}:
            if prev_operand:
                stream.append(("OP", "AND"))
            stream.append(("TERM", value))
            prev_operand = True
            continue

        if value == "-":
            if prev_operand:
                stream.append(("OP", "AND"))
            stream.append(("OP", "NOT"))
            prev_operand = False
            continue

        if value == "+":
            if prev_operand:
                stream.append(("OP", "AND"))
            prev_operand = False
            continue

        if value == "/":
            if prev_operand:
                stream.append(("OP", "OR"))
            prev_operand = False

    while stream and stream[-1][0] == "OP":
        stream.pop()

    if not stream:
        return "", []

    output: List[Tuple[str, str]] = []
    op_stack: List[str] = []
    precedence = {"OR": 1, "AND": 2, "NOT": 3}
    right_assoc = {"NOT"}

    for token_kind, token_value in stream:
        if token_kind == "TERM":
            output.append((token_kind, token_value))
            continue

        while op_stack:
            top = op_stack[-1]
            if (top not in right_assoc and precedence[top] >= precedence[token_value]) or (
                top in right_assoc and precedence[top] > precedence[token_value]
            ):
                output.append(("OP", op_stack.pop()))
                continue
            break
        op_stack.append(token_value)

    while op_stack:
        output.append(("OP", op_stack.pop()))

    expr_stack: List[str] = []
    expr_params: List[Any] = []
    content_expr = "LOWER(COALESCE(NULLIF(m.content_norm, ''), m.content, ''))"

    for token_kind, token_value in output:
        if token_kind == "TERM":
            expr_stack.append(f"({content_expr} LIKE ?)")
            expr_params.append(f"%{token_value.lower()}%")
            continue

        if token_value == "NOT":
            if not expr_stack:
                return "", []
            operand = expr_stack.pop()
            expr_stack.append(f"(NOT {operand})")
            continue

        if len(expr_stack) < 2:
            return "", []
        right = expr_stack.pop()
        left = expr_stack.pop()
        expr_stack.append(f"({left} {token_value} {right})")

    if len(expr_stack) != 1:
        return "", []

    return expr_stack[0], expr_params
