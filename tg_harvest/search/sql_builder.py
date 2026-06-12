import re
from typing import Any

from tg_harvest.search.expression import (
    SearchExprNode,
    build_candidate_fts_match,
    compile_like_clause,
    parse_query,
)
from tg_harvest.search.params import SearchParams, split_query_media_duration

_CJK_CHAR_RE = re.compile(
    r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\U00020000-\U0002ceaf]"
)


def _safe_split_from_sql(from_sql: str) -> tuple[str, str]:
    """
    安全地将 FROM 子句拆分为 (基础部分, 连接部分)。
    例如: "FROM messages m LEFT JOIN chats c ..." -> ("FROM messages m", "LEFT JOIN chats c ...")
    """
    s = from_sql.strip()
    # 寻找第一个 JOIN 关键字（不区分大小写）
    match = re.search(r"\s+(?:LEFT\s+|INNER\s+|CROSS\s+)?JOIN\s+", s, re.I)
    if not match:
        return s, ""

    pos = match.start()
    return s[:pos].strip(), s[pos:].strip()


_MEDIA_JOIN_SQL = (
    "LEFT JOIN message_media mm ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id"
)


def _ensure_media_join(from_sql: str) -> str:
    s = (from_sql or "").strip()
    if not s:
        return s
    if "JOIN message_media mm" in s:
        return s
    return f"{s} {_MEDIA_JOIN_SQL}"


def _term_supports_trigram_candidate(term: str) -> bool:
    compact = "".join(str(term or "").split())
    return len(compact) >= 3


def _term_supports_cjk_aux_candidate(term: str) -> bool:
    compact = "".join(str(term or "").split())
    return len(compact) in {1, 2} and all(
        _CJK_CHAR_RE.fullmatch(ch) for ch in compact
    )


def _quote_for_fts(term: str) -> str:
    return f'"{str(term or "").replace(chr(34), "")}"'


def _select_from_subquery(sql: str) -> str:
    return f"SELECT pk FROM ({sql})"


def _compile_candidate_node(
    node: SearchExprNode,
    *,
    universe_sql: str,
) -> tuple[str, list[Any]] | None:
    if node.kind in {"TERM", "PHRASE"}:
        if _term_supports_cjk_aux_candidate(node.value):
            return (
                "SELECT pk FROM message_search_terms WHERE term = ?",
                ["".join(str(node.value or "").split())],
            )
        if not _term_supports_trigram_candidate(node.value):
            return None
        return (
            "SELECT rowid AS pk FROM messages_fts WHERE messages_fts MATCH ?",
            [_quote_for_fts(node.value)],
        )

    if node.kind == "NOT":
        if node.left is None:
            return None
        compiled = _compile_candidate_node(node.left, universe_sql=universe_sql)
        if compiled is None:
            return None
        child_sql, child_params = compiled
        return (
            f"{universe_sql} EXCEPT {_select_from_subquery(child_sql)}",
            child_params,
        )

    if node.kind == "AND":
        if (
            node.left is not None
            and node.right is not None
            and node.right.kind == "NOT"
            and node.right.left is not None
        ):
            left = _compile_candidate_node(node.left, universe_sql=universe_sql)
            right_negated = _compile_candidate_node(
                node.right.left, universe_sql=universe_sql
            )
            if left is None:
                return None
            if right_negated is None:
                return left
            return (
                f"{_select_from_subquery(left[0])} EXCEPT {_select_from_subquery(right_negated[0])}",
                left[1] + right_negated[1],
            )

        if (
            node.left is not None
            and node.left.kind == "NOT"
            and node.left.left is not None
            and node.right is not None
        ):
            right = _compile_candidate_node(node.right, universe_sql=universe_sql)
            left_negated = _compile_candidate_node(
                node.left.left, universe_sql=universe_sql
            )
            if right is None:
                return None
            if left_negated is None:
                return right
            return (
                f"{_select_from_subquery(right[0])} EXCEPT {_select_from_subquery(left_negated[0])}",
                right[1] + left_negated[1],
            )

        left = (
            _compile_candidate_node(node.left, universe_sql=universe_sql)
            if node.left is not None
            else None
        )
        right = (
            _compile_candidate_node(node.right, universe_sql=universe_sql)
            if node.right is not None
            else None
        )
        if left is None:
            return right
        if right is None:
            return left
        return (
            f"{_select_from_subquery(left[0])} INTERSECT {_select_from_subquery(right[0])}",
            left[1] + right[1],
        )

    if node.kind == "OR":
        if (node.left is not None and node.left.kind == "NOT") or (
            node.right is not None and node.right.kind == "NOT"
        ):
            return None
        left = (
            _compile_candidate_node(node.left, universe_sql=universe_sql)
            if node.left is not None
            else None
        )
        right = (
            _compile_candidate_node(node.right, universe_sql=universe_sql)
            if node.right is not None
            else None
        )
        if left is None or right is None:
            return None
        return (
            f"{_select_from_subquery(left[0])} UNION {_select_from_subquery(right[0])}",
            left[1] + right[1],
        )

    return None


def _build_candidate_fts_sql(
    expr: SearchExprNode | None,
) -> tuple[str, list[Any]] | None:
    if expr is None:
        return None
    return _compile_candidate_node(expr, universe_sql="SELECT pk FROM messages")


def _candidate_uses_auxiliary_terms_only(candidate_sql: str | None) -> bool:
    sql = str(candidate_sql or "")
    return "message_search_terms" in sql and "messages_fts" not in sql


def _single_auxiliary_term(expr: SearchExprNode | None) -> str | None:
    if expr is None:
        return None
    if expr.kind not in {"TERM", "PHRASE"}:
        return None
    if not _term_supports_cjk_aux_candidate(expr.value):
        return None
    return "".join(str(expr.value or "").split())


def _has_boolean_structure(expr: SearchExprNode | None) -> bool:
    if expr is None:
        return False
    return expr.kind in {"AND", "OR", "NOT"}


def _build_search_query_spec(
    params: SearchParams,
    *,
    from_sql: str,
    fts_enabled: bool,
    max_count: int,
    force_like: bool = False,
) -> dict[str, Any]:
    raw_query = (params.raw_query or "").strip()
    parsed_text_query, parsed_duration_sec = split_query_media_duration(raw_query)
    duration_sec = (
        int(params.duration_sec)
        if params.duration_sec is not None
        else parsed_duration_sec
    )
    text_query = (params.text_query or parsed_text_query or "").strip()
    where_parts: list[str] = ["1=1"]
    sql_params: list[Any] = []

    has_text_filter = False
    expr = parse_query(text_query) if text_query else None
    match_query = build_candidate_fts_match(expr)

    actual_from_sql = from_sql

    use_fts_join = False
    candidate_sql: str | None = None
    candidate_params: list[Any] = []
    auxiliary_only_candidate = False
    # 搜索统一走规范化文本：优先 content_norm，缺失时回退 content。
    # 这样每个词只做一次 LIKE 扫描，避免对同一行重复匹配两遍。
    content_expr = "(LOWER(COALESCE(NULLIF(m.content_norm, ''), m.content, '')) LIKE ? ESCAPE '\\')"
    like_clause, like_params = compile_like_clause(expr, content_expr=content_expr)
    if like_clause:
        has_text_filter = True
        if not force_like and fts_enabled:
            candidate_plan = _build_candidate_fts_sql(expr)
            if candidate_plan is not None:
                candidate_sql, candidate_params = candidate_plan
                auxiliary_only_candidate = _candidate_uses_auxiliary_terms_only(
                    candidate_sql
                )
            elif match_query:
                use_fts_join = True
                where_parts.append("fts.messages_fts MATCH ?")
                sql_params.append(match_query)
        if not auxiliary_only_candidate:
            where_parts.append(like_clause)
        else:
            like_params = []

    sql_params = candidate_params + like_params + sql_params
    _append_scope_filters(where_parts, sql_params, params)
    if duration_sec is not None:
        where_parts.append("m.msg_type IN ('VIDEO', 'GIF', 'VIDEO_NOTE')")
        where_parts.append("mm.duration_sec = ?")
        sql_params.append(int(duration_sec))
    where_sql = " AND ".join(where_parts)

    order_expr, effective_sort, effective_order = _choose_sort(
        params.search_type, params.sort_by_req, params.order_req
    )

    base_from_sql, outer_from_sql = _safe_split_from_sql(actual_from_sql)

    inner_from_sql = base_from_sql
    needs_media_join = (
        duration_sec is not None or effective_sort in {"size", "duration"}
    )
    if needs_media_join:
        inner_from_sql = _ensure_media_join(inner_from_sql)
        outer_from_sql = _ensure_media_join(outer_from_sql)
    if candidate_sql:
        inner_from_sql += " JOIN candidate_pks cp ON cp.pk = m.pk "
    if use_fts_join:
        # P0级优化：使用明确的 JOIN 替代 IN 子查询。
        # 强制让 SQLite 识别 fts 作为过滤驱动表
        inner_from_sql += " JOIN messages_fts fts ON fts.rowid = m.pk "

    # 动态构建排序子句与必要的 JOIN
    # 我们根据排序需求决定子查询是否需要 JOIN 媒体表 (mm)
    if effective_sort in {"size", "duration"}:
        if effective_sort == "size":
            final_order_clause = (
                f"mm.file_size {effective_order}, "
                f"mm.chat_id {effective_order}, "
                f"mm.message_id {effective_order}"
            )
        else: # duration
            final_order_clause = (
                f"mm.duration_sec {effective_order}, "
                f"mm.chat_id {effective_order}, "
                f"mm.message_id {effective_order}"
            )
    elif effective_sort == "time":
        final_order_clause = (
            f"{order_expr} {effective_order}, "
            f"m.message_id {effective_order}, "
            f"m.pk {effective_order}"
        )
    else:
        # 默认回退逻辑
        final_order_clause = f"{order_expr} {effective_order}, m.msg_date_ts {effective_order}, m.pk {effective_order}"

    count_sql_prefix = ""
    if candidate_sql:
        count_sql_prefix = f"WITH candidate_pks AS ({candidate_sql}) "
    count_sql = (
        f"{count_sql_prefix}SELECT COUNT(*) AS c "
        f"FROM (SELECT m.pk {inner_from_sql} WHERE {where_sql} LIMIT ?)"
    )
    chat_facet_sql = f"""
        {count_sql_prefix}
        SELECT
            m.chat_id,
            COALESCE(c.chat_title, '') AS chat_title,
            COUNT(*) AS match_count,
            MAX(m.msg_date_ts) AS latest_msg_date_ts
        FROM (
            SELECT m.chat_id, m.msg_date_ts
            {inner_from_sql}
            WHERE {where_sql}
            LIMIT ?
        ) m
        LEFT JOIN chats c ON c.chat_id = m.chat_id
        GROUP BY m.chat_id
        ORDER BY match_count DESC, latest_msg_date_ts DESC, m.chat_id ASC
        LIMIT ?
    """

    query_cte_parts: list[str] = []
    if candidate_sql:
        query_cte_parts.append(f"candidate_pks AS ({candidate_sql})")
    query_cte_parts.append(
        f"""matched_pks AS (
            SELECT m.pk
            {inner_from_sql} WHERE {where_sql}
            ORDER BY {final_order_clause}
            LIMIT ? OFFSET ?
        )"""
    )
    query_sql = f"""
        WITH {", ".join(query_cte_parts)}
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
        FROM matched_pks mp
        JOIN messages m ON m.pk = mp.pk
        {outer_from_sql}
        ORDER BY {final_order_clause}
    """

    query_sql_skip: str | None = None
    single_auxiliary_term = _single_auxiliary_term(expr)
    if (
        effective_sort == "time"
        and single_auxiliary_term is not None
        and candidate_sql
        and auxiliary_only_candidate
    ):
        direct_from_sql = base_from_sql
        if outer_from_sql:
            direct_from_sql += f" {outer_from_sql}"

        skip_prefix = f"WITH candidate_pks AS ({candidate_sql}) "
        query_sql_skip = f"""
            {skip_prefix}
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
            {direct_from_sql}
            WHERE {where_sql}
              AND EXISTS (SELECT 1 FROM candidate_pks cp WHERE cp.pk = m.pk)
            ORDER BY {final_order_clause}
            LIMIT ? OFFSET ?
        """

    if effective_sort == "time" and _has_boolean_structure(expr):
        direct_from_sql = base_from_sql
        if candidate_sql:
            direct_from_sql += " JOIN candidate_pks cp ON cp.pk = m.pk "
        if use_fts_join:
            direct_from_sql += " JOIN messages_fts fts ON fts.rowid = m.pk "
        if outer_from_sql:
            direct_from_sql += f" {outer_from_sql}"

        skip_cte_parts = [
            part for part in query_cte_parts if not part.startswith("matched_pks AS")
        ]
        skip_prefix = f"WITH {', '.join(skip_cte_parts)} " if skip_cte_parts else ""
        query_sql_skip = f"""
            {skip_prefix}
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
            {direct_from_sql}
            WHERE {where_sql}
            ORDER BY {final_order_clause}
            LIMIT ? OFFSET ?
        """

    count_limit = max_count + 1

    return {
        "where_sql": where_sql,
        "sql_params": sql_params,
        "match_query": match_query,
        "raw_query": raw_query,
        "count_sql": count_sql,
        "chat_facet_sql": chat_facet_sql,
        "query_sql": query_sql,
        "query_sql_skip": query_sql_skip,
        "prefer_skip_query": bool(query_sql_skip),
        "effective_sort": effective_sort,
        "effective_order": effective_order,
        "count_limit": count_limit,
        "chat_facet_scan_limit": count_limit,
        "has_text_filter": has_text_filter,
        "uses_text_index": bool(candidate_sql or use_fts_join),
        "uses_auxiliary_terms": bool(
            candidate_sql and "message_search_terms" in candidate_sql
        ),
    }


def _append_scope_filters(
    where_parts: list[str], sql_params: list[Any], params: SearchParams
) -> None:
    if params.chat_id is not None:
        where_parts.append("m.chat_id = ?")
        sql_params.append(params.chat_id)

    if params.start_ts is not None:
        where_parts.append("m.msg_date_ts >= ?")
        sql_params.append(int(params.start_ts))

    if params.end_ts_exclusive is not None:
        where_parts.append("m.msg_date_ts < ?")
        sql_params.append(int(params.end_ts_exclusive))

    type_clause, type_params = _make_type_clause(params.search_type)
    if type_clause:
        where_parts.append(type_clause)
        sql_params.extend(type_params)


def _make_type_clause(search_type: str) -> tuple[str, list[Any]]:
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


def _choose_sort(search_type: str, sort_by: str, order: str) -> tuple[str, str, str]:
    st = (search_type or "all").lower()
    sb = (sort_by or "time").lower()
    od = "ASC" if str(order).lower() == "asc" else "DESC"
    if st in {"all", "text"} and sb in {"size", "duration"}:
        sb = "time"
    if st == "image" and sb == "duration":
        sb = "time"
    if sb == "size":
        return "COALESCE(mm.file_size, 0)", "size", od
    if sb == "duration":
        return "COALESCE(mm.duration_sec, 0)", "duration", od
    return "m.msg_date_ts", "time", od
