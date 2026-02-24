# -*- coding: utf-8 -*-
import sqlite3
from contextlib import closing
from typing import Callable

from flask import jsonify, request


def register_search_routes(
    app,
    *,
    logger,
    get_conn_fn,
    has_fts_fn: Callable[[sqlite3.Connection], bool],
    from_sql: str,
    page_size: int,
    max_count: int,
    tokenize_query_fn,
    to_fts_match_fn,
    map_search_items_fn,
    parse_search_params_fn,
    search_payload_service_fn,
) -> None:
    @app.post("/api/search")
    def api_search():
        data = request.get_json(silent=True) or {}
        try:
            params = parse_search_params_fn(data)
            with closing(get_conn_fn()) as conn:
                payload = search_payload_service_fn(
                    conn,
                    params,
                    fts_enabled=has_fts_fn(conn),
                    from_sql=from_sql,
                    page_size=page_size,
                    max_count=max_count,
                    tokenize_query_fn=tokenize_query_fn,
                    to_fts_match_fn=to_fts_match_fn,
                    map_search_items_fn=map_search_items_fn,
                )
            return jsonify(payload)
        except (ValueError, TypeError):
            return jsonify({"ok": False, "error": "参数格式错误"}), 400
        except sqlite3.Error:
            logger.exception("查询失败")
            return jsonify({"ok": False, "error": "查询失败"}), 500
        except Exception:
            logger.exception("系统异常")
            return jsonify({"ok": False, "error": "系统异常"}), 500
