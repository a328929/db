# -*- coding: utf-8 -*-
import sqlite3
import time
import threading
from collections import deque
from contextlib import closing
from typing import Any, Callable, Dict

from flask import jsonify, request

# 简单的 IP 限流字典：{ip: deque([timestamps])}
# 限制 60 秒内同一 IP 最多 20 次搜索
SEARCH_RATE_LIMIT = 20
SEARCH_WINDOW_SEC = 60
_search_rate_tracker: Dict[str, deque] = {}
_rate_lock = threading.Lock()


def _is_rate_limited(ip: str) -> bool:
    now = time.time()
    with _rate_lock:
        if ip not in _search_rate_tracker:
            _search_rate_tracker[ip] = deque()

        history = _search_rate_tracker[ip]
        # 清除过期的记录
        while history and history[0] < now - SEARCH_WINDOW_SEC:
            history.popleft()

        if len(history) >= SEARCH_RATE_LIMIT:
            return True

        history.append(now)
        return False


def _request_flag_is_true(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return False


def register_search_routes(
    app,
    *,
    logger,
    get_conn_fn,
    has_fts_fn: Callable[[sqlite3.Connection], bool],
    from_sql: str,
    page_size: int,
    max_count: int,
    map_search_items_fn,
    parse_search_params_fn,
    search_payload_service_fn,
) -> None:
    @app.post("/api/search")
    def api_search():
        if not request.is_json:
            return jsonify({"ok": False, "error": "请求必须为 JSON"}), 400
        data = request.get_json(silent=True)
        if not isinstance(data, dict):
            return jsonify({"ok": False, "error": "请求 JSON 格式错误"}), 400

        # 前端一次可见搜索会再发起一次 count_only 后台统计；它不应额外消耗用户搜索额度。
        if not _request_flag_is_true(data.get("count_only")):
            ip = request.remote_addr or "unknown"
            if _is_rate_limited(ip):
                return jsonify({"ok": False, "error": "查询过于频繁，请稍后再试"}), 429

        try:
            params = parse_search_params_fn(data)
            with closing(get_conn_fn()) as conn:
                detail_level = (request.args.get("detail") or "lite").strip().lower()
                if detail_level not in {"lite", "full"}:
                    detail_level = "lite"
                payload = search_payload_service_fn(
                    conn,
                    params,
                    fts_enabled=has_fts_fn(conn),
                    from_sql=from_sql,
                    page_size=page_size,
                    max_count=max_count,
                    map_search_items_fn=lambda rows: map_search_items_fn(
                        rows, detail_level=detail_level
                    ),
                )
            return jsonify(payload)
        except (ValueError, TypeError) as exc:
            message = str(exc).strip() or "参数格式错误"
            return jsonify({"ok": False, "error": message}), 400
        except sqlite3.Error:
            logger.exception("查询失败")
            return jsonify({"ok": False, "error": "查询失败"}), 500
        except Exception:
            logger.exception("系统异常")
            return jsonify({"ok": False, "error": "系统异常"}), 500
