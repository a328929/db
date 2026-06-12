import sqlite3
import threading
import time
from collections import deque
from collections.abc import Callable
from contextlib import closing
from typing import Any

from flask import jsonify, request

from tg_harvest.web.responses import json_error, logged_json_error, require_json_dict

# 简单的 IP 限流字典：{bucket:ip: deque([timestamps])}
# 普通搜索与后台 count_only 统计分桶限流，避免互相挤占额度。
SEARCH_RATE_LIMIT = 20
SEARCH_COUNT_ONLY_RATE_LIMIT = 60
SEARCH_WINDOW_SEC = 60
_search_rate_tracker: dict[str, deque] = {}
_rate_lock = threading.Lock()


def _prune_expired_rate_limit_keys_locked(now: float) -> None:
    expired_keys = []
    for tracker_key, history in _search_rate_tracker.items():
        while history and history[0] < now - SEARCH_WINDOW_SEC:
            history.popleft()
        if not history:
            expired_keys.append(tracker_key)
    for tracker_key in expired_keys:
        _search_rate_tracker.pop(tracker_key, None)


def _is_rate_limited(ip: str, *, bucket: str = "search") -> bool:
    now = time.time()
    limit = (
        SEARCH_COUNT_ONLY_RATE_LIMIT
        if bucket == "count_only"
        else SEARCH_RATE_LIMIT
    )
    tracker_key = f"{bucket}:{ip}"
    with _rate_lock:
        _prune_expired_rate_limit_keys_locked(now)
        if tracker_key not in _search_rate_tracker:
            _search_rate_tracker[tracker_key] = deque()

        history = _search_rate_tracker[tracker_key]
        if len(history) >= limit:
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
        data, error_response = require_json_dict()
        if error_response is not None:
            return error_response

        # 前端一次可见搜索会再发起一次 count_only 后台统计；它不应额外消耗用户搜索额度。
        # 但 count_only 仍会触发数据库统计，必须单独限流，避免被直接调用拖垮数据库。
        ip = request.remote_addr or "unknown"
        rate_bucket = (
            "count_only" if _request_flag_is_true(data.get("count_only")) else "search"
        )
        if _is_rate_limited(ip, bucket=rate_bucket):
            return json_error("查询过于频繁，请稍后再试", 429)

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
            return json_error(message, 400)
        except sqlite3.Error:
            return logged_json_error(logger, "查询失败", "查询失败")
        except Exception:
            return logged_json_error(logger, "系统异常", "系统异常")
