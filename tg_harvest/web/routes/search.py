import gzip
import sqlite3
import threading
import time
from collections import OrderedDict, deque
from contextlib import closing

from flask import jsonify, request

from tg_harvest.search.manticore_client import ManticoreError
from tg_harvest.search.params import parse_bool_flag
from tg_harvest.web.ip_utils import get_client_ip
from tg_harvest.web.responses import json_error, logged_json_error, require_json_dict

# 简单的 IP 限流字典：{bucket:ip: deque([timestamps])}
# 普通搜索与后台 count_only 统计分桶限流，避免互相挤占额度。
SEARCH_RATE_LIMIT = 20
SEARCH_COUNT_ONLY_RATE_LIMIT = 60
SEARCH_WINDOW_SEC = 60
SEARCH_RATE_LIMIT_MAX_TRACKED_KEYS = 10_000
_search_rate_tracker: OrderedDict[str, deque] = OrderedDict()
_rate_lock = threading.Lock()
_SEARCH_GZIP_MIN_BYTES = 1024
_SEARCH_INDEX_RETRY_AFTER_SECONDS = 60
_DATABASE_BUSY_RETRY_AFTER_SECONDS = 15


def _retryable_error(message: str, *, code: str, retry_after_seconds: int):
    response, status_code = json_error(
        message,
        503,
        code=code,
        retryable=True,
        retry_after_seconds=retry_after_seconds,
    )
    response.headers["Retry-After"] = str(retry_after_seconds)
    return response, status_code


def _is_sqlite_busy_error(exc: sqlite3.OperationalError) -> bool:
    error_code = getattr(exc, "sqlite_errorcode", None)
    if isinstance(error_code, int):
        primary_code = error_code & 0xFF
        return primary_code in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}
    message = str(exc).strip().lower()
    return message.startswith("database") and (
        "locked" in message or "busy" in message
    )


def _compress_search_response(response):
    response.vary.add("Accept-Encoding")
    if request.accept_encodings["gzip"] <= 0:
        return response
    if response.headers.get("Content-Encoding"):
        return response
    payload = response.get_data()
    if len(payload) < _SEARCH_GZIP_MIN_BYTES:
        return response
    response.set_data(gzip.compress(payload, compresslevel=5, mtime=0))
    response.headers["Content-Encoding"] = "gzip"
    return response


def _prune_expired_rate_limit_keys_locked(now: float) -> None:
    cutoff = now - SEARCH_WINDOW_SEC
    # Buckets are ordered by their last accepted request, so live buckets
    # after the first live entry cannot expire yet.
    while _search_rate_tracker:
        history = next(iter(_search_rate_tracker.values()))
        if history and history[-1] >= cutoff:
            break
        _search_rate_tracker.popitem(last=False)


def _is_rate_limited(ip: str, *, bucket: str = "search") -> bool:
    limit = (
        SEARCH_COUNT_ONLY_RATE_LIMIT
        if bucket == "count_only"
        else SEARCH_RATE_LIMIT
    )
    tracker_key = f"{bucket}:{ip}"
    with _rate_lock:
        now = time.monotonic()
        _prune_expired_rate_limit_keys_locked(now)
        if tracker_key not in _search_rate_tracker:
            if len(_search_rate_tracker) >= SEARCH_RATE_LIMIT_MAX_TRACKED_KEYS:
                # Refuse new identities instead of evicting active buckets. An
                # attacker cannot use high-cardinality addresses to reset their
                # own limit or make this in-process tracker grow unboundedly.
                return True
            _search_rate_tracker[tracker_key] = deque()

        history = _search_rate_tracker[tracker_key]
        while history and history[0] < now - SEARCH_WINDOW_SEC:
            history.popleft()
        if len(history) >= limit:
            return True

        history.append(now)
        _search_rate_tracker.move_to_end(tracker_key)
        return False


def register_search_routes(
    app,
    *,
    logger,
    get_conn_fn,
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
        ip = get_client_ip(request)
        rate_bucket = (
            "count_only"
            if parse_bool_flag(data.get("count_only"), default=False)
            else "search"
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
                    page_size=page_size,
                    max_count=max_count,
                    map_search_items_fn=lambda rows: map_search_items_fn(
                        rows, detail_level=detail_level
                    ),
                )
            return _compress_search_response(jsonify(payload))
        except (ValueError, TypeError) as exc:
            message = str(exc).strip() or "参数格式错误"
            return json_error(message, 400)
        except ManticoreError:
            return _retryable_error(
                "搜索索引暂不可用，请稍后重试",
                code="search_index_unavailable",
                retry_after_seconds=_SEARCH_INDEX_RETRY_AFTER_SECONDS,
            )
        except sqlite3.OperationalError as exc:
            if _is_sqlite_busy_error(exc):
                return _retryable_error(
                    "数据库繁忙，请稍后重试",
                    code="database_busy",
                    retry_after_seconds=_DATABASE_BUSY_RETRY_AFTER_SECONDS,
                )
            return logged_json_error(logger, "数据库查询异常", "系统异常")
        except sqlite3.Error:
            return logged_json_error(logger, "数据库查询异常", "系统异常")
        except Exception:
            return logged_json_error(logger, "系统异常", "系统异常")
