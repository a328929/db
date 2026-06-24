import json
from datetime import UTC, datetime
from typing import Any

from tg_harvest.domain.coerce import (
    clean_text as _clean_text,
    optional_int as _optional_int,
    safe_int as _safe_int,
)
from tg_harvest.domain.chat_titles import (
    chat_title_or_fallback as _chat_title_or_fallback,
)


def _percent(part: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(max(0.0, min(100.0, float(part) * 100.0 / float(total))), 1)


def _default_clone_title(chat_title: str) -> str:
    base = str(chat_title or "").strip() or "未命名群组"
    suffix = " 副本"
    max_len = 128
    if len(base) + len(suffix) <= max_len:
        return base + suffix
    return base[: max_len - len(suffix)].rstrip() + suffix


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _normalize_plan_json(plan: Any) -> str:
    if plan is None:
        return ""
    if isinstance(plan, str):
        return plan
    return json.dumps(plan, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _json_text(value: Any, *, default: str) -> str:
    if value is None:
        return default
    if isinstance(value, str):
        normalized = value.strip()
        return normalized if normalized else default
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _json_value(value: Any, *, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return default
    return value


def _normalize_bounded_int(
    value: Any,
    *,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        normalized = default
    return max(minimum, min(maximum, normalized))
