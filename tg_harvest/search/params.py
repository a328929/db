# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class SearchParams:
    raw_query: str
    search_type: str
    sort_by_req: str
    order_req: str
    page: int
    chat_id: Optional[int]
    skip_count: bool = False
    count_only: bool = False


def _parse_bool(value: Any, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return bool(default)
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off", ""}:
            return False
    return bool(default)


def _parse_search_params(data: Dict[str, Any]) -> SearchParams:
    raw_query = str(data.get("query", "") or "")
    search_type = str(data.get("search_type", "all") or "all").lower()
    sort_by_req = str(data.get("sort_by", "time") or "time").lower()
    order_req = str(data.get("order", "desc") or "desc").lower()

    page = max(int(data.get("page", 1) or 1), 1)
    chat_id_raw = data.get("chat_id", "all")
    chat_id = None if str(chat_id_raw).lower() == "all" else int(chat_id_raw)

    skip_count = _parse_bool(data.get("skip_count", False), default=False)
    count_only = _parse_bool(data.get("count_only", False), default=False)

    return SearchParams(
        raw_query=raw_query,
        search_type=search_type,
        sort_by_req=sort_by_req,
        order_req=order_req,
        page=page,
        chat_id=chat_id,
        skip_count=skip_count,
        count_only=count_only,
    )
