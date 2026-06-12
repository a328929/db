import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

MAX_SEARCH_PAGE = 500000
_DATE_YYYYMMDD_RE = re.compile(r"^\d{8}$")
_DATE_SEPARATED_RE = re.compile(r"^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})$")
_MEDIA_DURATION_RE = re.compile(
    r"(?P<open>[【\[])\s*(?P<body>\d{1,3}(?:[：:]\d{1,2}){1,2})\s*(?P<close>[】\]])"
)


@dataclass
class SearchParams:
    raw_query: str
    search_type: str
    sort_by_req: str
    order_req: str
    page: int
    chat_id: int | None
    skip_count: bool = False
    count_only: bool = False
    start_ts: int | None = None
    end_ts_exclusive: int | None = None
    text_query: str = ""
    duration_sec: int | None = None


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


def _local_timezone():
    return datetime.now().astimezone().tzinfo


def _parse_date_bound(value: Any, *, field_label: str, end_exclusive: bool) -> int | None:
    raw = str(value or "").strip()
    if not raw:
        return None

    if _DATE_YYYYMMDD_RE.fullmatch(raw):
        year = int(raw[0:4])
        month = int(raw[4:6])
        day = int(raw[6:8])
    else:
        match = _DATE_SEPARATED_RE.fullmatch(raw)
        if match is None:
            raise ValueError(f"{field_label}格式错误，请使用 YYYYMMDD 或 YYYY-MM-DD")
        year = int(match.group(1))
        month = int(match.group(2))
        day = int(match.group(3))

    try:
        dt = datetime(year, month, day, tzinfo=_local_timezone())
    except ValueError as exc:
        raise ValueError(f"{field_label}不是有效日期") from exc

    if end_exclusive:
        dt = dt + timedelta(days=1)
    return int(dt.timestamp())


def _parse_media_duration_token(body: str) -> int:
    parts = [part.strip() for part in re.split(r"[：:]", str(body or "").strip()) if part.strip()]
    if len(parts) not in {2, 3}:
        raise ValueError("视频时长格式错误，请使用【HH:MM:SS】或【MM:SS】")

    try:
        numbers = [int(part) for part in parts]
    except ValueError as exc:
        raise ValueError("视频时长格式错误，请使用【HH:MM:SS】或【MM:SS】") from exc

    if len(numbers) == 2:
        hours = 0
        minutes, seconds = numbers
    else:
        hours, minutes, seconds = numbers

    if minutes < 0 or minutes >= 60 or seconds < 0 or seconds >= 60 or hours < 0:
        raise ValueError("视频时长格式错误，请使用【HH:MM:SS】或【MM:SS】")

    return hours * 3600 + minutes * 60 + seconds


def split_query_media_duration(raw_query: str) -> tuple[str, int | None]:
    text = str(raw_query or "")
    if not text:
        return "", None

    duration_sec: int | None = None
    pieces: list[str] = []
    last_end = 0

    for match in _MEDIA_DURATION_RE.finditer(text):
        if match.start() > last_end:
            pieces.append(text[last_end:match.start()])

        token_duration = _parse_media_duration_token(match.group("body"))
        if duration_sec is None:
            duration_sec = token_duration
        elif duration_sec != token_duration:
            raise ValueError("搜索表达式只能包含一个视频时长条件")

        pieces.append(" ")
        last_end = match.end()

    if duration_sec is None:
        return text.strip(), None

    pieces.append(text[last_end:])
    cleaned = _cleanup_query_after_duration_removal("".join(pieces))
    return cleaned, duration_sec


def _cleanup_query_after_duration_removal(text: str) -> str:
    q = str(text or "")
    if not q.strip():
        return ""

    # 把因时长 token 移除后留下的空连接符清理掉。
    previous = None
    q = q.replace("＋", "+").replace("／", "/").replace("－", "-")
    while q != previous:
        previous = q
        q = re.sub(r"^\s*[+/\-]+\s*", "", q)
        q = re.sub(r"\s*[+/\-]+\s*$", "", q)
        q = re.sub(r"(?<=\()\s*[+/\-]+\s*(?=[)\s])", "", q)
        q = re.sub(r"\s*[+/\-]+\s*(?=[)\s])", " ", q)
        q = re.sub(r"(?<=[\s(])[+/\-]+(?=\s|$)", " ", q)
        q = re.sub(r"\(\s*\)", " ", q)
        q = re.sub(r"\s+", " ", q).strip()
    return q


def _parse_search_params(data: dict[str, Any]) -> SearchParams:
    raw_query = str(data.get("query", "") or "")
    text_query, duration_sec = split_query_media_duration(raw_query)
    search_type = str(data.get("search_type", "all") or "all").lower()
    sort_by_req = str(data.get("sort_by", "time") or "time").lower()
    order_req = str(data.get("order", "desc") or "desc").lower()

    page = max(int(data.get("page", 1) or 1), 1)
    if page > MAX_SEARCH_PAGE:
        raise ValueError(f"page 不能超过 {MAX_SEARCH_PAGE}")
    chat_id_raw = data.get("chat_id", "all")
    chat_id = None if str(chat_id_raw).lower() == "all" else int(chat_id_raw)

    skip_count = _parse_bool(data.get("skip_count", False), default=False)
    count_only = _parse_bool(data.get("count_only", False), default=False)
    start_ts = _parse_date_bound(
        data.get("start_date", ""), field_label="开始日期", end_exclusive=False
    )
    end_ts_exclusive = _parse_date_bound(
        data.get("end_date", ""), field_label="结束日期", end_exclusive=True
    )
    if (
        start_ts is not None
        and end_ts_exclusive is not None
        and start_ts >= end_ts_exclusive
    ):
        raise ValueError("开始日期不能晚于结束日期")

    return SearchParams(
        raw_query=raw_query,
        search_type=search_type,
        sort_by_req=sort_by_req,
        order_req=order_req,
        page=page,
        chat_id=chat_id,
        skip_count=skip_count,
        count_only=count_only,
        start_ts=start_ts,
        end_ts_exclusive=end_ts_exclusive,
        text_query=text_query,
        duration_sec=duration_sec,
    )
