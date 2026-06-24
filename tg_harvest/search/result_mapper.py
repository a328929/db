import sqlite3
from typing import Any

from tg_harvest.domain.coerce import safe_int
from tg_harvest.web.telegram_links import build_telegram_open_link

TYPE_FALLBACK_TITLE = {
    "PHOTO": "[无文案图片]",
    "VIDEO": "[无文案视频]",
    "GIF": "[无文案视频]",
    "VIDEO_NOTE": "[无文案视频]",
    "AUDIO": "[无文案音频]",
    "VOICE": "[无文案音频]",
    "FILE": "[无文案文件]",
    "TEXT": "[无文本内容]",
}


def build_result_title(row: sqlite3.Row) -> str:
    content = (_row_value(row, "content", "") or "").strip()
    if content:
        return content
    file_name = (_row_value(row, "file_name", "") or "").strip()
    if file_name:
        return file_name
    mt = (_row_value(row, "msg_type", "TEXT") or "TEXT").upper()
    return TYPE_FALLBACK_TITLE.get(mt, "[无文本内容]")


def _build_snippet(row: sqlite3.Row, max_len: int = 120) -> str:
    content = (_row_value(row, "content", "") or "").strip()
    if not content:
        return ""
    if len(content) <= max_len:
        return content
    return f"{content[:max_len]}…"


def _build_search_display_fields(
    row: sqlite3.Row, detail_level: str = "lite"
) -> dict[str, Any]:
    fields = {
        "file_name": _row_value(row, "file_name", "") or "",
        "title": build_result_title(row),
        "snippet": _build_snippet(row),
    }
    if detail_level == "full":
        fields["content"] = _row_value(row, "content", "") or ""
    return fields


def _row_value(row: sqlite3.Row, key: str, default: Any = None) -> Any:
    try:
        return row[key]
    except (IndexError, KeyError):
        return default


def _map_search_row(row: sqlite3.Row, detail_level: str = "lite") -> dict[str, Any]:
    chat_id = int(row["chat_id"])
    message_id = int(row["message_id"])
    file_size_raw = _row_value(row, "file_size")
    duration_sec_raw = _row_value(row, "duration_sec")
    file_size = int(file_size_raw) if file_size_raw is not None else None
    duration_sec = int(duration_sec_raw) if duration_sec_raw is not None else None
    item = {
        "pk": int(row["pk"]),
        "chat_id": chat_id,
        "chat_title": _row_value(row, "chat_title", "") or "",
        "message_id": message_id,
        "msg_date_text": _row_value(row, "msg_date_text", "") or "",
        "msg_type": _row_value(row, "msg_type", "TEXT") or "TEXT",
        "link": build_telegram_open_link(chat_id=chat_id, message_id=message_id),
        "file_size": file_size,
        "duration_sec": duration_sec,
        "is_promo": safe_int(_row_value(row, "is_promo", None)),
    }
    item.update(_build_search_display_fields(row, detail_level=detail_level))
    return item


def _map_search_items(
    rows: list[sqlite3.Row], detail_level: str = "lite"
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in rows:
        items.append(_map_search_row(row, detail_level=detail_level))
    return items
