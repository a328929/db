import sqlite3
from typing import Any, Dict, List

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
    content = (row["content"] or "").strip()
    if content:
        return content
    file_name = (row["file_name"] or "").strip()
    if file_name:
        return file_name
    mt = (row["msg_type"] or "TEXT").upper()
    return TYPE_FALLBACK_TITLE.get(mt, "[无文本内容]")


def _build_snippet(row: sqlite3.Row, max_len: int = 120) -> str:
    content = (row["content"] or "").strip()
    if not content:
        return ""
    if len(content) <= max_len:
        return content
    return f"{content[:max_len]}…"


def _build_search_display_fields(row: sqlite3.Row, detail_level: str = "lite") -> Dict[str, Any]:
    fields = {
        "file_name": row["file_name"] or "",
        "title": build_result_title(row),
        "snippet": _build_snippet(row),
    }
    if detail_level == "full":
        fields["content"] = row["content"] or ""
    return fields


def _map_search_row(row: sqlite3.Row, detail_level: str = "lite") -> Dict[str, Any]:
    file_size = int(row["file_size"]) if row["file_size"] is not None else None
    item = {
        "pk": int(row["pk"]),
        "chat_id": int(row["chat_id"]),
        "chat_title": row["chat_title"] or "",
        "message_id": int(row["message_id"]),
        "msg_date_text": row["msg_date_text"] or "",
        "msg_type": row["msg_type"] or "TEXT",
        "link": row["link"] or "",
        "file_size": file_size,
    }
    item.update(_build_search_display_fields(row, detail_level=detail_level))
    return item


def _map_search_items(rows: List[sqlite3.Row], detail_level: str = "lite") -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for row in rows:
        items.append(_map_search_row(row, detail_level=detail_level))
    return items
