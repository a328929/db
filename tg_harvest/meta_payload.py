import sqlite3
from typing import Any, Dict, Optional, Tuple

from pypinyin import lazy_pinyin


def _chat_title_or_fallback(chat_id: int, chat_title: Optional[str]) -> str:
    title = (chat_title or "").strip()
    return title if title else f"Chat {chat_id}"


def _is_cjk_char(ch: str) -> bool:
    if not ch:
        return False
    codepoint = ord(ch)
    return (
        0x4E00 <= codepoint <= 0x9FFF
        or 0x3400 <= codepoint <= 0x4DBF
        or 0x20000 <= codepoint <= 0x2A6DF
        or 0x2A700 <= codepoint <= 0x2B73F
        or 0x2B740 <= codepoint <= 0x2B81F
        or 0x2B820 <= codepoint <= 0x2CEAF
        or 0xF900 <= codepoint <= 0xFAFF
    )


def _chat_sort_key(chat_title: str, chat_id: int) -> Tuple[int, str, str, int]:
    normalized_title = (chat_title or "").strip() or f"Chat {chat_id}"
    first_char = normalized_title[0]

    if first_char.isdigit():
        category = 0
        lexical_key = normalized_title.casefold()
    elif _is_cjk_char(first_char):
        category = 1
        lexical_key = "".join(lazy_pinyin(normalized_title)).casefold()
    elif first_char.isascii() and first_char.isalpha():
        category = 2
        lexical_key = normalized_title.casefold()
    else:
        category = 3
        lexical_key = normalized_title.casefold()

    return category, lexical_key, normalized_title.casefold(), chat_id


def _build_meta_payload(conn: sqlite3.Connection, *, page_size: int) -> Dict[str, Any]:
    cur = conn.cursor()
    try:
        cur.execute("SELECT chat_id, chat_title FROM chats")
        chats = [
            {
                "chat_id": int(r["chat_id"]),
                "chat_title": _chat_title_or_fallback(int(r["chat_id"]), r["chat_title"]),
            }
            for r in cur.fetchall()
        ]
        chats.sort(key=lambda item: _chat_sort_key(item["chat_title"], int(item["chat_id"])))
        return {"ok": True, "chats": chats, "page_size": page_size}
    finally:
        cur.close()
