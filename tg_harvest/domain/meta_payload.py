import sqlite3
from typing import Any

from tg_harvest.domain.chat_titles import chat_sort_key, chat_title_or_fallback


def _build_meta_payload(conn: sqlite3.Connection, *, page_size: int) -> dict[str, Any]:
    cur = conn.cursor()
    try:
        cur.execute("SELECT chat_id, chat_title FROM chats")
        chats = [
            {
                "chat_id": int(r["chat_id"]),
                "chat_title": chat_title_or_fallback(
                    int(r["chat_id"]), r["chat_title"]
                ),
            }
            for r in cur.fetchall()
        ]
        chats.sort(
            key=lambda item: chat_sort_key(
                str(item["chat_title"]), int(item["chat_id"])
            )
        )
        return {"ok": True, "chats": chats, "page_size": page_size}
    finally:
        cur.close()
