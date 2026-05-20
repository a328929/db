# -*- coding: utf-8 -*-
import sqlite3
from typing import Any, Dict, Optional, Tuple

from tg_harvest.domain.meta_payload import _chat_sort_key, _chat_title_or_fallback


def build_admin_chats_payload(conn: sqlite3.Connection) -> Dict[str, Any]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                c.chat_id,
                c.chat_title,
                c.message_count
            FROM chats c
            """
        )
        # /api/admin/chats 主字段契约为 chat_id/chat_title/message_count；冗余别名字段已移除（前端兼容在 JS 内处理）。
        chats = [
            {
                "chat_id": int(row["chat_id"]),
                "chat_title": _chat_title_or_fallback(
                    int(row["chat_id"]), row["chat_title"]
                ),
                "message_count": int(row["message_count"] or 0),
            }
            for row in cur.fetchall()
        ]
        chats.sort(
            key=lambda item: _chat_sort_key(
                str(item.get("chat_title") or ""),
                int(str(item.get("chat_id") or 0)),
            )
        )
        return {"ok": True, "chats": chats}
    finally:
        cur.close()


def parse_admin_chat_id(raw_chat_id: Optional[str]) -> Optional[int]:
    value = (raw_chat_id or "").strip()
    if not value or value.lower() == "none":
        return None
    return int(value)


def build_admin_stats_payload(
    conn: sqlite3.Connection, chat_id: Optional[int]
) -> Tuple[Dict[str, Any], int]:
    cur = conn.cursor()
    try:
        if chat_id is None:
            cur.execute("SELECT COUNT(*) AS chat_count FROM chats")
            chat_count = int(cur.fetchone()["chat_count"] or 0)
            cur.execute(
                "SELECT COALESCE(SUM(message_count), 0) AS message_count FROM chats"
            )
            message_count = int(cur.fetchone()["message_count"] or 0)

            return {
                "ok": True,
                "scope": "all",
                "chat_count": chat_count,
                "message_count": message_count,
            }, 200

        cur.execute(
            """
            SELECT
                c.chat_id,
                c.chat_title,
                c.message_count
            FROM chats c
            WHERE c.chat_id = ?
            """,
            (chat_id,),
        )
        row = cur.fetchone()
        if row is None:
            return {"ok": False, "error": "chat_id 不存在"}, 404

        return {
            "ok": True,
            "scope": "chat",
            "chat_id": int(row["chat_id"]),
            "chat_title": _chat_title_or_fallback(
                int(row["chat_id"]), row["chat_title"]
            ),
            "message_count": int(row["message_count"] or 0),
        }, 200
    finally:
        cur.close()


def get_admin_chat_brief(
    conn: sqlite3.Connection, chat_id: int
) -> Optional[Dict[str, Any]]:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT chat_id, chat_title FROM chats WHERE chat_id = ? LIMIT 1",
            (chat_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        actual_chat_id = int(row["chat_id"])
        return {
            "chat_id": actual_chat_id,
            "chat_title": _chat_title_or_fallback(actual_chat_id, row["chat_title"]),
        }
    finally:
        cur.close()
