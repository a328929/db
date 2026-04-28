# -*- coding: utf-8 -*-
import sqlite3
from contextlib import closing
from typing import Optional

from flask import jsonify, render_template, request

from tg_harvest.web.telegram_links import build_telegram_link_bundle


def _parse_positive_int(raw_value: str, field_name: str) -> int:
    try:
        value = int(str(raw_value or "").strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} 参数非法") from exc
    if value <= 0:
        raise ValueError(f"{field_name} 参数非法")
    return value


def _load_chat_meta(
    get_conn_fn, chat_id: int
) -> tuple[Optional[str], Optional[str]]:
    with closing(get_conn_fn()) as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT chat_title, chat_username
                FROM chats
                WHERE chat_id = ?
                LIMIT 1
                """,
                (int(chat_id),),
            )
            row = cur.fetchone()
            if row is None:
                return None, None
            return row["chat_title"], row["chat_username"]
        finally:
            cur.close()


def register_open_telegram_routes(app, *, logger, get_conn_fn) -> None:
    @app.get("/open/telegram")
    def open_telegram_message():
        try:
            chat_id = _parse_positive_int(request.args.get("chat_id", ""), "chat_id")
            message_id = _parse_positive_int(
                request.args.get("message_id", ""), "message_id"
            )
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400

        try:
            chat_title, chat_username = _load_chat_meta(get_conn_fn, chat_id)
        except sqlite3.Error:
            logger.exception("读取 Telegram 跳转元数据失败")
            return jsonify({"ok": False, "error": "读取跳转元数据失败"}), 500
        except Exception:
            logger.exception("系统异常")
            return jsonify({"ok": False, "error": "系统异常"}), 500

        bundle = build_telegram_link_bundle(
            chat_id=chat_id,
            message_id=message_id,
            chat_username=chat_username,
        )
        return render_template(
            "open_telegram.html",
            chat_id=chat_id,
            chat_title=chat_title or f"Chat {chat_id}",
            message_id=message_id,
            telegram_app_link=bundle.app_link,
            telegram_web_link=bundle.web_link,
        )
