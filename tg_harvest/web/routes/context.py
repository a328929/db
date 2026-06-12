import sqlite3
from contextlib import closing

from flask import jsonify, render_template, request
from werkzeug.routing import BaseConverter

from tg_harvest.web.responses import json_error, logged_json_error


class SignedIntConverter(BaseConverter):
    regex = r"-?\d+"

    def to_python(self, value):
        return int(value)

    def to_url(self, value):
        return str(int(value))


def register_context_routes(
    app,
    *,
    logger,
    get_conn_fn,
    from_sql: str,
    map_search_items_fn,
) -> None:
    app.url_map.converters.setdefault("signed_int", SignedIntConverter)

    @app.get("/chat/<signed_int:chat_id>")
    def chat_context_page(chat_id):
        return render_template("context.html", chat_id=chat_id)

    @app.get("/api/chat/<signed_int:chat_id>/context")
    def api_chat_context(chat_id):
        msg_id_raw = request.args.get("msg_id", "")
        direction = request.args.get("direction", "around").strip().lower()

        if not msg_id_raw.isdigit():
            return json_error("无效的 msg_id", 400)
        if direction not in {"around", "before", "after"}:
            return json_error("无效的 direction", 400)

        msg_id = int(msg_id_raw)

        # 提取字段与搜索结果保持一致
        select_clause = """
            SELECT m.pk, m.chat_id, c.chat_title, c.chat_username, m.message_id, m.msg_date_text, m.msg_date_ts,
                   m.msg_type, m.content, m.grouped_id,
                   m.is_promo,
                   mm.file_name, mm.file_size, mm.mime_type, mm.media_kind, mm.duration_sec
        """
        base_query = f"{select_clause} {from_sql} WHERE m.chat_id = ?"

        try:
            with closing(get_conn_fn()) as conn:
                cur = conn.cursor()
                try:
                    rows = []

                    if direction == "around":
                        cur.execute(
                            f"{base_query} AND m.message_id < ? ORDER BY m.message_id DESC LIMIT 50",
                            (chat_id, msg_id),
                        )
                        before_rows = cur.fetchall()

                        cur.execute(
                            f"{base_query} AND m.message_id = ?",
                            (chat_id, msg_id),
                        )
                        anchor_row = cur.fetchone()

                        cur.execute(
                            f"{base_query} AND m.message_id > ? ORDER BY m.message_id ASC LIMIT 50",
                            (chat_id, msg_id),
                        )
                        after_rows = cur.fetchall()

                        rows = list(reversed(before_rows))
                        if anchor_row:
                            rows.append(anchor_row)
                        rows.extend(after_rows)

                    elif direction == "before":
                        cur.execute(
                            f"{base_query} AND m.message_id < ? ORDER BY m.message_id DESC LIMIT 100",
                            (chat_id, msg_id),
                        )
                        rows = list(reversed(cur.fetchall()))

                    elif direction == "after":
                        cur.execute(
                            f"{base_query} AND m.message_id > ? ORDER BY m.message_id ASC LIMIT 100",
                            (chat_id, msg_id),
                        )
                        rows = cur.fetchall()

                    if not rows:
                        return jsonify({"ok": True, "items": []})

                    items = map_search_items_fn(rows, detail_level="full")
                    return jsonify({"ok": True, "items": items})
                finally:
                    cur.close()

        except sqlite3.Error:
            return logged_json_error(logger, "获取上下文失败", "数据库查询失败")
        except Exception:
            return logged_json_error(logger, "系统异常", "系统内部错误")
