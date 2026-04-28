# -*- coding: utf-8 -*-
import sqlite3
from contextlib import closing
from flask import render_template, request, jsonify


def register_context_routes(
    app,
    *,
    logger,
    get_conn_fn,
    from_sql: str,
    map_search_items_fn,
) -> None:
    
    # 1. 页面路由：渲染上下文阅读器骨架
    @app.get("/chat/<int:chat_id>")
    def chat_context_page(chat_id):
        return render_template("context.html", chat_id=chat_id)

    # 2. 数据接口：精准定位，防断层的双向/单向查询
    @app.get("/api/chat/<int:chat_id>/context")
    def api_chat_context(chat_id):
        msg_id_raw = request.args.get("msg_id", "")
        direction = request.args.get("direction", "around").strip().lower()
        
        if not msg_id_raw.isdigit():
            return jsonify({"ok": False, "error": "无效的 msg_id"}), 400
        if direction not in {"around", "before", "after"}:
            return jsonify({"ok": False, "error": "无效的 direction"}), 400
        
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
                
                # 为了解决断层问题，必须使用 ORDER BY message_id LIMIT 的形式
                # 而不是 id BETWEEN x AND y
                
                rows = []
                
                if direction == "around":
                    # 向上找 50 条 (真实存在的记录，无视 ID 断层)
                    cur.execute(f"{base_query} AND m.message_id < ? ORDER BY m.message_id DESC LIMIT 50", (chat_id, msg_id))
                    before_rows = cur.fetchall()
                    
                    # 找锚点本身 (1 条)
                    cur.execute(f"{base_query} AND m.message_id = ?", (chat_id, msg_id))
                    anchor_row = cur.fetchone()
                    
                    # 向下找 50 条 (真实存在的记录)
                    cur.execute(f"{base_query} AND m.message_id > ? ORDER BY m.message_id ASC LIMIT 50", (chat_id, msg_id))
                    after_rows = cur.fetchall()
                    
                    # 拼接：反转之前的记录（使其按时间正序），加上锚点，加上之后的记录
                    rows = list(reversed(before_rows))
                    if anchor_row:
                        rows.append(anchor_row)
                    rows.extend(after_rows)
                    
                elif direction == "before":
                    # 点击“加载上一页”：向上找 100 条
                    cur.execute(f"{base_query} AND m.message_id < ? ORDER BY m.message_id DESC LIMIT 100", (chat_id, msg_id))
                    rows = list(reversed(cur.fetchall()))
                    
                elif direction == "after":
                    # 点击“加载下一页”：向下找 100 条
                    cur.execute(f"{base_query} AND m.message_id > ? ORDER BY m.message_id ASC LIMIT 100", (chat_id, msg_id))
                    rows = cur.fetchall()
                
                # 如果找不到数据且方向是 around，说明锚点本身已被删除且周围没有数据，或频道不存在
                if not rows:
                    return jsonify({"ok": True, "items": []})
                
                items = map_search_items_fn(rows, detail_level="full")
                return jsonify({"ok": True, "items": items})
                
        except sqlite3.Error:
            logger.exception("获取上下文失败")
            return jsonify({"ok": False, "error": "数据库查询失败"}), 500
        except Exception:
            logger.exception("系统异常")
            return jsonify({"ok": False, "error": "系统内部错误"}), 500
