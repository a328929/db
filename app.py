# -*- coding: utf-8 -*-
import math
import re
import sqlite3
import unicodedata
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, jsonify, render_template, request

# 引入统一数据库模块（P0级优化：统一并发策略）
from tg_harvest.db import connect_db, resolve_db_path as resolve_db_path_lib

# 解析 DB 路径（使用统一逻辑）
DB_PATH_STR = resolve_db_path_lib(os.getenv("TG_DB_NAME", "tg_data.db"))
DB_PATH = Path(DB_PATH_STR)

PAGE_SIZE = 100
MAX_COUNT = 50000

ZERO_WIDTH_RE = re.compile(r"[\u200b-\u200f\u2060\ufeff]")
CURLY_QUOTES_MAP = str.maketrans({"“": '"', "”": '"', "‘": "'", "’": "'"})

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


def create_app() -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")

    def get_conn() -> sqlite3.Connection:
        # P0级优化：使用带 WAL/超时配置的统一连接
        conn, _ = connect_db(str(DB_PATH))
        # connect_db 已配置 row_factory 和 pragma，这里无需重复
        return conn

    def norm_for_search(term: str) -> str:
        s = unicodedata.normalize("NFKC", term or "")
        s = ZERO_WIDTH_RE.sub("", s)
        return s.strip().lower()

    def tokenize_query(query: str) -> List[Tuple[str, str]]:
        q = (query or "").translate(CURLY_QUOTES_MAP)
        tokens: List[Tuple[str, str]] = []
        i, n = 0, len(q)
        while i < n:
            ch = q[i]
            if ch.isspace():
                i += 1
                continue
            if ch in "+-|":
                tokens.append(("OP", ch))
                i += 1
                continue
            if ch == '"':
                i += 1
                buf = []
                while i < n:
                    c = q[i]
                    if c == "\\" and i + 1 < n:
                        buf.append(q[i + 1])
                        i += 2
                        continue
                    if c == '"':
                        i += 1
                        break
                    buf.append(c)
                    i += 1
                term = norm_for_search("".join(buf))
                if term:
                    tokens.append(("PHRASE", term))
                continue

            buf = []
            while i < n and (not q[i].isspace()) and q[i] not in '+-|"':
                buf.append(q[i])
                i += 1
            term = norm_for_search("".join(buf))
            if term:
                tokens.append(("TERM", term))
        return tokens

    def fts_quote(value: str) -> str:
        # 统一用双引号包裹，避免 FTS5 特殊字符导致语法错误
        return f'"{value.replace(chr(34), "")}"'

    def to_fts_match(raw_query: str) -> str:
        tokens = tokenize_query(raw_query)
        if not tokens:
            return ""
        parts: List[str] = []
        prev_was_term = False
        pending_not = False
        for kind, value in tokens:
            if kind in {"TERM", "PHRASE"}:
                if pending_not and prev_was_term:
                    parts.append("NOT")
                    pending_not = False
                else:
                    if prev_was_term:
                        parts.append("AND")
                    pending_not = False
                if kind == "PHRASE":
                    parts.append(fts_quote(value))
                else:
                    parts.append(fts_quote(value))
                prev_was_term = True
                continue

            if value == "+":
                if parts and parts[-1] not in {"AND", "OR", "NOT"}:
                    parts.append("AND")
                prev_was_term = False
            elif value == "|":
                if parts and parts[-1] not in {"AND", "OR", "NOT"}:
                    parts.append("OR")
                prev_was_term = False
            elif value == "-":
                pending_not = True

        while parts and parts[-1] in {"AND", "OR", "NOT"}:
            parts.pop()
        return " ".join(parts)

    def split_positive_negative_terms(raw_query: str) -> Tuple[List[str], List[str]]:
        includes: List[str] = []
        excludes: List[str] = []
        pending_not = False
        for kind, value in tokenize_query(raw_query):
            if kind in {"TERM", "PHRASE"}:
                (excludes if pending_not else includes).append(value)
                pending_not = False
                continue
            if value == "-":
                pending_not = True
            elif value in {"+", "|"}:
                pending_not = False
        return includes, excludes

    def has_fts(conn: sqlite3.Connection) -> bool:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='messages_fts' LIMIT 1")
        return cur.fetchone() is not None

    def make_type_clause(search_type: str) -> Tuple[str, List[Any]]:
        st = (search_type or "all").lower()
        if st == "text":
            return "m.msg_type = 'TEXT'", []
        if st == "image":
            return "m.msg_type = 'PHOTO'", []
        if st == "video":
            return "m.msg_type IN ('VIDEO', 'GIF', 'VIDEO_NOTE')", []
        if st == "audio":
            return "m.msg_type IN ('AUDIO', 'VOICE')", []
        return "", []

    def choose_sort(search_type: str, sort_by: str, order: str) -> Tuple[str, str, str]:
        st = (search_type or "all").lower()
        sb = (sort_by or "time").lower()
        od = "ASC" if str(order).lower() == "asc" else "DESC"
        if st in {"all", "text"} and sb == "size":
            sb = "time"
        if sb == "size":
            return "COALESCE(mm.file_size, 0)", "size", od
        return "m.msg_date_ts", "time", od

    def build_result_title(row: sqlite3.Row) -> str:
        content = (row["content"] or "").strip()
        if content:
            return content
        file_name = (row["file_name"] or "").strip()
        if file_name:
            return file_name
        mt = (row["msg_type"] or "TEXT").upper()
        return TYPE_FALLBACK_TITLE.get(mt, "[无文本内容]")

    @app.get("/")
    def index():
        if not DB_PATH.exists():
            return ("未找到 tg_data.db。请把 app.py 放到数据库同一目录后再启动。", 500, {"Content-Type": "text/plain; charset=utf-8"})
        return render_template("index.html", page_size=PAGE_SIZE)

    @app.get("/api/meta")
    def api_meta():
        if not DB_PATH.exists():
            return jsonify({"ok": False, "error": "数据库不存在"}), 500
        try:
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT chat_id, chat_title
                    FROM chats
                    ORDER BY LOWER(chat_title) ASC, chat_id ASC
                """)
                chats = [{"chat_id": int(r["chat_id"]), "chat_title": (r["chat_title"] or f"Chat {r['chat_id']}").strip()} for r in cur.fetchall()]
            return jsonify({"ok": True, "chats": chats, "page_size": PAGE_SIZE})
        except sqlite3.Error as e:
            return jsonify({"ok": False, "error": f"读取群列表失败: {e}"}), 500

    @app.post("/api/search")
    def api_search():
        if not DB_PATH.exists():
            return jsonify({"ok": False, "error": "数据库不存在"}), 500
        data = request.get_json(silent=True) or {}
        raw_query = str(data.get("query", "") or "")
        chat_id_raw = data.get("chat_id", "all")
        search_type = str(data.get("search_type", "all") or "all").lower()
        sort_by_req = str(data.get("sort_by", "time") or "time").lower()
        order_req = str(data.get("order", "desc") or "desc").lower()
        try:
            page = max(int(data.get("page", 1) or 1), 1)
            chat_id = None if str(chat_id_raw).lower() == "all" else int(chat_id_raw)
            match_query = to_fts_match(raw_query)

            where_parts: List[str] = ["1=1"]
            params: List[Any] = []
            with get_conn() as conn:
                fts_enabled = has_fts(conn)

            if match_query and fts_enabled:
                where_parts.append("m.pk IN (SELECT rowid FROM messages_fts WHERE messages_fts MATCH ?)")
                params.append(match_query)
            elif raw_query.strip():
                includes, excludes = split_positive_negative_terms(raw_query)
                if includes:
                    for term in includes:
                        where_parts.append("LOWER(COALESCE(m.content, '')) LIKE ?")
                        params.append(f"%{term.lower()}%")
                if excludes:
                    for term in excludes:
                        where_parts.append("LOWER(COALESCE(m.content, '')) NOT LIKE ?")
                        params.append(f"%{term.lower()}%")
            if chat_id is not None:
                where_parts.append("m.chat_id = ?")
                params.append(chat_id)
            type_clause, type_params = make_type_clause(search_type)
            if type_clause:
                where_parts.append(type_clause)
                params.extend(type_params)
            where_sql = " AND ".join(where_parts)
            order_expr, effective_sort, effective_order = choose_sort(search_type, sort_by_req, order_req)

            from_sql = """
                FROM messages m
                LEFT JOIN chats c ON c.chat_id = m.chat_id
                LEFT JOIN message_media mm ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id
            """

            count_sql = f"SELECT COUNT(*) AS c FROM (SELECT m.pk {from_sql} WHERE {where_sql} LIMIT ?)"
            query_sql = f"""
                SELECT m.pk,m.chat_id,c.chat_title,m.message_id,m.msg_date_text,m.msg_date_ts,m.msg_type,m.link,m.content,m.grouped_id,
                       mm.file_name,mm.file_size,mm.mime_type,mm.media_kind
                {from_sql}
                WHERE {where_sql}
                ORDER BY {order_expr} {effective_order}, m.msg_date_ts {effective_order}, m.pk {effective_order}
                LIMIT ? OFFSET ?
            """
            with get_conn() as conn:
                cur = conn.cursor()
                cur.execute(count_sql, params + [MAX_COUNT + 1])
                counted = int(cur.fetchone()["c"] or 0)
                total_is_capped = counted > MAX_COUNT
                total = min(counted, MAX_COUNT)
                total_pages = math.ceil(total / PAGE_SIZE) if total > 0 else 0
                if total_pages > 0 and page > total_pages:
                    page = total_pages
                offset = (page - 1) * PAGE_SIZE if total_pages > 0 else 0
                cur.execute(query_sql, params + [PAGE_SIZE, offset])
                rows = cur.fetchall()

            items = []
            for r in rows:
                file_size = int(r["file_size"]) if r["file_size"] is not None else None
                items.append({
                    "pk": int(r["pk"]), "chat_id": int(r["chat_id"]), "chat_title": r["chat_title"] or "", "message_id": int(r["message_id"]),
                    "msg_date_text": r["msg_date_text"] or "", "msg_type": r["msg_type"] or "TEXT", "link": r["link"] or "", "content": r["content"] or "",
                    "file_name": r["file_name"] or "", "file_size": file_size, "title": build_result_title(r),
                })
            return jsonify({
                "ok": True, "query": raw_query, "fts_query": match_query, "page": page, "page_size": PAGE_SIZE,
                "total": total, "total_pages": total_pages, "total_is_capped": total_is_capped,
                "effective_sort": effective_sort, "effective_order": effective_order.lower(), "items": items,
            })
        except ValueError:
            return jsonify({"ok": False, "error": "参数格式错误"}), 400
        except sqlite3.Error as e:
            return jsonify({"ok": False, "error": f"查询失败: {e}"}), 500
        except Exception as e:
            return jsonify({"ok": False, "error": f"系统异常: {e}"}), 500

    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8890, debug=False)
