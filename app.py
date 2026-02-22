# -*- coding: utf-8 -*-
import math
import re
import sqlite3
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, jsonify, render_template, request

DB_PATH = Path(__file__).resolve().with_name("tg_data.db")
PAGE_SIZE = 100

# 用于搜索词归一化（增强对零宽字符、全角半角干扰的鲁棒性）
ZERO_WIDTH_RE = re.compile(r"[\u200b-\u200f\u2060\ufeff]")
CURLY_QUOTES_MAP = str.maketrans({"“": '"', "”": '"', "‘": "'", "’": "'"})

# 结果标题兜底文案
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
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

    def norm_for_search(term: str) -> str:
        """
        搜索词归一化：
        - NFKC（全角半角统一）
        - 去零宽字符
        - 小写化
        """
        s = unicodedata.normalize("NFKC", term or "")
        s = ZERO_WIDTH_RE.sub("", s)
        return s.strip().lower()

    # =========================
    # 搜索语法解析（支持 + / - / | / "短语"）
    # 说明：
    # - 使用一个简单布尔表达式解析器
    # - 优先级：NOT(-) > AND(+) / 隐式AND > OR(|)
    # - 空格/换行作为“分词边界”，相邻词默认视为 AND
    # - 连续符号会自动跳过（如 A++B）
    # =========================

    def tokenize_query(q: str) -> List[Tuple[str, str]]:
        """
        把用户输入拆成 token:
        - ("OP", "+") / ("OP", "-") / ("OP", "|")
        - ("TERM", "关键词")
        引号内内容整体作为 TERM，里面的 + - | 不再当操作符。
        """
        q = (q or "").translate(CURLY_QUOTES_MAP)
        tokens: List[Tuple[str, str]] = []
        i = 0
        n = len(q)

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
                        # 支持 \" 转义
                        buf.append(q[i + 1])
                        i += 2
                        continue
                    if c == '"':
                        i += 1
                        break
                    buf.append(c)
                    i += 1

                term = "".join(buf).strip()
                if term:
                    tokens.append(("TERM", term))
                continue

            # 普通词
            buf = []
            while i < n:
                c = q[i]
                if c in '+-|"':
                    break
                if c.isspace():
                    break
                buf.append(c)
                i += 1

            term = "".join(buf).strip()
            if term:
                tokens.append(("TERM", term))

        return tokens

    class QueryParser:
        """
        递归下降解析器，输出 AST（元组结构）
        AST 节点格式：
          ("TERM", "女孩")
          ("NOT", child)
          ("AND", left, right)
          ("OR", left, right)
        """
        def __init__(self, tokens: List[Tuple[str, str]]):
            self.tokens = tokens
            self.i = 0

        def peek(self) -> Optional[Tuple[str, str]]:
            if self.i >= len(self.tokens):
                return None
            return self.tokens[self.i]

        def pop(self) -> Optional[Tuple[str, str]]:
            t = self.peek()
            if t is not None:
                self.i += 1
            return t

        def parse(self):
            return self.parse_or()

        def parse_or(self):
            node = self.parse_and()
            if node is None:
                return None

            while True:
                t = self.peek()
                if t and t[0] == "OP" and t[1] == "|":
                    self.pop()
                    rhs = self.parse_and()
                    if rhs is None:
                        # 连续 | 或尾部 | 时自动忽略
                        continue
                    node = ("OR", node, rhs)
                    continue
                break
            return node

        def parse_and(self):
            node = self.parse_unary()

            # 跳过开头无效符号（例如 ++女孩）
            while node is None:
                t = self.peek()
                if t is None:
                    return None
                if t[0] == "OP" and t[1] in {"+", "|"}:
                    self.pop()
                    node = self.parse_unary()
                    continue
                self.pop()
                node = self.parse_unary()

            while True:
                t = self.peek()
                if t is None:
                    break

                # OR 交给 parse_or 处理
                if t[0] == "OP" and t[1] == "|":
                    break

                # 显式 AND（+）
                if t[0] == "OP" and t[1] == "+":
                    self.pop()
                    rhs = self.parse_unary()

                    # 处理连续 +++
                    while rhs is None:
                        t2 = self.peek()
                        if t2 is None:
                            break
                        if t2[0] == "OP" and t2[1] in {"+", "|"}:
                            if t2[1] == "|":
                                break
                            self.pop()
                            rhs = self.parse_unary()
                            continue
                        self.pop()
                        rhs = self.parse_unary()

                    if rhs is None:
                        break

                    node = ("AND", node, rhs)
                    continue

                # 隐式 AND（相邻词、换行）
                rhs = self.parse_unary()
                if rhs is None:
                    t3 = self.peek()
                    if t3 is None or (t3[0] == "OP" and t3[1] == "|"):
                        break
                    self.pop()
                    continue

                node = ("AND", node, rhs)

            return node

        def parse_unary(self):
            negate = False

            # 连续 - 视为一次 NOT；连续 + 直接跳过
            while True:
                t = self.peek()
                if t is None:
                    return None

                if t[0] == "OP" and t[1] == "-":
                    negate = True
                    self.pop()
                    continue

                if t[0] == "OP" and t[1] == "+":
                    self.pop()
                    continue

                break

            t = self.peek()
            if t is None:
                return None

            if t[0] == "OP" and t[1] == "|":
                return None

            if t[0] != "TERM":
                self.pop()
                return None

            term = self.pop()[1]
            node = ("TERM", term)
            if negate:
                return ("NOT", node)
            return node

    # 搜索字段（兼顾内容 + 归一化内容 + 文件名）
    # 这样对随机零宽字符、全角半角干扰会更稳一些（content_norm 已被你采集时清洗过）
    SEARCH_EXPR = "LOWER(COALESCE(m.content,'') || ' ' || COALESCE(m.content_norm,'') || ' ' || COALESCE(mm.file_name,''))"

    def ast_to_sql(node) -> Tuple[str, List[str]]:
        """
        AST -> SQL where 片段 + 参数
        全部走 LIKE，避免 FTS 语法和你自定义语法冲突。
        """
        if node is None:
            return "", []

        kind = node[0]

        if kind == "TERM":
            term = norm_for_search(node[1])
            if not term:
                return "", []
            return f"({SEARCH_EXPR} LIKE ?)", [f"%{term}%"]

        if kind == "NOT":
            child_sql, child_params = ast_to_sql(node[1])
            if not child_sql:
                return "", []
            return f"(NOT {child_sql})", child_params

        if kind in {"AND", "OR"}:
            left_sql, left_params = ast_to_sql(node[1])
            right_sql, right_params = ast_to_sql(node[2])

            if left_sql and right_sql:
                op = "AND" if kind == "AND" else "OR"
                return f"({left_sql} {op} {right_sql})", left_params + right_params
            if left_sql:
                return left_sql, left_params
            return right_sql, right_params

        return "", []

    def make_type_clause(search_type: str) -> Tuple[str, List[Any]]:
        """
        按你页面上的“搜索类型”映射数据库 msg_type
        """
        st = (search_type or "all").lower()

        if st == "text":
            return "m.msg_type = 'TEXT'", []

        if st == "image":
            # 只把图片映射为 PHOTO
            return "m.msg_type = 'PHOTO'", []

        if st == "video":
            # GIF / VIDEO_NOTE 也按视频归类，使用上更顺手
            return "m.msg_type IN ('VIDEO', 'GIF', 'VIDEO_NOTE')", []

        if st == "audio":
            # VOICE 归到音频
            return "m.msg_type IN ('AUDIO', 'VOICE')", []

        return "", []

    def choose_sort(search_type: str, sort_by: str, order: str) -> Tuple[str, str, str]:
        """
        返回 (order_expr, effective_sort, effective_order)
        并在后端做一次兜底校验，防止前端状态冲突。
        """
        st = (search_type or "all").lower()
        sb = (sort_by or "time").lower()
        od = "ASC" if str(order).lower() == "asc" else "DESC"

        # 文本/全部时，强制时间排序
        if st in {"all", "text"} and sb == "size":
            sb = "time"

        if sb == "size":
            # NULL -> 0，避免排序报错
            expr = "COALESCE(mm.file_size, 0)"
        else:
            expr = "m.msg_date_ts"
            sb = "time"

        return expr, sb, od

    def build_result_title(row: sqlite3.Row) -> str:
        """
        焦点一（h3）标题内容：
        - 文本消息 => 内容
        - 媒体有配文 => 配文
        - 媒体无配文 => file_name
        - file_name也无 => 按类型兜底
        """
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
            return (
                "未找到 tg_data.db。请把 app.py 放到数据库同一目录后再启动。",
                500,
                {"Content-Type": "text/plain; charset=utf-8"},
            )
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
                chats = [
                    {
                        "chat_id": int(r["chat_id"]),
                        "chat_title": (r["chat_title"] or f"Chat {r['chat_id']}").strip(),
                    }
                    for r in cur.fetchall()
                ]
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
            page = int(data.get("page", 1) or 1)
        except Exception:
            page = 1
        if page < 1:
            page = 1

        try:
            chat_id: Optional[int]
            if str(chat_id_raw).lower() == "all":
                chat_id = None
            else:
                chat_id = int(chat_id_raw)

            # 解析搜索语法
            tokens = tokenize_query(raw_query)
            parser = QueryParser(tokens)
            ast = parser.parse()
            search_sql, search_params = ast_to_sql(ast)

            where_parts: List[str] = ["1=1"]
            params: List[Any] = []

            if chat_id is not None:
                where_parts.append("m.chat_id = ?")
                params.append(chat_id)

            type_clause, type_params = make_type_clause(search_type)
            if type_clause:
                where_parts.append(type_clause)
                params.extend(type_params)

            if search_sql:
                where_parts.append(search_sql)
                params.extend(search_params)

            where_sql = " AND ".join(where_parts)
            order_expr, effective_sort, effective_order = choose_sort(search_type, sort_by_req, order_req)

            from_sql = """
                FROM messages m
                LEFT JOIN chats c ON c.chat_id = m.chat_id
                LEFT JOIN message_media mm ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id
            """

            count_sql = f"SELECT COUNT(*) AS c {from_sql} WHERE {where_sql}"

            # 排序增加稳定次序，避免翻页抖动
            query_sql = f"""
                SELECT
                    m.pk,
                    m.chat_id,
                    c.chat_title,
                    m.message_id,
                    m.msg_date_text,
                    m.msg_date_ts,
                    m.msg_type,
                    m.link,
                    m.content,
                    m.grouped_id,
                    mm.file_name,
                    mm.file_size,
                    mm.mime_type,
                    mm.media_kind
                {from_sql}
                WHERE {where_sql}
                ORDER BY {order_expr} {effective_order}, m.msg_date_ts {effective_order}, m.pk {effective_order}
                LIMIT ? OFFSET ?
            """

            with get_conn() as conn:
                cur = conn.cursor()

                cur.execute(count_sql, params)
                total = int(cur.fetchone()["c"] or 0)

                total_pages = math.ceil(total / PAGE_SIZE) if total > 0 else 0
                if total_pages > 0 and page > total_pages:
                    page = total_pages

                offset = (page - 1) * PAGE_SIZE if total_pages > 0 else 0

                cur.execute(query_sql, params + [PAGE_SIZE, offset])
                rows = cur.fetchall()

            items: List[Dict[str, Any]] = []
            for r in rows:
                try:
                    file_size = int(r["file_size"]) if r["file_size"] is not None else None
                except Exception:
                    file_size = None

                items.append({
                    "pk": int(r["pk"]),
                    "chat_id": int(r["chat_id"]),
                    "chat_title": r["chat_title"] or "",
                    "message_id": int(r["message_id"]),
                    "msg_date_text": r["msg_date_text"] or "",
                    "msg_type": r["msg_type"] or "TEXT",
                    "link": r["link"] or "",
                    "content": r["content"] or "",
                    "file_name": r["file_name"] or "",
                    "file_size": file_size,
                    "title": build_result_title(r),
                })

            return jsonify({
                "ok": True,
                "query": raw_query,
                "page": page,
                "page_size": PAGE_SIZE,
                "total": total,
                "total_pages": total_pages,
                "effective_sort": effective_sort,   # 前端显示/渲染元数据要用
                "effective_order": effective_order.lower(),
                "items": items,
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
    # 如果你要局域网访问，把 host 改成 0.0.0.0
    app.run(host="0.0.0.0", port=8890, debug=False)
