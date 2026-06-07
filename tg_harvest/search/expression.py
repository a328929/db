# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from tg_harvest.domain.normalize import normalize_search_term


QUERY_SYMBOLS_MAP = str.maketrans(
    {
        "“": '"',
        "”": '"',
        "‘": "'",
        "’": "'",
        "＋": "+",
        "／": "/",
        "－": "-",
        "（": "(",
        "）": ")",
    }
)

TOKEN_TERM = "TERM"
TOKEN_PHRASE = "PHRASE"
TOKEN_AND = "AND"
TOKEN_OR = "OR"
TOKEN_NOT = "NOT"
TOKEN_LPAREN = "LPAREN"
TOKEN_RPAREN = "RPAREN"
_START_TOKENS = {TOKEN_TERM, TOKEN_PHRASE, TOKEN_NOT, TOKEN_LPAREN}


@dataclass(frozen=True)
class ExprToken:
    kind: str
    value: str


@dataclass(frozen=True)
class SearchExprNode:
    kind: str
    value: str = ""
    left: Optional["SearchExprNode"] = None
    right: Optional["SearchExprNode"] = None


def norm_for_search(term: str) -> str:
    return normalize_search_term(term)


def lex_query(query: str) -> List[ExprToken]:
    q = (query or "").translate(QUERY_SYMBOLS_MAP)
    tokens: List[ExprToken] = []
    i, n = 0, len(q)
    while i < n:
        ch = q[i]
        if ch.isspace():
            i += 1
            continue
        if ch == "+":
            tokens.append(ExprToken(TOKEN_AND, ch))
            i += 1
            continue
        if ch == "/":
            tokens.append(ExprToken(TOKEN_OR, ch))
            i += 1
            continue
        if ch == "-":
            tokens.append(ExprToken(TOKEN_NOT, ch))
            i += 1
            continue
        if ch == "(":
            tokens.append(ExprToken(TOKEN_LPAREN, ch))
            i += 1
            continue
        if ch == ")":
            tokens.append(ExprToken(TOKEN_RPAREN, ch))
            i += 1
            continue
        if ch == '"':
            i += 1
            buf: List[str] = []
            closed = False
            while i < n:
                c = q[i]
                if c == "\\" and i + 1 < n:
                    buf.append(q[i + 1])
                    i += 2
                    continue
                if c == '"':
                    i += 1
                    closed = True
                    break
                buf.append(c)
                i += 1
            if not closed:
                raise ValueError("短语引号未闭合")
            term = norm_for_search("".join(buf))
            if term:
                tokens.append(ExprToken(TOKEN_PHRASE, term))
            continue

        buf: List[str] = []
        while i < n and (not q[i].isspace()) and q[i] not in '+-/()"':
            buf.append(q[i])
            i += 1
        term = norm_for_search("".join(buf))
        if term:
            tokens.append(ExprToken(TOKEN_TERM, term))
    return tokens


def parse_query(raw_query: str) -> Optional[SearchExprNode]:
    normalized_query = (raw_query or "").strip()
    if not normalized_query:
        return None
    if len(normalized_query) > 1000:
        raise ValueError("搜索表达式过长")

    parser = _SearchExprParser(lex_query(raw_query))
    return parser.parse()


class _SearchExprParser:
    def __init__(self, tokens: Sequence[ExprToken]) -> None:
        self.tokens = list(tokens)
        self.index = 0

    def parse(self) -> SearchExprNode:
        if not self.tokens:
            raise ValueError("搜索表达式为空")
        expr = self._parse_and_expr()
        if self._peek() is not None:
            raise ValueError("搜索表达式存在无法解析的尾部内容")
        return expr

    def _parse_and_expr(self) -> SearchExprNode:
        node = self._parse_or_expr()
        while True:
            next_token = self._peek()
            if next_token is None or next_token.kind == TOKEN_RPAREN:
                break
            if next_token.kind == TOKEN_AND:
                self.index += 1
            elif next_token.kind not in _START_TOKENS:
                raise ValueError("搜索表达式存在非法连接符")
            right = self._parse_or_expr()
            node = SearchExprNode("AND", left=node, right=right)
        return node

    def _parse_or_expr(self) -> SearchExprNode:
        node = self._parse_unary_expr()
        while self._match(TOKEN_OR):
            right = self._parse_unary_expr()
            node = SearchExprNode("OR", left=node, right=right)
        return node

    def _parse_unary_expr(self) -> SearchExprNode:
        if self._match(TOKEN_NOT):
            child = self._parse_unary_expr()
            return SearchExprNode("NOT", left=child)
        return self._parse_primary()

    def _parse_primary(self) -> SearchExprNode:
        token = self._peek()
        if token is None:
            raise ValueError("搜索表达式意外结束")
        if token.kind == TOKEN_LPAREN:
            self.index += 1
            expr = self._parse_and_expr()
            if not self._match(TOKEN_RPAREN):
                raise ValueError("括号未闭合")
            return expr
        if token.kind == TOKEN_TERM:
            self.index += 1
            return SearchExprNode("TERM", value=token.value)
        if token.kind == TOKEN_PHRASE:
            self.index += 1
            return SearchExprNode("PHRASE", value=token.value)
        if token.kind == TOKEN_RPAREN:
            raise ValueError("存在多余的右括号")
        raise ValueError("搜索表达式存在非法位置的操作符")

    def _peek(self) -> Optional[ExprToken]:
        if self.index >= len(self.tokens):
            return None
        return self.tokens[self.index]

    def _match(self, kind: str) -> bool:
        token = self._peek()
        if token is None or token.kind != kind:
            return False
        self.index += 1
        return True


def compile_like_clause(
    expr: Optional[SearchExprNode], *, content_expr: str
) -> Tuple[str, List[Any]]:
    if expr is None:
        return "", []
    sql, params = _compile_like_node(expr, content_expr)
    return sql, params


def _compile_like_node(
    node: SearchExprNode, content_expr: str
) -> Tuple[str, List[Any]]:
    if node.kind in {"TERM", "PHRASE"}:
        like_val = f"%{_escape_like_value(node.value.lower())}%"
        return f"({content_expr})", [like_val]
    if node.kind == "NOT":
        if node.left is None:
            raise ValueError("NOT 操作缺少目标")
        inner_sql, inner_params = _compile_like_node(node.left, content_expr)
        return f"(NOT {inner_sql})", inner_params
    if node.kind in {"AND", "OR"}:
        if node.left is None or node.right is None:
            raise ValueError("二元操作缺少操作数")
        left_sql, left_params = _compile_like_node(node.left, content_expr)
        right_sql, right_params = _compile_like_node(node.right, content_expr)
        return (
            f"({left_sql} {node.kind} {right_sql})",
            left_params + right_params,
        )
    raise ValueError("未知搜索表达式节点")


def _escape_like_value(value: str) -> str:
    return (
        str(value or "")
        .replace("\\", "\\\\")
        .replace("%", "\\%")
        .replace("_", "\\_")
    )


def build_candidate_fts_match(expr: Optional[SearchExprNode]) -> str:
    mandatory_terms = [
        term for term in _collect_mandatory_terms(expr) if _supports_trigram_fts(term)
    ]
    if not mandatory_terms:
        return ""
    return " AND ".join(_quote_for_fts(term) for term in mandatory_terms)


def _supports_trigram_fts(term: str) -> bool:
    compact = "".join(str(term or "").split())
    return len(compact) >= 3


def _collect_mandatory_terms(expr: Optional[SearchExprNode]) -> List[str]:
    if expr is None:
        return []
    return list(_mandatory_terms(expr))


def _mandatory_terms(node: SearchExprNode) -> Tuple[str, ...]:
    if node.kind in {"TERM", "PHRASE"}:
        return (node.value,)
    if node.kind == "NOT":
        return ()
    if node.kind == "AND":
        return _merge_unique(_mandatory_terms(node.left), _mandatory_terms(node.right))
    if node.kind == "OR":
        left_terms = _mandatory_terms(node.left)
        right_terms = _mandatory_terms(node.right)
        return tuple(term for term in left_terms if term in set(right_terms))
    return ()


def _merge_unique(left: Sequence[str], right: Sequence[str]) -> Tuple[str, ...]:
    merged: List[str] = []
    seen = set()
    for term in list(left) + list(right):
        if term in seen:
            continue
        seen.add(term)
        merged.append(term)
    return tuple(merged)


def _quote_for_fts(term: str) -> str:
    return f'"{term.replace(chr(34), "")}"'


def to_fts_match(raw_query: str) -> str:
    return build_candidate_fts_match(parse_query(raw_query))


def expr_to_debug_dict(expr: Optional[SearchExprNode]) -> Optional[Dict[str, Any]]:
    if expr is None:
        return None
    return {
        "kind": expr.kind,
        "value": expr.value,
        "left": expr_to_debug_dict(expr.left),
        "right": expr_to_debug_dict(expr.right),
    }
