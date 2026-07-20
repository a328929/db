from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

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
    left: SearchExprNode | None = None
    right: SearchExprNode | None = None


def norm_for_search(term: str) -> str:
    return normalize_search_term(term)


def lex_query(query: str) -> list[ExprToken]:
    q = (query or "").translate(QUERY_SYMBOLS_MAP)
    tokens: list[ExprToken] = []
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
            buf: list[str] = []
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

        buf: list[str] = []
        while i < n and (not q[i].isspace()) and q[i] not in '+-/()"':
            buf.append(q[i])
            i += 1
        term = norm_for_search("".join(buf))
        if term:
            tokens.append(ExprToken(TOKEN_TERM, term))
    return tokens


def parse_query(raw_query: str) -> SearchExprNode | None:
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

    def _peek(self) -> ExprToken | None:
        if self.index >= len(self.tokens):
            return None
        return self.tokens[self.index]

    def _match(self, kind: str) -> bool:
        token = self._peek()
        if token is None or token.kind != kind:
            return False
        self.index += 1
        return True


def expr_to_debug_dict(expr: SearchExprNode | None) -> dict[str, Any] | None:
    if expr is None:
        return None
    return {
        "kind": expr.kind,
        "value": expr.value,
        "left": expr_to_debug_dict(expr.left),
        "right": expr_to_debug_dict(expr.right),
    }
