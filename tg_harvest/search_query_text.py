from typing import List, Tuple

from tg_harvest.normalize import normalize_search_term


CURLY_QUOTES_MAP = str.maketrans({"“": '"', "”": '"', "‘": "'", "’": "'"})


def norm_for_search(term: str) -> str:
    return normalize_search_term(term)


def tokenize_query(query: str) -> List[Tuple[str, str]]:
    q = (query or "").translate(CURLY_QUOTES_MAP)
    tokens: List[Tuple[str, str]] = []
    i, n = 0, len(q)
    while i < n:
        ch = q[i]
        if ch.isspace():
            i += 1
            continue
        if ch in "+-/":
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
        while i < n and (not q[i].isspace()) and q[i] not in '+-/"':
            buf.append(q[i])
            i += 1
        term = norm_for_search("".join(buf))
        if term:
            tokens.append(("TERM", term))
    return tokens


def _handle_fts_term_or_phrase(
    term_value: str,
    parts: List[str],
    deferred_not_terms: List[str],
    prev_was_term: bool,
    pending_not: bool,
    positive_terms: int,
) -> Tuple[bool, bool, int]:
    quoted = f'"{term_value.replace(chr(34), "")}"'
    if pending_not:
        if prev_was_term:
            parts.append("NOT")
            parts.append(quoted)
            return True, False, positive_terms
        # 前置负词（如 -bar foo）先挂起，后续有正向词时再拼接 NOT。
        deferred_not_terms.append(quoted)
        return False, False, positive_terms

    if prev_was_term:
        parts.append("AND")
    parts.append(quoted)
    return True, False, positive_terms + 1


def _handle_fts_op_token(op_value: str, parts: List[str], prev_was_term: bool, pending_not: bool) -> Tuple[bool, bool]:
    if op_value == "+" and parts and parts[-1] not in {"AND", "OR", "NOT"}:
        parts.append("AND")
        return False, pending_not
    if op_value == "/" and parts and parts[-1] not in {"AND", "OR", "NOT"}:
        parts.append("OR")
        return False, pending_not
    if op_value == "-":
        return prev_was_term, True
    return prev_was_term, pending_not


def _finalize_fts_match(parts: List[str], deferred_not_terms: List[str], positive_terms: int) -> str:
    # 纯负词查询（如 -bar）不走 FTS，交给 LIKE fallback。
    if positive_terms == 0:
        return ""

    for term in deferred_not_terms:
        parts.append("NOT")
        parts.append(term)

    while parts and parts[-1] in {"AND", "OR", "NOT"}:
        parts.pop()
    return " ".join(parts)


def to_fts_match(raw_query: str) -> str:
    tokens = tokenize_query(raw_query)
    if not tokens:
        return ""

    parts: List[str] = []
    deferred_not_terms: List[str] = []
    prev_was_term = False
    pending_not = False
    positive_terms = 0

    for kind, value in tokens:
        if kind in {"TERM", "PHRASE"}:
            prev_was_term, pending_not, positive_terms = _handle_fts_term_or_phrase(
                value,
                parts,
                deferred_not_terms,
                prev_was_term,
                pending_not,
                positive_terms,
            )
            continue

        prev_was_term, pending_not = _handle_fts_op_token(value, parts, prev_was_term, pending_not)

    return _finalize_fts_match(parts, deferred_not_terms, positive_terms)
