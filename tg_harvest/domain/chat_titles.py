from typing import Any

try:
    from pypinyin import lazy_pinyin as _lazy_pinyin
except Exception:
    _lazy_pinyin = None  # type: ignore

_LEADING_SORT_PUNCTUATION = "\"'([{<`*_~ -_/\\|"


def chat_title_or_fallback(chat_id: int, chat_title: Any) -> str:
    title = str(chat_title or "").strip()
    return title if title else f"Chat {chat_id}"


def is_cjk_char(ch: str) -> bool:
    if not ch:
        return False
    codepoint = ord(ch)
    return (
        0x4E00 <= codepoint <= 0x9FFF
        or 0x3400 <= codepoint <= 0x4DBF
        or 0x20000 <= codepoint <= 0x2A6DF
        or 0x2A700 <= codepoint <= 0x2B73F
        or 0x2B740 <= codepoint <= 0x2B81F
        or 0x2B820 <= codepoint <= 0x2CEAF
        or 0xF900 <= codepoint <= 0xFAFF
    )


def chat_sort_key(chat_title: Any, chat_id: int) -> tuple[int, str, str, int]:
    normalized_title = chat_title_or_fallback(chat_id, chat_title)
    sort_title = normalized_title.lstrip(_LEADING_SORT_PUNCTUATION) or normalized_title
    first_char = sort_title[0]
    if _lazy_pinyin is not None:
        lexical_key = "".join(_lazy_pinyin(sort_title)).casefold()
    else:
        lexical_key = sort_title.casefold()

    if first_char.isdigit():
        category = 0
    elif is_cjk_char(first_char) or (first_char.isascii() and first_char.isalpha()):
        category = 1
    else:
        category = 2

    return category, lexical_key, sort_title.casefold(), chat_id
