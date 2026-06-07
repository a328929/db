# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlencode, quote


@dataclass(frozen=True)
class TelegramLinkBundle:
    app_link: str
    fallback_app_link: str
    web_link: str
    open_link: str


@dataclass(frozen=True)
class TelegramChatLinkBundle:
    app_link: str
    web_link: str


def normalize_chat_username(chat_username: Optional[str]) -> str:
    return str(chat_username or "").strip().lstrip("@")


def normalize_chat_type(chat_type: Optional[str]) -> str:
    return str(chat_type or "").strip().lower().lstrip("_")


def is_direct_openmessage_chat_type(chat_type: Optional[str]) -> bool:
    return normalize_chat_type(chat_type) in {"chat", "user"}


def _query_string(params: dict, *, single_message: bool = False) -> str:
    query = urlencode(params)
    if single_message:
        query = f"{query}&single" if query else "single"
    return query


def _append_web_single_flag(url: str, *, single_message: bool = False) -> str:
    if not single_message:
        return url
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}single"


def normalize_private_chat_id(chat_id: int) -> int:
    original = int(chat_id)
    value = abs(original)
    raw = str(value)
    if original < 0 and raw.startswith("100") and len(raw) > 3:
        return int(raw[3:])
    return value


def build_telegram_web_link(
    *,
    chat_id: int,
    message_id: int,
    chat_username: Optional[str] = None,
    chat_type: Optional[str] = None,
    single_message: bool = False,
) -> str:
    username = normalize_chat_username(chat_username)
    if username and not is_direct_openmessage_chat_type(chat_type):
        return _append_web_single_flag(
            f"https://t.me/{quote(username, safe='')}/{int(message_id)}",
            single_message=single_message,
        )
    if is_direct_openmessage_chat_type(chat_type):
        return ""
    return _append_web_single_flag(
        f"https://t.me/c/{normalize_private_chat_id(chat_id)}/{int(message_id)}",
        single_message=single_message,
    )


def build_telegram_app_link(
    *,
    chat_id: int,
    message_id: int,
    chat_username: Optional[str] = None,
    chat_type: Optional[str] = None,
    single_message: bool = False,
) -> str:
    username = normalize_chat_username(chat_username)
    if is_direct_openmessage_chat_type(chat_type):
        return "tg://openmessage?" + urlencode(
            {"chat_id": int(chat_id), "message_id": int(message_id)}
        )
    if username:
        return "tg://resolve?" + _query_string(
            {"domain": username, "post": int(message_id)},
            single_message=single_message,
        )
    return "tg://privatepost?" + _query_string(
        {"channel": normalize_private_chat_id(chat_id), "post": int(message_id)},
        single_message=single_message,
    )


def build_telegram_fallback_app_link(
    *,
    chat_id: int,
    message_id: int,
    chat_username: Optional[str] = None,
    chat_type: Optional[str] = None,
    single_message: bool = False,
) -> str:
    username = normalize_chat_username(chat_username)
    if is_direct_openmessage_chat_type(chat_type) or not username:
        return ""

    # Public channels and public megagroups resolve most accurately through
    # their public post URL. Keep the numeric privatepost URI as a manual
    # fallback in case the locally stored username has become stale.
    return "tg://privatepost?" + _query_string(
        {"channel": normalize_private_chat_id(chat_id), "post": int(message_id)},
        single_message=single_message,
    )


def build_telegram_open_link(*, chat_id: int, message_id: int) -> str:
    return "/open/telegram?" + urlencode(
        {"chat_id": int(chat_id), "message_id": int(message_id)}
    )


def build_telegram_chat_web_link(*, chat_username: Optional[str] = None) -> str:
    username = normalize_chat_username(chat_username)
    if not username:
        return ""
    return f"https://t.me/{quote(username, safe='')}"


def build_telegram_chat_app_link(
    *, chat_id: int, chat_username: Optional[str] = None
) -> str:
    username = normalize_chat_username(chat_username)
    if username:
        return "tg://resolve?" + urlencode({"domain": username})
    return "tg://openmessage?" + urlencode({"chat_id": int(chat_id)})


def build_telegram_chat_link_bundle(
    *, chat_id: int, chat_username: Optional[str] = None
) -> TelegramChatLinkBundle:
    return TelegramChatLinkBundle(
        app_link=build_telegram_chat_app_link(
            chat_id=chat_id,
            chat_username=chat_username,
        ),
        web_link=build_telegram_chat_web_link(chat_username=chat_username),
    )


def build_telegram_link_bundle(
    *,
    chat_id: int,
    message_id: int,
    chat_username: Optional[str] = None,
    chat_type: Optional[str] = None,
    single_message: bool = False,
) -> TelegramLinkBundle:
    return TelegramLinkBundle(
        app_link=build_telegram_app_link(
            chat_id=chat_id,
            message_id=message_id,
            chat_username=chat_username,
            chat_type=chat_type,
            single_message=single_message,
        ),
        fallback_app_link=build_telegram_fallback_app_link(
            chat_id=chat_id,
            message_id=message_id,
            chat_username=chat_username,
            chat_type=chat_type,
            single_message=single_message,
        ),
        web_link=build_telegram_web_link(
            chat_id=chat_id,
            message_id=message_id,
            chat_username=chat_username,
            chat_type=chat_type,
            single_message=single_message,
        ),
        open_link=build_telegram_open_link(chat_id=chat_id, message_id=message_id),
    )
