# -*- coding: utf-8 -*-
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlencode, quote


@dataclass(frozen=True)
class TelegramLinkBundle:
    app_link: str
    web_link: str
    open_link: str


def normalize_chat_username(chat_username: Optional[str]) -> str:
    return str(chat_username or "").strip().lstrip("@")


def build_telegram_web_link(
    *, chat_id: int, message_id: int, chat_username: Optional[str] = None
) -> str:
    username = normalize_chat_username(chat_username)
    if username:
        return f"https://t.me/{quote(username, safe='')}/{int(message_id)}"
    return f"https://t.me/c/{int(chat_id)}/{int(message_id)}"


def build_telegram_app_link(
    *, chat_id: int, message_id: int, chat_username: Optional[str] = None
) -> str:
    username = normalize_chat_username(chat_username)
    if username:
        return "tg://resolve?" + urlencode(
            {"domain": username, "post": int(message_id)}
        )
    return "tg://privatepost?" + urlencode(
        {"channel": int(chat_id), "post": int(message_id)}
    )


def build_telegram_open_link(*, chat_id: int, message_id: int) -> str:
    return "/open/telegram?" + urlencode(
        {"chat_id": int(chat_id), "message_id": int(message_id)}
    )


def build_telegram_link_bundle(
    *, chat_id: int, message_id: int, chat_username: Optional[str] = None
) -> TelegramLinkBundle:
    return TelegramLinkBundle(
        app_link=build_telegram_app_link(
            chat_id=chat_id, message_id=message_id, chat_username=chat_username
        ),
        web_link=build_telegram_web_link(
            chat_id=chat_id, message_id=message_id, chat_username=chat_username
        ),
        open_link=build_telegram_open_link(chat_id=chat_id, message_id=message_id),
    )
