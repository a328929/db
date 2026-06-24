from typing import Any


def with_chat_links(rows, build_telegram_chat_link_bundle_fn):
    items = []
    for row in rows:
        item = dict(row)
        bundle = build_telegram_chat_link_bundle_fn(
            chat_id=int(item["chat_id"]),
            chat_username=item.get("chat_username"),
        )
        item["telegram_app_link"] = bundle.app_link
        item["telegram_web_link"] = bundle.web_link
        item["has_public_link"] = bool(item["telegram_web_link"])
        items.append(item)
    return items


def with_prefixed_chat_links(
    item: dict[str, Any],
    *,
    prefix: str,
    build_telegram_chat_link_bundle_fn,
    chat_id: Any,
    chat_username: Any,
) -> dict[str, Any]:
    normalized_chat_id = int(chat_id or 0)
    if normalized_chat_id:
        bundle = build_telegram_chat_link_bundle_fn(
            chat_id=normalized_chat_id,
            chat_username=chat_username,
        )
        item[f"{prefix}_telegram_app_link"] = bundle.app_link
        item[f"{prefix}_telegram_web_link"] = bundle.web_link
        item[f"{prefix}_has_public_link"] = bool(bundle.web_link)
        return item

    item[f"{prefix}_telegram_app_link"] = ""
    item[f"{prefix}_telegram_web_link"] = ""
    item[f"{prefix}_has_public_link"] = False
    return item
