from typing import Any

from tg_harvest.admin_jobs.sessions import bind_client_event_loop

CLONE_MEDIA_GROUP_API_SCAN_RADIUS = 25
CLONE_MEDIA_GROUP_API_SCAN_LIMIT = 100
CLONE_MEDIA_GROUP_API_EXPAND_BATCH_SIZE = 25
CLONE_MEDIA_GROUP_API_EXPAND_MAX_ITEMS = 300
CLONE_MEDIA_GROUP_ALBUM_MAX_ITEMS = 10
CLONE_ALBUM_PHOTO_VIDEO_KINDS = frozenset({"photo", "video"})
CLONE_ALBUM_SAME_TYPE_KINDS = frozenset({"audio", "document"})


def _class_name(value: Any) -> str:
    return value.__class__.__name__.lower() if value is not None else ""


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    try:
        return list(value)
    except TypeError:
        return [value]


def _message_id(message: Any) -> int | None:
    raw_id = getattr(message, "id", None)
    if raw_id in (None, ""):
        return None
    try:
        return int(raw_id)
    except (TypeError, ValueError):
        return None


def _message_grouped_id(message: Any) -> int | None:
    raw_grouped_id = getattr(message, "grouped_id", None)
    if raw_grouped_id in (None, ""):
        return None
    try:
        return int(raw_grouped_id)
    except (TypeError, ValueError):
        return None


def _message_has_media(message: Any) -> bool:
    if message is None:
        return False
    return bool(
        getattr(message, "media", None)
        or getattr(message, "photo", None)
        or getattr(message, "video", None)
        or getattr(message, "audio", None)
        or getattr(message, "voice", None)
        or getattr(message, "document", None)
    )


def _document_media_kind(document: Any) -> str:
    attributes = getattr(document, "attributes", None) or []
    for attribute in attributes:
        attribute_name = _class_name(attribute)
        if "documentattributeaudio" in attribute_name:
            return "voice" if bool(getattr(attribute, "voice", False)) else "audio"
        if "documentattributevideo" in attribute_name:
            return (
                "video_note"
                if bool(getattr(attribute, "round_message", False))
                else "video"
            )
        if "documentattributeanimated" in attribute_name:
            return "animation"

    mime_type = str(getattr(document, "mime_type", "") or "").lower()
    if mime_type.startswith("video/"):
        return "video"
    if mime_type.startswith("audio/"):
        return "audio"
    if mime_type.startswith("image/"):
        return "photo"
    return "document"


def _message_media_kind(message: Any) -> str:
    explicit_kind = str(
        getattr(message, "media_kind", None)
        or getattr(message, "kind", None)
        or ""
    ).strip().lower()
    if explicit_kind:
        return explicit_kind

    if getattr(message, "photo", None) is not None:
        return "photo"
    if getattr(message, "video", None) is not None:
        return "video"
    if getattr(message, "audio", None) is not None:
        return "audio"
    if getattr(message, "voice", None) is not None:
        return "voice"
    if getattr(message, "document", None) is not None:
        return _document_media_kind(message.document)

    media = getattr(message, "media", None)
    media_name = _class_name(media)
    if "messagemediaphoto" in media_name:
        return "photo"
    if "messagemediadocument" in media_name:
        return _document_media_kind(getattr(media, "document", None))
    return "unknown" if media is not None else "none"


def _media_group_copy_strategy(items: list[dict[str, Any]]) -> dict[str, Any]:
    kinds = [str(item.get("media_kind") or "unknown").lower() for item in items]
    if len(kinds) <= 1:
        return {
            "copy_strategy": "single",
            "album_compatible": False,
            "album_reason": "single_item",
        }

    kind_set = set(kinds)
    if kind_set <= CLONE_ALBUM_PHOTO_VIDEO_KINDS:
        return {
            "copy_strategy": "album",
            "album_compatible": True,
            "album_reason": "photo_video_group",
        }

    if len(kind_set) == 1 and next(iter(kind_set)) in CLONE_ALBUM_SAME_TYPE_KINDS:
        only_kind = next(iter(kind_set))
        return {
            "copy_strategy": "album",
            "album_compatible": True,
            "album_reason": f"same_{only_kind}_group",
        }

    return {
        "copy_strategy": "sequential",
        "album_compatible": False,
        "album_reason": "mixed_non_album_media:" + ",".join(kinds),
    }


def clone_album_message_id_chunks(
    message_ids: list[int],
    *,
    max_items: int = CLONE_MEDIA_GROUP_ALBUM_MAX_ITEMS,
) -> list[list[int]]:
    """Split album-compatible copy ids into Telegram-sized stable chunks."""
    try:
        normalized_max_items = int(max_items)
    except (TypeError, ValueError):
        normalized_max_items = CLONE_MEDIA_GROUP_ALBUM_MAX_ITEMS
    normalized_max_items = max(1, normalized_max_items)
    normalized_ids = [int(message_id) for message_id in message_ids if message_id]
    return [
        normalized_ids[index : index + normalized_max_items]
        for index in range(0, len(normalized_ids), normalized_max_items)
    ]


def _dedupe_sorted_messages(messages: list[Any]) -> list[Any]:
    by_id: dict[int, Any] = {}
    for message in messages:
        message_id = _message_id(message)
        if message_id is None or message_id in by_id:
            continue
        by_id[message_id] = message
    return [by_id[message_id] for message_id in sorted(by_id)]


def _get_messages_by_ids(client: Any, entity: Any, message_ids: list[int]) -> list[Any]:
    normalized_ids = [int(message_id) for message_id in message_ids if message_id]
    if not normalized_ids:
        return []
    with bind_client_event_loop(client):
        result = client.get_messages(entity, ids=normalized_ids)
    return _as_list(result)


def _get_messages_window(
    client: Any,
    entity: Any,
    *,
    min_message_id: int,
    max_message_id: int,
    scan_radius: int,
) -> list[Any]:
    min_id = max(0, int(min_message_id) - int(scan_radius) - 1)
    max_id = int(max_message_id) + int(scan_radius) + 1
    limit = min(
        CLONE_MEDIA_GROUP_API_SCAN_LIMIT,
        max(1, max_id - min_id + 1),
    )
    with bind_client_event_loop(client):
        result = client.get_messages(entity, min_id=min_id, max_id=max_id, limit=limit)
    return _as_list(result)


def _get_messages_between_ids(
    client: Any,
    entity: Any,
    *,
    min_id: int,
    max_id: int,
    limit: int,
) -> list[Any]:
    with bind_client_event_loop(client):
        result = client.get_messages(
            entity,
            min_id=max(0, int(min_id)),
            max_id=max(0, int(max_id)),
            limit=max(1, int(limit)),
        )
    return _as_list(result)


def _media_group_messages(
    messages: list[Any],
    grouped_id: int,
) -> list[Any]:
    return [
        message
        for message in messages
        if _message_has_media(message)
        and _message_grouped_id(message) == int(grouped_id)
        and _message_id(message) is not None
    ]


def _message_id_set(messages: list[Any]) -> set[int]:
    return {
        int(message_id)
        for message_id in (_message_id(message) for message in messages)
        if message_id is not None
    }


def _expand_media_group_direction(
    client: Any,
    entity: Any,
    *,
    grouped_id: int,
    known_messages: dict[int, Any],
    direction: int,
    batch_size: int,
    max_items: int,
) -> None:
    while known_messages and len(known_messages) < max_items:
        known_ids = sorted(known_messages)
        if direction < 0:
            cursor = known_ids[0]
            if cursor <= 1:
                return
            min_id = max(0, cursor - batch_size - 1)
            max_id = cursor
        else:
            cursor = known_ids[-1]
            min_id = cursor
            max_id = cursor + batch_size + 1

        batch_messages = _get_messages_between_ids(
            client,
            entity,
            min_id=min_id,
            max_id=max_id,
            limit=batch_size,
        )
        group_batch = _media_group_messages(batch_messages, grouped_id)
        new_messages = [
            message
            for message in group_batch
            for message_id in [_message_id(message)]
            if message_id is not None and int(message_id) not in known_messages
        ]
        if not new_messages:
            return

        remaining_capacity = max(0, max_items - len(known_messages))
        if remaining_capacity <= 0:
            return
        for message in new_messages[:remaining_capacity]:
            message_id = _message_id(message)
            if message_id is None:
                continue
            known_messages[int(message_id)] = message


def _expand_media_group_messages(
    client: Any,
    entity: Any,
    *,
    grouped_id: int,
    initial_messages: list[Any],
    batch_size: int = CLONE_MEDIA_GROUP_API_EXPAND_BATCH_SIZE,
    max_items: int = CLONE_MEDIA_GROUP_API_EXPAND_MAX_ITEMS,
) -> list[Any]:
    try:
        normalized_batch_size = int(batch_size)
    except (TypeError, ValueError):
        normalized_batch_size = CLONE_MEDIA_GROUP_API_EXPAND_BATCH_SIZE
    normalized_batch_size = max(1, min(100, normalized_batch_size))

    try:
        normalized_max_items = int(max_items)
    except (TypeError, ValueError):
        normalized_max_items = CLONE_MEDIA_GROUP_API_EXPAND_MAX_ITEMS
    normalized_max_items = max(1, normalized_max_items)

    known_messages = {
        int(message_id): message
        for message in _media_group_messages(initial_messages, grouped_id)
        for message_id in [_message_id(message)]
        if message_id is not None
    }
    if not known_messages:
        return []

    _expand_media_group_direction(
        client,
        entity,
        grouped_id=grouped_id,
        known_messages=known_messages,
        direction=-1,
        batch_size=normalized_batch_size,
        max_items=normalized_max_items,
    )
    _expand_media_group_direction(
        client,
        entity,
        grouped_id=grouped_id,
        known_messages=known_messages,
        direction=1,
        batch_size=normalized_batch_size,
        max_items=normalized_max_items,
    )
    return [known_messages[message_id] for message_id in sorted(known_messages)]


def clone_api_resolve_media_message(
    client: Any,
    source_entity: Any,
    source_message_id: int,
) -> dict[str, Any]:
    """Resolve a single source media message through Telegram before copying."""
    messages = _get_messages_by_ids(client, source_entity, [int(source_message_id)])
    for message in messages:
        message_id = _message_id(message)
        if message_id == int(source_message_id) and _message_has_media(message):
            return {
                "ok": True,
                "message_id": message_id,
                "grouped_id": _message_grouped_id(message),
                "media_kind": _message_media_kind(message),
                "error": "",
            }
    return {
        "ok": False,
        "message_id": int(source_message_id),
        "grouped_id": None,
        "error": "API 未能读取到这条源媒体消息",
    }


def clone_api_resolve_media_group(
    client: Any,
    source_entity: Any,
    anchor_message_ids: list[int],
    *,
    scan_radius: int = CLONE_MEDIA_GROUP_API_SCAN_RADIUS,
) -> dict[str, Any]:
    """Resolve a media group from source API using DB message ids only as anchors."""
    anchors = sorted({int(message_id) for message_id in anchor_message_ids if message_id})
    if not anchors:
        return {
            "ok": False,
            "message_ids": [],
            "grouped_id": None,
            "error": "数据库缺少可用于 API 解析的媒体组锚点消息",
            "resolution": "missing_anchors",
        }

    anchor_messages = [
        message
        for message in _get_messages_by_ids(client, source_entity, anchors)
        if _message_has_media(message)
    ]
    if not anchor_messages:
        return {
            "ok": False,
            "message_ids": [],
            "grouped_id": None,
            "error": "API 未能读取到媒体组锚点消息",
            "resolution": "missing_anchor_messages",
        }

    api_grouped_id = None
    for message in anchor_messages:
        api_grouped_id = _message_grouped_id(message)
        if api_grouped_id is not None:
            break

    if api_grouped_id is None:
        anchor_ids = [
            message_id
            for message_id in (_message_id(message) for message in anchor_messages)
            if message_id is not None
        ]
        items = [
            {
                "message_id": int(message_id),
                "grouped_id": None,
                "media_kind": _message_media_kind(message),
            }
            for message in anchor_messages
            for message_id in [_message_id(message)]
            if message_id is not None
        ]
        copy_plan = _media_group_copy_strategy(items)
        return {
            "ok": bool(anchor_ids),
            "message_ids": sorted(anchor_ids),
            "items": sorted(items, key=lambda item: int(item["message_id"])),
            "grouped_id": None,
            "error": "" if anchor_ids else "API 锚点消息没有媒体组信息",
            "resolution": "single_media_anchors",
            **copy_plan,
        }

    window_messages = _get_messages_window(
        client,
        source_entity,
        min_message_id=min(anchors),
        max_message_id=max(anchors),
        scan_radius=scan_radius,
    )
    candidates = _dedupe_sorted_messages([*anchor_messages, *window_messages])
    initial_group_messages = _media_group_messages(candidates, api_grouped_id)
    group_messages = _expand_media_group_messages(
        client,
        source_entity,
        grouped_id=api_grouped_id,
        initial_messages=initial_group_messages,
    )
    message_ids = [
        int(message_id)
        for message_id in (_message_id(message) for message in group_messages)
        if message_id is not None
    ]
    if not message_ids:
        return {
            "ok": False,
            "message_ids": [],
            "grouped_id": api_grouped_id,
            "error": "API 未能解析出媒体组成员",
            "resolution": "empty_group",
        }
    items = [
        {
            "message_id": int(message_id),
            "grouped_id": api_grouped_id,
            "media_kind": _message_media_kind(message),
        }
        for message in group_messages
        for message_id in [_message_id(message)]
        if message_id is not None
    ]
    items.sort(key=lambda item: int(item["message_id"]))
    copy_plan = _media_group_copy_strategy(items)
    initial_message_ids = _message_id_set(initial_group_messages)
    expanded_message_ids = set(message_ids)
    return {
        "ok": True,
        "message_ids": sorted(set(message_ids)),
        "items": items,
        "grouped_id": api_grouped_id,
        "error": "",
        "resolution": (
            "api_group_expanded"
            if expanded_message_ids != initial_message_ids
            else "api_group_window"
        ),
        "api_window_item_count": len(initial_message_ids),
        "api_expanded_item_count": len(expanded_message_ids),
        **copy_plan,
    }
