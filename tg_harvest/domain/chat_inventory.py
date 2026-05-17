# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, List, Mapping, Sequence, Set


@dataclass(frozen=True)
class ChatInventoryRow:
    chat_id: int
    chat_title: str
    chat_username: str = ""
    chat_type: str = ""
    is_public: int = 0
    unavailable_reason: str = ""


def load_known_chat_ids(conn: Any) -> Set[int]:
    cur = conn.cursor()
    try:
        cur.execute("SELECT chat_id FROM chats")
        return {_chat_id_identity(row["chat_id"]) for row in cur.fetchall()}
    finally:
        cur.close()


def _chat_id_identity(raw_chat_id: Any) -> int:
    original = int(raw_chat_id)
    value = abs(original)
    raw = str(value)
    if original < 0 and raw.startswith("100") and len(raw) > 3:
        return int(raw[3:])
    return value


def _is_joined_group_or_channel(dialog: Any) -> bool:
    entity = getattr(dialog, "entity", None)
    if entity is None:
        return False

    entity_type = entity.__class__.__name__.lower().lstrip("_")
    if not (
        getattr(dialog, "is_group", False)
        or getattr(dialog, "is_channel", False)
        or entity_type in {"channelforbidden", "chatforbidden"}
    ):
        return False

    if bool(getattr(entity, "left", False)):
        return False
    if bool(getattr(entity, "deactivated", False)):
        return False
    return True


def _normalize_reason_text(value: Any) -> str:
    text = str(value or "").strip()
    return " ".join(text.split())


def _restriction_reason_text(entity: Any) -> str:
    reasons = getattr(entity, "restriction_reason", None) or []
    if not isinstance(reasons, (list, tuple)):
        reasons = [reasons]

    parts: List[str] = []
    for reason in reasons:
        text = _normalize_reason_text(getattr(reason, "text", ""))
        if text:
            parts.append(text)
            continue
        fallback = _normalize_reason_text(getattr(reason, "reason", ""))
        if fallback:
            parts.append(fallback)

    return "；".join(dict.fromkeys(parts))


def _dialog_unavailable_reason(dialog: Any) -> str:
    entity = getattr(dialog, "entity", None)
    if entity is None:
        return ""

    entity_type = entity.__class__.__name__.lower().lstrip("_")
    if entity_type in {"channelforbidden", "chatforbidden"}:
        return "Telegram 返回该会话不可访问"

    if bool(getattr(entity, "restricted", False)):
        reason = _restriction_reason_text(entity)
        if reason:
            return f"Telegram 限制显示：{reason}"
        return "Telegram 限制显示该会话"

    return ""


def _row_from_dialog(dialog: Any) -> ChatInventoryRow | None:
    if not _is_joined_group_or_channel(dialog):
        return None

    entity = getattr(dialog, "entity", None)
    chat_id = int(getattr(entity, "id", 0) or 0)
    if chat_id <= 0:
        return None

    title = (getattr(dialog, "title", None) or getattr(entity, "title", None) or "").strip()
    if not title:
        title = "未命名"

    username = str(getattr(entity, "username", None) or "").strip().lstrip("@")
    return ChatInventoryRow(
        chat_id=chat_id,
        chat_title=title,
        chat_username=username,
        chat_type=entity.__class__.__name__,
        is_public=1 if username else 0,
        unavailable_reason=_dialog_unavailable_reason(dialog),
    )


def load_joined_chat_inventory(dialogs: Iterable[Any]) -> List[ChatInventoryRow]:
    rows: List[ChatInventoryRow] = []
    seen_chat_ids: Set[int] = set()

    for dialog in dialogs:
        row = _row_from_dialog(dialog)
        if row is None:
            continue
        identity = _chat_id_identity(row.chat_id)
        if identity in seen_chat_ids:
            continue
        seen_chat_ids.add(identity)
        rows.append(row)

    return rows


def find_missing_joined_chats(
    dialogs: Iterable[Any], known_chat_ids: Set[int]
) -> List[ChatInventoryRow]:
    rows: List[ChatInventoryRow] = []
    known_identities = {_chat_id_identity(chat_id) for chat_id in known_chat_ids}

    for row in load_joined_chat_inventory(dialogs):
        if row.unavailable_reason:
            continue
        if _chat_id_identity(row.chat_id) in known_identities:
            continue
        rows.append(row)

    rows.sort(key=lambda item: (item.chat_title.casefold(), item.chat_id))
    return rows


def _mapping_value(row: Mapping[str, Any], key: str, default: Any = "") -> Any:
    value = row.get(key, default)
    return default if value is None else value


def find_database_chats_not_joined(
    database_rows: Iterable[Mapping[str, Any]],
    joined_chat_ids: Set[int],
    unavailable_chat_reasons: Mapping[int, str] | None = None,
) -> List[dict]:
    joined_identities = {_chat_id_identity(chat_id) for chat_id in joined_chat_ids}
    unavailable_reasons = {
        _chat_id_identity(chat_id): str(reason or "").strip()
        for chat_id, reason in (unavailable_chat_reasons or {}).items()
        if str(reason or "").strip()
    }
    rows: List[dict] = []
    seen_chat_ids: Set[int] = set()

    for row in database_rows:
        chat_id = int(_mapping_value(row, "chat_id", 0) or 0)
        if chat_id == 0:
            continue
        identity = _chat_id_identity(chat_id)
        if identity in seen_chat_ids:
            continue
        if identity in joined_identities and identity not in unavailable_reasons:
            continue
        seen_chat_ids.add(identity)
        scan_reason = unavailable_reasons.get(identity) or "账号未加入"
        rows.append(
            {
                "chat_id": chat_id,
                "chat_title": str(_mapping_value(row, "chat_title", "")).strip()
                or f"Chat {chat_id}",
                "chat_username": (
                    str(_mapping_value(row, "chat_username", "")).strip().lstrip("@")
                ),
                "chat_type": str(_mapping_value(row, "chat_type", "")),
                "message_count": int(_mapping_value(row, "message_count", 0) or 0),
                "last_seen_at": str(_mapping_value(row, "last_seen_at", "")),
                "scan_reason": scan_reason,
            }
        )

    return rows


def write_missing_chat_report(
    rows: Sequence[ChatInventoryRow], output_path: str | Path
) -> Path:
    path = Path(output_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(f"{row.chat_title} | ID: {row.chat_id}\n")

    return path
