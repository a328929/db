# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, List, Sequence, Set


@dataclass(frozen=True)
class ChatInventoryRow:
    chat_id: int
    chat_title: str


def load_known_chat_ids(conn: Any) -> Set[int]:
    cur = conn.cursor()
    try:
        cur.execute("SELECT chat_id FROM chats")
        return {int(row["chat_id"]) for row in cur.fetchall()}
    finally:
        cur.close()


def _is_joined_group_or_channel(dialog: Any) -> bool:
    if not (getattr(dialog, "is_group", False) or getattr(dialog, "is_channel", False)):
        return False

    entity = getattr(dialog, "entity", None)
    if entity is None:
        return False

    if bool(getattr(entity, "left", False)):
        return False
    if bool(getattr(entity, "deactivated", False)):
        return False
    return True


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

    return ChatInventoryRow(chat_id=chat_id, chat_title=title)


def find_missing_joined_chats(
    dialogs: Iterable[Any], known_chat_ids: Set[int]
) -> List[ChatInventoryRow]:
    rows: List[ChatInventoryRow] = []
    seen_chat_ids: Set[int] = set()

    for dialog in dialogs:
        row = _row_from_dialog(dialog)
        if row is None:
            continue
        if row.chat_id in known_chat_ids or row.chat_id in seen_chat_ids:
            continue
        seen_chat_ids.add(row.chat_id)
        rows.append(row)

    rows.sort(key=lambda item: (item.chat_title.casefold(), item.chat_id))
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
