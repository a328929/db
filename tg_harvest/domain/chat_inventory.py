from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tg_harvest.domain.coerce import optional_int as _optional_int
from tg_harvest.domain.chat_ids import stored_chat_id_from_entity_id


@dataclass(frozen=True)
class ChatInventoryRow:
    chat_id: int
    chat_title: str
    chat_username: str = ""
    chat_type: str = ""
    is_public: int = 0
    unavailable_reason: str = ""
    last_message_at: str = ""
    last_message_ts: int | None = None


@dataclass(frozen=True)
class RestrictedChatInventoryRow:
    chat_id: int
    chat_title: str
    chat_username: str = ""
    chat_type: str = ""
    is_public: int = 0
    restriction_platforms: str = ""
    restriction_reasons: str = ""
    restriction_text: str = ""
    risk_flags: str = ""
    last_message_at: str = ""
    last_message_ts: int | None = None


@dataclass(frozen=True)
class SessionChatRecoveryRow:
    chat_id: int
    chat_title: str
    chat_username: str = ""
    chat_type: str = "SessionEntity"
    is_public: int = 0
    source_session: str = ""
    source_entity_id: int | None = None
    source_access_hash: int | None = None
    session_entity_date: str = ""
    session_entity_ts: int | None = None


_RESTRICTION_TOKEN_SPLIT_RE = re.compile(r"[、,，;；|/]+")
UNAVAILABLE_RESTRICTION_PLATFORM = "all"
UNAVAILABLE_RESTRICTION_REASONS = frozenset({"terms", "tos"})


def load_known_chat_ids(conn: Any) -> set[int]:
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
    return not bool(getattr(entity, "deactivated", False))


def _dialog_unavailable_reason(dialog: Any) -> str:
    entity = getattr(dialog, "entity", None)
    if entity is None:
        return ""

    entity_type = entity.__class__.__name__.lower().lstrip("_")
    if entity_type in {"channelforbidden", "chatforbidden"}:
        return "Telegram 返回该会话不可访问"

    return ""


def _dialog_last_message_fields(dialog: Any) -> tuple[str, int | None]:
    message = getattr(dialog, "message", None)
    dt = getattr(message, "date", None) if message is not None else None
    if dt is None:
        return "", None
    try:
        ts = int(dt.timestamp())
        text = dt.strftime("%Y-%m-%d %H:%M:%S")
        return text, ts
    except Exception:
        return str(dt), None


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
    last_message_at, last_message_ts = _dialog_last_message_fields(dialog)
    return ChatInventoryRow(
        chat_id=chat_id,
        chat_title=title,
        chat_username=username,
        chat_type=entity.__class__.__name__,
        is_public=1 if username else 0,
        unavailable_reason=_dialog_unavailable_reason(dialog),
        last_message_at=last_message_at,
        last_message_ts=last_message_ts,
    )


def load_joined_chat_inventory(dialogs: Iterable[Any]) -> list[ChatInventoryRow]:
    rows: list[ChatInventoryRow] = []
    seen_chat_ids: set[int] = set()

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


def _dedupe_nonempty(values: Iterable[Any]) -> list[str]:
    items: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        items.append(text)
    return items


def _restriction_reason_parts(entity: Any) -> tuple[str, str, str]:
    raw_reasons = getattr(entity, "restriction_reason", None) or []
    if not isinstance(raw_reasons, (list, tuple)):
        raw_reasons = [raw_reasons]

    platforms = []
    reasons = []
    texts = []
    for raw_reason in raw_reasons:
        if raw_reason is None:
            continue
        platforms.append(getattr(raw_reason, "platform", ""))
        reasons.append(getattr(raw_reason, "reason", ""))
        texts.append(getattr(raw_reason, "text", ""))

    return (
        "、".join(_dedupe_nonempty(platforms)),
        "、".join(_dedupe_nonempty(reasons)),
        "；".join(_dedupe_nonempty(texts)),
    )


def _split_restriction_tokens(value: Any) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    return [
        part.strip().lower()
        for part in _RESTRICTION_TOKEN_SPLIT_RE.split(text)
        if part.strip()
    ]


def entity_has_all_platform_terms_restriction(entity: Any) -> bool:
    platforms, reasons, _reason_text = _restriction_reason_parts(entity)
    platform_tokens = _split_restriction_tokens(platforms)
    reason_tokens = _split_restriction_tokens(reasons)
    return (
        UNAVAILABLE_RESTRICTION_PLATFORM in platform_tokens
        and any(reason in UNAVAILABLE_RESTRICTION_REASONS for reason in reason_tokens)
    )


def _entity_risk_flags(entity: Any) -> str:
    flag_labels = [
        ("restricted", "restricted"),
        ("scam", "scam"),
        ("fake", "fake"),
    ]
    return "、".join(
        label for attr, label in flag_labels if bool(getattr(entity, attr, False))
    )


def find_restricted_joined_chats(dialogs: Iterable[Any]) -> list[RestrictedChatInventoryRow]:
    rows: list[RestrictedChatInventoryRow] = []
    seen_chat_ids: set[int] = set()

    for dialog in dialogs:
        base_row = _row_from_dialog(dialog)
        if base_row is None or base_row.unavailable_reason:
            continue

        entity = getattr(dialog, "entity", None)
        platforms, reasons, reason_text = _restriction_reason_parts(entity)
        risk_flags = _entity_risk_flags(entity)
        if not any((platforms, reasons, reason_text, risk_flags)):
            continue

        identity = _chat_id_identity(base_row.chat_id)
        if identity in seen_chat_ids:
            continue
        seen_chat_ids.add(identity)
        rows.append(
            RestrictedChatInventoryRow(
                chat_id=base_row.chat_id,
                chat_title=base_row.chat_title,
                chat_username=base_row.chat_username,
                chat_type=base_row.chat_type,
                is_public=base_row.is_public,
                restriction_platforms=platforms,
                restriction_reasons=reasons,
                restriction_text=reason_text
                or (
                    "Telegram 标记为内容受限"
                    if "restricted" in risk_flags.split("、")
                    else ""
                ),
                risk_flags=risk_flags,
                last_message_at=base_row.last_message_at,
                last_message_ts=base_row.last_message_ts,
            )
        )

    rows.sort(key=lambda item: (item.chat_title.casefold(), item.chat_id))
    return rows


def find_missing_joined_chats(
    dialogs: Iterable[Any], known_chat_ids: set[int]
) -> list[ChatInventoryRow]:
    rows: list[ChatInventoryRow] = []
    known_identities = {_chat_id_identity(chat_id) for chat_id in known_chat_ids}

    for row in load_joined_chat_inventory(dialogs):
        if row.unavailable_reason:
            continue
        if _chat_id_identity(row.chat_id) in known_identities:
            continue
        rows.append(row)

    rows.sort(key=lambda item: (item.chat_title.casefold(), item.chat_id))
    return rows


def _generic_row_value(row: Any, key: str, default: Any = "") -> Any:
    if isinstance(row, Mapping):
        return _mapping_value(row, key, default)
    try:
        value = row[key]
    except Exception:
        value = getattr(row, key, default)
    return default if value is None else value


def filter_database_chats_to_joined(
    database_rows: Iterable[Any], joined_rows: Iterable[ChatInventoryRow]
) -> list[Any]:
    joined_identities = {
        _chat_id_identity(row.chat_id)
        for row in joined_rows
        if not str(row.unavailable_reason or "").strip()
    }
    rows: list[Any] = []
    seen_chat_identities: set[int] = set()

    for row in database_rows:
        try:
            chat_id = int(_generic_row_value(row, "chat_id", 0) or 0)
        except (TypeError, ValueError):
            continue
        if chat_id == 0:
            continue
        identity = _chat_id_identity(chat_id)
        if identity in seen_chat_identities:
            continue
        seen_chat_identities.add(identity)
        if identity not in joined_identities:
            continue
        rows.append(row)

    return rows


def _mapping_value(row: Mapping[str, Any], key: str, default: Any = "") -> Any:
    value = row.get(key, default)
    return default if value is None else value


def find_database_chats_not_joined(
    database_rows: Iterable[Mapping[str, Any]],
    joined_chat_ids: set[int],
    unavailable_chat_reasons: Mapping[int, str] | None = None,
) -> list[dict]:
    joined_identities = {_chat_id_identity(chat_id) for chat_id in joined_chat_ids}
    unavailable_reasons = {
        _chat_id_identity(chat_id): str(reason or "").strip()
        for chat_id, reason in (unavailable_chat_reasons or {}).items()
        if str(reason or "").strip()
    }
    rows: list[dict] = []
    seen_chat_ids: set[int] = set()

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
                "last_message_at": str(_mapping_value(row, "last_message_at", "")),
                "last_message_ts": _mapping_value(row, "last_message_ts", None),
                "scan_reason": scan_reason,
            }
        )

    return rows


def discover_session_files(base_session_name: str | Path) -> list[Path]:
    base = Path(str(base_session_name))
    if base.suffix != ".session":
        base = Path(str(base) + ".session")

    candidates: dict[str, Path] = {}
    if base.exists() and base.is_file():
        candidates[str(base.resolve())] = base.resolve()

    parent = base.parent
    if parent.exists():
        for path in parent.glob("*.session"):
            if path.is_file():
                candidates[str(path.resolve())] = path.resolve()

    return sorted(
        candidates.values(),
        key=lambda path: (
            0 if path.name == base.name else 1,
            path.name.casefold(),
        ),
    )


def _session_entity_date_fields(raw_ts: Any) -> tuple[str, int | None]:
    try:
        ts = int(raw_ts)
    except (TypeError, ValueError):
        return "", None
    if ts <= 0:
        return "", None
    try:
        dt = datetime.fromtimestamp(ts, UTC)
        return dt.strftime("%Y-%m-%d %H:%M:%S"), ts
    except Exception:
        return "", ts


def _read_session_entity_rows(path: Path) -> list[SessionChatRecoveryRow]:
    uri = path.resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, hash, username, name, date
                FROM entities
                WHERE id < 0
                ORDER BY date DESC, name COLLATE NOCASE ASC, id ASC
                """
            )
            rows: list[SessionChatRecoveryRow] = []
            for row in cur.fetchall():
                source_entity_id = int(row["id"])
                chat_id = stored_chat_id_from_entity_id(source_entity_id)
                if chat_id <= 0:
                    continue
                username = str(row["username"] or "").strip().lstrip("@")
                title = str(row["name"] or "").strip()
                if not title:
                    title = username or f"Chat {chat_id}"
                date_text, date_ts = _session_entity_date_fields(row["date"])
                rows.append(
                    SessionChatRecoveryRow(
                        chat_id=chat_id,
                        chat_title=title,
                        chat_username=username,
                        chat_type="SessionEntity",
                        is_public=1 if username else 0,
                        source_session=path.name,
                        source_entity_id=source_entity_id,
                        source_access_hash=_optional_int(row["hash"]),
                        session_entity_date=date_text,
                        session_entity_ts=date_ts,
                    )
                )
            return rows
        finally:
            cur.close()
    finally:
        conn.close()


def _recovery_row_sort_key(row: SessionChatRecoveryRow) -> tuple[int, int, str]:
    return (
        -(row.session_entity_ts or 0),
        0 if row.chat_username else 1,
        row.chat_title.casefold(),
    )


def scan_session_chat_recovery_rows(
    session_files: Iterable[str | Path],
) -> tuple[list[SessionChatRecoveryRow], list[str]]:
    rows_by_chat_id: dict[int, SessionChatRecoveryRow] = {}
    errors: list[str] = []

    for raw_path in session_files:
        path = Path(str(raw_path))
        try:
            source_rows = _read_session_entity_rows(path)
        except Exception as exc:
            errors.append(f"{path.name}: {type(exc).__name__}: {exc}")
            continue

        for row in source_rows:
            existing = rows_by_chat_id.get(row.chat_id)
            if existing is None or _recovery_row_sort_key(row) < _recovery_row_sort_key(
                existing
            ):
                rows_by_chat_id[row.chat_id] = row

    rows = sorted(
        rows_by_chat_id.values(),
        key=lambda row: (row.chat_title.casefold(), row.chat_id),
    )
    return rows, errors
