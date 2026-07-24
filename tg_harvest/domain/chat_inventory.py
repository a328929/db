from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tg_harvest.domain.chat_ids import stored_chat_id_from_entity_id
from tg_harvest.domain.coerce import (
    clean_username as _clean_username,
)
from tg_harvest.domain.coerce import (
    optional_int as _optional_int,
)
from tg_harvest.domain.coerce import (
    safe_int as _safe_int,
)


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
    scan_source_account: str = ""
    """Account key that observed this state (e.g., 'primary', 'secondary').

    Used to distinguish between group-level unavailability (all accounts see it)
    and account-level access failures (only specific accounts blocked).
    """


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
    membership_scope: str = "joined"
    last_message_at: str = ""
    last_message_ts: int | None = None
    scan_job_id: str = ""
    scanned_at: str = ""


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
    availability_reason: str = ""
    session_entity_date: str = ""
    session_entity_ts: int | None = None


_RESTRICTION_TOKEN_SPLIT_RE = re.compile(r"[、,，;；|/]+")
UNAVAILABLE_RESTRICTION_PLATFORM = "all"
UNAVAILABLE_RESTRICTION_REASONS = frozenset({"terms", "tos"})
ChatIdentity = tuple[str, int]


def classify_chat_access_failure_text(value: Any) -> str:
    error_text = str(value or "").strip().lower()
    if not error_text or any(
        token in error_text
        for token in (
            "floodwait",
            "flood wait",
            "频控",
            "timeout",
            "timed out",
            "network",
            "database",
            "数据库忙",
        )
    ):
        return ""
    if "userbanned" in error_text or "账号已被该群组/频道封禁" in error_text:
        return "account_banned"
    if any(
        token in error_text
        for token in (
            "channelprivate",
            "chatforbidden",
            "您已被踢出该群组",
            "群组已转为私有",
        )
    ):
        return "access_denied"
    if any(
        token in error_text
        for token in (
            "could not find the input entity",
            "not exist",
            "该群组/频道已解散或不存在",
            "本地实体缓存未命中",
        )
    ):
        return "entity_unavailable"
    return ""


def load_known_chat_ids(conn: Any) -> set[int]:
    cur = conn.cursor()
    try:
        cur.execute("SELECT chat_id FROM chats")
        return {_chat_id_identity(row["chat_id"]) for row in cur.fetchall()}
    finally:
        cur.close()


def load_known_chat_identities(conn: Any) -> set[ChatIdentity]:
    cur = conn.cursor()
    try:
        cur.execute("SELECT chat_id, chat_type FROM chats")
        keys: set[ChatIdentity] = set()
        for row in cur.fetchall():
            keys.update(chat_identity_candidates(row["chat_id"], row["chat_type"]))
        return keys
    finally:
        cur.close()


def _chat_id_identity(raw_chat_id: Any) -> int:
    original = int(raw_chat_id)
    value = abs(original)
    raw = str(value)
    if original < 0 and raw.startswith("100") and len(raw) > 3:
        return int(raw[3:])
    return value


def normalize_chat_type_category(chat_type: Any) -> str:
    normalized = str(chat_type or "").strip().lower().lstrip("_")
    if normalized.startswith("channel"):
        return "channel"
    if normalized.startswith("chat"):
        return "chat"
    return ""


def chat_identity_key(chat_id: Any, chat_type: Any) -> ChatIdentity:
    return (
        normalize_chat_type_category(chat_type),
        _chat_id_identity(chat_id),
    )


def chat_identity_candidates(chat_id: Any, chat_type: Any) -> set[ChatIdentity]:
    identity = _chat_id_identity(chat_id)
    kind = normalize_chat_type_category(chat_type)
    if kind:
        return {(kind, identity)}
    return {("chat", identity), ("channel", identity)}


def _identity_candidates_from_item(item: Any) -> set[ChatIdentity]:
    if isinstance(item, ChatInventoryRow):
        return chat_identity_candidates(item.chat_id, item.chat_type)
    if isinstance(item, Mapping):
        if "chat_id" not in item:
            return set()
        return chat_identity_candidates(
            item.get("chat_id", 0),
            item.get("chat_type", ""),
        )
    if isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], str):
        return chat_identity_candidates(item[1], item[0])
    try:
        return chat_identity_candidates(int(item), "")
    except (TypeError, ValueError):
        return set()


def _normalize_identity_keys(items: Iterable[Any]) -> set[ChatIdentity]:
    keys: set[ChatIdentity] = set()
    for item in items:
        keys.update(_identity_candidates_from_item(item))
    return keys


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


def _row_from_dialog(dialog: Any, source_account: str = "") -> ChatInventoryRow | None:
    if not _is_joined_group_or_channel(dialog):
        return None

    entity = getattr(dialog, "entity", None)
    chat_id = _safe_int(getattr(entity, "id", None))
    if chat_id <= 0:
        return None

    title = (getattr(dialog, "title", None) or getattr(entity, "title", None) or "").strip()
    if not title:
        title = "未命名"

    username = _clean_username(getattr(entity, "username", None))
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
        scan_source_account=str(source_account or ""),
    )


def load_joined_chat_inventory(dialogs: Iterable[Any], source_account: str = "") -> list[ChatInventoryRow]:
    """Extract inventory of joined groups/channels from Telegram dialogs.

    Args:
        dialogs: Iterable of Telegram dialog objects from client.iter_dialogs().
        source_account: Account key that performed the scan (e.g., 'primary', 'secondary').
            Used to track which account observed unavailable states.

    Returns:
        List of ChatInventoryRow for groups/channels the account has joined.
        Excludes:
        - User/bot dialogs (not groups/channels)
        - Dialogs marked as 'left' or 'deactivated'
        - Duplicate chat IDs (keeps first occurrence)

    Notes:
        - Handles both accessible and forbidden (ChannelForbidden) chats
        - Preserves last message timestamp when available
        - Normalizes usernames to lowercase without '@'
    """
    rows: list[ChatInventoryRow] = []
    seen_chat_ids: set[ChatIdentity] = set()

    for dialog in dialogs:
        row = _row_from_dialog(dialog, source_account)
        if row is None:
            continue
        identity = chat_identity_key(row.chat_id, row.chat_type)
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


def restricted_chat_row_from_entity(
    entity: Any,
    *,
    chat_id: int | None = None,
    chat_title: str = "",
    chat_username: str = "",
    last_message_at: str = "",
    last_message_ts: int | None = None,
    membership_scope: str = "public_unjoined",
) -> RestrictedChatInventoryRow | None:
    entity_type = entity.__class__.__name__
    entity_id = _safe_int(getattr(entity, "id", None))
    stored_id = int(chat_id if chat_id is not None else entity_id)
    if entity_id <= 0 or stored_id == 0:
        return None

    platforms, reasons, reason_text = _restriction_reason_parts(entity)
    risk_flags = _entity_risk_flags(entity)
    if not any((platforms, reasons, reason_text, risk_flags)):
        return None

    username = _clean_username(getattr(entity, "username", None)) or _clean_username(
        chat_username
    )
    title = str(getattr(entity, "title", None) or chat_title or "").strip()
    return RestrictedChatInventoryRow(
        chat_id=stored_id,
        chat_title=title or f"Chat {stored_id}",
        chat_username=username,
        chat_type=entity_type,
        is_public=1 if username else 0,
        restriction_platforms=platforms,
        restriction_reasons=reasons,
        restriction_text=reason_text
        or (
            "Telegram 标记为内容受限"
            if "restricted" in risk_flags.split("、")
            else ""
        ),
        risk_flags=risk_flags,
        membership_scope=str(membership_scope or "public_unjoined"),
        last_message_at=str(last_message_at or ""),
        last_message_ts=last_message_ts,
    )


def unavailable_chat_risk_row(
    *,
    chat_id: int,
    chat_title: str,
    chat_username: str = "",
    chat_type: str = "",
    risk_type: str = "access_unavailable",
    risk_message: str = "群组当前不可访问",
    membership_scope: str = "joined",
    last_message_at: str = "",
    last_message_ts: int | None = None,
) -> RestrictedChatInventoryRow:
    username = _clean_username(chat_username)
    return RestrictedChatInventoryRow(
        chat_id=int(chat_id),
        chat_title=str(chat_title or "").strip() or f"Chat {int(chat_id)}",
        chat_username=username,
        chat_type=str(chat_type or ""),
        is_public=1 if username else 0,
        restriction_text=str(risk_message or "群组当前不可访问"),
        risk_flags=str(risk_type or "access_unavailable"),
        membership_scope=str(membership_scope or "joined"),
        last_message_at=str(last_message_at or ""),
        last_message_ts=last_message_ts,
    )


def find_restricted_joined_chats(dialogs: Iterable[Any], source_account: str = "") -> list[RestrictedChatInventoryRow]:
    """Find joined groups/channels with Telegram risk flags or access restrictions.

    Args:
        dialogs: Iterable of Telegram dialog objects.
        source_account: Account key performing the scan.

    Returns:
        List of RestrictedChatInventoryRow for chats with any of:
        - Telegram restriction reasons (porn, copyright, spam, etc.)
        - Risk flags (restricted, scam, fake)
        - Unavailable status (ChannelForbidden, ChatForbidden)

        Sorted by: title (case-insensitive), chat_id.

    Notes:
        - Extracts restriction_reason array from entities (platform, reason, text)
        - Merges multiple restrictions with '、' separator
        - Sets membership_scope='joined' for all results
        - Chats without any restrictions/risks are excluded
    """
    rows: list[RestrictedChatInventoryRow] = []
    seen_chat_ids: set[ChatIdentity] = set()

    for dialog in dialogs:
        base_row = _row_from_dialog(dialog, source_account)
        if base_row is None:
            continue

        if base_row.unavailable_reason:
            restricted_row = unavailable_chat_risk_row(
                chat_id=base_row.chat_id,
                chat_title=base_row.chat_title,
                chat_username=base_row.chat_username,
                chat_type=base_row.chat_type,
                risk_type="access_unavailable",
                risk_message=base_row.unavailable_reason,
                membership_scope="joined",
                last_message_at=base_row.last_message_at,
                last_message_ts=base_row.last_message_ts,
            )
        else:
            entity = getattr(dialog, "entity", None)
            restricted_row = restricted_chat_row_from_entity(
                entity,
                chat_id=base_row.chat_id,
                chat_title=base_row.chat_title,
                chat_username=base_row.chat_username,
                last_message_at=base_row.last_message_at,
                last_message_ts=base_row.last_message_ts,
                membership_scope="joined",
            )
        if restricted_row is None:
            continue

        identity = chat_identity_key(base_row.chat_id, base_row.chat_type)
        if identity in seen_chat_ids:
            continue
        seen_chat_ids.add(identity)
        rows.append(restricted_row)

    rows.sort(key=lambda item: (item.chat_title.casefold(), item.chat_id))
    return rows


def filter_missing_joined_rows(
    joined_rows: Iterable[ChatInventoryRow],
    known_chat_ids: Iterable[Any],
    *,
    include_unavailable: bool = False,
) -> list[ChatInventoryRow]:
    rows: list[ChatInventoryRow] = []
    known_identities = _normalize_identity_keys(known_chat_ids)

    for row in joined_rows:
        if row.unavailable_reason and not include_unavailable:
            continue
        if not known_identities.isdisjoint(
            chat_identity_candidates(row.chat_id, row.chat_type)
        ):
            continue
        rows.append(row)

    rows.sort(
        key=lambda item: (
            1 if str(item.unavailable_reason or "").strip() else 0,
            item.chat_title.casefold(),
            item.chat_id,
        )
    )
    return rows


def find_missing_joined_chats(
    dialogs: Iterable[Any],
    known_chat_ids: Iterable[Any],
    *,
    include_unavailable: bool = False,
    source_account: str = "",
) -> list[ChatInventoryRow]:
    """Find groups/channels the account has joined but are not in the database.

    Args:
        dialogs: Iterable of Telegram dialog objects.
        known_chat_ids: Collection of chat IDs or (chat_type, chat_id) tuples
            already present in the database.
        include_unavailable: If True, include chats marked as unavailable/forbidden.
            If False (default), only return accessible chats.
        source_account: Account key performing the scan.

    Returns:
        Sorted list of ChatInventoryRow for joined chats missing from the database.
        Sorted by: unavailable status (accessible first), title (case-insensitive), chat_id.

    Notes:
        - Uses identity normalization to handle Telegram's multiple ID formats
          (positive, negative, -100 prefix)
        - A chat is considered "known" if any of its identity candidates match
    """
    return filter_missing_joined_rows(
        load_joined_chat_inventory(dialogs, source_account),
        known_chat_ids,
        include_unavailable=include_unavailable,
    )


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
    joined_identities: set[ChatIdentity] = set()
    for row in joined_rows:
        if str(row.unavailable_reason or "").strip():
            continue
        joined_identities.update(chat_identity_candidates(row.chat_id, row.chat_type))
    rows: list[Any] = []
    seen_chat_identities: set[tuple[int, str]] = set()

    for row in database_rows:
        try:
            chat_id = int(_generic_row_value(row, "chat_id", 0) or 0)
        except (TypeError, ValueError):
            continue
        if chat_id == 0:
            continue
        dedupe_key = (
            _chat_id_identity(chat_id),
            normalize_chat_type_category(_generic_row_value(row, "chat_type", "")),
        )
        identity_candidates = chat_identity_candidates(
            chat_id,
            _generic_row_value(row, "chat_type", ""),
        )
        if dedupe_key in seen_chat_identities:
            continue
        seen_chat_identities.add(dedupe_key)
        if joined_identities.isdisjoint(identity_candidates):
            continue
        rows.append(row)

    return rows


def _mapping_value(row: Mapping[str, Any], key: str, default: Any = "") -> Any:
    value = row.get(key, default)
    return default if value is None else value


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
                username = _clean_username(row["username"])
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
