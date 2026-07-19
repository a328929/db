import logging
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from tg_harvest.storage.connection import synchronized_write

UPSERT_CHAT_SQL = """
INSERT INTO chats(chat_id, chat_title, chat_username, is_public, chat_type, first_seen_at, last_seen_at)
VALUES (?, ?, ?, ?, ?, datetime('now'), datetime('now'))
ON CONFLICT(chat_id) DO UPDATE SET
    chat_title = excluded.chat_title,
    chat_username = excluded.chat_username,
    is_public = excluded.is_public,
    chat_type = excluded.chat_type,
    last_seen_at = datetime('now')
"""

UPSERT_MESSAGE_SQL = """
INSERT INTO messages(
    chat_id, message_id, msg_date_text, msg_date_ts, sender_id,
    content, content_norm, pure_hash, dedupe_hash,
    msg_type, grouped_id, has_media,
    is_promo, promo_score, promo_reasons, dedupe_eligible, guard_reason, text_len,
    created_at, updated_at
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, datetime('now'), datetime('now'))
ON CONFLICT(chat_id, message_id) DO UPDATE SET
    msg_date_text=excluded.msg_date_text,
    msg_date_ts=excluded.msg_date_ts,
    sender_id=excluded.sender_id,
    content=excluded.content,
    content_norm=excluded.content_norm,
    pure_hash=excluded.pure_hash,
    dedupe_hash=excluded.dedupe_hash,
    msg_type=excluded.msg_type,
    grouped_id=excluded.grouped_id,
    has_media=excluded.has_media,
    is_promo=excluded.is_promo,
    promo_score=excluded.promo_score,
    promo_reasons=excluded.promo_reasons,
    dedupe_eligible=excluded.dedupe_eligible,
    guard_reason=excluded.guard_reason,
    text_len=excluded.text_len,
    updated_at=datetime('now')
"""

UPSERT_MEDIA_SQL = """
INSERT INTO message_media(
    chat_id, message_id, media_kind, file_unique_id, file_name, file_ext, mime_type,
    file_size, width, height, duration_sec, grouped_id, media_fingerprint, meta_json, updated_at
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?, datetime('now'))
ON CONFLICT(chat_id, message_id) DO UPDATE SET
    media_kind=excluded.media_kind,
    file_unique_id=excluded.file_unique_id,
    file_name=excluded.file_name,
    file_ext=excluded.file_ext,
    mime_type=excluded.mime_type,
    file_size=excluded.file_size,
    width=excluded.width,
    height=excluded.height,
    duration_sec=excluded.duration_sec,
    grouped_id=excluded.grouped_id,
    media_fingerprint=excluded.media_fingerprint,
    meta_json=excluded.meta_json,
    updated_at=datetime('now')
WHERE message_media.media_kind IS NOT excluded.media_kind
   OR message_media.file_unique_id IS NOT excluded.file_unique_id
   OR message_media.file_name IS NOT excluded.file_name
   OR message_media.file_ext IS NOT excluded.file_ext
   OR message_media.mime_type IS NOT excluded.mime_type
   OR message_media.file_size IS NOT excluded.file_size
   OR message_media.width IS NOT excluded.width
   OR message_media.height IS NOT excluded.height
   OR message_media.duration_sec IS NOT excluded.duration_sec
   OR message_media.grouped_id IS NOT excluded.grouped_id
   OR message_media.media_fingerprint IS NOT excluded.media_fingerprint
   OR message_media.meta_json IS NOT excluded.meta_json
"""

_DELETE_MEDIA_KEY_BATCH_SIZE = 400
_MESSAGE_LOOKUP_BATCH_SIZE = 400
_MESSAGE_VALUE_COLUMNS = (
    "msg_date_text",
    "msg_date_ts",
    "sender_id",
    "content",
    "content_norm",
    "pure_hash",
    "dedupe_hash",
    "msg_type",
    "grouped_id",
    "has_media",
    "is_promo",
    "promo_score",
    "promo_reasons",
    "dedupe_eligible",
    "guard_reason",
    "text_len",
)
_GROUPED_ID_VALUE_INDEX = _MESSAGE_VALUE_COLUMNS.index("grouped_id")
_FTS_DOCSIZE_TABLE = "messages_fts_docsize"

_MEDIA_VALUE_COLUMNS = (
    "media_kind",
    "file_unique_id",
    "file_name",
    "file_ext",
    "mime_type",
    "file_size",
    "width",
    "height",
    "duration_sec",
    "grouped_id",
    "media_fingerprint",
    "meta_json",
)


def _has_media_flag(value: Any) -> bool:
    """Interpret persisted/API boolean-ish values consistently.

    Telegram parsing normally supplies an integer flag, but older callers can
    hand us SQLite-compatible text such as ``"0"``.  Python treats every
    non-empty string as true, which would leave stale media rows behind when a
    message is converted to text.
    """
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"", "0", "false", "no", "off", "none", "null"}:
            return False
        if normalized in {"1", "true", "yes", "on"}:
            return True
    return bool(value)


@dataclass(frozen=True)
class BatchUpsertResult:
    """Describe the rows changed by one atomic ingest batch.

    ``affected_grouped_ids`` contains both the previous and new group IDs for
    rows that changed.  ``persisted_change_count`` is the number of unique
    message keys whose persisted state changed (including media or index
    repairs).  Keeping this result explicit prevents callers from inferring
    changes from the scanned input, which is especially important for
    incremental tail rescans.
    """

    affected_grouped_ids: frozenset[int] = frozenset()
    persisted_change_count: int = 0

    # ``batch_upsert`` historically returned a grouped-id set. Keep the small
    # read-only set protocol so older internal integrations can migrate to the
    # richer result without changing their control flow.
    def __iter__(self):
        return iter(self.affected_grouped_ids)

    def __len__(self) -> int:
        return len(self.affected_grouped_ids)

    def __contains__(self, grouped_id: object) -> bool:
        return grouped_id in self.affected_grouped_ids

    def __eq__(self, other: object) -> bool:
        if isinstance(other, BatchUpsertResult):
            return (
                self.affected_grouped_ids == other.affected_grouped_ids
                and self.persisted_change_count == other.persisted_change_count
            )
        if isinstance(other, (set, frozenset)):
            return self.affected_grouped_ids == frozenset(other)
        return NotImplemented

    @classmethod
    def from_callback(
        cls, result: Any, *, fallback_change_count: int
    ) -> "BatchUpsertResult":
        """Normalize legacy writer callbacks that returned ``None``/a set."""
        if isinstance(result, cls):
            return result
        if result is None:
            return cls(frozenset(), max(0, int(fallback_change_count)))
        grouped_id_values: set[int] = set()
        try:
            iterator = iter(result)
        except TypeError:
            iterator = iter(())
        for grouped_id in iterator:
            if grouped_id is None:
                continue
            try:
                grouped_id_values.add(int(grouped_id))
            except (TypeError, ValueError):
                # A legacy callback may contain a malformed sentinel beside
                # valid group IDs; preserve the valid IDs rather than dropping
                # the entire result.
                continue
        return cls(frozenset(grouped_id_values), max(0, int(fallback_change_count)))


def _table_exists(cur: sqlite3.Cursor, table_name: str) -> bool:
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table_name,),
    )
    return cur.fetchone() is not None


def _fts_runtime_available(cur: sqlite3.Cursor) -> bool:
    """Check that the FTS virtual table and its live sync triggers exist."""
    cur.execute(
        """
        SELECT type, name, sql
        FROM sqlite_master
        WHERE (type = 'table' AND name IN ('messages_fts', 'messages_fts_docsize'))
           OR (type = 'trigger' AND name IN (
                  'trg_messages_fts_insert',
                  'trg_messages_fts_delete',
                  'trg_messages_fts_update'
              ))
        """
    )
    tables: set[str] = set()
    triggers: dict[str, str] = {}
    for row in cur.fetchall():
        object_type = str(row["type"] if isinstance(row, sqlite3.Row) else row[0])
        object_name = str(row["name"] if isinstance(row, sqlite3.Row) else row[1])
        if object_type == "table":
            tables.add(object_name)
        elif object_type == "trigger":
            trigger_sql = row["sql"] if isinstance(row, sqlite3.Row) else row[2]
            triggers[object_name] = str(trigger_sql or "").lower()
    required_markers = {
        "trg_messages_fts_insert": ("nullif(new.content_norm, '')", "new.content"),
        "trg_messages_fts_delete": (
            "'delete'",
            "old.pk",
            "nullif(old.content_norm, '')",
            "old.content",
        ),
        "trg_messages_fts_update": (
            "'delete'",
            "old.pk",
            "new.pk",
            "nullif(old.content_norm, '')",
            "old.content",
            "nullif(new.content_norm, '')",
            "new.content",
        ),
    }
    return {"messages_fts", _FTS_DOCSIZE_TABLE}.issubset(tables) and all(
        name in triggers
        and all(marker in triggers[name] for marker in markers)
        for name, markers in required_markers.items()
    )


def _search_indexes_need_trigger_refresh(
    cur: sqlite3.Cursor, *, fts_runtime_available: bool | None = None
) -> bool:
    """Return whether startup deliberately left a search index incomplete."""
    if not _table_exists(cur, "message_search_terms_meta"):
        return False
    cur.execute(
        """
        SELECT key, value
        FROM message_search_terms_meta
        WHERE key IN ('fts_index_status', 'cjk_terms_version', 'cjk_terms_rebuild_state')
        """
    )
    values = {
        str(row["key"]): str(row["value"] or "")
        for row in cur.fetchall()
    }
    if fts_runtime_available is None:
        fts_runtime_available = _fts_runtime_available(cur)
    fts_incomplete = (
        values.get("fts_index_status") not in (None, "", "ready")
        and fts_runtime_available
    )
    return (
        fts_incomplete
        or values.get("cjk_terms_version") not in (None, "", "3")
        or bool(values.get("cjk_terms_rebuild_state"))
    )


def _load_message_keys_missing_fts(
    cur: sqlite3.Cursor,
    keys: list[tuple[int, int]],
    *,
    fts_runtime_available: bool | None = None,
) -> set[tuple[int, int]]:
    if fts_runtime_available is None:
        fts_runtime_available = _fts_runtime_available(cur)
    if not keys or not fts_runtime_available:
        return set()
    missing: set[tuple[int, int]] = set()
    for start in range(0, len(keys), _MESSAGE_LOOKUP_BATCH_SIZE):
        part = keys[start : start + _MESSAGE_LOOKUP_BATCH_SIZE]
        placeholders = ",".join("(?, ?)" for _ in part)
        params: list[int] = []
        for chat_id, message_id in part:
            params.extend((chat_id, message_id))
        cur.execute(
            f"""
            WITH target_messages(chat_id, message_id) AS (VALUES {placeholders})
            SELECT m.chat_id, m.message_id
            FROM messages AS m
            JOIN target_messages AS t
              ON t.chat_id = m.chat_id AND t.message_id = m.message_id
            LEFT JOIN {_FTS_DOCSIZE_TABLE} AS d ON d.id = m.pk
            WHERE d.id IS NULL
            """,
            params,
        )
        missing.update((int(row["chat_id"]), int(row["message_id"])) for row in cur.fetchall())
    return missing


def _seed_missing_fts_rows(
    cur: sqlite3.Cursor,
    keys: set[tuple[int, int]],
    *,
    fts_runtime_available: bool | None = None,
) -> set[tuple[int, int]]:
    """Restore absent external-content rows before an UPDATE trigger runs."""
    if fts_runtime_available is None:
        fts_runtime_available = _fts_runtime_available(cur)
    if not keys or not fts_runtime_available:
        return set()
    normalized = sorted(keys)
    seeded_rows: list[tuple[int, str]] = []
    seeded_keys: set[tuple[int, int]] = set()
    for start in range(0, len(normalized), _MESSAGE_LOOKUP_BATCH_SIZE):
        part = normalized[start : start + _MESSAGE_LOOKUP_BATCH_SIZE]
        placeholders = ",".join("(?, ?)" for _ in part)
        params: list[int] = []
        for chat_id, message_id in part:
            params.extend((chat_id, message_id))
        cur.execute(
            f"""
            WITH target_messages(chat_id, message_id) AS (VALUES {placeholders})
            SELECT m.chat_id, m.message_id, m.pk,
                   COALESCE(NULLIF(m.content_norm, ''), m.content, '') AS search_text
            FROM messages AS m
            JOIN target_messages AS t
              ON t.chat_id = m.chat_id AND t.message_id = m.message_id
            """,
            params,
        )
        for row in cur.fetchall():
            seeded_rows.append((int(row["pk"]), str(row["search_text"] or "")))
            seeded_keys.add((int(row["chat_id"]), int(row["message_id"])))
    if seeded_rows:
        cur.executemany(
            # A partially damaged FTS5 index can retain the content row while
            # losing its shadow ``docsize`` row.  ``OR REPLACE`` makes this
            # repair idempotent for both the usual missing-row case and that
            # half-written state, while rebuilding the row's postings from the
            # canonical message content.
            "INSERT OR REPLACE INTO messages_fts(rowid, content) VALUES (?, ?)",
            seeded_rows,
        )
    if _table_exists(cur, "message_search_terms_rebuild_queue"):
        cur.executemany(
            """
            INSERT INTO message_search_terms_rebuild_queue(pk, reason, queued_at)
            VALUES (?, 'index_repair', datetime('now'))
            ON CONFLICT(pk) DO UPDATE SET
                reason = excluded.reason,
                queued_at = excluded.queued_at
            """,
            [(pk, ) for pk, _search_text in seeded_rows],
        )
    return seeded_keys


def get_last_message_id(conn: sqlite3.Connection, chat_id: int) -> int:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT COALESCE(MAX(message_id), 0) AS m FROM messages WHERE chat_id=?",
            (chat_id,),
        )
        return int(cur.fetchone()["m"])
    finally:
        cur.close()


@synchronized_write
def upsert_chat(conn: sqlite3.Connection, row: tuple):
    cur = conn.cursor()
    try:
        cur.execute(UPSERT_CHAT_SQL, row)
    finally:
        cur.close()
    conn.commit()


def _batch_upsert_messages(cur: sqlite3.Cursor, msg_rows: list[tuple]):
    if not msg_rows:
        return
    cur.executemany(UPSERT_MESSAGE_SQL, msg_rows)


def _batch_upsert_media(cur: sqlite3.Cursor, media_rows: list[tuple]):
    if not media_rows:
        return
    cur.executemany(UPSERT_MEDIA_SQL, media_rows)


def _delete_stale_media_for_non_media_messages(
    cur: sqlite3.Cursor, msg_rows: list[tuple], media_rows: list[tuple]
) -> tuple[set[tuple[int, int]], set[int]]:
    if not msg_rows:
        return set(), set()
    normalized_messages = _normalize_rows_by_key(msg_rows)
    # The final message row is authoritative when duplicate keys appear in a
    # batch.  A stale media payload from an earlier parse must not keep a row
    # alive after the message has become text.  Media-only batches still keep
    # their existing behavior because they have no message flag to consult.
    media_keys = set()
    for media_row in media_rows:
        key = (int(media_row[0]), int(media_row[1]))
        message_row = normalized_messages.get(key)
        if message_row is None or (
            len(message_row) > 11 and _has_media_flag(message_row[11])
        ):
            media_keys.add(key)
    stale_keys = sorted({
        (int(row[0]), int(row[1]))
        for row in normalized_messages.values()
        if len(row) > 11
        and not _has_media_flag(row[11])
        and (int(row[0]), int(row[1])) not in media_keys
    })
    if not stale_keys:
        return set(), set()

    stale_rows: list[tuple[int, int, int | None]] = []
    for start in range(0, len(stale_keys), _DELETE_MEDIA_KEY_BATCH_SIZE):
        batch = stale_keys[start : start + _DELETE_MEDIA_KEY_BATCH_SIZE]
        placeholders = ",".join(["(?, ?)"] * len(batch))
        params: list[int] = []
        for chat_id, message_id in batch:
            params.extend((chat_id, message_id))
        cur.execute(
            f"""
            SELECT chat_id, message_id, grouped_id
            FROM message_media
            WHERE (chat_id, message_id) IN ({placeholders})
            """,
            params,
        )
        stale_rows.extend(
            (
                int(row["chat_id"]),
                int(row["message_id"]),
                int(row["grouped_id"]) if row["grouped_id"] is not None else None,
            )
            for row in cur.fetchall()
        )

    for start in range(0, len(stale_keys), _DELETE_MEDIA_KEY_BATCH_SIZE):
        batch = stale_keys[start : start + _DELETE_MEDIA_KEY_BATCH_SIZE]
        placeholders = ",".join(["(?, ?)"] * len(batch))
        params: list[int] = []
        for chat_id, message_id in batch:
            params.extend([chat_id, message_id])
        cur.execute(
            f"""
            DELETE FROM message_media
            WHERE (chat_id, message_id) IN ({placeholders})
            """,
            params,
        )
    affected_grouped_ids = {
        int(grouped_id)
        for _chat_id, _message_id, grouped_id in stale_rows
        if grouped_id is not None
    }
    deleted_keys = {
        (chat_id, message_id) for chat_id, message_id, _grouped_id in stale_rows
    }
    for key in deleted_keys:
        message_row = normalized_messages.get(key)
        if message_row is not None and len(message_row) > 10:
            grouped_id = message_row[10]
            if grouped_id is not None:
                affected_grouped_ids.add(int(grouped_id))
    return deleted_keys, affected_grouped_ids


def _count_message_keys_by_chat(keys: list[tuple[int, int]]) -> dict[int, int]:
    counts: dict[int, int] = defaultdict(int)
    for chat_id, _message_id in keys:
        counts[int(chat_id)] += 1
    return dict(counts)


def _load_existing_message_values(
    cur: sqlite3.Cursor, keys: list[tuple[int, int]]
) -> dict[tuple[int, int], tuple]:
    existing: dict[tuple[int, int], tuple] = {}
    selected_columns = ", ".join(f"m.{column}" for column in _MESSAGE_VALUE_COLUMNS)
    for start in range(0, len(keys), _MESSAGE_LOOKUP_BATCH_SIZE):
        part = keys[start : start + _MESSAGE_LOOKUP_BATCH_SIZE]
        placeholders = ",".join(["(?, ?)"] * len(part))
        params: list[int] = []
        for chat_id, message_id in part:
            params.extend([chat_id, message_id])
        cur.execute(
            f"""
            WITH target_messages(chat_id, message_id) AS (
                VALUES {placeholders}
            )
            SELECT m.chat_id, m.message_id, {selected_columns}
            FROM messages m
            JOIN target_messages t
              ON t.chat_id = m.chat_id
             AND t.message_id = m.message_id
            """,
            params,
        )
        for row in cur.fetchall():
            key = (int(row["chat_id"]), int(row["message_id"]))
            existing[key] = tuple(row[column] for column in _MESSAGE_VALUE_COLUMNS)
    return existing


def _load_existing_media_values(
    cur: sqlite3.Cursor, keys: list[tuple[int, int]]
) -> tuple[dict[tuple[int, int], tuple], dict[tuple[int, int], int]]:
    existing: dict[tuple[int, int], tuple] = {}
    source_grouped_ids: dict[tuple[int, int], int] = {}
    if not keys:
        return existing, source_grouped_ids
    selected_columns = ", ".join(f"mm.{column}" for column in _MEDIA_VALUE_COLUMNS)
    for start in range(0, len(keys), _MESSAGE_LOOKUP_BATCH_SIZE):
        part = keys[start : start + _MESSAGE_LOOKUP_BATCH_SIZE]
        placeholders = ",".join(["(?, ?)"] * len(part))
        params: list[int] = []
        for chat_id, message_id in part:
            params.extend((chat_id, message_id))
        cur.execute(
            f"""
            WITH target_media(chat_id, message_id) AS (VALUES {placeholders})
            SELECT
                t.chat_id AS target_chat_id,
                t.message_id AS target_message_id,
                mm.chat_id AS media_chat_id,
                m.grouped_id AS source_grouped_id,
                {selected_columns}
            FROM target_media AS t
            LEFT JOIN message_media AS mm
              ON mm.chat_id = t.chat_id AND mm.message_id = t.message_id
            LEFT JOIN messages AS m
              ON m.chat_id = t.chat_id AND m.message_id = t.message_id
            """,
            params,
        )
        for row in cur.fetchall():
            key = (int(row["target_chat_id"]), int(row["target_message_id"]))
            if row["source_grouped_id"] is not None:
                source_grouped_ids[key] = int(row["source_grouped_id"])
            if row["media_chat_id"] is not None:
                existing[key] = tuple(
                    row[column] for column in _MEDIA_VALUE_COLUMNS
                )
    return existing, source_grouped_ids


def _normalize_rows_by_key(rows: list[tuple]) -> dict[tuple[int, int], tuple]:
    """Apply the same last-row-wins semantics as ``executemany`` conflicts."""
    normalized: dict[tuple[int, int], tuple] = {}
    for row in rows:
        normalized[(int(row[0]), int(row[1]))] = row
    return normalized


def unique_message_key_count(rows: list[tuple]) -> int:
    """Count message keys using the same duplicate semantics as batch writes."""
    return len(_normalize_rows_by_key(rows))


def _prepare_message_upserts(
    cur: sqlite3.Cursor, msg_rows: list[tuple]
) -> tuple[
    list[tuple],
    dict[int, int],
    set[int],
    set[tuple[int, int]],
    set[tuple[int, int]],
]:
    normalized_rows = _normalize_rows_by_key(msg_rows)
    keys = sorted(normalized_rows)
    if not keys:
        return [], {}, set(), set(), set()

    existing_values = _load_existing_message_values(cur, keys)
    original_keys = set(existing_values)
    affected_grouped_ids: set[int] = set()
    missing_fts_keys: set[tuple[int, int]] = set()
    seeded_fts_keys: set[tuple[int, int]] = set()
    refresh_incomplete_indexes = False
    if original_keys:
        fts_runtime_available = _fts_runtime_available(cur)
        missing_fts_keys = _load_message_keys_missing_fts(
            cur,
            sorted(original_keys),
            fts_runtime_available=fts_runtime_available,
        )
        seeded_fts_keys = _seed_missing_fts_rows(
            cur,
            missing_fts_keys,
            fts_runtime_available=fts_runtime_available,
        )
        refresh_incomplete_indexes = _search_indexes_need_trigger_refresh(
            cur,
            fts_runtime_available=fts_runtime_available,
        )
    rows_to_write: list[tuple] = []
    new_keys: set[tuple[int, int]] = set()
    changed_keys: set[tuple[int, int]] = set()

    for key, row in normalized_rows.items():
        values = tuple(row[2 : 2 + len(_MESSAGE_VALUE_COLUMNS)])
        previous_values = existing_values.get(key)
        values_changed = previous_values != values
        force_index_refresh = (
            refresh_incomplete_indexes
            and key in original_keys
            and key not in missing_fts_keys
        )
        if (
            values_changed or force_index_refresh
        ):
            if previous_values is not None:
                previous_grouped_id = previous_values[_GROUPED_ID_VALUE_INDEX]
                if previous_grouped_id is not None:
                    affected_grouped_ids.add(int(previous_grouped_id))
            grouped_id = values[_GROUPED_ID_VALUE_INDEX]
            if grouped_id is not None:
                affected_grouped_ids.add(int(grouped_id))
            rows_to_write.append(row)
            existing_values[key] = values
            changed_keys.add(key)
        if key not in original_keys:
            new_keys.add(key)

    return (
        rows_to_write,
        _count_message_keys_by_chat(sorted(new_keys)),
        affected_grouped_ids,
        changed_keys,
        seeded_fts_keys,
    )


def _prepare_media_upserts(
    cur: sqlite3.Cursor, media_rows: list[tuple]
) -> tuple[list[tuple], set[int], set[tuple[int, int]]]:
    normalized_rows = _normalize_rows_by_key(media_rows)
    keys = sorted(normalized_rows)
    if not keys:
        return [], set(), set()

    existing_values, source_grouped_ids = _load_existing_media_values(cur, keys)
    rows_to_write: list[tuple] = []
    affected_grouped_ids: set[int] = set()
    changed_keys: set[tuple[int, int]] = set()
    grouped_id_index = _MEDIA_VALUE_COLUMNS.index("grouped_id")
    for key, row in normalized_rows.items():
        values = tuple(row[2 : 2 + len(_MEDIA_VALUE_COLUMNS)])
        previous_values = existing_values.get(key)
        if previous_values == values:
            continue
        if previous_values is not None:
            previous_grouped_id = previous_values[grouped_id_index]
            if previous_grouped_id is not None:
                affected_grouped_ids.add(int(previous_grouped_id))
        source_grouped_id = source_grouped_ids.get(key)
        if source_grouped_id is not None:
            affected_grouped_ids.add(source_grouped_id)
        grouped_id = values[grouped_id_index]
        if grouped_id is not None:
            affected_grouped_ids.add(int(grouped_id))
        rows_to_write.append(row)
        changed_keys.add(key)
    return rows_to_write, affected_grouped_ids, changed_keys


def _increment_chat_message_summaries(
    cur: sqlite3.Cursor, new_counts_by_chat: dict[int, int]
) -> None:
    for chat_id, new_count in sorted(new_counts_by_chat.items()):
        if new_count <= 0:
            continue
        cur.execute(
            """
            UPDATE chats
            SET
                message_count = COALESCE(message_count, 0) + ?,
                last_message_created_at = COALESCE((
                    SELECT MAX(created_at)
                    FROM messages
                    WHERE messages.chat_id = chats.chat_id
                ), '')
            WHERE chat_id = ?
            """,
            (int(new_count), int(chat_id)),
        )


def load_grouped_ids_for_messages(
    conn: sqlite3.Connection, message_keys: list[tuple[int, int]]
) -> set[int]:
    if not message_keys:
        return set()
    unique_keys = sorted({(int(chat_id), int(message_id)) for chat_id, message_id in message_keys})
    cur = conn.cursor()
    try:
        grouped_ids: set[int] = set()
        for start in range(0, len(unique_keys), 400):
            part = unique_keys[start : start + 400]
            placeholders = ",".join(["(?, ?)"] * len(part))
            params: list[int] = []
            for chat_id, message_id in part:
                params.extend([chat_id, message_id])
            cur.execute(
                f"""
                WITH target_messages(chat_id, message_id) AS (
                    VALUES {placeholders}
                )
                SELECT DISTINCT m.grouped_id
                FROM messages m
                JOIN target_messages t
                  ON t.chat_id = m.chat_id
                 AND t.message_id = m.message_id
                WHERE m.grouped_id IS NOT NULL
                """,
                params,
            )
            for row in cur.fetchall():
                grouped_ids.add(int(row["grouped_id"]))
        return grouped_ids
    finally:
        cur.close()


@synchronized_write
def batch_upsert(
    conn: sqlite3.Connection, msg_rows: list[tuple], media_rows: list[tuple]
) -> BatchUpsertResult:
    if not msg_rows and not media_rows:
        return BatchUpsertResult()
    cur = conn.cursor()
    owns_transaction = not conn.in_transaction
    savepoint_name = f"batch_upsert_{id(cur):x}"
    transaction_started = False

    def rollback_batch() -> None:
        """Roll back only this batch, preserving a caller-owned transaction."""
        nonlocal transaction_started
        if not transaction_started:
            return
        if owns_transaction:
            try:
                conn.rollback()
            except sqlite3.Error:
                logging.exception("消息批量写入失败后的数据库回滚也失败")
        else:
            try:
                cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
            except sqlite3.Error:
                logging.exception("消息批量写入失败后的 SAVEPOINT 回滚也失败")
            try:
                cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            except sqlite3.Error:
                logging.exception("消息批量写入失败后的 SAVEPOINT 释放也失败")
        transaction_started = False

    try:
        try:
            if owns_transaction:
                cur.execute("BEGIN IMMEDIATE")
            else:
                # Do not commit or roll back unrelated work already owned by
                # the caller.  The savepoint keeps this batch atomic inside it.
                cur.execute(f"SAVEPOINT {savepoint_name}")
            transaction_started = True
            (
                message_rows_to_write,
                new_counts_by_chat,
                message_grouped_ids,
                changed_message_keys,
                seeded_fts_keys,
            ) = _prepare_message_upserts(cur, msg_rows)
            media_rows_to_write, media_grouped_ids, changed_media_keys = (
                _prepare_media_upserts(cur, media_rows)
            )
            _batch_upsert_messages(cur, message_rows_to_write)
            _batch_upsert_media(cur, media_rows_to_write)
            deleted_media_keys, stale_grouped_ids = (
                _delete_stale_media_for_non_media_messages(cur, msg_rows, media_rows)
            )
            _increment_chat_message_summaries(cur, new_counts_by_chat)
            result = BatchUpsertResult(
                affected_grouped_ids=frozenset(
                    message_grouped_ids | media_grouped_ids | stale_grouped_ids
                ),
                persisted_change_count=len(
                    changed_message_keys
                    | changed_media_keys
                    | deleted_media_keys
                    | seeded_fts_keys
                ),
            )
            if owns_transaction:
                conn.commit()
            else:
                cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            transaction_started = False
            return result
        except sqlite3.Error:
            rollback_batch()
            logging.exception(
                "消息批量写入事务失败，已回滚: messages=%s media=%s",
                len(msg_rows),
                len(media_rows),
            )
            raise
        except Exception:
            # This covers a programming/invariant failure after BEGIN. It must
            # receive the same rollback treatment but is never retried here.
            rollback_batch()
            logging.exception(
                "消息批量写入事务发生未知错误，已回滚: messages=%s media=%s",
                len(msg_rows),
                len(media_rows),
            )
            raise
    finally:
        cur.close()
