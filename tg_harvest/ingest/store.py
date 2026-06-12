import sqlite3
from contextlib import suppress
from typing import Any

from tg_harvest.domain.dedupe import build_message_dedupe_hash
from tg_harvest.domain.normalize import _safe_json
from tg_harvest.domain.promo import build_single_promo_features
from tg_harvest.storage.connection import synchronized_write
from tg_harvest.storage.introspection import table_columns as _table_columns
from tg_harvest.storage.search_text_state import (
    indexed_messages_from_clause,
    indexed_unsearchable_message_predicate,
)

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
"""

_DELETE_MEDIA_KEY_BATCH_SIZE = 400

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
) -> None:
    if not msg_rows:
        return
    media_keys = {(int(row[0]), int(row[1])) for row in media_rows}
    stale_keys = [
        (int(row[0]), int(row[1]))
        for row in msg_rows
        if (int(row[0]), int(row[1])) not in media_keys
    ]
    if not stale_keys:
        return
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


@synchronized_write
def _backfill_message_media_placeholders_batch(
    conn: sqlite3.Connection, *, chat_id: int | None, batch_size: int
) -> int:
    cur = conn.cursor()
    try:
        where_sql = "m.has_media = 1 AND mm.chat_id IS NULL"
        params: list[Any] = []
        if chat_id is not None:
            where_sql += " AND m.chat_id = ?"
            params.append(int(chat_id))
        params.append(int(batch_size))

        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            f"""
            INSERT OR IGNORE INTO message_media(
                chat_id, message_id, media_kind, file_unique_id, file_name, file_ext,
                mime_type, file_size, width, height, duration_sec, grouped_id,
                media_fingerprint, meta_json, updated_at
            )
            SELECT
                m.chat_id,
                m.message_id,
                m.msg_type,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                NULL,
                m.grouped_id,
                NULL,
                NULL,
                datetime('now')
            FROM messages m
            LEFT JOIN message_media mm
              ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id
            WHERE {where_sql}
            ORDER BY m.pk ASC
            LIMIT ?
            """,
            params,
        )
        inserted = int(cur.rowcount or 0)
        conn.commit()
        return inserted
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        cur.close()


def backfill_missing_message_media_placeholders(
    conn: sqlite3.Connection,
    *,
    chat_id: int | None = None,
    batch_size: int = 50000,
    log_fn: Any | None = None,
) -> int:
    total_inserted = 0
    safe_batch_size = max(100, int(batch_size))

    while True:
        inserted = _backfill_message_media_placeholders_batch(
            conn, chat_id=chat_id, batch_size=safe_batch_size
        )
        if inserted <= 0:
            break
        total_inserted += inserted
        if log_fn:
            scope = f"chat_id={chat_id}" if chat_id is not None else "all_chats"
            log_fn(
                f"message_media 占位回填中：作用域={scope}，本批新增 {inserted} 条，累计 {total_inserted} 条"
            )

    return total_inserted


def _message_filename_backfill_update_sql(
    message_columns: set[str], update_unsearchable_predicate: str
) -> str:
    assignments = [
        "content = ?",
        "content_norm = ?",
    ]
    optional_columns = [
        ("pure_hash", "pure_hash = ?"),
        ("dedupe_hash", "dedupe_hash = ?"),
        ("is_promo", "is_promo = ?"),
        ("promo_score", "promo_score = ?"),
        ("promo_reasons", "promo_reasons = ?"),
        ("dedupe_eligible", "dedupe_eligible = ?"),
        ("guard_reason", "guard_reason = ?"),
        ("text_len", "text_len = ?"),
    ]
    assignments.extend(
        sql for column_name, sql in optional_columns if column_name in message_columns
    )
    if "updated_at" in message_columns:
        assignments.append("updated_at = datetime('now')")

    return f"""
        UPDATE messages
        SET {", ".join(assignments)}
        WHERE chat_id = ? AND message_id = ?
          AND {update_unsearchable_predicate}
    """


def _message_filename_backfill_params(
    message_columns: set[str], row: sqlite3.Row, cfg: Any | None
) -> tuple:
    file_name = str(row["file_name"] or "").strip()
    msg_type = str(row["msg_type"] or "FILE")
    has_media = bool(int(row["has_media"] or 0))
    media_fingerprint = row["media_fingerprint"]
    features = build_single_promo_features(
        file_name,
        msg_type=msg_type,
        has_media=has_media,
        cfg=cfg,
    )

    params: list[Any] = [
        file_name,
        features["content_norm"],
    ]
    if "pure_hash" in message_columns:
        params.append(features["pure_hash"])
    if "dedupe_hash" in message_columns:
        params.append(
            build_message_dedupe_hash(
                text_pure_hash=features["pure_hash"],
                has_media=has_media,
                media_fingerprint=media_fingerprint,
            )
        )
    if "is_promo" in message_columns:
        params.append(int(features["is_promo"]))
    if "promo_score" in message_columns:
        params.append(int(features["promo_score"]))
    if "promo_reasons" in message_columns:
        params.append(_safe_json(features["promo_reasons"]))
    if "dedupe_eligible" in message_columns:
        params.append(int(features["dedupe_eligible"]))
    if "guard_reason" in message_columns:
        params.append(features["guard_reason"])
    if "text_len" in message_columns:
        params.append(int(features["text_len"]))

    params.extend([int(row["chat_id"]), int(row["message_id"])])
    return tuple(params)


@synchronized_write
def _backfill_message_search_text_from_filenames_batch(
    conn: sqlite3.Connection,
    *,
    chat_id: int | None,
    batch_size: int,
    after_pk: int,
    cfg: Any | None,
) -> tuple[int, int]:
    cur = conn.cursor()
    try:
        message_columns = _table_columns(cur, "messages")
        media_columns = _table_columns(cur, "message_media")
        unsearchable_predicate = indexed_unsearchable_message_predicate(cur, alias="m")
        messages_from_sql = indexed_messages_from_clause(
            cur,
            alias="m",
            chat_scoped=chat_id is not None,
        )
        update_unsearchable_predicate = indexed_unsearchable_message_predicate(
            cur, alias=""
        )
        where_sql = f"""
            m.has_media = 1
            AND {unsearchable_predicate}
            AND COALESCE(NULLIF(TRIM(mm.file_name), ''), '') <> ''
            AND m.pk > ?
        """
        params: list[Any] = [int(after_pk)]
        if chat_id is not None:
            where_sql += " AND m.chat_id = ?"
            params.append(int(chat_id))
        params.append(int(batch_size))
        msg_type_select = "m.msg_type" if "msg_type" in message_columns else "'FILE'"
        has_media_select = "m.has_media" if "has_media" in message_columns else "1"
        media_fingerprint_select = (
            "mm.media_fingerprint"
            if "media_fingerprint" in media_columns
            else "NULL"
        )

        cur.execute(
            f"""
            SELECT
                m.pk,
                m.chat_id,
                m.message_id,
                {msg_type_select} AS msg_type,
                {has_media_select} AS has_media,
                mm.file_name,
                {media_fingerprint_select} AS media_fingerprint
            FROM {messages_from_sql}
            JOIN message_media mm
              ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id
            WHERE {where_sql}
            ORDER BY m.pk ASC
            LIMIT ?
            """,
            params,
        )
        rows = cur.fetchall()
        if not rows:
            return 0, 0
        last_pk = int(rows[-1]["pk"])
        update_sql = _message_filename_backfill_update_sql(
            message_columns, update_unsearchable_predicate
        )
        update_params = [
            _message_filename_backfill_params(message_columns, row, cfg)
            for row in rows
        ]

        cur.execute("BEGIN IMMEDIATE")
        cur.executemany(update_sql, update_params)
        conn.commit()
        return int(cur.rowcount or 0), last_pk
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        cur.close()


def backfill_message_search_text_from_filenames(
    conn: sqlite3.Connection,
    *,
    chat_id: int | None = None,
    batch_size: int = 5000,
    log_fn: Any | None = None,
    cfg: Any | None = None,
) -> int:
    if cfg is None:
        from tg_harvest.config import CFG

        cfg = CFG

    total_updated = 0
    safe_batch_size = max(100, int(batch_size))
    last_pk = 0

    while True:
        updated, next_last_pk = _backfill_message_search_text_from_filenames_batch(
            conn,
            chat_id=chat_id,
            batch_size=safe_batch_size,
            after_pk=last_pk,
            cfg=cfg,
        )
        if next_last_pk <= 0:
            break
        last_pk = next_last_pk
        total_updated += updated
        if log_fn:
            scope = f"chat_id={chat_id}" if chat_id is not None else "all_chats"
            log_fn(
                f"messages 文本回填中：作用域={scope}，本批补齐 {updated} 条，累计 {total_updated} 条"
            )

    return total_updated


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
):
    if not msg_rows and not media_rows:
        return
    cur = conn.cursor()
    try:
        try:
            cur.execute("BEGIN IMMEDIATE")
            _batch_upsert_messages(cur, msg_rows)
            _batch_upsert_media(cur, media_rows)
            _delete_stale_media_for_non_media_messages(cur, msg_rows, media_rows)
            conn.commit()
        except Exception:
            with suppress(Exception):
                conn.rollback()
            raise
    finally:
        cur.close()
