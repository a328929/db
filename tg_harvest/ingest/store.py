# -*- coding: utf-8 -*-
import sqlite3
from typing import Any, List, Optional, Tuple

from tg_harvest.storage.connection import synchronized_write
from tg_harvest.storage.search_text_state import indexed_messages_from_clause
from tg_harvest.storage.search_text_state import indexed_unsearchable_message_predicate

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


def _batch_upsert_messages(cur: sqlite3.Cursor, msg_rows: List[tuple]):
    if not msg_rows:
        return
    cur.executemany(UPSERT_MESSAGE_SQL, msg_rows)


def _batch_upsert_media(cur: sqlite3.Cursor, media_rows: List[tuple]):
    if not media_rows:
        return
    cur.executemany(UPSERT_MEDIA_SQL, media_rows)


def _delete_stale_media_for_non_media_messages(
    cur: sqlite3.Cursor, msg_rows: List[tuple], media_rows: List[tuple]
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
    cur.executemany(
        "DELETE FROM message_media WHERE chat_id = ? AND message_id = ?",
        stale_keys,
    )


@synchronized_write
def _backfill_message_media_placeholders_batch(
    conn: sqlite3.Connection, *, chat_id: Optional[int], batch_size: int
) -> int:
    cur = conn.cursor()
    try:
        where_sql = "m.has_media = 1 AND mm.chat_id IS NULL"
        params: List[Any] = []
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
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        cur.close()


def backfill_missing_message_media_placeholders(
    conn: sqlite3.Connection,
    *,
    chat_id: Optional[int] = None,
    batch_size: int = 50000,
    log_fn: Optional[Any] = None,
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


@synchronized_write
def _backfill_message_search_text_from_filenames_batch(
    conn: sqlite3.Connection,
    *,
    chat_id: Optional[int],
    batch_size: int,
    after_pk: int,
) -> Tuple[int, int]:
    cur = conn.cursor()
    try:
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
            AND COALESCE(NULLIF(mm.file_name, ''), '') <> ''
            AND m.pk > ?
        """
        params: List[Any] = [int(after_pk)]
        if chat_id is not None:
            where_sql += " AND m.chat_id = ?"
            params.append(int(chat_id))
        params.append(int(batch_size))

        cur.execute(
            f"""
            SELECT m.pk, m.chat_id, m.message_id, mm.file_name
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

        cur.execute("BEGIN IMMEDIATE")
        cur.executemany(
            f"""
            UPDATE messages
            SET content = ?,
                content_norm = ?,
                updated_at = datetime('now')
            WHERE chat_id = ? AND message_id = ?
              AND {update_unsearchable_predicate}
            """,
            [
                (
                    str(row["file_name"]).strip(),
                    str(row["file_name"]).strip(),
                    int(row["chat_id"]),
                    int(row["message_id"]),
                )
                for row in rows
            ],
        )
        conn.commit()
        return int(cur.rowcount or 0), last_pk
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        cur.close()


def backfill_message_search_text_from_filenames(
    conn: sqlite3.Connection,
    *,
    chat_id: Optional[int] = None,
    batch_size: int = 5000,
    log_fn: Optional[Any] = None,
) -> int:
    total_updated = 0
    safe_batch_size = max(100, int(batch_size))
    last_pk = 0

    while True:
        updated, next_last_pk = _backfill_message_search_text_from_filenames_batch(
            conn,
            chat_id=chat_id,
            batch_size=safe_batch_size,
            after_pk=last_pk,
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


@synchronized_write
def batch_upsert(
    conn: sqlite3.Connection, msg_rows: List[tuple], media_rows: List[tuple]
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
            try:
                conn.rollback()
            except Exception:
                pass
            raise
    finally:
        cur.close()
