import logging
import sqlite3
from collections import defaultdict

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


def _message_keys_from_rows(msg_rows: list[tuple]) -> list[tuple[int, int]]:
    return sorted({(int(row[0]), int(row[1])) for row in msg_rows})


def _count_message_keys_by_chat(keys: list[tuple[int, int]]) -> dict[int, int]:
    counts: dict[int, int] = defaultdict(int)
    for chat_id, _message_id in keys:
        counts[int(chat_id)] += 1
    return dict(counts)


def _count_existing_message_keys_by_chat(
    cur: sqlite3.Cursor, keys: list[tuple[int, int]]
) -> dict[int, int]:
    counts: dict[int, int] = defaultdict(int)
    for start in range(0, len(keys), 400):
        part = keys[start : start + 400]
        placeholders = ",".join(["(?, ?)"] * len(part))
        params: list[int] = []
        for chat_id, message_id in part:
            params.extend([chat_id, message_id])
        cur.execute(
            f"""
            WITH target_messages(chat_id, message_id) AS (
                VALUES {placeholders}
            )
            SELECT m.chat_id, COUNT(*) AS c
            FROM messages m
            JOIN target_messages t
              ON t.chat_id = m.chat_id
             AND t.message_id = m.message_id
            GROUP BY m.chat_id
            """,
            params,
        )
        for row in cur.fetchall():
            counts[int(row["chat_id"])] += int(row["c"] or 0)
    return dict(counts)


def _new_message_counts_by_chat(
    cur: sqlite3.Cursor, msg_rows: list[tuple]
) -> dict[int, int]:
    keys = _message_keys_from_rows(msg_rows)
    if not keys:
        return {}
    submitted_counts = _count_message_keys_by_chat(keys)
    existing_counts = _count_existing_message_keys_by_chat(cur, keys)
    return {
        chat_id: submitted_count - int(existing_counts.get(chat_id, 0) or 0)
        for chat_id, submitted_count in submitted_counts.items()
        if submitted_count > int(existing_counts.get(chat_id, 0) or 0)
    }


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
):
    if not msg_rows and not media_rows:
        return
    cur = conn.cursor()
    try:
        try:
            cur.execute("BEGIN IMMEDIATE")
            new_counts_by_chat = _new_message_counts_by_chat(cur, msg_rows)
            _batch_upsert_messages(cur, msg_rows)
            _batch_upsert_media(cur, media_rows)
            _delete_stale_media_for_non_media_messages(cur, msg_rows, media_rows)
            _increment_chat_message_summaries(cur, new_counts_by_chat)
            conn.commit()
        except sqlite3.Error:
            try:
                conn.rollback()
            except sqlite3.Error:
                logging.exception("消息批量写入失败后的数据库回滚也失败")
            logging.exception(
                "消息批量写入事务失败，已回滚: messages=%s media=%s",
                len(msg_rows),
                len(media_rows),
            )
            raise
        except Exception:
            # This covers a programming/invariant failure after BEGIN. It must
            # receive the same rollback treatment but is never retried here.
            try:
                conn.rollback()
            except sqlite3.Error:
                logging.exception("消息批量写入未知错误后的数据库回滚也失败")
            logging.exception(
                "消息批量写入事务发生未知错误，已回滚: messages=%s media=%s",
                len(msg_rows),
                len(media_rows),
            )
            raise
    finally:
        cur.close()
