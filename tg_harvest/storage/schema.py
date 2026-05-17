# -*- coding: utf-8 -*-
import sqlite3
import logging
import re
from typing import Iterable, Optional, Sequence, Tuple

from . import connection as _db_runtime
from . import fts as _fts
from . import indexes as _indexes
from . import search_terms as _search_terms
from .search_text_state import SEARCH_TEXT_PRESENT_COLUMN
from .search_text_state import search_text_present_column_sql
from .search_text_state import table_has_column as _table_has_column
from .connection import SqliteFeatures

# =========================
# Schema 初始化
# =========================


def _create_chats_table(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS chats (
        chat_id          INTEGER PRIMARY KEY,
        chat_title       TEXT NOT NULL,
        chat_username    TEXT,
        is_public        INTEGER NOT NULL DEFAULT 0,
        chat_type        TEXT,
        message_count    INTEGER NOT NULL DEFAULT 0,
        first_seen_at    TEXT NOT NULL DEFAULT (datetime('now')),
        last_seen_at     TEXT NOT NULL DEFAULT (datetime('now'))
    ){strict_suffix}
    """)


def _create_messages_table(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS messages (
        pk                   INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id              INTEGER NOT NULL,
        message_id           INTEGER NOT NULL,
        msg_date_text        TEXT NOT NULL,
        msg_date_ts          INTEGER NOT NULL,
        sender_id            INTEGER,

        content              TEXT,
        content_norm         TEXT,
        pure_hash            TEXT,
        dedupe_hash          TEXT,

        msg_type             TEXT NOT NULL,
        grouped_id           INTEGER,
        has_media            INTEGER NOT NULL DEFAULT 0,

        is_promo             INTEGER NOT NULL DEFAULT 0,
        promo_score          INTEGER NOT NULL DEFAULT 0,
        promo_reasons        TEXT,
        dedupe_eligible      INTEGER NOT NULL DEFAULT 0,
        guard_reason         TEXT,
        text_len             INTEGER NOT NULL DEFAULT 0,

        visual_hash          TEXT,
        visual_hash_algo     TEXT,
        visual_embed_ref     TEXT,
        search_text_present  INTEGER GENERATED ALWAYS AS (
            CASE WHEN COALESCE(NULLIF(TRIM(content_norm), ''), NULLIF(TRIM(content), ''), '') <> ''
                 THEN 1 ELSE 0 END
        ) VIRTUAL,

        created_at           TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at           TEXT NOT NULL DEFAULT (datetime('now')),

        UNIQUE(chat_id, message_id),
        FOREIGN KEY(chat_id) REFERENCES chats(chat_id)
    ){strict_suffix}
    """)


def _create_message_media_table(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS message_media (
        chat_id              INTEGER NOT NULL,
        message_id           INTEGER NOT NULL,
        media_kind           TEXT,
        file_unique_id       TEXT,
        file_name            TEXT,
        file_ext             TEXT,
        mime_type            TEXT,
        file_size            INTEGER,
        width                INTEGER,
        height               INTEGER,
        duration_sec         INTEGER,
        grouped_id           INTEGER,

        media_fingerprint    TEXT,
        meta_json            TEXT,

        updated_at           TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (chat_id, message_id),
        FOREIGN KEY(chat_id, message_id) REFERENCES messages(chat_id, message_id) ON DELETE CASCADE
    ){strict_suffix}
    """)


def _create_media_groups_table(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS media_groups (
        chat_id              INTEGER NOT NULL,
        grouped_id           INTEGER NOT NULL,

        first_message_id     INTEGER,
        first_msg_date_ts    INTEGER,
        last_message_id      INTEGER,
        last_msg_date_ts     INTEGER,

        item_count           INTEGER NOT NULL DEFAULT 0,
        active_items         INTEGER NOT NULL DEFAULT 0,

        types_csv            TEXT,
        captions_concat      TEXT,
        caption_norm         TEXT,
        pure_hash            TEXT,      -- caption 模板 hash
        media_sig_hash       TEXT,      -- 组内媒体指纹签名
        dedupe_hash          TEXT,      -- 当前主去重键（文本优先）

        is_promo             INTEGER NOT NULL DEFAULT 0,
        promo_score          INTEGER NOT NULL DEFAULT 0,
        promo_reasons        TEXT,
        dedupe_eligible      INTEGER NOT NULL DEFAULT 0,
        guard_reason         TEXT,

        created_at           TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at           TEXT NOT NULL DEFAULT (datetime('now')),

        PRIMARY KEY(chat_id, grouped_id),
        FOREIGN KEY(chat_id) REFERENCES chats(chat_id)
    ){strict_suffix}
    """)


def _create_dedupe_tables(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS dedupe_runs (
        batch_id                 TEXT PRIMARY KEY,
        chat_id                  INTEGER NOT NULL,
        mode                     TEXT NOT NULL,
        threshold                INTEGER NOT NULL,
        promo_threshold          INTEGER NOT NULL,
        dup_hash_count_solo      INTEGER NOT NULL DEFAULT 0,
        dup_hash_count_group_txt INTEGER NOT NULL DEFAULT 0,
        dup_hash_count_group_med INTEGER NOT NULL DEFAULT 0,
        target_count             INTEGER NOT NULL DEFAULT 0,
        started_at               TEXT NOT NULL DEFAULT (datetime('now')),
        finished_at              TEXT
    ){strict_suffix}
    """)

    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS dedupe_actions (
        id                   INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id             TEXT NOT NULL,
        chat_id              INTEGER NOT NULL,
        pk                   INTEGER NOT NULL,
        message_id           INTEGER NOT NULL,
        grouped_id           INTEGER,
        dedupe_hash          TEXT,
        pure_hash            TEXT,
        action               TEXT NOT NULL,
        reason               TEXT NOT NULL,
        created_at           TEXT NOT NULL DEFAULT (datetime('now'))
    ){strict_suffix}
    """)


def _create_message_search_terms_table(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS message_search_terms (
        pk      INTEGER NOT NULL,
        term    TEXT NOT NULL,
        PRIMARY KEY (term, pk),
        FOREIGN KEY(pk) REFERENCES messages(pk) ON DELETE CASCADE
    ){strict_suffix}
    """)


def _create_message_search_terms_rebuild_queue_table(
    cur: sqlite3.Cursor, strict_suffix: str
):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS message_search_terms_rebuild_queue (
        pk         INTEGER PRIMARY KEY,
        reason     TEXT,
        queued_at  TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY(pk) REFERENCES messages(pk) ON DELETE CASCADE
    ){strict_suffix}
    """)


def _create_message_search_terms_meta_table(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS message_search_terms_meta (
        key     TEXT PRIMARY KEY,
        value   TEXT NOT NULL
    ){strict_suffix}
    """)


def _create_admin_job_tables(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS admin_jobs (
        job_id               TEXT PRIMARY KEY,
        job_type             TEXT NOT NULL,
        status               TEXT NOT NULL,
        target_chat_id       INTEGER,
        target_label         TEXT,
        created_at           TEXT NOT NULL,
        updated_at           TEXT NOT NULL,
        owner_instance_id    TEXT,
        owner_pid            INTEGER,
        heartbeat_at         TEXT NOT NULL DEFAULT (datetime('now')),
        progress_current     INTEGER NOT NULL DEFAULT 0,
        progress_total       INTEGER,
        progress_stage       TEXT NOT NULL DEFAULT 'queued',
        last_logged_current  INTEGER NOT NULL DEFAULT 0
    ){strict_suffix}
    """)

    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS admin_job_logs (
        job_id               TEXT NOT NULL,
        seq                  INTEGER NOT NULL,
        ts                   TEXT NOT NULL,
        message              TEXT NOT NULL,
        PRIMARY KEY (job_id, seq),
        FOREIGN KEY(job_id) REFERENCES admin_jobs(job_id) ON DELETE CASCADE
    ){strict_suffix}
    """)


def _create_admin_missing_chats_table(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS admin_missing_chats (
        chat_id              INTEGER PRIMARY KEY,
        chat_title           TEXT NOT NULL,
        chat_username        TEXT,
        chat_type            TEXT,
        is_public            INTEGER NOT NULL DEFAULT 0,
        last_message_at      TEXT,
        last_message_ts      INTEGER,
        scan_job_id          TEXT,
        scanned_at           TEXT NOT NULL
    ){strict_suffix}
    """)


def _create_admin_absent_chats_table(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS admin_absent_chats (
        chat_id              INTEGER PRIMARY KEY,
        chat_title           TEXT NOT NULL,
        chat_username        TEXT,
        chat_type            TEXT,
        message_count        INTEGER NOT NULL DEFAULT 0,
        last_seen_at         TEXT,
        last_message_at      TEXT,
        last_message_ts      INTEGER,
        scan_reason          TEXT,
        scan_job_id          TEXT,
        scanned_at           TEXT NOT NULL
    ){strict_suffix}
    """)


def _create_admin_restricted_chats_table(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS admin_restricted_chats (
        chat_id                  INTEGER PRIMARY KEY,
        chat_title               TEXT NOT NULL,
        chat_username            TEXT,
        chat_type                TEXT,
        is_public                INTEGER NOT NULL DEFAULT 0,
        restriction_platforms    TEXT,
        restriction_reasons      TEXT,
        restriction_text         TEXT,
        risk_flags               TEXT,
        last_message_at          TEXT,
        last_message_ts          INTEGER,
        scan_job_id              TEXT,
        scanned_at               TEXT NOT NULL
    ){strict_suffix}
    """)


def _create_tables(cur: sqlite3.Cursor, strict_suffix: str):
    _create_chats_table(cur, strict_suffix)
    _create_messages_table(cur, strict_suffix)
    _create_message_media_table(cur, strict_suffix)
    _create_media_groups_table(cur, strict_suffix)
    _create_dedupe_tables(cur, strict_suffix)
    _create_message_search_terms_table(cur, strict_suffix)
    _create_message_search_terms_rebuild_queue_table(cur, strict_suffix)
    _create_message_search_terms_meta_table(cur, strict_suffix)
    _create_admin_job_tables(cur, strict_suffix)
    _create_admin_missing_chats_table(cur, strict_suffix)
    _create_admin_absent_chats_table(cur, strict_suffix)
    _create_admin_restricted_chats_table(cur, strict_suffix)


def _column_exists(cur: sqlite3.Cursor, table_name: str, column_name: str) -> bool:
    return _table_has_column(cur, table_name, column_name)


_DYNAMIC_DATETIME_DEFAULT_RE = re.compile(
    r"\s+DEFAULT\s*\(\s*datetime\s*\(\s*['\"]now['\"]\s*\)\s*\)",
    re.IGNORECASE,
)


def _column_sql_has_dynamic_datetime_default(column_sql: str) -> bool:
    return _DYNAMIC_DATETIME_DEFAULT_RE.search(str(column_sql or "")) is not None


def _sqlite_add_column_compatible_sql(column_sql: str) -> str:
    # SQLite cannot ADD COLUMN with DEFAULT(datetime('now')) on existing tables.
    # Use an ALTER-compatible constant default, then backfill current rows below.
    return _DYNAMIC_DATETIME_DEFAULT_RE.sub(" DEFAULT ''", str(column_sql or ""))


def _add_column_compatible(
    cur: sqlite3.Cursor, table_name: str, column_name: str, column_sql: str
) -> None:
    if not _column_sql_has_dynamic_datetime_default(column_sql):
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_sql}")
        return

    cur.execute(
        f"ALTER TABLE {table_name} ADD COLUMN {_sqlite_add_column_compatible_sql(column_sql)}"
    )
    cur.execute(
        f"""
        UPDATE {table_name}
        SET {column_name} = datetime('now')
        WHERE {column_name} IS NULL OR {column_name} = ''
        """
    )


def _ensure_table_columns(
    cur: sqlite3.Cursor, table_name: str, column_defs: Sequence[Tuple[str, str]]
) -> None:
    if not _table_exists(cur, table_name):
        return
    for column_name, column_sql in column_defs:
        if _column_exists(cur, table_name, column_name):
            continue
        _add_column_compatible(cur, table_name, column_name, column_sql)


def _ensure_chats_schema(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "chats",
        [
            ("is_public", "is_public INTEGER NOT NULL DEFAULT 0"),
            ("chat_type", "chat_type TEXT"),
            ("message_count", "message_count INTEGER NOT NULL DEFAULT 0"),
            ("first_seen_at", "first_seen_at TEXT NOT NULL DEFAULT (datetime('now'))"),
            ("last_seen_at", "last_seen_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )


def _ensure_messages_runtime_columns(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "messages",
        [
            ("msg_date_text", "msg_date_text TEXT NOT NULL DEFAULT ''"),
            ("msg_date_ts", "msg_date_ts INTEGER NOT NULL DEFAULT 0"),
            ("sender_id", "sender_id INTEGER"),
            ("content", "content TEXT"),
            ("content_norm", "content_norm TEXT"),
            ("pure_hash", "pure_hash TEXT"),
            ("dedupe_hash", "dedupe_hash TEXT"),
            ("msg_type", "msg_type TEXT NOT NULL DEFAULT 'TEXT'"),
            ("grouped_id", "grouped_id INTEGER"),
            ("has_media", "has_media INTEGER NOT NULL DEFAULT 0"),
            ("is_promo", "is_promo INTEGER NOT NULL DEFAULT 0"),
            ("promo_score", "promo_score INTEGER NOT NULL DEFAULT 0"),
            ("promo_reasons", "promo_reasons TEXT"),
            ("dedupe_eligible", "dedupe_eligible INTEGER NOT NULL DEFAULT 0"),
            ("guard_reason", "guard_reason TEXT"),
            ("text_len", "text_len INTEGER NOT NULL DEFAULT 0"),
            ("visual_hash", "visual_hash TEXT"),
            ("visual_hash_algo", "visual_hash_algo TEXT"),
            ("visual_embed_ref", "visual_embed_ref TEXT"),
            (SEARCH_TEXT_PRESENT_COLUMN, search_text_present_column_sql()),
            ("created_at", "created_at TEXT NOT NULL DEFAULT (datetime('now'))"),
            ("updated_at", "updated_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )


def _ensure_message_media_runtime_columns(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "message_media",
        [
            ("media_kind", "media_kind TEXT"),
            ("file_unique_id", "file_unique_id TEXT"),
            ("file_name", "file_name TEXT"),
            ("file_ext", "file_ext TEXT"),
            ("mime_type", "mime_type TEXT"),
            ("file_size", "file_size INTEGER"),
            ("width", "width INTEGER"),
            ("height", "height INTEGER"),
            ("duration_sec", "duration_sec INTEGER"),
            ("grouped_id", "grouped_id INTEGER"),
            ("media_fingerprint", "media_fingerprint TEXT"),
            ("meta_json", "meta_json TEXT"),
            ("updated_at", "updated_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )


def _ensure_media_groups_runtime_columns(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "media_groups",
        [
            ("first_message_id", "first_message_id INTEGER"),
            ("first_msg_date_ts", "first_msg_date_ts INTEGER"),
            ("last_message_id", "last_message_id INTEGER"),
            ("last_msg_date_ts", "last_msg_date_ts INTEGER"),
            ("item_count", "item_count INTEGER NOT NULL DEFAULT 0"),
            ("active_items", "active_items INTEGER NOT NULL DEFAULT 0"),
            ("types_csv", "types_csv TEXT"),
            ("captions_concat", "captions_concat TEXT"),
            ("caption_norm", "caption_norm TEXT"),
            ("pure_hash", "pure_hash TEXT"),
            ("media_sig_hash", "media_sig_hash TEXT"),
            ("dedupe_hash", "dedupe_hash TEXT"),
            ("is_promo", "is_promo INTEGER NOT NULL DEFAULT 0"),
            ("promo_score", "promo_score INTEGER NOT NULL DEFAULT 0"),
            ("promo_reasons", "promo_reasons TEXT"),
            ("dedupe_eligible", "dedupe_eligible INTEGER NOT NULL DEFAULT 0"),
            ("guard_reason", "guard_reason TEXT"),
            ("created_at", "created_at TEXT NOT NULL DEFAULT (datetime('now'))"),
            ("updated_at", "updated_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )


def _ensure_dedupe_schema(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "dedupe_runs",
        [
            ("dup_hash_count_solo", "dup_hash_count_solo INTEGER NOT NULL DEFAULT 0"),
            (
                "dup_hash_count_group_txt",
                "dup_hash_count_group_txt INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "dup_hash_count_group_med",
                "dup_hash_count_group_med INTEGER NOT NULL DEFAULT 0",
            ),
            ("target_count", "target_count INTEGER NOT NULL DEFAULT 0"),
            ("started_at", "started_at TEXT NOT NULL DEFAULT (datetime('now'))"),
            ("finished_at", "finished_at TEXT"),
        ],
    )
    _ensure_table_columns(
        cur,
        "dedupe_actions",
        [
            ("grouped_id", "grouped_id INTEGER"),
            ("dedupe_hash", "dedupe_hash TEXT"),
            ("pure_hash", "pure_hash TEXT"),
            ("created_at", "created_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )


def _ensure_admin_job_schema(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "admin_jobs",
        [
            ("owner_instance_id", "owner_instance_id TEXT"),
            ("owner_pid", "owner_pid INTEGER"),
            ("heartbeat_at", "heartbeat_at TEXT NOT NULL DEFAULT (datetime('now'))"),
            ("progress_current", "progress_current INTEGER NOT NULL DEFAULT 0"),
            ("progress_total", "progress_total INTEGER"),
            ("progress_stage", "progress_stage TEXT NOT NULL DEFAULT 'queued'"),
            ("last_logged_current", "last_logged_current INTEGER NOT NULL DEFAULT 0"),
        ],
    )


def _ensure_admin_missing_chats_schema(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "admin_missing_chats",
        [
            ("chat_username", "chat_username TEXT"),
            ("chat_type", "chat_type TEXT"),
            ("is_public", "is_public INTEGER NOT NULL DEFAULT 0"),
            ("last_message_at", "last_message_at TEXT"),
            ("last_message_ts", "last_message_ts INTEGER"),
            ("scan_job_id", "scan_job_id TEXT"),
            ("scanned_at", "scanned_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )


def _ensure_admin_absent_chats_schema(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "admin_absent_chats",
        [
            ("chat_username", "chat_username TEXT"),
            ("chat_type", "chat_type TEXT"),
            ("message_count", "message_count INTEGER NOT NULL DEFAULT 0"),
            ("last_seen_at", "last_seen_at TEXT"),
            ("last_message_at", "last_message_at TEXT"),
            ("last_message_ts", "last_message_ts INTEGER"),
            ("scan_reason", "scan_reason TEXT"),
            ("scan_job_id", "scan_job_id TEXT"),
            ("scanned_at", "scanned_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )
    cur.execute(
        """
        DELETE FROM admin_absent_chats
        WHERE scan_reason LIKE 'Telegram 限制显示%'
        """
    )


def _ensure_admin_restricted_chats_schema(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "admin_restricted_chats",
        [
            ("chat_username", "chat_username TEXT"),
            ("chat_type", "chat_type TEXT"),
            ("is_public", "is_public INTEGER NOT NULL DEFAULT 0"),
            ("restriction_platforms", "restriction_platforms TEXT"),
            ("restriction_reasons", "restriction_reasons TEXT"),
            ("restriction_text", "restriction_text TEXT"),
            ("risk_flags", "risk_flags TEXT"),
            ("last_message_at", "last_message_at TEXT"),
            ("last_message_ts", "last_message_ts INTEGER"),
            ("scan_job_id", "scan_job_id TEXT"),
            ("scanned_at", "scanned_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )


def _ensure_chat_summary_columns(cur: sqlite3.Cursor) -> None:
    if not _column_exists(cur, "chats", "message_count"):
        cur.execute(
            "ALTER TABLE chats ADD COLUMN message_count INTEGER NOT NULL DEFAULT 0"
        )


def _count_chat_message_count_mismatches(cur: sqlite3.Cursor) -> int:
    cur.execute(
        """
        SELECT COUNT(*) AS c
        FROM (
            SELECT c.chat_id
            FROM chats c
            LEFT JOIN (
                SELECT chat_id, COUNT(*) AS real_count
                FROM messages
                GROUP BY chat_id
            ) m ON m.chat_id = c.chat_id
            WHERE COALESCE(c.message_count, 0) <> COALESCE(m.real_count, 0)
        )
        """
    )
    return int(cur.fetchone()["c"] or 0)


def _heal_chat_message_counts_if_needed(cur: sqlite3.Cursor) -> None:
    mismatch_count = _count_chat_message_count_mismatches(cur)
    if mismatch_count <= 0:
        return
    logging.warning(
        f"检测到 chats.message_count 与 messages 实际条数不一致，开始修复 {mismatch_count} 个群聊摘要"
    )
    _refresh_chat_message_counts(cur, chat_ids=None)


def _table_exists(cur: sqlite3.Cursor, table_name: str) -> bool:
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    )
    return cur.fetchone() is not None


def _get_table_sql(cur: sqlite3.Cursor, table_name: str) -> str:
    cur.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    )
    row = cur.fetchone()
    if not row:
        return ""
    return str(row["sql"] if isinstance(row, sqlite3.Row) else row[0] or "")


def _drop_messages_compat_objects(cur: sqlite3.Cursor) -> None:
    cur.execute("DROP VIEW IF EXISTS v_messages_enriched")


def _drop_messages_link_column_with_rebuild(
    cur: sqlite3.Cursor, strict_suffix: str
) -> None:
    _drop_messages_compat_objects(cur)
    cur.execute("DROP TABLE IF EXISTS messages__legacy_drop_link")
    cur.execute("ALTER TABLE messages RENAME TO messages__legacy_drop_link")
    _create_messages_table(cur, strict_suffix)
    cur.execute(
        """
        INSERT INTO messages(
            pk, chat_id, message_id, msg_date_text, msg_date_ts, sender_id,
            content, content_norm, pure_hash, dedupe_hash,
            msg_type, grouped_id, has_media,
            is_promo, promo_score, promo_reasons, dedupe_eligible, guard_reason, text_len,
            visual_hash, visual_hash_algo, visual_embed_ref,
            created_at, updated_at
        )
        SELECT
            pk, chat_id, message_id, msg_date_text, msg_date_ts, sender_id,
            content, content_norm, pure_hash, dedupe_hash,
            msg_type, grouped_id, has_media,
            is_promo, promo_score, promo_reasons, dedupe_eligible, guard_reason, text_len,
            visual_hash, visual_hash_algo, visual_embed_ref,
            created_at, updated_at
        FROM messages__legacy_drop_link
        """
    )
    cur.execute("DROP TABLE messages__legacy_drop_link")


def _finalize_messages_link_migration(
    cur: sqlite3.Cursor, strict_suffix: str
) -> None:
    if not _table_exists(cur, "messages__legacy_drop_link"):
        return

    logging.info("检测到 messages.link 删除迁移未完成，继续执行收尾")
    _drop_messages_compat_objects(cur)
    if not _table_exists(cur, "messages"):
        _create_messages_table(cur, strict_suffix)

    cur.execute(
        """
        INSERT OR IGNORE INTO messages(
            pk, chat_id, message_id, msg_date_text, msg_date_ts, sender_id,
            content, content_norm, pure_hash, dedupe_hash,
            msg_type, grouped_id, has_media,
            is_promo, promo_score, promo_reasons, dedupe_eligible, guard_reason, text_len,
            visual_hash, visual_hash_algo, visual_embed_ref,
            created_at, updated_at
        )
        SELECT
            pk, chat_id, message_id, msg_date_text, msg_date_ts, sender_id,
            content, content_norm, pure_hash, dedupe_hash,
            msg_type, grouped_id, has_media,
            is_promo, promo_score, promo_reasons, dedupe_eligible, guard_reason, text_len,
            visual_hash, visual_hash_algo, visual_embed_ref,
            created_at, updated_at
        FROM messages__legacy_drop_link
        """
    )
    cur.execute("DROP TABLE messages__legacy_drop_link")


def _ensure_messages_schema(cur: sqlite3.Cursor, feats: SqliteFeatures) -> None:
    strict_suffix = " STRICT" if feats.supports_strict else ""
    _drop_messages_compat_objects(cur)
    _finalize_messages_link_migration(cur, strict_suffix)
    _ensure_messages_runtime_columns(cur)

    if not _column_exists(cur, "messages", "link"):
        return

    logging.info("检测到废弃列 messages.link，开始执行迁移删除该列")
    _drop_messages_compat_objects(cur)
    try:
        cur.execute("ALTER TABLE messages DROP COLUMN link")
    except sqlite3.Error as exc:
        logging.warning(f"原生 DROP COLUMN 失败，回退到重建 messages 表: {exc}")
        _drop_messages_link_column_with_rebuild(cur, strict_suffix)


def _rebuild_message_media_table(cur: sqlite3.Cursor, strict_suffix: str) -> None:
    cur.execute("DROP TABLE IF EXISTS message_media__legacy_fk_fix")
    cur.execute("ALTER TABLE message_media RENAME TO message_media__legacy_fk_fix")
    _create_message_media_table(cur, strict_suffix)
    cur.execute(
        """
        INSERT INTO message_media(
            chat_id, message_id, media_kind, file_unique_id, file_name, file_ext,
            mime_type, file_size, width, height, duration_sec, grouped_id,
            media_fingerprint, meta_json, updated_at
        )
        SELECT
            chat_id, message_id, media_kind, file_unique_id, file_name, file_ext,
            mime_type, file_size, width, height, duration_sec, grouped_id,
            media_fingerprint, meta_json, updated_at
        FROM message_media__legacy_fk_fix
        """
    )
    cur.execute("DROP TABLE message_media__legacy_fk_fix")


def _ensure_message_media_schema(cur: sqlite3.Cursor, feats: SqliteFeatures) -> None:
    strict_suffix = " STRICT" if feats.supports_strict else ""
    _ensure_message_media_runtime_columns(cur)
    sql = _get_table_sql(cur, "message_media")
    if not sql:
        return
    if "messages__legacy_drop_link" not in sql:
        return
    logging.warning(
        "检测到 message_media 外键仍指向废弃表 messages__legacy_drop_link，开始修复表结构"
    )
    _rebuild_message_media_table(cur, strict_suffix)


def _refresh_chat_message_counts(
    cur: sqlite3.Cursor, chat_ids: Optional[Sequence[int]] = None
) -> None:
    if chat_ids is None:
        cur.execute(
            """
            UPDATE chats
            SET message_count = COALESCE((
                SELECT COUNT(*)
                FROM messages
                WHERE messages.chat_id = chats.chat_id
            ), 0)
            """
        )
        return

    normalized_chat_ids = sorted({int(chat_id) for chat_id in chat_ids})
    if not normalized_chat_ids:
        return

    placeholders = ",".join(["?"] * len(normalized_chat_ids))
    cur.execute(
        f"""
        UPDATE chats
        SET message_count = COALESCE((
            SELECT COUNT(*)
            FROM messages
            WHERE messages.chat_id = chats.chat_id
        ), 0)
        WHERE chat_id IN ({placeholders})
        """,
        normalized_chat_ids,
    )


@_db_runtime.synchronized_write
def create_schema(
    conn: sqlite3.Connection, feats: SqliteFeatures, force_heal_fts: int = 0
):
    cur = conn.cursor()
    try:
        strict_suffix = " STRICT" if feats.supports_strict else ""
        _create_tables(cur, strict_suffix)
        _ensure_chats_schema(cur)
        _ensure_messages_schema(cur, feats)
        _ensure_message_media_schema(cur, feats)
        _ensure_media_groups_runtime_columns(cur)
        _ensure_dedupe_schema(cur)
        _ensure_admin_job_schema(cur)
        _ensure_admin_missing_chats_schema(cur)
        _ensure_admin_absent_chats_schema(cur)
        _ensure_admin_restricted_chats_schema(cur)
        _ensure_chat_summary_columns(cur)
        _heal_chat_message_counts_if_needed(cur)
        _indexes._create_indexes(cur)
        _search_terms._create_message_search_terms_queue_triggers(cur)

        if feats.supports_fts5:
            cur.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='messages_fts'"
            )
            row = cur.fetchone()
            table_sql = (
                (row["sql"] if isinstance(row, sqlite3.Row) else row[0]) if row else ""
            )

            # 如果 FTS 表不存在或分词器不匹配，重建整个 FTS 系统
            if not table_sql or "trigram" not in table_sql.lower():
                logging.info("正在初始化或重建 FTS5 Trigram 索引表...")
                cur.execute("DROP TABLE IF EXISTS messages_fts")
                _fts._create_fts_schema(cur) # 调用这个会同时创建表和触发器
                _fts._sync_fts_from_scratch(cur)
            else:
                fts_triggers_current = _fts._fts_triggers_are_current(cur)
                # 确保触发器存在且内容为当前版本（旧库可能缺失或使用 content 而非 content_norm）。
                _fts._create_fts_triggers(cur)
                _fts._heal_fts_if_needed(
                    cur,
                    force_heal=(force_heal_fts == 1 or not fts_triggers_current),
                    rebuild_reason="FTS 触发器已升级" if not fts_triggers_current else "",
                )
        else:
            _fts._drop_fts_triggers(cur)

        _search_terms._heal_message_search_terms_if_needed(
            conn, force_heal=(force_heal_fts == 1)
        )

        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        cur.close()

@_db_runtime.synchronized_write
def refresh_chat_message_counts(
    conn: sqlite3.Connection, chat_ids: Optional[Iterable[int]] = None
) -> None:
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        _refresh_chat_message_counts(
            cur, None if chat_ids is None else list(chat_ids)
        )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        cur.close()
