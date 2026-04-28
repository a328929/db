# -*- coding: utf-8 -*-
import sqlite3
import logging
import re
from typing import Any, Iterable, Optional, Sequence, Tuple, List

from . import connection as _db_runtime
from .connection import SqliteFeatures


DB_WRITE_LOCK = _db_runtime.DB_WRITE_LOCK


def synchronized_write(func):
    def wrapper(*args, **kwargs):
        acquired = DB_WRITE_LOCK.acquire(timeout=15)
        if not acquired:
            logging.error(f"无法在 15s 内获取数据库写锁: {func.__name__}")
            raise RuntimeError("数据库忙，请稍后再试")
        try:
            return func(*args, **kwargs)
        finally:
            DB_WRITE_LOCK.release()

    return wrapper


def detect_sqlite_features(conn: sqlite3.Connection) -> SqliteFeatures:
    return _db_runtime.detect_sqlite_features(conn)


def resolve_db_path(raw_name: str) -> str:
    return _db_runtime.resolve_db_path(raw_name)


def connect_db(
    db_name: str, cache_mb: int = 256, mmap_mb: int = 512
) -> Tuple[sqlite3.Connection, SqliteFeatures]:
    return _db_runtime.connect_db(db_name, cache_mb=cache_mb, mmap_mb=mmap_mb)


def connect_configured_db(*, cfg: Optional[Any] = None) -> Tuple[sqlite3.Connection, SqliteFeatures]:
    runtime_cfg = cfg
    if runtime_cfg is None:
        from tg_harvest.config import CFG

        runtime_cfg = CFG
    return connect_db(
        str(runtime_cfg.db_name),
        cache_mb=int(runtime_cfg.sqlite_cache_mb),
        mmap_mb=int(runtime_cfg.sqlite_mmap_mb),
    )


def ensure_configured_db(
    *,
    cfg: Optional[Any] = None,
    force_heal_fts: Optional[int] = None,
) -> Tuple[sqlite3.Connection, SqliteFeatures]:
    runtime_cfg = cfg
    if runtime_cfg is None:
        from tg_harvest.config import CFG

        runtime_cfg = CFG
    conn, feats = connect_configured_db(cfg=runtime_cfg)
    create_schema(
        conn,
        feats,
        force_heal_fts=int(
            runtime_cfg.force_heal_fts if force_heal_fts is None else force_heal_fts
        ),
    )
    return conn, feats

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


def _column_exists(cur: sqlite3.Cursor, table_name: str, column_name: str) -> bool:
    cur.execute(f"PRAGMA table_info({table_name})")
    return any(str(row[1]) == column_name for row in cur.fetchall())


def _ensure_table_columns(
    cur: sqlite3.Cursor, table_name: str, column_defs: Sequence[Tuple[str, str]]
) -> None:
    if not _table_exists(cur, table_name):
        return
    for column_name, column_sql in column_defs:
        if _column_exists(cur, table_name, column_name):
            continue
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_sql}")


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


def _create_message_indexes(cur: sqlite3.Cursor):
    # 基础与主键引用索引
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_chat_id ON messages(chat_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_msg_id ON messages(message_id)")
    
    # 核心业务索引：按频道日期倒序排序（Web 列表主视图）
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_chat_date ON messages(chat_id, msg_date_ts DESC)")
    
    # 媒体组关联索引（支持相册视图）
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_grouped_id ON messages(chat_id, grouped_id) WHERE grouped_id IS NOT NULL")
    
    # 去重与内容标识索引
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_pure_hash ON messages(chat_id, pure_hash) WHERE pure_hash <> ''")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_dedupe_hash ON messages(chat_id, dedupe_hash) WHERE dedupe_hash <> ''")
    
    # 推广内容识别与排序索引
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_promo ON messages(chat_id, is_promo, promo_score DESC, msg_date_ts DESC)")
    
    # 发送者与消息类型聚合索引（支持筛选）
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(chat_id, sender_id, msg_date_ts DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_type ON messages(chat_id, msg_type, msg_date_ts DESC)")
    
    # 全局时间轴（用于跨频道搜索展示）
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_date_global ON messages(msg_date_ts DESC)")


def _create_media_indexes(cur: sqlite3.Cursor):
    # 媒体引用与文件唯一性索引
    cur.execute("CREATE INDEX IF NOT EXISTS idx_media_file_ref ON message_media(chat_id, message_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_media_unique_id ON message_media(chat_id, file_unique_id) WHERE file_unique_id IS NOT NULL AND file_unique_id <> ''")
    
    # 核心性能索引：文件指纹（用于跨频道去重检测）
    cur.execute("CREATE INDEX IF NOT EXISTS idx_media_fingerprint ON message_media(chat_id, media_fingerprint) WHERE media_fingerprint IS NOT NULL AND media_fingerprint <> ''")
    
    # 核心排序索引：按文件大小排序
    cur.execute("CREATE INDEX IF NOT EXISTS idx_media_sort_size ON message_media(chat_id, file_size DESC, message_id DESC)")
    
    # 核心排序索引：按媒体时长排序
    cur.execute("CREATE INDEX IF NOT EXISTS idx_media_sort_duration ON message_media(chat_id, duration_sec DESC, message_id DESC)")
    
    # 类型与元数据过滤索引
    cur.execute("CREATE INDEX IF NOT EXISTS idx_media_kind ON message_media(chat_id, media_kind)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_media_mime ON message_media(chat_id, mime_type)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_media_grouped_id ON message_media(chat_id, grouped_id) WHERE grouped_id IS NOT NULL")


def _create_media_group_indexes(cur: sqlite3.Cursor):
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mg_pure_hash ON media_groups(chat_id, pure_hash) WHERE pure_hash <> ''")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mg_media_sig ON media_groups(chat_id, media_sig_hash) WHERE media_sig_hash <> ''")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mg_dedupe_hash ON media_groups(chat_id, dedupe_hash) WHERE dedupe_hash <> ''")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mg_promo ON media_groups(chat_id, is_promo, item_count DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mg_time ON media_groups(chat_id, first_msg_date_ts DESC)")


def _create_dedupe_indexes(cur: sqlite3.Cursor):
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dedupe_runs_batch ON dedupe_runs(batch_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dedupe_actions_batch ON dedupe_actions(batch_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dedupe_actions_chat_time ON dedupe_actions(chat_id, created_at DESC)")


def _create_message_search_term_indexes(cur: sqlite3.Cursor):
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_message_search_terms_pk ON message_search_terms(pk)"
    )


def _create_admin_job_indexes(cur: sqlite3.Cursor):
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_admin_jobs_status_updated ON admin_jobs(status, updated_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_admin_jobs_target_chat ON admin_jobs(target_chat_id, status)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_admin_jobs_status_heartbeat ON admin_jobs(status, heartbeat_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_admin_job_logs_job_seq ON admin_job_logs(job_id, seq)"
    )


def _create_indexes(cur: sqlite3.Cursor):
    _create_message_indexes(cur)
    _create_media_indexes(cur)
    _create_media_group_indexes(cur)
    _create_dedupe_indexes(cur)
    _create_message_search_term_indexes(cur)
    _create_admin_job_indexes(cur)


def _create_fts_table(cur: sqlite3.Cursor):
    cur.execute("""
    CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts
    USING fts5(
        content,
        content='messages',
        content_rowid='pk',
        tokenize='trigram'
    )
    """)


def _drop_fts_triggers(cur: sqlite3.Cursor) -> None:
    for trigger_name in (
        "trg_messages_fts_insert",
        "trg_messages_fts_delete",
        "trg_messages_fts_update",
    ):
        cur.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")


def _create_fts_triggers(cur: sqlite3.Cursor):
    """
    重构 FTS5 触发器，确保数据在增删改时绝对同步。
    逻辑：同步内容时优先取标准化字段 content_norm。
    """
    _drop_fts_triggers(cur)

    # 1. 插入触发器
    cur.execute("""
    CREATE TRIGGER trg_messages_fts_insert AFTER INSERT ON messages BEGIN
        INSERT INTO messages_fts(rowid, content) 
        VALUES (new.pk, COALESCE(NULLIF(new.content_norm, ''), new.content, ''));
    END;
    """)

    # 2. 删除触发器
    cur.execute("""
    CREATE TRIGGER trg_messages_fts_delete AFTER DELETE ON messages BEGIN
        INSERT INTO messages_fts(messages_fts, rowid, content) 
        VALUES ('delete', old.pk, COALESCE(NULLIF(old.content_norm, ''), old.content, ''));
    END;
    """)
    
    # 3. 更新触发器（确保文案修改后，搜索索引同步更新）
    cur.execute("""
    CREATE TRIGGER trg_messages_fts_update AFTER UPDATE OF content, content_norm ON messages BEGIN
        INSERT INTO messages_fts(messages_fts, rowid, content) 
        VALUES ('delete', old.pk, COALESCE(NULLIF(old.content_norm, ''), old.content, ''));
        INSERT INTO messages_fts(rowid, content) 
        VALUES (new.pk, COALESCE(NULLIF(new.content_norm, ''), new.content, ''));
    END;
    """)


def _create_message_search_terms_queue_triggers(cur: sqlite3.Cursor):
    cur.execute("""
    CREATE TRIGGER IF NOT EXISTS trg_message_terms_queue_insert
    AFTER INSERT ON messages BEGIN
        INSERT INTO message_search_terms_rebuild_queue(pk, reason, queued_at)
        VALUES (new.pk, 'insert', datetime('now'))
        ON CONFLICT(pk) DO UPDATE SET
            reason = excluded.reason,
            queued_at = excluded.queued_at;
    END;
    """)

    cur.execute("""
    CREATE TRIGGER IF NOT EXISTS trg_message_terms_queue_update
    AFTER UPDATE OF content, content_norm ON messages BEGIN
        INSERT INTO message_search_terms_rebuild_queue(pk, reason, queued_at)
        VALUES (new.pk, 'update', datetime('now'))
        ON CONFLICT(pk) DO UPDATE SET
            reason = excluded.reason,
            queued_at = excluded.queued_at;
    END;
    """)

    cur.execute("""
    CREATE TRIGGER IF NOT EXISTS trg_message_terms_delete
    AFTER DELETE ON messages BEGIN
        DELETE FROM message_search_terms WHERE pk = old.pk;
        DELETE FROM message_search_terms_rebuild_queue WHERE pk = old.pk;
    END;
    """)


def _create_fts_schema(cur: sqlite3.Cursor):
    _create_fts_table(cur)
    _create_fts_triggers(cur)


def _count_message_search_terms(cur: sqlite3.Cursor) -> int:
    cur.execute("SELECT COUNT(*) AS c FROM message_search_terms")
    return int(cur.fetchone()["c"] or 0)


def _sync_fts_from_scratch(cur: sqlite3.Cursor):
    """从 messages 表全量同步数据到 FTS 表的内部工具函数"""
    cur.execute("DELETE FROM messages_fts")
    cur.execute("""
        INSERT INTO messages_fts(rowid, content)
        SELECT pk, COALESCE(NULLIF(content_norm, ''), content, '')
        FROM messages
    """)


_CJK_CHAR_RE = re.compile(
    r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\U00020000-\U0002ceaf]"
)


def extract_cjk_bigrams(text: str) -> List[str]:
    raw = str(text or "")
    compact = "".join(raw.split())
    if len(compact) < 2:
        return []

    seen = set()
    out: List[str] = []
    for idx in range(len(compact) - 1):
        token = compact[idx : idx + 2]
        if len(token) != 2:
            continue
        if not (_CJK_CHAR_RE.fullmatch(token[0]) and _CJK_CHAR_RE.fullmatch(token[1])):
            continue
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def extract_cjk_search_terms(text: str) -> List[str]:
    raw = str(text or "")
    compact = "".join(raw.split())
    if not compact:
        return []

    seen = set()
    out: List[str] = []

    for ch in compact:
        if not _CJK_CHAR_RE.fullmatch(ch):
            continue
        if ch in seen:
            continue
        seen.add(ch)
        out.append(ch)

    for token in extract_cjk_bigrams(compact):
        if token in seen:
            continue
        seen.add(token)
        out.append(token)

    return out


def _extract_cjk_unigrams(text: str) -> List[str]:
    return [term for term in extract_cjk_search_terms(text) if len(term) == 1]


_MESSAGE_SEARCH_TERMS_VERSION_KEY = "cjk_terms_version"
_MESSAGE_SEARCH_TERMS_VERSION = "2"
_MESSAGE_SEARCH_TERMS_BACKFILL_MODE_KEY = "cjk_terms_backfill_mode"
_MESSAGE_SEARCH_TERMS_BACKFILL_LAST_PK_KEY = "cjk_terms_backfill_last_pk"


def _read_message_search_terms_version(cur: sqlite3.Cursor) -> str:
    if not _table_exists(cur, "message_search_terms_meta"):
        return ""
    cur.execute(
        "SELECT value FROM message_search_terms_meta WHERE key = ? LIMIT 1",
        (_MESSAGE_SEARCH_TERMS_VERSION_KEY,),
    )
    row = cur.fetchone()
    if row is None:
        return ""
    return str(row["value"] if isinstance(row, sqlite3.Row) else row[0] or "")


def _write_message_search_terms_version(cur: sqlite3.Cursor) -> None:
    cur.execute(
        """
        INSERT INTO message_search_terms_meta(key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (_MESSAGE_SEARCH_TERMS_VERSION_KEY, _MESSAGE_SEARCH_TERMS_VERSION),
    )


def _read_message_search_terms_meta(cur: sqlite3.Cursor, key: str) -> str:
    if not _table_exists(cur, "message_search_terms_meta"):
        return ""
    cur.execute("SELECT value FROM message_search_terms_meta WHERE key = ? LIMIT 1", (key,))
    row = cur.fetchone()
    if row is None:
        return ""
    return str(row["value"] if isinstance(row, sqlite3.Row) else row[0] or "")


def _write_message_search_terms_meta(
    cur: sqlite3.Cursor, key: str, value: str
) -> None:
    cur.execute(
        """
        INSERT INTO message_search_terms_meta(key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )


def _delete_message_search_terms_meta(cur: sqlite3.Cursor, key: str) -> None:
    if not _table_exists(cur, "message_search_terms_meta"):
        return
    cur.execute("DELETE FROM message_search_terms_meta WHERE key = ?", (key,))


def _mark_message_search_terms_backfill(cur: sqlite3.Cursor, *, mode: str) -> None:
    _write_message_search_terms_meta(
        cur, _MESSAGE_SEARCH_TERMS_BACKFILL_MODE_KEY, mode
    )
    _write_message_search_terms_meta(
        cur, _MESSAGE_SEARCH_TERMS_BACKFILL_LAST_PK_KEY, "0"
    )


def message_search_terms_are_current(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()
    try:
        return _read_message_search_terms_version(cur) == _MESSAGE_SEARCH_TERMS_VERSION
    finally:
        cur.close()


def _sync_message_search_terms_from_scratch(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM message_search_terms")

        last_pk = 0
        batch_size = 5000
        while True:
            cur.execute(
                """
                SELECT pk, COALESCE(NULLIF(content_norm, ''), content, '') AS search_text
                FROM messages
                WHERE pk > ?
                ORDER BY pk ASC
                LIMIT ?
                """,
                (last_pk, batch_size),
            )
            rows = cur.fetchall()
            if not rows:
                break

            inserts: List[Tuple[str, int]] = []
            for row in rows:
                pk = int(row["pk"])
                last_pk = pk
                for token in extract_cjk_search_terms(str(row["search_text"] or "")):
                    inserts.append((token, pk))

            if inserts:
                cur.executemany(
                    "INSERT OR IGNORE INTO message_search_terms(term, pk) VALUES (?, ?)",
                    inserts,
                )
            conn.commit()
        _write_message_search_terms_version(cur)
        conn.commit()
    finally:
        cur.close()


def _backfill_message_search_term_unigrams(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    try:
        last_pk = 0
        batch_size = 5000
        while True:
            cur.execute(
                """
                SELECT pk, COALESCE(NULLIF(content_norm, ''), content, '') AS search_text
                FROM messages
                WHERE pk > ?
                ORDER BY pk ASC
                LIMIT ?
                """,
                (last_pk, batch_size),
            )
            rows = cur.fetchall()
            if not rows:
                break

            inserts: List[Tuple[str, int]] = []
            for row in rows:
                pk = int(row["pk"])
                last_pk = pk
                for token in _extract_cjk_unigrams(str(row["search_text"] or "")):
                    inserts.append((token, pk))

            if inserts:
                cur.executemany(
                    "INSERT OR IGNORE INTO message_search_terms(term, pk) VALUES (?, ?)",
                    inserts,
                )
            conn.commit()

        _write_message_search_terms_version(cur)
        conn.commit()
    finally:
        cur.close()


@synchronized_write
def backfill_message_search_terms_upgrade_batch(
    conn: sqlite3.Connection, *, batch_size: int = 5000
) -> int:
    cur = conn.cursor()
    try:
        if not _table_exists(cur, "message_search_terms_meta"):
            return 0

        mode = _read_message_search_terms_meta(
            cur, _MESSAGE_SEARCH_TERMS_BACKFILL_MODE_KEY
        )
        if mode not in {"full", "unigram"}:
            return 0

        raw_last_pk = _read_message_search_terms_meta(
            cur, _MESSAGE_SEARCH_TERMS_BACKFILL_LAST_PK_KEY
        )
        try:
            last_pk = max(0, int(raw_last_pk or "0"))
        except ValueError:
            last_pk = 0

        cur.execute("BEGIN IMMEDIATE")
        if mode == "full" and last_pk == 0:
            cur.execute("DELETE FROM message_search_terms")

        cur.execute(
            """
            SELECT pk, COALESCE(NULLIF(content_norm, ''), content, '') AS search_text
            FROM messages
            WHERE pk > ?
            ORDER BY pk ASC
            LIMIT ?
            """,
            (last_pk, max(1, int(batch_size))),
        )
        rows = cur.fetchall()
        if not rows:
            _write_message_search_terms_version(cur)
            _delete_message_search_terms_meta(cur, _MESSAGE_SEARCH_TERMS_BACKFILL_MODE_KEY)
            _delete_message_search_terms_meta(
                cur, _MESSAGE_SEARCH_TERMS_BACKFILL_LAST_PK_KEY
            )
            conn.commit()
            return 0

        inserts: List[Tuple[str, int]] = []
        next_last_pk = last_pk
        for row in rows:
            pk = int(row["pk"])
            next_last_pk = pk
            search_text = str(row["search_text"] or "")
            tokens = (
                extract_cjk_search_terms(search_text)
                if mode == "full"
                else _extract_cjk_unigrams(search_text)
            )
            for token in tokens:
                inserts.append((token, pk))

        if inserts:
            cur.executemany(
                "INSERT OR IGNORE INTO message_search_terms(term, pk) VALUES (?, ?)",
                inserts,
            )
        _write_message_search_terms_meta(
            cur, _MESSAGE_SEARCH_TERMS_BACKFILL_LAST_PK_KEY, str(next_last_pk)
        )
        conn.commit()
        return len(rows)
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        cur.close()


def _heal_message_search_terms_if_needed(
    conn: sqlite3.Connection, *, force_heal: bool = False
) -> None:
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM message_search_terms LIMIT 1")
        has_data = cur.fetchone() is not None
        cur.execute("SELECT 1 FROM messages LIMIT 1")
        has_messages = cur.fetchone() is not None
        index_version = _read_message_search_terms_version(cur)
    finally:
        cur.close()

    if not has_messages:
        cur = conn.cursor()
        try:
            _write_message_search_terms_version(cur)
            conn.commit()
        finally:
            cur.close()
        return
    if (
        has_data
        and not force_heal
        and index_version == _MESSAGE_SEARCH_TERMS_VERSION
    ):
        return

    if force_heal:
        logging.warning("配置强制开启中文短词辅助索引后台修复...")
        mode = "full"
    elif has_data:
        logging.info("检测到中文短词辅助索引版本过旧，已安排后台升级单字索引")
        mode = "unigram"
    else:
        logging.info("检测到中文短词辅助索引为空，已安排后台首次同步")
        mode = "full"

    cur = conn.cursor()
    try:
        _mark_message_search_terms_backfill(cur, mode=mode)
        conn.commit()
    finally:
        cur.close()


@synchronized_write
def drain_message_search_terms_rebuild_queue(
    conn: sqlite3.Connection, *, batch_size: int = 5000
) -> int:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='message_search_terms_rebuild_queue' LIMIT 1"
        )
        if cur.fetchone() is None:
            return 0

        cur.execute(
            "SELECT pk FROM message_search_terms_rebuild_queue ORDER BY queued_at ASC, pk ASC LIMIT ?",
            (max(1, int(batch_size)),),
        )
        queued_rows = cur.fetchall()
        if not queued_rows:
            return 0

        pks = [int(row["pk"]) for row in queued_rows]
        placeholders = ",".join(["?"] * len(pks))

        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            f"""
            SELECT pk, COALESCE(NULLIF(content_norm, ''), content, '') AS search_text
            FROM messages
            WHERE pk IN ({placeholders})
            """,
            pks,
        )
        rows = cur.fetchall()

        cur.execute(
            f"DELETE FROM message_search_terms WHERE pk IN ({placeholders})",
            pks,
        )

        inserts: List[Tuple[str, int]] = []
        for row in rows:
            pk = int(row["pk"])
            for token in extract_cjk_search_terms(str(row["search_text"] or "")):
                inserts.append((token, pk))
        if inserts:
            cur.executemany(
                "INSERT OR IGNORE INTO message_search_terms(term, pk) VALUES (?, ?)",
                inserts,
            )

        cur.execute(
            f"DELETE FROM message_search_terms_rebuild_queue WHERE pk IN ({placeholders})",
            pks,
        )
        conn.commit()
        return len(pks)
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        cur.close()


def _heal_fts_if_needed(cur: sqlite3.Cursor, force_heal: bool = False):
    """
    优化后的 FTS 检查逻辑：
    不再每次启动都执行耗时的全表 COUNT(*)。
    仅在 FTS 表完全为空，或明确设置了 force_heal=True 时才启动全量同步。
    """
    try:
        # 1. 快速检查 FTS 是否为空
        cur.execute("SELECT 1 FROM messages_fts LIMIT 1")
        has_data = cur.fetchone() is not None

        if not has_data or force_heal:
            if force_heal:
                logging.warning("配置强制开启 FTS 索引修复...")
            else:
                logging.info(
                    "检测到 FTS 索引为空，正在执行首次同步（大数据库可能耗时几秒）..."
                )

            _sync_fts_from_scratch(cur)
            logging.info("FTS 索引同步成功完成")
        else:
            # 正常启动，仅做跳过提示（不消耗 I/O）
            logging.debug("FTS 索引已存在，跳过耗时的全量计数校验")

    except sqlite3.Error as e:
        logging.error(f"FTS 检查阶段遇到数据库错误: {e}")


@synchronized_write
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
        _ensure_chat_summary_columns(cur)
        _heal_chat_message_counts_if_needed(cur)
        _create_indexes(cur)
        _create_message_search_terms_queue_triggers(cur)

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
                _create_fts_schema(cur) # 调用这个会同时创建表和触发器
                _sync_fts_from_scratch(cur)
            else:
                _create_fts_triggers(cur) # 确保触发器存在（如果是旧版本数据库可能没创建）
                _heal_fts_if_needed(cur, force_heal=(force_heal_fts == 1))
        else:
            _drop_fts_triggers(cur)

        _heal_message_search_terms_if_needed(
            conn, force_heal=(force_heal_fts == 1)
        )

        conn.commit()
    finally:
        cur.close()

@synchronized_write
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
