# -*- coding: utf-8 -*-
import sqlite3
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import Tuple

# =========================
# SQLite 连接 / 能力检测
# =========================


@dataclass
class SqliteFeatures:
    version_str: str
    version_tuple: Tuple[int, int, int]
    supports_strict: bool
    supports_fts5: bool


def parse_version(v: str) -> Tuple[int, int, int]:
    try:
        p = v.split(".")
        return (int(p[0]), int(p[1]), int(p[2]))
    except Exception:
        return (0, 0, 0)


def _read_sqlite_version(cur: sqlite3.Cursor) -> str:
    cur.execute("SELECT sqlite_version() AS v")
    row = cur.fetchone()
    # 兼容 row_factory
    return row["v"] if isinstance(row, sqlite3.Row) else row[0]


def _detect_fts5_support(cur: sqlite3.Cursor) -> bool:
    supports_fts5 = False
    try:
        cur.execute("PRAGMA compile_options;")
        # fetchall 返回的是 tuple list，如 [('ENABLE_FTS5',), ...]
        opts = {str(r[0]) for r in cur.fetchall()}
        supports_fts5 = any("ENABLE_FTS5" in x for x in opts)
    except Exception:
        try:
            cur.execute("CREATE VIRTUAL TABLE IF NOT EXISTS __fts5_probe USING fts5(x)")
            cur.execute("DROP TABLE IF EXISTS __fts5_probe")
            supports_fts5 = True
        except Exception:
            supports_fts5 = False
    return supports_fts5


def detect_sqlite_features(conn: sqlite3.Connection) -> SqliteFeatures:
    cur = conn.cursor()
    try:
        v = _read_sqlite_version(cur)
        vt = parse_version(v)
        supports_strict = vt >= (3, 37, 0)
        supports_fts5 = _detect_fts5_support(cur)
        return SqliteFeatures(v, vt, supports_strict, supports_fts5)
    finally:
        cur.close()



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
        link                 TEXT,
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


def _create_tables(cur: sqlite3.Cursor, strict_suffix: str):
    _create_chats_table(cur, strict_suffix)
    _create_messages_table(cur, strict_suffix)
    _create_message_media_table(cur, strict_suffix)
    _create_media_groups_table(cur, strict_suffix)
    _create_dedupe_tables(cur, strict_suffix)


def _create_message_indexes(cur: sqlite3.Cursor):
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_chat_date ON messages(chat_id, msg_date_ts DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_chat_msgid ON messages(chat_id, message_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_grouped ON messages(chat_id, grouped_id) WHERE grouped_id IS NOT NULL")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_hash ON messages(chat_id, pure_hash) WHERE pure_hash <> ''")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_dedupe_hash ON messages(chat_id, dedupe_hash) WHERE dedupe_hash <> ''")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_promo ON messages(chat_id, is_promo, dedupe_eligible, grouped_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(chat_id, sender_id, msg_date_ts DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_type_date ON messages(chat_id, msg_type, msg_date_ts DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_score ON messages(chat_id, promo_score DESC, msg_date_ts DESC)")


def _create_media_indexes(cur: sqlite3.Cursor):
    cur.execute("CREATE INDEX IF NOT EXISTS idx_media_fileid ON message_media(chat_id, file_unique_id) WHERE file_unique_id IS NOT NULL AND file_unique_id <> ''")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_media_fingerprint ON message_media(chat_id, media_fingerprint) WHERE media_fingerprint IS NOT NULL AND media_fingerprint <> ''")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_media_kind ON message_media(chat_id, media_kind)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_media_size ON message_media(chat_id, file_size)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_media_mime ON message_media(chat_id, mime_type)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_media_grouped ON message_media(chat_id, grouped_id) WHERE grouped_id IS NOT NULL")


def _create_media_group_indexes(cur: sqlite3.Cursor):
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mg_hash ON media_groups(chat_id, pure_hash) WHERE pure_hash <> ''")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mg_media_sig ON media_groups(chat_id, media_sig_hash) WHERE media_sig_hash <> ''")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mg_dedupe_hash ON media_groups(chat_id, dedupe_hash) WHERE dedupe_hash <> ''")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mg_promo ON media_groups(chat_id, is_promo, dedupe_eligible, item_count)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mg_time ON media_groups(chat_id, first_msg_date_ts DESC)")


def _create_dedupe_indexes(cur: sqlite3.Cursor):
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dedupe_actions_batch ON dedupe_actions(batch_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dedupe_actions_chat_time ON dedupe_actions(chat_id, created_at DESC)")


def _create_indexes(cur: sqlite3.Cursor):
    _create_message_indexes(cur)
    _create_media_indexes(cur)
    _create_media_group_indexes(cur)
    _create_dedupe_indexes(cur)


def _create_views(cur: sqlite3.Cursor):
    # v_messages_enriched 视图已停用；保留该函数作为 schema 调用链兼容点，便于未来扩展。
    return


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


def _create_fts_triggers(cur: sqlite3.Cursor):
    cur.execute("""
    CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
        INSERT INTO messages_fts(rowid, content) VALUES (new.pk, COALESCE(NULLIF(new.content_norm, ''), new.content, ''));
    END;
    """)
    cur.execute("""
    CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN
        INSERT INTO messages_fts(messages_fts, rowid, content) VALUES ('delete', old.pk, COALESCE(NULLIF(old.content_norm, ''), old.content, ''));
    END;
    """)
    cur.execute("""
    CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE OF content, content_norm ON messages BEGIN
        INSERT INTO messages_fts(messages_fts, rowid, content) VALUES ('delete', old.pk, COALESCE(NULLIF(old.content_norm, ''), old.content, ''));
        INSERT INTO messages_fts(rowid, content) VALUES (new.pk, COALESCE(NULLIF(new.content_norm, ''), new.content, ''));
    END;
    """)


def _create_fts_schema(cur: sqlite3.Cursor):
    _create_fts_table(cur)
    _create_fts_triggers(cur)


def _count_messages(cur: sqlite3.Cursor) -> int:
    cur.execute("SELECT COUNT(*) AS c FROM messages")
    return int(cur.fetchone()["c"] or 0)


def _count_messages_fts(cur: sqlite3.Cursor) -> int:
    cur.execute("SELECT COUNT(*) AS c FROM messages_fts")
    return int(cur.fetchone()["c"] or 0)


def _rebuild_messages_fts(cur: sqlite3.Cursor):
    cur.execute("DELETE FROM messages_fts")
    cur.execute("""
    INSERT INTO messages_fts(rowid, content)
    SELECT pk, COALESCE(NULLIF(content_norm, ''), content, '')
    FROM messages
    """)


def _heal_fts_if_needed(cur: sqlite3.Cursor):
    # ----------------------------------------------------------------------
    # P0 级优化：增强 FTS 自愈逻辑
    # 旧逻辑：仅当 messages_fts count=0 时才 rebuild
    # 新逻辑：检查行数是否一致，不一致则 rebuild
    # ----------------------------------------------------------------------
    total_msgs = _count_messages(cur)

    if total_msgs > 0:
        total_fts = _count_messages_fts(cur)

        # 如果偏差超过一定比例（比如 > 0）或 FTS 为 0，就应当修复
        # 为稳健起见，只要不等就 rebuild，因为 FTS 必须准确
        if total_fts != total_msgs:
            logging.warning(f"检测到 FTS 索引不一致 (messages={total_msgs}, fts={total_fts})，正在重建 FTS...")
            _rebuild_messages_fts(cur)
            logging.info("FTS 重建完成")
        else:
            # 即使数量相等，也要防范 content 不一致的情况（虽然比较少见，但数量检查是最快的第一道防线）
            # 深度检查太慢，暂不执行
            pass


def _create_fts_if_supported(cur: sqlite3.Cursor, feats: SqliteFeatures):
    if not feats.supports_fts5:
        return
    _create_fts_schema(cur)
    _heal_fts_if_needed(cur)



def _create_core_schema(cur: sqlite3.Cursor, strict_suffix: str):
    _create_tables(cur, strict_suffix)
    _create_indexes(cur)
    _create_views(cur)


def _create_optional_schema(cur: sqlite3.Cursor, feats: SqliteFeatures):
    _create_fts_if_supported(cur, feats)


def create_schema(conn: sqlite3.Connection, feats: SqliteFeatures):
    cur = conn.cursor()
    try:
        strict_suffix = " STRICT" if feats.supports_strict else ""
        _create_core_schema(cur, strict_suffix)
        if feats.supports_fts5:
            cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='messages_fts'")
            row = cur.fetchone()
            table_sql = (row["sql"] if isinstance(row, sqlite3.Row) else row[0]) if row else ""
            if table_sql and "trigram" not in table_sql.lower():
                cur.execute("DROP TABLE messages_fts")
                _create_fts_table(cur)
                cur.execute("""
                INSERT INTO messages_fts(rowid, content)
                SELECT pk, COALESCE(NULLIF(content_norm, ''), content, '')
                FROM messages
                """)
        _create_optional_schema(cur, feats)
        conn.commit()
    finally:
        cur.close()

# =========================
# Public API
# =========================


def resolve_db_path(raw_name: str) -> str:
    """
    解析数据库路径，默认为项目根目录下的 tg_data.db
    """
    p = Path(raw_name or "tg_data.db")
    if p.is_absolute():
        return str(p)
    # 假设本文件在 tg_harvest/db.py，则 parent.parent 为项目根目录
    return str((Path(__file__).resolve().parent.parent / p).resolve())


def _open_connection(db_name: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_name, timeout=60)  # 增加默认超时
    conn.row_factory = sqlite3.Row
    return conn


def _apply_core_pragmas(cur: sqlite3.Cursor):
    # 性能 / 稳定性（大群友好）
    # 关键优化：WAL 模式防止读写阻塞
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA foreign_keys=ON;")
    cur.execute("PRAGMA cache_size=-64000;")  # ~64MB
    cur.execute("PRAGMA busy_timeout=10000;")  # 10s (Web端并发关键)
    cur.execute("PRAGMA wal_autocheckpoint=1000;")  # 稍微频繁一点 checkpoint


def _apply_optional_pragmas(cur: sqlite3.Cursor):
    try:
        cur.execute("PRAGMA mmap_size=268435456;")  # 256MB
    except Exception:
        pass
    try:
        cur.execute("PRAGMA journal_size_limit=67108864;")  # 64MB
    except Exception:
        pass


def _apply_pragmas(conn: sqlite3.Connection):
    cur = conn.cursor()
    try:
        _apply_core_pragmas(cur)
        _apply_optional_pragmas(cur)
    finally:
        cur.close()


def _load_sqlite_features(conn: sqlite3.Connection) -> SqliteFeatures:
    return detect_sqlite_features(conn)


def connect_db(db_name: str) -> Tuple[sqlite3.Connection, SqliteFeatures]:
    """
    统一的数据库连接入口，应用最佳实践配置（WAL, mmap, etc.）
    """
    conn = _open_connection(db_name)
    _apply_pragmas(conn)
    feats = _load_sqlite_features(conn)
    # logging.info(f"SQLite={feats.version_str} | STRICT={feats.supports_strict} | FTS5={feats.supports_fts5}")
    return conn, feats
