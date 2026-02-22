# -*- coding: utf-8 -*-
import os
import re
import html
import sqlite3
import time
import json
import hashlib
import logging
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any, Iterable, Set

from telethon.sync import TelegramClient
from telethon.errors import FloodWaitError, RPCError


# =========================
# 配置区（支持环境变量覆盖）
# =========================

def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name, None)
    return (v if v is not None else default).strip()


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name, None)
    if v is None:
        return int(default)
    try:
        return int(v.strip())
    except Exception:
        return int(default)


def _resolve_db_path(raw_name: str) -> str:
    p = Path(raw_name or "tg_data.db")
    if p.is_absolute():
        return str(p)
    return str((Path(__file__).resolve().parent.parent / p).resolve())


@dataclass
class AppConfig:
    api_id: int
    api_hash: str
    session_name: str

    db_name: str
    target_group: str
    scan_existing_chats: int

    dedup_mode: str
    dedup_threshold: int

    batch_size: int
    rescan_tail_ids: int
    media_caption_guard_len: int
    promo_score_threshold: int
    log_every: int

    @classmethod
    def load(cls) -> "AppConfig":
        return cls(
            api_id=_env_int("TG_API_ID", 2040),
            api_hash=_env_str("TG_API_HASH", "b18441a1ff607e10a989891a5462e627"),
            session_name=_env_str("TG_SESSION_NAME", "my_session"),

            db_name=_resolve_db_path(_env_str("TG_DB_NAME", "tg_data.db")),
            target_group=_env_str("TG_TARGET_GROUP", "顶级萝莉内部群"),
            scan_existing_chats=_env_int("TG_SCAN_DB_CHATS", 0),

            dedup_mode=_env_str("TG_DEDUP_MODE", "PURGE_ALL").upper(),
            dedup_threshold=_env_int("TG_DEDUP_THRESHOLD", 2),

            batch_size=_env_int("TG_BATCH_SIZE", 1000),
            rescan_tail_ids=_env_int("TG_RESCAN_TAIL_IDS", 1000),
            media_caption_guard_len=_env_int("TG_MEDIA_CAPTION_GUARD_LEN", 58),
            promo_score_threshold=_env_int("TG_PROMO_SCORE_THRESHOLD", 3),
            log_every=_env_int("TG_LOG_EVERY", 1000),
        )


CFG = AppConfig.load()


def _is_enabled(v: int) -> bool:
    return int(v) == 1


# =========================
# 引流关键词 / 强特征（可继续扩充）
# =========================

PROMO_KEYWORDS = [
    # 中文
    "防失联", "失联", "备用", "车队", "备份群", "满足你的", "备用群", "新群", "新频道", "频道", "频道号", "频道链接",
    "永久地址", "最新地址", "发布页", "导航", "进群", "加群", "拉群", "群主",
    "私聊", "联系", "引擎", "联系方式", "联系客服", "客服", "商务", "酒馆", "体验", "商务合作", "合作", "代理", "推广",
    "资源群", "福利群", "免费进群", "搜索", "入口", "加我", "找我", "金品", "咨询", "群里见", "搜群",
    "飞机", "电报", "纸飞机", "telegram", "tg", "channel", "group", "join", "contact",
    "wechat", "whatsapp", "business", "support",
    "微信", "微 信", "vx", "wx", "QQ", "qq", "q群", "qq群", "飞机号", "频道号",
    "JISOU帮你精准找到",
]

HARD_PROMO_MARKERS = [
    "t.me/", "telegram.me", "joinchat", "/+",
    "私聊", "联系客服", "联系我", "加群", "进群",
    "vx", "wx", "微信", "wechat", "qq", "@", "频道链接", "发布页", "导航", "jiso",
]

CTA_WORDS = [
    "点击", "加入", "进群", "加群", "私信", "联系我", "车队", "酒馆", "体验", "联系客服", "搜索", "满足你的", "引擎", "合作",
    "进入", "JISOU帮你精准找到", "查看", "金品", "复制", "搜索", "加我", "咨询", "订阅", "关注", "打开", "扫码",
]

# 用于“压缩串”匹配（去掉分隔符后的关键词）
PROMO_KEYWORDS_COMPACT = sorted({re.sub(r"[\W_]+", "", unicodedata.normalize("NFKC", k).lower()) for k in PROMO_KEYWORDS if k.strip()})
CTA_WORDS_COMPACT = sorted({re.sub(r"[\W_]+", "", unicodedata.normalize("NFKC", k).lower()) for k in CTA_WORDS if k.strip()})


@dataclass
class SqliteFeatures:
    version_str: str
    version_tuple: Tuple[int, int, int]
    supports_strict: bool
    supports_fts5: bool


# =========================
# 日志
# =========================

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )


# =========================
# SQLite 连接 / 能力检测
# =========================

def parse_version(v: str) -> Tuple[int, int, int]:
    try:
        p = v.split(".")
        return (int(p[0]), int(p[1]), int(p[2]))
    except Exception:
        return (0, 0, 0)


def detect_sqlite_features(conn: sqlite3.Connection) -> SqliteFeatures:
    cur = conn.cursor()
    cur.execute("SELECT sqlite_version() AS v")
    v = cur.fetchone()["v"]
    vt = parse_version(v)

    supports_strict = vt >= (3, 37, 0)

    supports_fts5 = False
    try:
        cur.execute("PRAGMA compile_options;")
        opts = {str(r[0]) for r in cur.fetchall()}
        supports_fts5 = any("ENABLE_FTS5" in x for x in opts)
    except Exception:
        try:
            cur.execute("CREATE VIRTUAL TABLE IF NOT EXISTS __fts5_probe USING fts5(x)")
            cur.execute("DROP TABLE IF EXISTS __fts5_probe")
            supports_fts5 = True
        except Exception:
            supports_fts5 = False

    return SqliteFeatures(v, vt, supports_strict, supports_fts5)


def connect_db(db_name: str) -> Tuple[sqlite3.Connection, SqliteFeatures]:
    conn = sqlite3.connect(db_name, timeout=30)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 性能 / 稳定性（大群友好）
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA foreign_keys=ON;")
    cur.execute("PRAGMA cache_size=-64000;")          # ~64MB
    cur.execute("PRAGMA busy_timeout=5000;")
    cur.execute("PRAGMA wal_autocheckpoint=2000;")

    try:
        cur.execute("PRAGMA mmap_size=268435456;")    # 256MB
    except Exception:
        pass
    try:
        cur.execute("PRAGMA journal_size_limit=67108864;")  # 64MB
    except Exception:
        pass

    feats = detect_sqlite_features(conn)
    logging.info(f"SQLite={feats.version_str} | STRICT={feats.supports_strict} | FTS5={feats.supports_fts5}")
    return conn, feats


# =========================
# Schema / 迁移
# =========================

def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (name,))
    return cur.fetchone() is not None


def get_table_columns(conn: sqlite3.Connection, table_name: str) -> Set[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    return {row["name"] for row in cur.fetchall()}


def ensure_columns(conn: sqlite3.Connection, table_name: str, cols: Dict[str, str]):
    """
    轻量迁移：缺列就补，避免老库直接炸。
    cols: {"col_name": "TEXT NOT NULL DEFAULT ''", ...}
    """
    if not table_exists(conn, table_name):
        return
    existing = get_table_columns(conn, table_name)
    cur = conn.cursor()
    for name, ddl in cols.items():
        if name not in existing:
            logging.info(f"迁移: ALTER TABLE {table_name} ADD COLUMN {name}")
            cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {name} {ddl}")
    conn.commit()


def create_schema(conn: sqlite3.Connection, feats: SqliteFeatures):
    cur = conn.cursor()
    strict_suffix = " STRICT" if feats.supports_strict else ""

    # 聊天信息
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

    # 消息主表（加入 dedupe_hash）
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

    # 媒体元信息（1:1）
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

    # 媒体组聚合（加入 media_sig_hash / dedupe_hash）
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

    # 去重任务审计
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

    conn.commit()

    # 轻量补列迁移（老库升级）
    ensure_columns(conn, "messages", {
        "content_norm": "TEXT",
        "pure_hash": "TEXT",
        "dedupe_hash": "TEXT",
        "is_promo": "INTEGER NOT NULL DEFAULT 0",
        "promo_score": "INTEGER NOT NULL DEFAULT 0",
        "promo_reasons": "TEXT",
        "dedupe_eligible": "INTEGER NOT NULL DEFAULT 0",
        "guard_reason": "TEXT",
        "text_len": "INTEGER NOT NULL DEFAULT 0",
        "visual_hash": "TEXT",
        "visual_hash_algo": "TEXT",
        "visual_embed_ref": "TEXT",
        "updated_at": "TEXT NOT NULL DEFAULT (datetime('now'))",
    })
    ensure_columns(conn, "message_media", {
        "media_fingerprint": "TEXT",
        "meta_json": "TEXT",
        "updated_at": "TEXT NOT NULL DEFAULT (datetime('now'))",
    })
    ensure_columns(conn, "media_groups", {
        "caption_norm": "TEXT",
        "pure_hash": "TEXT",
        "media_sig_hash": "TEXT",
        "dedupe_hash": "TEXT",
        "is_promo": "INTEGER NOT NULL DEFAULT 0",
        "promo_score": "INTEGER NOT NULL DEFAULT 0",
        "promo_reasons": "TEXT",
        "dedupe_eligible": "INTEGER NOT NULL DEFAULT 0",
        "guard_reason": "TEXT",
        "active_items": "INTEGER NOT NULL DEFAULT 0",
        "updated_at": "TEXT NOT NULL DEFAULT (datetime('now'))",
    })
    ensure_columns(conn, "dedupe_runs", {
        "dup_hash_count_group_txt": "INTEGER NOT NULL DEFAULT 0",
        "dup_hash_count_group_med": "INTEGER NOT NULL DEFAULT 0",
    })
    ensure_columns(conn, "dedupe_actions", {
        "dedupe_hash": "TEXT",
        "pure_hash": "TEXT",
    })

    cur = conn.cursor()

    # 索引
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_chat_date ON messages(chat_id, msg_date_ts DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_chat_msgid ON messages(chat_id, message_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_grouped ON messages(chat_id, grouped_id) WHERE grouped_id IS NOT NULL")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_hash ON messages(chat_id, pure_hash) WHERE pure_hash <> ''")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_dedupe_hash ON messages(chat_id, dedupe_hash) WHERE dedupe_hash <> ''")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_promo ON messages(chat_id, is_promo, dedupe_eligible, grouped_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_sender ON messages(chat_id, sender_id, msg_date_ts DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_type_date ON messages(chat_id, msg_type, msg_date_ts DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_score ON messages(chat_id, promo_score DESC, msg_date_ts DESC)")

    cur.execute("CREATE INDEX IF NOT EXISTS idx_media_fileid ON message_media(chat_id, file_unique_id) WHERE file_unique_id IS NOT NULL AND file_unique_id <> ''")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_media_fingerprint ON message_media(chat_id, media_fingerprint) WHERE media_fingerprint IS NOT NULL AND media_fingerprint <> ''")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_media_kind ON message_media(chat_id, media_kind)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_media_size ON message_media(chat_id, file_size)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_media_mime ON message_media(chat_id, mime_type)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_media_grouped ON message_media(chat_id, grouped_id) WHERE grouped_id IS NOT NULL")

    cur.execute("CREATE INDEX IF NOT EXISTS idx_mg_hash ON media_groups(chat_id, pure_hash) WHERE pure_hash <> ''")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mg_media_sig ON media_groups(chat_id, media_sig_hash) WHERE media_sig_hash <> ''")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mg_dedupe_hash ON media_groups(chat_id, dedupe_hash) WHERE dedupe_hash <> ''")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mg_promo ON media_groups(chat_id, is_promo, dedupe_eligible, item_count)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mg_time ON media_groups(chat_id, first_msg_date_ts DESC)")

    cur.execute("CREATE INDEX IF NOT EXISTS idx_dedupe_actions_batch ON dedupe_actions(batch_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dedupe_actions_chat_time ON dedupe_actions(chat_id, created_at DESC)")

    # 视图
    cur.execute("""
    CREATE VIEW IF NOT EXISTS v_messages_enriched AS
    SELECT
        m.pk, m.chat_id, m.message_id, m.msg_date_text, m.msg_date_ts, m.sender_id,
        m.content, m.content_norm, m.pure_hash, m.dedupe_hash,
        m.msg_type, m.grouped_id, m.link, m.has_media,
        m.is_promo, m.promo_score, m.promo_reasons, m.dedupe_eligible, m.guard_reason, m.text_len,
        m.created_at, m.updated_at,
        c.chat_title, c.chat_username,
        mm.media_kind, mm.file_unique_id, mm.file_name, mm.file_ext, mm.mime_type,
        mm.file_size, mm.width, mm.height, mm.duration_sec, mm.media_fingerprint, mm.meta_json
    FROM messages m
    LEFT JOIN chats c
      ON c.chat_id = m.chat_id
    LEFT JOIN message_media mm
      ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id
    """)

    # FTS
    if feats.supports_fts5:
        cur.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts
        USING fts5(
            content,
            content='messages',
            content_rowid='pk',
            tokenize='unicode61 remove_diacritics 2'
        )
        """)
        cur.execute("""
        CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
            INSERT INTO messages_fts(rowid, content) VALUES (new.pk, COALESCE(new.content, ''));
        END;
        """)
        cur.execute("""
        CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN
            INSERT INTO messages_fts(messages_fts, rowid, content) VALUES ('delete', old.pk, COALESCE(old.content, ''));
        END;
        """)
        cur.execute("""
        CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE OF content ON messages BEGIN
            INSERT INTO messages_fts(messages_fts, rowid, content) VALUES ('delete', old.pk, COALESCE(old.content, ''));
            INSERT INTO messages_fts(rowid, content) VALUES (new.pk, COALESCE(new.content, ''));
        END;
        """)

        cur.execute("SELECT COUNT(*) AS c FROM messages")
        total = int(cur.fetchone()["c"] or 0)
        if total > 0:
            cur.execute("SELECT COUNT(*) AS c FROM messages_fts")
            c2 = int(cur.fetchone()["c"] or 0)
            if c2 == 0:
                logging.info("FTS 为空，执行 rebuild")
                cur.execute("INSERT INTO messages_fts(messages_fts) VALUES ('rebuild')")

    conn.commit()


# =========================
# 文本清洗 / 广告评分（增强版）
# =========================

# ---- 基础正则 ----
ZERO_WIDTH_RE = re.compile(r"[\u200b-\u200f\u2060\ufeff\u180e]")
CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")
MULTISPACE_RE = re.compile(r"\s+")
DIGIT_RE = re.compile(r"\d+")

# 标准 URL / tg
URL_RE = re.compile(r"(https?://\S+|t\.me/\S+|telegram\.me/\S+)", re.I)
INVITE_RE = re.compile(r"(?:https?://)?t\.me/(?:joinchat/|\+)[A-Za-z0-9_-]+", re.I)

# 更宽松的“拆字/插符号” tg 链接检测
OBF_TME_RE = re.compile(
    r"(?:h\s*t\s*t\s*p\s*s?\s*[:：]?\s*/\s*/\s*)?"
    r"(?:t\s*[\.\-_/\\]?\s*m\s*[\.\-_/\\]?\s*e|telegram\s*[\.\-_/\\]?\s*me)"
    r"\s*/\s*[A-Za-z0-9_+\-/]+",
    re.I
)

# @mention（允许 @ 后有轻微空格）
MENTION_RE = re.compile(r"(?<!\w)@\s*[A-Za-z0-9_]{3,}")

# 联系方式（增强版，容忍插空格/符号）
WECHAT_RE = re.compile(
    r"(?:"
    r"v[\s\W_]*x|w[\s\W_]*x|we[\s\W_]*chat|微[\s\W_]*信"
    r")"
    r"(?:号|id|ID)?"
    r"\s*[:：]?\s*"
    r"[A-Za-z0-9][A-Za-z0-9_\-]{3,}",
    re.I
)

QQ_RE = re.compile(
    r"(?:q[\s\W_]*q|q[\s\W_]*群|q[\s\W_]*q[\s\W_]*群|扣扣)"
    r"(?:号|群|群号)?"
    r"\s*[:：]?\s*"
    r"\d{5,}",
    re.I
)

PHONE_RE = re.compile(r"(?<!\d)(?:\+?86[\s\-]?)?1[3-9]\d(?:[\s\-]?\d){8}(?!\d)")

# “关键词 + 账号”组合（比如：联系：abc12345）
CONTACT_ID_RE = re.compile(
    r"(?:联系(?:方式)?|联系我|客服(?:微信)?|商务(?:合作)?|投稿|咨询|加我|找我|vx|wx|微信|wechat|qq|tg|telegram)"
    r"\s*[:：]?\s*"
    r"[A-Za-z0-9_@\-\+]{4,}",
    re.I
)

# 常见乱码/花字分隔符（用于压缩）
NOISE_SEP_RE = re.compile(r"[\s\W_]+", re.UNICODE)
NON_WORD_CJK_RE = re.compile(r"[^\w\u4e00-\u9fff]+", re.UNICODE)

# 超长疑似随机串（抗机器人扰动）
LONG_MIXED_TOKEN_RE = re.compile(r"(?i)^(?=.*[a-z])(?=.*\d)[a-z0-9]{8,}$")
LONG_ALPHA_GIBBERISH_RE = re.compile(r"(?i)^[bcdfghjklmnpqrstvwxyz]{10,}$")

# 重复字符压缩（如 vxxxxxx / 微微微微）
REPEAT_CHAR_RE = re.compile(r"(.)\1{3,}", re.UNICODE)

# emoji / pictograph 粗略过滤（不追求完美）
EMOJI_BLOCK_RE = re.compile(
    r"[\U0001F000-\U0001FAFF\U00002700-\U000027BF\U00002600-\U000026FF]+",
    re.UNICODE
)


# 常见同形字符折叠（只放一小批高频，别搞太激进）
CONFUSABLE_MAP = str.maketrans({
    # 西里尔/希腊常见混淆 -> 拉丁
    "а": "a", "е": "e", "о": "o", "р": "p", "с": "c", "у": "y", "х": "x", "і": "i", "ј": "j",
    "Α": "A", "Β": "B", "Ε": "E", "Ζ": "Z", "Η": "H", "Ι": "I", "Κ": "K", "Μ": "M", "Ν": "N",
    "Ο": "O", "Ρ": "P", "Τ": "T", "Υ": "Y", "Χ": "X",
    "а": "a", "А": "A", "В": "B", "С": "C", "Е": "E", "Н": "H", "К": "K", "М": "M", "О": "O", "Р": "P", "Т": "T", "Х": "X",
    # 常见全角符号在 NFKC 里大多会处理，这里补一点
    "＠": "@", "／": "/", "：": ":", "．": ".", "。": ".",
})


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return "[]"


def _safe_lower_nfkc(text: str) -> str:
    s = unicodedata.normalize("NFKC", text or "")
    s = html.unescape(s)
    s = s.translate(CONFUSABLE_MAP)
    s = ZERO_WIDTH_RE.sub("", s)
    s = CONTROL_RE.sub(" ", s)
    return s.lower()


def _collapse_repeats(s: str) -> str:
    # 连续重复字符压到最多2个，抗“垃垃垃垃圾圾圾”
    return REPEAT_CHAR_RE.sub(r"\1\1", s)


def _clean_visual_noise(s: str) -> str:
    # 去 emoji / 花字块，再做轻度重复压缩
    s = EMOJI_BLOCK_RE.sub(" ", s)
    s = _collapse_repeats(s)
    return s


def _compact_for_detection(s: str) -> str:
    """
    检测用压缩串：
    - 去空格和大部分符号，把“微 x 信”“t . me / +xxx”压成连续串
    """
    s = _safe_lower_nfkc(s)
    s = _clean_visual_noise(s)
    s = NOISE_SEP_RE.sub("", s)
    return s


def _light_normalize(s: str) -> str:
    s = _safe_lower_nfkc(s)
    s = _clean_visual_noise(s)
    s = MULTISPACE_RE.sub(" ", s).strip()
    return s


def _is_noise_token(tok: str) -> bool:
    """
    用于 hash 归一化时剔除“随机扰动串”
    目标：像 a8f3k2m1 / xqzptklllj 这种。
    """
    if not tok:
        return False

    # 含中文通常不算噪音
    if re.search(r"[\u4e00-\u9fff]", tok):
        return False

    t = tok.strip().lower()
    if len(t) < 6:
        return False

    # 保留可能有意义的常见 token
    whitelist = {
        "telegram", "wechat", "whatsapp", "channel", "group", "contact", "support",
        "tme", "joinchat", "http", "https"
    }
    if t in whitelist:
        return False

    # 纯字母但像一串辅音轰炸
    if LONG_ALPHA_GIBBERISH_RE.match(t):
        return True

    # 字母数字混合长串
    if LONG_MIXED_TOKEN_RE.match(t):
        return True

    # 超长且元音极少
    if re.fullmatch(r"[a-z]{10,}", t):
        vowels = sum(1 for c in t if c in "aeiou")
        if vowels <= 1:
            return True

    return False


def normalize_text_for_hash(text: str) -> str:
    """
    模板识别用“强归一化”：
    - 抗零宽/全角/emoji/同形字
    - 去链接/去联系方式/去数字
    - 去随机噪声 token
    - 去大部分符号
    """
    if not text:
        return ""

    s = _safe_lower_nfkc(text)
    s = _clean_visual_noise(s)

    # 先替换强信号（顺序重要）
    s = OBF_TME_RE.sub(" TG_LINK ", s)
    s = URL_RE.sub(" URL ", s)
    s = INVITE_RE.sub(" TG_INVITE ", s)

    s = MENTION_RE.sub(" MENTION ", s)
    s = WECHAT_RE.sub(" WECHAT_ID ", s)
    s = QQ_RE.sub(" QQ_ID ", s)
    s = PHONE_RE.sub(" PHONE ", s)
    s = CONTACT_ID_RE.sub(" CONTACT_ID ", s)

    # 一些被拆开的弱形式再兜一层（压缩串级别）
    compact = _compact_for_detection(s)
    # 如果压缩串里出现明显 tg/wechat/qq 形态，给原串注入占位，增强稳定性
    marker_tokens: List[str] = []
    if "tme" in compact or "telegramme" in compact or "joinchat" in compact:
        marker_tokens.append("TGLINK")
    if "vx" in compact or "wx" in compact or "wechat" in compact or "微信" in compact:
        marker_tokens.append("WECHAT")
    if "qq" in compact or "qq群" in compact or "q群" in compact:
        marker_tokens.append("QQ")
    if marker_tokens:
        s += " " + " ".join(marker_tokens)

    # 统一空白
    s = MULTISPACE_RE.sub(" ", s).strip()

    # token 级去噪
    tokens = []
    for tok in re.split(r"\s+", s):
        if not tok:
            continue
        # 清掉 token 级多余符号
        t = re.sub(r"^[^\w\u4e00-\u9fff]+|[^\w\u4e00-\u9fff]+$", "", tok, flags=re.UNICODE)
        if not t:
            continue

        # 去数字（模板去重里通常是噪音）
        t = DIGIT_RE.sub("", t)

        # 去随机干扰串
        if _is_noise_token(t):
            continue

        if t:
            tokens.append(t)

    s = " ".join(tokens)

    # 最后再做一次“只留字母数字中文”的收缩，形成稳定模板
    s = NON_WORD_CJK_RE.sub("", s)
    s = _collapse_repeats(s)
    return s.strip()


def normalize_text_light(text: str) -> str:
    if not text:
        return ""
    s = _light_normalize(text)
    return s


def make_hash(text: str) -> str:
    if not text:
        return ""
    return hashlib.blake2b(text.encode("utf-8"), digest_size=16).hexdigest()


def contains_hard_promo_markers(text: str) -> bool:
    if not text:
        return False
    s = _safe_lower_nfkc(text)
    compact = _compact_for_detection(text)

    # 原始标记
    if any(x.lower() in s for x in HARD_PROMO_MARKERS):
        return True

    # 压缩串标记（抗插符号/拆字）
    compact_markers = ["tme", "telegramme", "joinchat", "vx", "wx", "wechat", "微信", "qq群", "q群", "客服", "加群", "进群", "发布页", "导航"]
    if any(m in compact for m in compact_markers):
        return True

    # 宽松 regex
    if OBF_TME_RE.search(s):
        return True
    if WECHAT_RE.search(s) or QQ_RE.search(s) or CONTACT_ID_RE.search(s):
        return True
    return False


def build_media_fingerprint(file_unique_id: Optional[str],
                            mime_type: Optional[str],
                            file_size: Optional[int],
                            width: Optional[int],
                            height: Optional[int],
                            duration_sec: Optional[int]) -> str:
    """
    不下载文件的情况下做“媒体指纹”：
    - 优先 file_unique_id（最稳）
    - 否则退化为 mime/size/wh/dur
    """
    if file_unique_id:
        return "fid:" + str(file_unique_id)

    parts = [
        f"mime={mime_type or ''}",
        f"size={file_size or 0}",
        f"w={width or 0}",
        f"h={height or 0}",
        f"d={duration_sec or 0}",
    ]
    raw = "|".join(parts)
    return "meta:" + make_hash(raw)


def make_media_group_signature(media_fingerprints: List[str], msg_types: List[str], item_count: int) -> str:
    """
    媒体组签名（用于“相册广告”二次去重通道）
    - 指纹排序后签名，抗 caption 随机扰动
    """
    fps = [x for x in media_fingerprints if x]
    if not fps:
        return ""

    # 排序提升稳定性（即使顺序偶发有变，也尽量不影响）
    fps_sorted = sorted(fps)
    types_sorted = sorted([t for t in msg_types if t])
    raw = f"n={item_count}|types={','.join(types_sorted)}|fps={'|'.join(fps_sorted)}"
    return make_hash(raw)


def build_message_dedupe_hash(text_pure_hash: str,
                              has_media: bool,
                              media_fingerprint: Optional[str]) -> str:
    """
    单条消息去重键：
    - 优先文本模板 hash（原逻辑兼容）
    - 文本为空时，媒体消息回退到媒体指纹签名
    """
    if text_pure_hash:
        return text_pure_hash
    if has_media and media_fingerprint:
        return "m:" + make_hash(media_fingerprint)
    return ""


def _count_compact_keyword_hits(compact: str, keywords_compact: List[str]) -> int:
    if not compact:
        return 0
    hits = 0
    seen = set()
    for k in keywords_compact:
        if not k or len(k) < 2:
            continue
        # 短词（vx/wx/tg/qq）要求更谨慎
        if len(k) <= 2:
            continue
        if k in compact:
            seen.add(k)
    hits = len(seen)
    return hits


def _score_promo_signals(text: str) -> Tuple[int, List[str], Dict[str, int]]:
    """
    广告打分（增强版）
    同时看：
    - 原文（raw）
    - 轻归一化（light）
    - 压缩串（compact，抗插符号/拆字）
    """
    raw = text or ""
    s = _light_normalize(raw)
    compact = _compact_for_detection(raw)

    url_hits = len(URL_RE.findall(s))
    obf_tg_hits = len(OBF_TME_RE.findall(s))
    invite_hits = len(INVITE_RE.findall(s))
    mention_hits = len(MENTION_RE.findall(s))
    wechat_hits = len(WECHAT_RE.findall(s))
    qq_hits = len(QQ_RE.findall(s))
    phone_hits = len(PHONE_RE.findall(s))
    contact_id_hits = len(CONTACT_ID_RE.findall(s))

    kw_hits = _count_compact_keyword_hits(compact, PROMO_KEYWORDS_COMPACT)
    cta_hits = _count_compact_keyword_hits(compact, CTA_WORDS_COMPACT)

    # 额外模式：压缩串中直接出现 tg/wechat/qq 形态
    compact_tg = int(any(x in compact for x in ["tme", "telegramme", "joinchat"]))
    compact_wechat = int(any(x in compact for x in ["vx", "wx", "wechat", "微信"]))
    compact_qq = int(any(x in compact for x in ["qq", "qq群", "q群"]))

    # 可疑“插符号拆字”模式，如 v-x / t . m e / w x
    obfuscation_hits = 0
    if re.search(r"(?:v[\s\W_]*x|w[\s\W_]*x|t[\s\W_]*\.?[\s\W_]*m[\s\W_]*\.?[\s\W_]*e)", s, re.I):
        obfuscation_hits += 1

    # 链接/联系方式强信号
    score = 0
    reasons: List[str] = []

    if url_hits:
        score += 3 + min(url_hits - 1, 2)
        reasons.append(f"url:{url_hits}")

    if obf_tg_hits:
        score += 3 + min(obf_tg_hits - 1, 2)
        reasons.append(f"obf_tg:{obf_tg_hits}")

    if invite_hits:
        score += 4 + min(invite_hits - 1, 2)
        reasons.append(f"invite:{invite_hits}")

    if mention_hits:
        score += 1 if mention_hits == 1 else (2 + min(mention_hits - 2, 2))
        reasons.append(f"mention:{mention_hits}")

    if wechat_hits:
        score += 3 + min(wechat_hits - 1, 2)
        reasons.append(f"wechat:{wechat_hits}")

    if qq_hits:
        score += 2 + min(qq_hits - 1, 2)
        reasons.append(f"qq:{qq_hits}")

    if phone_hits:
        score += 2
        reasons.append(f"phone:{phone_hits}")

    if contact_id_hits:
        score += 2 + min(contact_id_hits, 2)
        reasons.append(f"contact_id:{contact_id_hits}")

    if kw_hits:
        kw_score = min(kw_hits * 2, 10)
        score += kw_score
        reasons.append(f"kw:{kw_hits}")

    if cta_hits:
        score += min(cta_hits, 3)
        reasons.append(f"cta:{cta_hits}")

    if compact_tg:
        score += 2
        reasons.append("compact_tg")
    if compact_wechat:
        score += 1
        reasons.append("compact_wechat")
    if compact_qq:
        score += 1
        reasons.append("compact_qq")
    if obfuscation_hits:
        score += 1
        reasons.append("obfuscation")

    # 组合加权：联系方式/链接 + CTA
    contact_total = (url_hits + obf_tg_hits + invite_hits + wechat_hits + qq_hits + phone_hits + contact_id_hits)
    if contact_total > 0 and cta_hits > 0:
        score += 1
        reasons.append("combo:contact+cta")

    # 组合加权：tg/群/频道关键词 + 联系方式
    if kw_hits >= 2 and contact_total > 0:
        score += 1
        reasons.append("combo:kw+contact")

    stats = {
        "url_hits": url_hits,
        "obf_tg_hits": obf_tg_hits,
        "invite_hits": invite_hits,
        "mention_hits": mention_hits,
        "wechat_hits": wechat_hits,
        "qq_hits": qq_hits,
        "phone_hits": phone_hits,
        "contact_id_hits": contact_id_hits,
        "kw_hits": kw_hits,
        "cta_hits": cta_hits,
        "compact_tg": compact_tg,
        "compact_wechat": compact_wechat,
        "compact_qq": compact_qq,
        "obfuscation_hits": obfuscation_hits,
    }
    return score, reasons, stats


def is_generic_media_caption(text: str,
                             msg_type: str,
                             has_media: bool,
                             promo_stats: Optional[Dict[str, int]] = None,
                             guard_len: int = 58) -> bool:
    """
    保护“普通媒体标题”，避免误删：
    - 媒体消息
    - 文案短
    - 没明显引流痕迹
    """
    if not has_media:
        return False

    s = (text or "").strip()
    if not s:
        return True  # 纯媒体无caption，默认不参与文本去重（防误杀）

    low = _light_normalize(s)

    has_hard = bool(
        URL_RE.search(low) or OBF_TME_RE.search(low) or INVITE_RE.search(low) or
        WECHAT_RE.search(low) or QQ_RE.search(low) or PHONE_RE.search(low) or CONTACT_ID_RE.search(low)
    )

    if promo_stats is not None:
        if (
            promo_stats["url_hits"] + promo_stats["obf_tg_hits"] + promo_stats["invite_hits"] +
            promo_stats["wechat_hits"] + promo_stats["qq_hits"] + promo_stats["phone_hits"] +
            promo_stats["contact_id_hits"]
        ) > 0:
            has_hard = True

    if len(s) <= guard_len and not has_hard:
        kw_hits = 0
        if promo_stats is not None:
            kw_hits = int(promo_stats.get("kw_hits", 0))
        else:
            kw_hits = _count_compact_keyword_hits(_compact_for_detection(low), PROMO_KEYWORDS_COMPACT)

        mention_hits = int((promo_stats or {}).get("mention_hits", len(MENTION_RE.findall(low))))
        if kw_hits <= 1 and mention_hits <= 1 and not contains_hard_promo_markers(s):
            return True

    # 很短标题保护（比如“第12集”“预告”“花絮”）
    if msg_type in {"PHOTO", "VIDEO", "GIF", "AUDIO", "FILE"} and not contains_hard_promo_markers(s):
        plain = normalize_text_for_hash(s)
        if 0 < len(plain) <= 12:
            return True

    return False


def build_single_promo_features(text: str, msg_type: str, has_media: bool, cfg: AppConfig) -> Dict[str, Any]:
    score, reasons, stats = _score_promo_signals(text)
    raw = text or ""

    is_promo = 1 if score >= cfg.promo_score_threshold else 0
    guard_reason = None
    dedupe_eligible = 0

    if is_promo:
        generic_guard = is_generic_media_caption(
            raw, msg_type=msg_type, has_media=has_media, promo_stats=stats, guard_len=cfg.media_caption_guard_len
        )

        if generic_guard and not contains_hard_promo_markers(raw):
            guard_reason = "GENERIC_MEDIA_CAPTION_GUARD"
            dedupe_eligible = 0
        else:
            dedupe_eligible = 1
    else:
        dedupe_eligible = 0

    norm_for_hash = normalize_text_for_hash(raw)
    pure_hash = make_hash(norm_for_hash) if norm_for_hash else ""

    return {
        "is_promo": is_promo,
        "promo_score": score,
        "promo_reasons": reasons,
        "dedupe_eligible": dedupe_eligible,
        "guard_reason": guard_reason,
        "content_norm": normalize_text_light(raw),
        "pure_hash": pure_hash,
        "text_len": len(raw),
    }


def build_group_promo_features(captions_concat: str,
                               item_count: int,
                               types_csv: str,
                               media_sig_hash: str,
                               cfg: AppConfig) -> Dict[str, Any]:
    """
    媒体组级广告识别（解决“相册广告”）
    """
    raw = (captions_concat or "").strip()

    # caption 可能为空，但媒体组仍可有媒体签名
    if not raw:
        return {
            "is_promo": 0,
            "promo_score": 0,
            "promo_reasons": [],
            "dedupe_eligible": 0,
            "guard_reason": "EMPTY_MEDIA_GROUP_CAPTION",
            "caption_norm": "",
            "pure_hash": "",
            "dedupe_hash": "",
        }

    score, reasons, stats = _score_promo_signals(raw)

    # 多媒体组 + 明显导流特征，加一点权重
    if item_count >= 2 and (
        contains_hard_promo_markers(raw)
        or stats["url_hits"] > 0
        or stats["obf_tg_hits"] > 0
        or stats["invite_hits"] > 0
        or stats["wechat_hits"] > 0
        or stats["qq_hits"] > 0
        or stats["contact_id_hits"] > 0
    ):
        score += 1
        reasons.append(f"group_bonus:{item_count}")

    is_promo = 1 if score >= cfg.promo_score_threshold else 0
    guard_reason = None
    dedupe_eligible = 0

    if is_promo:
        short_plain = normalize_text_for_hash(raw)
        no_hard = not contains_hard_promo_markers(raw)
        no_contacts = (stats["wechat_hits"] + stats["qq_hits"] + stats["phone_hits"] + stats["contact_id_hits"] == 0)
        no_links = (stats["url_hits"] + stats["obf_tg_hits"] + stats["invite_hits"] == 0)
        mentions_ok = (stats["mention_hits"] <= 1)
        kw_few = (stats["kw_hits"] <= 1)

        if len(raw) <= cfg.media_caption_guard_len and no_hard and no_contacts and no_links and mentions_ok and kw_few:
            guard_reason = "GENERIC_MEDIA_GROUP_CAPTION_GUARD"
            dedupe_eligible = 0
        elif 0 < len(short_plain) <= 12 and no_hard and no_contacts and no_links:
            guard_reason = "SHORT_MEDIA_GROUP_TITLE_GUARD"
            dedupe_eligible = 0
        else:
            dedupe_eligible = 1

    caption_norm = normalize_text_light(raw)
    pure_hash = make_hash(normalize_text_for_hash(raw)) if raw else ""

    # 组主 dedupe 键：文本模板优先，空文案则回退媒体签名
    if pure_hash:
        dedupe_hash = pure_hash
    elif media_sig_hash:
        dedupe_hash = "gm:" + media_sig_hash
    else:
        dedupe_hash = ""

    return {
        "is_promo": is_promo,
        "promo_score": score,
        "promo_reasons": reasons,
        "dedupe_eligible": dedupe_eligible,
        "guard_reason": guard_reason,
        "caption_norm": caption_norm,
        "pure_hash": pure_hash,
        "dedupe_hash": dedupe_hash,
    }


# =========================
# Telegram 消息解析
# =========================

def classify_msg_type(message) -> str:
    try:
        if getattr(message, "sticker", None):
            return "STICKER"
        if getattr(message, "gif", None):
            return "GIF"
        if getattr(message, "voice", None):
            return "VOICE"
        if getattr(message, "video_note", None):
            return "VIDEO_NOTE"
        if getattr(message, "audio", None):
            return "AUDIO"
        if getattr(message, "video", None):
            return "VIDEO"
        if getattr(message, "photo", None):
            return "PHOTO"
        if getattr(message, "document", None):
            return "FILE"
        if getattr(message, "poll", None):
            return "POLL"
        if getattr(message, "contact", None):
            return "CONTACT"
        if getattr(message, "geo", None):
            return "GEO"
        return "TEXT"
    except Exception:
        return "TEXT"


def extract_message_text(message) -> str:
    for attr in ("raw_text", "message", "text"):
        try:
            v = getattr(message, attr, None)
            if v:
                return str(v).strip()
        except Exception:
            continue
    return ""


def extract_media_meta(message, msg_type: str) -> Dict[str, Any]:
    out = {
        "media_kind": msg_type if msg_type != "TEXT" else None,
        "file_unique_id": None,
        "file_name": None,
        "file_ext": None,
        "mime_type": None,
        "file_size": None,
        "width": None,
        "height": None,
        "duration_sec": None,
        "media_fingerprint": None,
        "meta_json": None,
    }
    if msg_type == "TEXT":
        return out

    extra = {}

    try:
        f = getattr(message, "file", None)
        if f is not None:
            for k in ("id", "name", "ext", "mime_type", "size", "width", "height", "duration", "title", "performer", "emoji"):
                try:
                    v = getattr(f, k, None)
                except Exception:
                    v = None
                if v is None:
                    continue

                if k == "id":
                    out["file_unique_id"] = str(v)
                elif k == "name":
                    out["file_name"] = str(v)
                elif k == "ext":
                    out["file_ext"] = str(v)
                elif k == "mime_type":
                    out["mime_type"] = str(v)
                elif k == "size":
                    try:
                        out["file_size"] = int(v)
                    except Exception:
                        pass
                elif k == "width":
                    try:
                        out["width"] = int(v)
                    except Exception:
                        pass
                elif k == "height":
                    try:
                        out["height"] = int(v)
                    except Exception:
                        pass
                elif k == "duration":
                    try:
                        out["duration_sec"] = int(v)
                    except Exception:
                        pass
                else:
                    extra[k] = v
    except Exception as e:
        extra["file_wrapper_error"] = str(e)

    # 兜底取媒体 ID
    if not out["file_unique_id"]:
        try:
            p = getattr(message, "photo", None)
            if p is not None and hasattr(p, "id"):
                out["file_unique_id"] = str(getattr(p, "id"))
        except Exception:
            pass

    if not out["file_unique_id"]:
        try:
            d = getattr(message, "document", None)
            if d is not None and hasattr(d, "id"):
                out["file_unique_id"] = str(getattr(d, "id"))
        except Exception:
            pass

    try:
        extra["views"] = getattr(message, "views", None)
        extra["forwards"] = getattr(message, "forwards", None)
        extra["edit_date"] = str(getattr(message, "edit_date", None)) if getattr(message, "edit_date", None) else None
    except Exception:
        pass

    extra = {k: v for k, v in extra.items() if v is not None}
    out["meta_json"] = _safe_json(extra) if extra else None

    out["media_fingerprint"] = build_media_fingerprint(
        file_unique_id=out["file_unique_id"],
        mime_type=out["mime_type"],
        file_size=out["file_size"],
        width=out["width"],
        height=out["height"],
        duration_sec=out["duration_sec"],
    )
    return out


def resolve_target_entity(client: TelegramClient, target: str):
    """
    优先 username / 链接 / id；失败再扫 dialogs 标题
    """
    t = (target or "").strip()

    try:
        cleaned = t.replace("https://t.me/", "").replace("http://t.me/", "").strip("/")
        if cleaned.startswith("@"):
            cleaned = cleaned.lstrip("@")
        if cleaned and (cleaned != t or t.startswith("@") or re.fullmatch(r"-?\d+", t)):
            return client.get_entity(cleaned)
    except Exception:
        pass

    try:
        for d in client.get_dialogs():
            if (d.title or "").strip() == t:
                return d.entity
    except Exception:
        pass

    try:
        for d in client.get_dialogs():
            if t and t in (d.title or ""):
                return d.entity
    except Exception:
        pass

    return None


def get_existing_chat_ids(conn: sqlite3.Connection) -> List[int]:
    cur = conn.cursor()
    cur.execute("SELECT chat_id FROM chats ORDER BY last_seen_at DESC, first_seen_at DESC")
    out: List[int] = []
    for row in cur.fetchall():
        try:
            out.append(int(row["chat_id"]))
        except Exception:
            continue
    return out


def collect_target_entities(conn: sqlite3.Connection, client: TelegramClient, cfg: AppConfig) -> List[Any]:
    entities: List[Any] = []
    seen_chat_ids: Set[int] = set()

    def _append_entity(ent: Any):
        if not ent:
            return
        try:
            cid = int(getattr(ent, "id", 0))
        except Exception:
            cid = 0
        if cid and cid in seen_chat_ids:
            return
        if cid:
            seen_chat_ids.add(cid)
        entities.append(ent)

    if _is_enabled(cfg.scan_existing_chats):
        chat_ids = get_existing_chat_ids(conn)
        logging.info(f"参数 TG_SCAN_DB_CHATS=1，尝试扫描数据库已有会话数: {len(chat_ids)}")
        for cid in chat_ids:
            try:
                _append_entity(client.get_entity(cid))
            except Exception as e:
                logging.warning(f"跳过 chat_id={cid}（无法解析实体）: {e}")

    if cfg.target_group.strip():
        entity = resolve_target_entity(client, cfg.target_group)
        if entity:
            _append_entity(entity)
        elif not entities:
            logging.error("❌ 未找到该群组/频道，请检查名称 / 用户名 / 链接")

    return entities


def build_msg_link(entity, msg_id: int) -> str:
    username = getattr(entity, "username", None)
    if username:
        return f"https://t.me/{username}/{msg_id}"

    raw_id = str(getattr(entity, "id", ""))
    if raw_id.startswith("-100"):
        raw_id = raw_id[4:]
    else:
        raw_id = raw_id.lstrip("-")
    return f"https://t.me/c/{raw_id}/{msg_id}"


# =========================
# UPSERT SQL
# =========================

UPSERT_CHAT_SQL = """
INSERT INTO chats(chat_id, chat_title, chat_username, is_public, chat_type, last_seen_at)
VALUES (?, ?, ?, ?, ?, datetime('now'))
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
    msg_type, grouped_id, link, has_media,
    is_promo, promo_score, promo_reasons, dedupe_eligible, guard_reason, text_len,
    updated_at
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, datetime('now'))
ON CONFLICT(chat_id, message_id) DO UPDATE SET
    msg_date_text     = excluded.msg_date_text,
    msg_date_ts       = excluded.msg_date_ts,
    sender_id         = excluded.sender_id,
    content           = excluded.content,
    content_norm      = excluded.content_norm,
    pure_hash         = excluded.pure_hash,
    dedupe_hash       = excluded.dedupe_hash,
    msg_type          = excluded.msg_type,
    grouped_id        = excluded.grouped_id,
    link              = excluded.link,
    has_media         = excluded.has_media,
    is_promo          = excluded.is_promo,
    promo_score       = excluded.promo_score,
    promo_reasons     = excluded.promo_reasons,
    dedupe_eligible   = excluded.dedupe_eligible,
    guard_reason      = excluded.guard_reason,
    text_len          = excluded.text_len,
    updated_at        = datetime('now')
"""

UPSERT_MEDIA_SQL = """
INSERT INTO message_media(
    chat_id, message_id, media_kind, file_unique_id, file_name, file_ext, mime_type,
    file_size, width, height, duration_sec, grouped_id, media_fingerprint, meta_json, updated_at
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?, datetime('now'))
ON CONFLICT(chat_id, message_id) DO UPDATE SET
    media_kind        = excluded.media_kind,
    file_unique_id    = excluded.file_unique_id,
    file_name         = excluded.file_name,
    file_ext          = excluded.file_ext,
    mime_type         = excluded.mime_type,
    file_size         = excluded.file_size,
    width             = excluded.width,
    height            = excluded.height,
    duration_sec      = excluded.duration_sec,
    grouped_id        = excluded.grouped_id,
    media_fingerprint = excluded.media_fingerprint,
    meta_json         = excluded.meta_json,
    updated_at        = datetime('now')
"""

UPSERT_MEDIA_GROUP_SQL = """
INSERT INTO media_groups(
    chat_id, grouped_id,
    first_message_id, first_msg_date_ts, last_message_id, last_msg_date_ts,
    item_count, active_items, types_csv,
    captions_concat, caption_norm, pure_hash, media_sig_hash, dedupe_hash,
    is_promo, promo_score, promo_reasons, dedupe_eligible, guard_reason,
    updated_at
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, datetime('now'))
ON CONFLICT(chat_id, grouped_id) DO UPDATE SET
    first_message_id = excluded.first_message_id,
    first_msg_date_ts = excluded.first_msg_date_ts,
    last_message_id = excluded.last_message_id,
    last_msg_date_ts = excluded.last_msg_date_ts,
    item_count = excluded.item_count,
    active_items = excluded.active_items,
    types_csv = excluded.types_csv,
    captions_concat = excluded.captions_concat,
    caption_norm = excluded.caption_norm,
    pure_hash = excluded.pure_hash,
    media_sig_hash = excluded.media_sig_hash,
    dedupe_hash = excluded.dedupe_hash,
    is_promo = excluded.is_promo,
    promo_score = excluded.promo_score,
    promo_reasons = excluded.promo_reasons,
    dedupe_eligible = excluded.dedupe_eligible,
    guard_reason = excluded.guard_reason,
    updated_at = datetime('now')
"""


# =========================
# DB 写入 / 查询辅助
# =========================

def get_last_message_id(conn: sqlite3.Connection, chat_id: int) -> int:
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(message_id), 0) AS m FROM messages WHERE chat_id=?", (chat_id,))
    return int(cur.fetchone()["m"])


def upsert_chat(conn: sqlite3.Connection, row: tuple):
    # 使用独立游标并在提交前显式关闭，避免 "SQL statements in progress"
    # （某些 SQLite/Python 组合在连接级 execute + 立刻 commit 时会触发）。
    cur = conn.cursor()
    try:
        cur.execute(UPSERT_CHAT_SQL, row)
    finally:
        cur.close()
    conn.commit()


def batch_upsert(conn: sqlite3.Connection, msg_rows: List[tuple], media_rows: List[tuple]):
    if not msg_rows and not media_rows:
        return
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        if msg_rows:
            cur.executemany(UPSERT_MESSAGE_SQL, msg_rows)
        if media_rows:
            cur.executemany(UPSERT_MEDIA_SQL, media_rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def chunked(seq: List[Any], n: int) -> Iterable[List[Any]]:
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _rebuild_media_groups_for_ids(conn: sqlite3.Connection, chat_id: int, grouped_ids: List[int], cfg: AppConfig):
    if not grouped_ids:
        return

    cur = conn.cursor()

    for part in chunked(sorted(set(grouped_ids)), 500):
        placeholders = ",".join(["?"] * len(part))

        # 先删旧聚合
        cur.execute(f"DELETE FROM media_groups WHERE chat_id=? AND grouped_id IN ({placeholders})", [chat_id] + part)

        # 拉明细，Python 聚合，保证顺序和签名稳定
        cur.execute(f"""
            SELECT
                m.grouped_id,
                m.message_id,
                m.msg_date_ts,
                m.msg_type,
                COALESCE(m.content, '') AS content,
                mm.media_fingerprint
            FROM messages m
            LEFT JOIN message_media mm
              ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id
            WHERE m.chat_id = ?
              AND m.grouped_id IN ({placeholders})
            ORDER BY m.grouped_id ASC, m.message_id ASC
        """, [chat_id] + part)
        rows = cur.fetchall()

        bucket: Dict[int, Dict[str, Any]] = {}
        for r in rows:
            gid = int(r["grouped_id"])
            if gid not in bucket:
                bucket[gid] = {
                    "first_message_id": None,
                    "first_msg_date_ts": None,
                    "last_message_id": None,
                    "last_msg_date_ts": None,
                    "item_count": 0,
                    "types": [],
                    "captions": [],
                    "media_fingerprints": [],
                }

            b = bucket[gid]
            mid = int(r["message_id"])
            ts = int(r["msg_date_ts"])

            if b["first_message_id"] is None or mid < b["first_message_id"]:
                b["first_message_id"] = mid
            if b["first_msg_date_ts"] is None or ts < b["first_msg_date_ts"]:
                b["first_msg_date_ts"] = ts
            if b["last_message_id"] is None or mid > b["last_message_id"]:
                b["last_message_id"] = mid
            if b["last_msg_date_ts"] is None or ts > b["last_msg_date_ts"]:
                b["last_msg_date_ts"] = ts

            b["item_count"] += 1
            b["types"].append(r["msg_type"] or "")
            if r["content"]:
                b["captions"].append(str(r["content"]))
            if r["media_fingerprint"]:
                b["media_fingerprints"].append(str(r["media_fingerprint"]))

        up_rows = []
        for gid, b in bucket.items():
            item_count = int(b["item_count"])
            active_items = item_count
            types_csv = ",".join(sorted(set([x for x in b["types"] if x])))
            captions_concat = "\n".join([c for c in b["captions"] if c]).strip()

            media_sig_hash = make_media_group_signature(
                media_fingerprints=b["media_fingerprints"],
                msg_types=b["types"],
                item_count=item_count
            )

            features = build_group_promo_features(
                captions_concat=captions_concat,
                item_count=item_count,
                types_csv=types_csv,
                media_sig_hash=media_sig_hash,
                cfg=cfg
            )

            up_rows.append((
                chat_id, gid,
                b["first_message_id"], b["first_msg_date_ts"],
                b["last_message_id"], b["last_msg_date_ts"],
                item_count, active_items, types_csv,
                captions_concat,
                features["caption_norm"],
                features["pure_hash"],
                media_sig_hash,
                features["dedupe_hash"],
                int(features["is_promo"]),
                int(features["promo_score"]),
                _safe_json(features["promo_reasons"]),
                int(features["dedupe_eligible"]),
                features["guard_reason"],
            ))

        if up_rows:
            cur.executemany(UPSERT_MEDIA_GROUP_SQL, up_rows)

    conn.commit()


def refresh_media_groups_for_chat(conn: sqlite3.Connection, chat_id: int, cfg: AppConfig, grouped_ids: Optional[Set[int]] = None):
    """
    重建/增量刷新 media_groups（增强版）
    - 全量：先取所有 grouped_id，再分批聚合
    - 增量：只刷新本轮 touched grouped_id
    """
    cur = conn.cursor()

    if grouped_ids is None:
        cur.execute("SELECT DISTINCT grouped_id FROM messages WHERE chat_id=? AND grouped_id IS NOT NULL", (chat_id,))
        gids = [int(r["grouped_id"]) for r in cur.fetchall() if r["grouped_id"] is not None]
        cur.execute("DELETE FROM media_groups WHERE chat_id=?", (chat_id,))
        conn.commit()
        _rebuild_media_groups_for_ids(conn, chat_id, gids, cfg)
        return

    gids = [int(x) for x in grouped_ids if x is not None]
    if not gids:
        return
    _rebuild_media_groups_for_ids(conn, chat_id, gids, cfg)


# =========================
# 去重（单条 + 媒体组双通道，硬删除）
# =========================

def dedupe_promotional_duplicates(
    conn: sqlite3.Connection,
    chat_id: int,
    mode: str = "PURGE_ALL",
    threshold: int = 2,
    promo_score_threshold: int = 3,
) -> Tuple[int, int, int, int, Set[int]]:
    """
    双通道去重（硬删除）：
    A) 单条消息（非媒体组）按 dedupe_hash 去重（文本优先 / 空文本回退媒体指纹）
    B) 媒体组（grouped_id）去重
       - 文本模板通道（pure_hash）
       - 媒体签名通道（media_sig_hash）【抗随机乱码文案】

    返回：
    (处理条数, 单条重复模板数, 媒体组文本模板数, 媒体组媒体签名数, 受影响grouped_id集合)
    """
    batch_id = datetime.now(timezone.utc).strftime("dedupe_%Y%m%d_%H%M%S")
    cur = conn.cursor()
    mode = (mode or "PURGE_ALL").upper()

    cur.execute("""
        INSERT OR REPLACE INTO dedupe_runs(batch_id, chat_id, mode, threshold, promo_threshold, started_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
    """, (batch_id, chat_id, mode, int(threshold), int(promo_score_threshold)))
    conn.commit()

    # ========== A) 单条消息（非媒体组）重复模板 ==========
    cur.execute("DROP TABLE IF EXISTS temp_dup_hashes_solo")
    cur.execute("""
        CREATE TEMP TABLE temp_dup_hashes_solo AS
        SELECT dedupe_hash
        FROM messages
        WHERE chat_id = ?
          AND grouped_id IS NULL
          AND is_promo = 1
          AND dedupe_eligible = 1
          AND dedupe_hash <> ''
        GROUP BY dedupe_hash
        HAVING COUNT(*) >= ?
    """, (chat_id, threshold))
    cur.execute("SELECT COUNT(*) AS c FROM temp_dup_hashes_solo")
    dup_hash_count_solo = int(cur.fetchone()["c"] or 0)

    # ========== B1) 媒体组文本模板重复 ==========
    cur.execute("DROP TABLE IF EXISTS temp_dup_hashes_group_txt")
    cur.execute("""
        CREATE TEMP TABLE temp_dup_hashes_group_txt AS
        SELECT pure_hash
        FROM media_groups
        WHERE chat_id = ?
          AND item_count >= 2
          AND is_promo = 1
          AND dedupe_eligible = 1
          AND pure_hash <> ''
        GROUP BY pure_hash
        HAVING COUNT(*) >= ?
    """, (chat_id, threshold))
    cur.execute("SELECT COUNT(*) AS c FROM temp_dup_hashes_group_txt")
    dup_hash_count_group_txt = int(cur.fetchone()["c"] or 0)

    # ========== B2) 媒体组媒体签名重复（抗随机文案） ==========
    cur.execute("DROP TABLE IF EXISTS temp_dup_hashes_group_med")
    cur.execute("""
        CREATE TEMP TABLE temp_dup_hashes_group_med AS
        SELECT media_sig_hash
        FROM media_groups
        WHERE chat_id = ?
          AND item_count >= 2
          AND is_promo = 1
          AND dedupe_eligible = 1
          AND media_sig_hash <> ''
        GROUP BY media_sig_hash
        HAVING COUNT(*) >= ?
    """, (chat_id, threshold))
    cur.execute("SELECT COUNT(*) AS c FROM temp_dup_hashes_group_med")
    dup_hash_count_group_med = int(cur.fetchone()["c"] or 0)

    # 汇总目标消息
    cur.execute("DROP TABLE IF EXISTS temp_targets")
    cur.execute("CREATE TEMP TABLE temp_targets(pk INTEGER PRIMARY KEY)")

    # A: 单条目标
    cur.execute("""
        INSERT OR IGNORE INTO temp_targets(pk)
        SELECT pk
        FROM messages
        WHERE chat_id = ?
          AND grouped_id IS NULL
          AND is_promo = 1
          AND dedupe_eligible = 1
          AND dedupe_hash IN (SELECT dedupe_hash FROM temp_dup_hashes_solo)
    """, (chat_id,))

    # B: 命中的媒体组（文本 OR 媒体签名）
    cur.execute("DROP TABLE IF EXISTS temp_target_groups")
    cur.execute("""
        CREATE TEMP TABLE temp_target_groups AS
        SELECT DISTINCT grouped_id
        FROM media_groups
        WHERE chat_id = ?
          AND item_count >= 2
          AND is_promo = 1
          AND dedupe_eligible = 1
          AND (
            pure_hash IN (SELECT pure_hash FROM temp_dup_hashes_group_txt)
            OR media_sig_hash IN (SELECT media_sig_hash FROM temp_dup_hashes_group_med)
          )
    """, (chat_id,))

    cur.execute("""
        INSERT OR IGNORE INTO temp_targets(pk)
        SELECT m.pk
        FROM messages m
        WHERE m.chat_id = ?
          AND m.grouped_id IN (SELECT grouped_id FROM temp_target_groups)
    """, (chat_id,))

    # KEEP_FIRST：每个模板保留最早单条 / 每个组模板保留最早组
    if mode == "KEEP_FIRST":
        # 单条
        cur.execute("DROP TABLE IF EXISTS temp_keep_solo")
        cur.execute("""
            CREATE TEMP TABLE temp_keep_solo AS
            SELECT pk
            FROM (
                SELECT pk,
                       ROW_NUMBER() OVER (
                           PARTITION BY dedupe_hash
                           ORDER BY msg_date_ts ASC, message_id ASC, pk ASC
                       ) AS rn
                FROM messages
                WHERE chat_id = ?
                  AND grouped_id IS NULL
                  AND is_promo = 1
                  AND dedupe_eligible = 1
                  AND dedupe_hash IN (SELECT dedupe_hash FROM temp_dup_hashes_solo)
            )
            WHERE rn = 1
        """, (chat_id,))
        cur.execute("DELETE FROM temp_targets WHERE pk IN (SELECT pk FROM temp_keep_solo)")

        # 组（文本模板保留）
        cur.execute("DROP TABLE IF EXISTS temp_keep_groups_txt")
        cur.execute("""
            CREATE TEMP TABLE temp_keep_groups_txt AS
            SELECT mg.grouped_id
            FROM media_groups mg
            JOIN (
                SELECT pure_hash, MIN(first_message_id) AS min_msgid
                FROM media_groups
                WHERE chat_id = ?
                  AND item_count >= 2
                  AND is_promo = 1
                  AND dedupe_eligible = 1
                  AND pure_hash IN (SELECT pure_hash FROM temp_dup_hashes_group_txt)
                GROUP BY pure_hash
            ) k
              ON mg.pure_hash = k.pure_hash AND mg.first_message_id = k.min_msgid
            WHERE mg.chat_id = ?
            GROUP BY mg.pure_hash
        """, (chat_id, chat_id))

        # 组（媒体签名保留）
        cur.execute("DROP TABLE IF EXISTS temp_keep_groups_med")
        cur.execute("""
            CREATE TEMP TABLE temp_keep_groups_med AS
            SELECT mg.grouped_id
            FROM media_groups mg
            JOIN (
                SELECT media_sig_hash, MIN(first_message_id) AS min_msgid
                FROM media_groups
                WHERE chat_id = ?
                  AND item_count >= 2
                  AND is_promo = 1
                  AND dedupe_eligible = 1
                  AND media_sig_hash IN (SELECT media_sig_hash FROM temp_dup_hashes_group_med)
                GROUP BY media_sig_hash
            ) k
              ON mg.media_sig_hash = k.media_sig_hash AND mg.first_message_id = k.min_msgid
            WHERE mg.chat_id = ?
            GROUP BY mg.media_sig_hash
        """, (chat_id, chat_id))

        # 合并保留组（被任一通道保留即可）
        cur.execute("DROP TABLE IF EXISTS temp_keep_groups_final")
        cur.execute("""
            CREATE TEMP TABLE temp_keep_groups_final AS
            SELECT grouped_id FROM temp_keep_groups_txt
            UNION
            SELECT grouped_id FROM temp_keep_groups_med
        """)

        cur.execute("""
            DELETE FROM temp_targets
            WHERE pk IN (
                SELECT m.pk
                FROM messages m
                WHERE m.chat_id = ?
                  AND m.grouped_id IN (SELECT grouped_id FROM temp_keep_groups_final)
            )
        """, (chat_id,))

    cur.execute("SELECT COUNT(*) AS c FROM temp_targets")
    target_count = int(cur.fetchone()["c"] or 0)

    if target_count == 0:
        cur.execute("""
            UPDATE dedupe_runs
            SET dup_hash_count_solo=?,
                dup_hash_count_group_txt=?,
                dup_hash_count_group_med=?,
                target_count=0,
                finished_at=datetime('now')
            WHERE batch_id=?
        """, (dup_hash_count_solo, dup_hash_count_group_txt, dup_hash_count_group_med, batch_id))
        conn.commit()
        return 0, dup_hash_count_solo, dup_hash_count_group_txt, dup_hash_count_group_med, set()

    # 审计记录
    cur.execute("""
        INSERT INTO dedupe_actions(batch_id, chat_id, pk, message_id, grouped_id, dedupe_hash, pure_hash, action, reason)
        SELECT ?, m.chat_id, m.pk, m.message_id, m.grouped_id, m.dedupe_hash, m.pure_hash, 'HARD_DELETE',
               'DEDUPE_PROMO_HASH_OR_MEDIA_GROUP'
        FROM messages m
        WHERE m.pk IN (SELECT pk FROM temp_targets)
    """, (batch_id,))

    cur.execute("""
        SELECT DISTINCT grouped_id
        FROM messages
        WHERE pk IN (SELECT pk FROM temp_targets)
          AND grouped_id IS NOT NULL
    """)
    affected_group_ids = {int(r["grouped_id"]) for r in cur.fetchall()}

    # 硬删除（级联清理 message_media，FTS 触发器同步）
    cur.execute("DELETE FROM messages WHERE pk IN (SELECT pk FROM temp_targets)")

    cur.execute("""
        UPDATE dedupe_runs
        SET dup_hash_count_solo=?,
            dup_hash_count_group_txt=?,
            dup_hash_count_group_med=?,
            target_count=?,
            finished_at=datetime('now')
        WHERE batch_id=?
    """, (dup_hash_count_solo, dup_hash_count_group_txt, dup_hash_count_group_med, target_count, batch_id))

    conn.commit()
    return target_count, dup_hash_count_solo, dup_hash_count_group_txt, dup_hash_count_group_med, affected_group_ids


# =========================
# 检索函数（后续做 UI / API 复用）
# =========================

def get_chat_stats(conn: sqlite3.Connection, chat_id: int) -> Dict[str, int]:
    cur = conn.cursor()
    out = {}

    cur.execute("SELECT COUNT(*) AS c FROM messages WHERE chat_id=?", (chat_id,))
    out["total_messages"] = int(cur.fetchone()["c"] or 0)

    cur.execute("SELECT COUNT(*) AS c FROM messages WHERE chat_id=? AND has_media=1", (chat_id,))
    out["media_messages"] = int(cur.fetchone()["c"] or 0)

    cur.execute("SELECT COUNT(*) AS c FROM messages WHERE chat_id=? AND is_promo=1", (chat_id,))
    out["promo_messages"] = int(cur.fetchone()["c"] or 0)

    cur.execute("SELECT COUNT(*) AS c FROM messages WHERE chat_id=? AND is_promo=1 AND dedupe_eligible=1", (chat_id,))
    out["promo_dedupe_eligible_messages"] = int(cur.fetchone()["c"] or 0)

    cur.execute("SELECT COUNT(*) AS c FROM messages WHERE chat_id=? AND is_promo=1 AND dedupe_eligible=0", (chat_id,))
    out["promo_guarded_messages"] = int(cur.fetchone()["c"] or 0)

    cur.execute("SELECT COUNT(*) AS c FROM media_groups WHERE chat_id=?", (chat_id,))
    out["media_groups"] = int(cur.fetchone()["c"] or 0)

    cur.execute("SELECT COUNT(*) AS c FROM media_groups WHERE chat_id=? AND is_promo=1", (chat_id,))
    out["promo_media_groups"] = int(cur.fetchone()["c"] or 0)

    cur.execute("SELECT COUNT(*) AS c FROM media_groups WHERE chat_id=? AND is_promo=1 AND dedupe_eligible=0", (chat_id,))
    out["guarded_media_groups"] = int(cur.fetchone()["c"] or 0)

    return out


# =========================
# 主流程
# =========================

def run_harvest():
    setup_logging()
    conn, feats = connect_db(CFG.db_name)
    create_schema(conn, feats)

    try:
        with TelegramClient(CFG.session_name, CFG.api_id, CFG.api_hash) as client:
            entities = collect_target_entities(conn, client, CFG)
            if not entities:
                logging.error("❌ 无可处理的群组/频道（可检查 TG_TARGET_GROUP 或将 TG_SCAN_DB_CHATS 设为 1）")
                return

            logging.info(f"本轮待处理群组/频道数: {len(entities)}")

            for idx, entity in enumerate(entities, start=1):
                chat_id = int(getattr(entity, "id", 0))
                chat_title = getattr(entity, "title", CFG.target_group) or CFG.target_group
                chat_username = getattr(entity, "username", None)
                is_public = 1 if chat_username else 0
                chat_type = entity.__class__.__name__

                logging.info(f"[{idx}/{len(entities)}] 正在处理: {chat_title} (chat_id={chat_id})")
                upsert_chat(conn, (chat_id, chat_title, chat_username, is_public, chat_type))

                last_id = get_last_message_id(conn, chat_id)
                first_sync = (last_id == 0)
                scan_from_id = max(last_id - CFG.rescan_tail_ids, 0)

                if first_sync:
                    logging.info("首次同步，开始全量抓取...")
                else:
                    logging.info(f"增量同步：last_id={last_id}，回扫到 > {scan_from_id}")

                count_seen = 0
                count_written = 0
                msg_rows: List[tuple] = []
                media_rows: List[tuple] = []
                touched_group_ids: Set[int] = set()

                iterator = iter(client.iter_messages(entity, min_id=scan_from_id, reverse=True))

                while True:
                    try:
                        message = next(iterator)
                    except StopIteration:
                        break
                    except FloodWaitError as e:
                        wait_s = int(getattr(e, "seconds", 5))
                        logging.warning(f"⏳ FloodWait，等待 {wait_s}s")
                        time.sleep(wait_s)
                        continue
                    except RPCError as e:
                        logging.warning(f"Telegram RPC 错误：{e}")
                        time.sleep(2)
                        continue
                    except Exception as e:
                        logging.warning(f"消息迭代异常：{e}")
                        time.sleep(1)
                        continue

                    count_seen += 1

                    try:
                        dt = getattr(message, "date", None)
                        if dt is None:
                            continue

                        msg_date_ts = int(dt.timestamp())
                        msg_date_text = dt.strftime("%Y-%m-%d %H:%M:%S")

                        msg_type = classify_msg_type(message)
                        has_media = 0 if msg_type == "TEXT" else 1

                        content = extract_message_text(message)

                        msg_id = int(getattr(message, "id"))
                        sender_id = getattr(message, "sender_id", None)
                        try:
                            sender_id = int(sender_id) if sender_id is not None else None
                        except Exception:
                            sender_id = None

                        grouped_id = getattr(message, "grouped_id", None)
                        try:
                            grouped_id = int(grouped_id) if grouped_id is not None else None
                        except Exception:
                            grouped_id = None

                        if grouped_id is not None:
                            touched_group_ids.add(grouped_id)

                        link = build_msg_link(entity, msg_id)

                        # 媒体元信息先抽出来（dedupe_hash 可能要用到 media_fingerprint）
                        mmeta = extract_media_meta(message, msg_type) if has_media else None

                        features = build_single_promo_features(
                            content,
                            msg_type=msg_type,
                            has_media=bool(has_media),
                            cfg=CFG
                        )

                        message_dedupe_hash = build_message_dedupe_hash(
                            text_pure_hash=features["pure_hash"],
                            has_media=bool(has_media),
                            media_fingerprint=(mmeta or {}).get("media_fingerprint")
                        )

                        msg_rows.append((
                            chat_id, msg_id, msg_date_text, msg_date_ts, sender_id,
                            content, features["content_norm"], features["pure_hash"], message_dedupe_hash,
                            msg_type, grouped_id, link, has_media,
                            int(features["is_promo"]), int(features["promo_score"]), _safe_json(features["promo_reasons"]),
                            int(features["dedupe_eligible"]), features["guard_reason"], int(features["text_len"])
                        ))

                        if has_media and mmeta is not None:
                            media_rows.append((
                                chat_id, msg_id,
                                mmeta["media_kind"], mmeta["file_unique_id"], mmeta["file_name"], mmeta["file_ext"],
                                mmeta["mime_type"], mmeta["file_size"], mmeta["width"], mmeta["height"], mmeta["duration_sec"],
                                grouped_id, mmeta["media_fingerprint"], mmeta["meta_json"]
                            ))

                        if len(msg_rows) >= CFG.batch_size:
                            batch_upsert(conn, msg_rows, media_rows)
                            count_written += len(msg_rows)
                            msg_rows.clear()
                            media_rows.clear()

                            if count_seen % CFG.log_every == 0:
                                logging.info(f"扫描 {count_seen} | 写入/更新 {count_written}")

                    except Exception as e:
                        logging.warning(f"⚠️ 跳过一条消息（解析失败）: {e}")

                # 扫尾写入
                if msg_rows or media_rows:
                    batch_upsert(conn, msg_rows, media_rows)
                    count_written += len(msg_rows)
                    msg_rows.clear()
                    media_rows.clear()

                # 刷新媒体组聚合
                if first_sync:
                    logging.info("刷新 media_groups（全量）...")
                    refresh_media_groups_for_chat(conn, chat_id, cfg=CFG, grouped_ids=None)
                else:
                    logging.info(f"刷新 media_groups（增量，涉及组数={len(touched_group_ids)}）...")
                    refresh_media_groups_for_chat(conn, chat_id, cfg=CFG, grouped_ids=touched_group_ids)

                # 数据库级去重（单条 + 相册）
                logging.info("执行数据库级去重（硬删除，含媒体组广告去重）...")
                deduped_count, dup_hash_solo, dup_hash_group_txt, dup_hash_group_med, affected_group_ids = dedupe_promotional_duplicates(
                    conn,
                    chat_id=chat_id,
                    mode=CFG.dedup_mode,
                    threshold=CFG.dedup_threshold,
                    promo_score_threshold=CFG.promo_score_threshold,
                )

                # 去重后刷新受影响媒体组（item_count 会变化）
                if affected_group_ids:
                    refresh_media_groups_for_chat(conn, chat_id, cfg=CFG, grouped_ids=affected_group_ids)

                # 统计
                stats = get_chat_stats(conn, chat_id)

                # SQLite 优化
                try:
                    conn.execute("PRAGMA optimize;").fetchall()
                except Exception:
                    pass
                try:
                    conn.execute("PRAGMA wal_checkpoint(TRUNCATE);").fetchall()
                except Exception:
                    pass

                logging.info("✅ 处理完成")
                logging.info(f"群组: {chat_title} (chat_id={chat_id})")
                logging.info(f"本轮扫描消息: {count_seen}")
                logging.info(f"本轮写入/更新: {count_written}")
                logging.info(f"命中重复模板(单条): {dup_hash_solo}")
                logging.info(f"命中重复模板(媒体组-文案): {dup_hash_group_txt}")
                logging.info(f"命中重复模板(媒体组-媒体签名): {dup_hash_group_med}")
                logging.info(f"本轮硬删除条数: {deduped_count}")
                logging.info(f"数据库总记录: {stats['total_messages']}")
                logging.info(f"媒体消息: {stats['media_messages']}")
                logging.info(
                    f"引流候选消息: {stats['promo_messages']}（可自动去重: {stats['promo_dedupe_eligible_messages']} | 受保护: {stats['promo_guarded_messages']}）"
                )
                logging.info(f"媒体组总数: {stats['media_groups']}")
                logging.info(f"引流候选媒体组: {stats['promo_media_groups']}（受保护: {stats['guarded_media_groups']}）")

    finally:
        conn.close()


if __name__ == "__main__":
    run_harvest()
