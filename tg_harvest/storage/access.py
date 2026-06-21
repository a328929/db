import sqlite3
from collections.abc import Callable
from pathlib import Path

import tg_harvest.storage.fts as _fts

BASE_TABLE = "messages m"
JOINS = [
    "LEFT JOIN chats c ON c.chat_id = m.chat_id",
    "LEFT JOIN message_media mm ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id",
]

FROM_SQL = f"FROM {BASE_TABLE} {' '.join(JOINS)}"


def get_conn(
    *,
    db_path: str | Path,
    connect_db_fn: Callable[..., tuple[sqlite3.Connection, object]],
    cache_mb: int = 256,
    mmap_mb: int = 512,
) -> sqlite3.Connection:
    conn, _ = connect_db_fn(str(db_path), cache_mb=cache_mb, mmap_mb=mmap_mb)
    return conn


def has_fts(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='messages_fts' LIMIT 1"
        )
        if cur.fetchone() is None:
            return False
        try:
            cur.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='messages_fts_docsize' LIMIT 1"
            )
            if cur.fetchone() is None:
                return False
        except sqlite3.Error:
            return False
    finally:
        cur.close()
    return _fts.fts_index_is_marked_ready(conn)
