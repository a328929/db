import sqlite3
from pathlib import Path
from typing import Callable, Tuple, Union


FROM_SQL = """
    FROM messages m
    LEFT JOIN chats c ON c.chat_id = m.chat_id
    LEFT JOIN message_media mm ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id
"""


def get_conn(
    *,
    db_path: Union[str, Path],
    connect_db_fn: Callable[[str], Tuple[sqlite3.Connection, object]],
) -> sqlite3.Connection:
    conn, _ = connect_db_fn(str(db_path))
    return conn


def has_fts(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='messages_fts' LIMIT 1")
        return cur.fetchone() is not None
    finally:
        cur.close()
