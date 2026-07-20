import sqlite3
from collections.abc import Callable
from pathlib import Path


def get_conn(
    *,
    db_path: str | Path,
    connect_db_fn: Callable[..., tuple[sqlite3.Connection, object]],
    cache_mb: int = 256,
    mmap_mb: int = 512,
) -> sqlite3.Connection:
    conn, _ = connect_db_fn(str(db_path), cache_mb=cache_mb, mmap_mb=mmap_mb)
    return conn
