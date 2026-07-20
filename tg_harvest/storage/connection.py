import logging
import sqlite3
import threading
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tg_harvest.runtime import paths as _runtime_paths

DB_WRITE_LOCK = threading.RLock()


def synchronized_write(func):
    """
    装饰器：确保被装饰的函数在执行时持有全局写入锁。
    增加了获取锁的超时机制（15秒），防止 Web 线程被后台长任务永久阻塞导致服务死锁。
    """

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


@dataclass
class SqliteFeatures:
    version_str: str
    version_tuple: tuple[int, int, int]
    supports_strict: bool


def parse_version(v: str) -> tuple[int, int, int]:
    try:
        p = v.split(".")
        return (int(p[0]), int(p[1]), int(p[2]))
    except Exception:
        return (0, 0, 0)


def _read_sqlite_version(cur: sqlite3.Cursor) -> str:
    cur.execute("SELECT sqlite_version() AS v")
    row = cur.fetchone()
    return row["v"] if isinstance(row, sqlite3.Row) else row[0]


def detect_sqlite_features(conn: sqlite3.Connection) -> SqliteFeatures:
    cur = conn.cursor()
    try:
        v = _read_sqlite_version(cur)
        vt = parse_version(v)
        supports_strict = vt >= (3, 37, 0)
        return SqliteFeatures(v, vt, supports_strict)
    finally:
        cur.close()


def resolve_db_path(raw_name: str) -> str:
    return _runtime_paths.resolve_db_path(raw_name)


def _open_connection(db_name: str) -> sqlite3.Connection:
    Path(db_name).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_name, timeout=300)
    conn.row_factory = sqlite3.Row
    return conn


def _apply_core_pragmas(
    cur: sqlite3.Cursor, cache_mb: int = 256, *, set_journal_mode: bool = True
):
    if set_journal_mode:
        cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA foreign_keys=ON;")

    cache_kib = -(cache_mb * 1024)
    cur.execute(f"PRAGMA cache_size={cache_kib};")

    cur.execute("PRAGMA busy_timeout=30000;")
    cur.execute("PRAGMA wal_autocheckpoint=1000;")


def _apply_optional_pragmas(cur: sqlite3.Cursor, mmap_mb: int = 512):
    with suppress(Exception):
        mmap_bytes = mmap_mb * 1024 * 1024
        cur.execute(f"PRAGMA mmap_size={mmap_bytes};")
    with suppress(Exception):
        cur.execute("PRAGMA journal_size_limit=67108864;")


def _apply_pragmas(
    conn: sqlite3.Connection,
    cache_mb: int = 256,
    mmap_mb: int = 512,
    *,
    set_journal_mode: bool = True,
):
    cur = conn.cursor()
    try:
        _apply_core_pragmas(
            cur, cache_mb=cache_mb, set_journal_mode=set_journal_mode
        )
        _apply_optional_pragmas(cur, mmap_mb=mmap_mb)
    finally:
        cur.close()


def _load_sqlite_features(conn: sqlite3.Connection) -> SqliteFeatures:
    return detect_sqlite_features(conn)


def connect_db(
    db_name: str,
    cache_mb: int = 256,
    mmap_mb: int = 512,
    *,
    set_journal_mode: bool = True,
) -> tuple[sqlite3.Connection, SqliteFeatures]:
    conn = _open_connection(db_name)
    _runtime_paths.secure_sqlite_artifacts(db_name)
    _apply_pragmas(
        conn, cache_mb=cache_mb, mmap_mb=mmap_mb, set_journal_mode=set_journal_mode
    )
    _runtime_paths.secure_sqlite_artifacts(db_name)
    feats = _load_sqlite_features(conn)
    return conn, feats


def _resolve_runtime_cfg(cfg: Any | None = None) -> Any:
    if cfg is not None:
        return cfg
    from tg_harvest.config import CFG

    return CFG


def connect_configured_db(*, cfg: Any | None = None) -> tuple[sqlite3.Connection, SqliteFeatures]:
    runtime_cfg = _resolve_runtime_cfg(cfg)
    return connect_db(
        str(runtime_cfg.db_name),
        cache_mb=int(runtime_cfg.sqlite_cache_mb),
        mmap_mb=int(runtime_cfg.sqlite_mmap_mb),
    )


def ensure_configured_db(
    *,
    cfg: Any | None = None,
) -> tuple[sqlite3.Connection, SqliteFeatures]:
    runtime_cfg = _resolve_runtime_cfg(cfg)
    conn, feats = connect_configured_db(cfg=runtime_cfg)
    from .schema import create_schema

    create_schema(conn, feats)
    _runtime_paths.secure_sqlite_artifacts(str(runtime_cfg.db_name))
    return conn, feats
