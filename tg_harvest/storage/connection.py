# -*- coding: utf-8 -*-
import logging
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Tuple

from tg_harvest.runtime.paths import resolve_db_path as _resolve_runtime_db_path


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
    return row["v"] if isinstance(row, sqlite3.Row) else row[0]


def _detect_fts5_support(cur: sqlite3.Cursor) -> bool:
    supports_fts5 = False
    try:
        cur.execute("PRAGMA compile_options;")
        opts = {str(r[0]) for r in cur.fetchall()}
        supports_fts5 = any("ENABLE_FTS5" in x for x in opts)
    except sqlite3.Error:
        try:
            cur.execute("CREATE VIRTUAL TABLE IF NOT EXISTS __fts5_probe USING fts5(x)")
            cur.execute("DROP TABLE IF EXISTS __fts5_probe")
            supports_fts5 = True
        except sqlite3.Error:
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


def resolve_db_path(raw_name: str) -> str:
    return _resolve_runtime_db_path(raw_name)


def _open_connection(db_name: str) -> sqlite3.Connection:
    Path(db_name).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_name, timeout=300)
    conn.row_factory = sqlite3.Row
    return conn


def _apply_core_pragmas(cur: sqlite3.Cursor, cache_mb: int = 256):
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA foreign_keys=ON;")

    cache_kib = -(cache_mb * 1024)
    cur.execute(f"PRAGMA cache_size={cache_kib};")

    cur.execute("PRAGMA busy_timeout=30000;")
    cur.execute("PRAGMA wal_autocheckpoint=1000;")


def _apply_optional_pragmas(cur: sqlite3.Cursor, mmap_mb: int = 512):
    try:
        mmap_bytes = mmap_mb * 1024 * 1024
        cur.execute(f"PRAGMA mmap_size={mmap_bytes};")
    except Exception:
        pass
    try:
        cur.execute("PRAGMA journal_size_limit=67108864;")
    except Exception:
        pass


def _apply_pragmas(conn: sqlite3.Connection, cache_mb: int = 256, mmap_mb: int = 512):
    cur = conn.cursor()
    try:
        _apply_core_pragmas(cur, cache_mb=cache_mb)
        _apply_optional_pragmas(cur, mmap_mb=mmap_mb)
    finally:
        cur.close()


def _load_sqlite_features(conn: sqlite3.Connection) -> SqliteFeatures:
    return detect_sqlite_features(conn)


def connect_db(
    db_name: str, cache_mb: int = 256, mmap_mb: int = 512
) -> Tuple[sqlite3.Connection, SqliteFeatures]:
    conn = _open_connection(db_name)
    _apply_pragmas(conn, cache_mb=cache_mb, mmap_mb=mmap_mb)
    feats = _load_sqlite_features(conn)
    return conn, feats


def _resolve_runtime_cfg(cfg: Optional[Any] = None) -> Any:
    if cfg is not None:
        return cfg
    from tg_harvest.config import CFG

    return CFG


def connect_configured_db(*, cfg: Optional[Any] = None) -> Tuple[sqlite3.Connection, SqliteFeatures]:
    runtime_cfg = _resolve_runtime_cfg(cfg)
    return connect_db(
        str(runtime_cfg.db_name),
        cache_mb=int(runtime_cfg.sqlite_cache_mb),
        mmap_mb=int(runtime_cfg.sqlite_mmap_mb),
    )


def ensure_configured_db(
    *,
    cfg: Optional[Any] = None,
    force_heal_fts: Optional[int] = None,
    skip_fts_auto_heal: Optional[int] = None,
) -> Tuple[sqlite3.Connection, SqliteFeatures]:
    runtime_cfg = _resolve_runtime_cfg(cfg)
    conn, feats = connect_configured_db(cfg=runtime_cfg)
    from .schema import create_schema

    create_schema(
        conn,
        feats,
        force_heal_fts=int(
            runtime_cfg.force_heal_fts if force_heal_fts is None else force_heal_fts
        ),
        skip_fts_auto_heal=int(
            getattr(runtime_cfg, "skip_fts_auto_heal", 0)
            if skip_fts_auto_heal is None
            else skip_fts_auto_heal
        ),
    )
    return conn, feats
