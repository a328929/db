import hashlib
import os
import sqlite3
import threading
from typing import Any

_COUNT_CACHE_LOCK = threading.Lock()
_COUNT_CACHE: dict[tuple[Any, ...], tuple[int, bool, int]] = {}
_COUNT_CACHE_MAX_ENTRIES = 256


def _read_data_version(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA data_version")
        row = cur.fetchone()
        if row is None:
            return 0
        return int(row[0] if not isinstance(row, sqlite3.Row) else row[0])
    finally:
        cur.close()


def _read_database_fingerprint(conn: sqlite3.Connection) -> tuple[Any, ...]:
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA database_list")
        rows = cur.fetchall()
    finally:
        cur.close()

    main_path = ""
    for row in rows:
        name = row["name"] if isinstance(row, sqlite3.Row) else row[1]
        if str(name) != "main":
            continue
        raw_file = row["file"] if isinstance(row, sqlite3.Row) else row[2]
        main_path = str(raw_file or "")
        break

    if not main_path:
        return ("memory", _read_data_version(conn))

    main_path = os.path.abspath(main_path)
    stats: list[tuple[str, int | None, int | None]] = []
    for path in (main_path, f"{main_path}-wal"):
        is_wal = path == f"{main_path}-wal"
        try:
            st = os.stat(path)
        except OSError:
            stats.append((path, 0, None) if is_wal else (path, None, None))
            continue
        size = int(st.st_size)
        if is_wal and size == 0:
            stats.append((path, 0, None))
            continue
        stats.append((path, size, int(st.st_mtime_ns)))
    return ("file", tuple(stats))


def _format_data_version(fingerprint: tuple[Any, ...]) -> str:
    raw = repr(fingerprint).encode("utf-8", "surrogatepass")
    return hashlib.blake2b(raw, digest_size=12).hexdigest()


def _make_count_cache_key(
    conn: sqlite3.Connection,
    *,
    count_sql: str,
    sql_params: list[Any],
    count_limit: int,
    page_size: int,
) -> tuple[Any, ...]:
    return (
        _read_database_fingerprint(conn),
        count_sql,
        tuple(sql_params),
        int(count_limit),
        int(page_size),
    )


def _get_cached_count(
    cache_key: tuple[Any, ...],
) -> tuple[int, bool, int] | None:
    with _COUNT_CACHE_LOCK:
        return _COUNT_CACHE.get(cache_key)


def _put_cached_count(
    cache_key: tuple[Any, ...], value: tuple[int, bool, int]
) -> None:
    with _COUNT_CACHE_LOCK:
        _COUNT_CACHE[cache_key] = value
        if len(_COUNT_CACHE) > _COUNT_CACHE_MAX_ENTRIES:
            oldest_key = next(iter(_COUNT_CACHE))
            _COUNT_CACHE.pop(oldest_key, None)
