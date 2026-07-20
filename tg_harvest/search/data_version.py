from __future__ import annotations

import hashlib
import os
import sqlite3
from typing import Any


def read_database_fingerprint(conn: sqlite3.Connection) -> tuple[Any, ...]:
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
        raw_path = row["file"] if isinstance(row, sqlite3.Row) else row[2]
        main_path = str(raw_path or "")
        break
    if not main_path:
        return ("memory", int(conn.total_changes))

    main_path = os.path.abspath(main_path)
    stats: list[tuple[str, int | None, int | None]] = []
    for path in (main_path, f"{main_path}-wal"):
        try:
            stat = os.stat(path)
        except OSError:
            stats.append((path, 0, None) if path.endswith("-wal") else (path, None, None))
        else:
            stats.append((path, int(stat.st_size), int(stat.st_mtime_ns)))
    return ("file", tuple(stats))


def format_data_version(fingerprint: tuple[Any, ...]) -> str:
    raw = repr(fingerprint).encode("utf-8", "surrogatepass")
    return hashlib.blake2b(raw, digest_size=12).hexdigest()
