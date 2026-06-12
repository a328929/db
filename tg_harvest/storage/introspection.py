import sqlite3
from typing import Any


def table_columns(cur: sqlite3.Cursor | Any, table_name: str) -> set[str]:
    try:
        cur.execute(f"PRAGMA table_xinfo({table_name})")
    except sqlite3.Error:
        cur.execute(f"PRAGMA table_info({table_name})")
    return {
        str(row["name"] if isinstance(row, sqlite3.Row) or hasattr(row, "keys") else row[1])
        for row in cur.fetchall()
    }
