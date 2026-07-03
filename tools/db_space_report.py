#!/usr/bin/env python3
import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _format_bytes(value: int) -> str:
    size = float(value or 0)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} TiB"


def _fetch_one_int(cur: sqlite3.Cursor, sql: str) -> int:
    cur.execute(sql)
    row = cur.fetchone()
    return int((row[0] if row else 0) or 0)


def _print_kv(label: str, value: object) -> None:
    print(f"{label}: {value}")


def _print_db_overview(cur: sqlite3.Cursor, db_path: Path) -> None:
    page_size = _fetch_one_int(cur, "PRAGMA page_size")
    page_count = _fetch_one_int(cur, "PRAGMA page_count")
    freelist_count = _fetch_one_int(cur, "PRAGMA freelist_count")
    _print_kv("database", db_path)
    _print_kv("file_size", _format_bytes(db_path.stat().st_size if db_path.exists() else 0))
    _print_kv("page_size", page_size)
    _print_kv("page_count", page_count)
    _print_kv("freelist_pages", freelist_count)
    _print_kv("freelist_bytes", _format_bytes(page_size * freelist_count))


def _print_row_counts(cur: sqlite3.Cursor) -> None:
    print("\nrow_counts:")
    for table in (
        "chats",
        "messages",
        "message_media",
        "media_groups",
        "message_search_terms",
        "message_search_terms_rebuild_queue",
        "messages_fts_docsize",
    ):
        try:
            count = _fetch_one_int(cur, f"SELECT COUNT(*) FROM {table}")
        except sqlite3.Error:
            count = 0
        print(f"  {table}: {count}")


def _print_dbstat_top(cur: sqlite3.Cursor, limit: int) -> None:
    print("\nobjects_by_size:")
    try:
        cur.execute(
            """
            SELECT name, SUM(pgsize) AS bytes
            FROM dbstat
            GROUP BY name
            ORDER BY bytes DESC
            LIMIT ?
            """,
            (int(limit),),
        )
    except sqlite3.Error as exc:
        print(f"  dbstat_unavailable: {exc}")
        return
    for name, size_bytes in cur.fetchall():
        print(f"  {name}: {_format_bytes(int(size_bytes or 0))}")


def _print_text_storage(cur: sqlite3.Cursor) -> None:
    print("\ntext_storage:")
    queries = {
        "messages.content": "SELECT SUM(LENGTH(COALESCE(content, ''))) FROM messages",
        "messages.content_norm": "SELECT SUM(LENGTH(COALESCE(content_norm, ''))) FROM messages",
        "messages.equal_content_norm_rows": """
            SELECT COUNT(*)
            FROM messages
            WHERE COALESCE(content_norm, '') <> ''
              AND content_norm = content
        """,
        "media_groups.captions_concat": "SELECT SUM(LENGTH(COALESCE(captions_concat, ''))) FROM media_groups",
        "media_groups.caption_norm": "SELECT SUM(LENGTH(COALESCE(caption_norm, ''))) FROM media_groups",
        "message_media.meta_json": "SELECT SUM(LENGTH(COALESCE(meta_json, ''))) FROM message_media",
    }
    for label, sql in queries.items():
        try:
            value = _fetch_one_int(cur, sql)
        except sqlite3.Error:
            value = 0
        if label.endswith("_rows"):
            print(f"  {label}: {value}")
        else:
            print(f"  {label}: {_format_bytes(value)}")


def main() -> int:
    from tg_harvest.config import CFG

    parser = argparse.ArgumentParser(description="Report SQLite database space usage.")
    parser.add_argument("--db", default=str(CFG.db_name), help="SQLite database path")
    parser.add_argument("--top", type=int, default=30, help="Number of dbstat objects to show")
    args = parser.parse_args()

    db_path = Path(args.db).expanduser().resolve()
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        cur = conn.cursor()
        _print_db_overview(cur, db_path)
        _print_row_counts(cur)
        _print_dbstat_top(cur, args.top)
        _print_text_storage(cur)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
