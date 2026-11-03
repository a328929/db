"""Small indexes and scan watermarks used by exact, destructive cleanup jobs."""

import sqlite3
from dataclasses import dataclass

from tg_harvest.storage.connection import synchronized_write

EMPTY_SEARCH_TEXT_SQL = (
    "NULLIF(COALESCE(NULLIF(TRIM(content_norm), ''), TRIM(content), ''), '') IS NULL"
)
_MAX_SAVED_SCANS = 128


def create_cleanup_scan_tables(cur: sqlite3.Cursor, strict_suffix: str) -> None:
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS admin_cleanup_scan_state (
            singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
            generation INTEGER NOT NULL DEFAULT 0,
            max_inserted_pk INTEGER NOT NULL
        ){strict_suffix}
    """)
    # MAX(pk) uses the integer primary key, even on an existing large database.
    cur.execute("""
        INSERT OR IGNORE INTO admin_cleanup_scan_state(singleton, max_inserted_pk)
        SELECT 1, COALESCE(MAX(pk), 0) FROM messages
    """)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS admin_cleanup_scans (
            scope_key TEXT NOT NULL,
            norm_pattern TEXT NOT NULL,
            raw_pattern TEXT NOT NULL,
            generation INTEGER NOT NULL,
            upper_pk INTEGER NOT NULL,
            PRIMARY KEY(scope_key, norm_pattern, raw_pattern)
        ){strict_suffix}
    """)


def configure_cleanup_scan_triggers(cur: sqlite3.Cursor) -> None:
    # Appending messages preserves earlier scans. Explicitly reusing an old PK,
    # changing searchable text, or moving a message invalidates all watermarks.
    # Deletions cannot introduce a match and therefore need no tracking.
    cur.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_cleanup_messages_insert
        AFTER INSERT ON messages BEGIN
            UPDATE admin_cleanup_scan_state
            SET generation = generation + (new.pk <= max_inserted_pk),
                max_inserted_pk = MAX(max_inserted_pk, new.pk)
            WHERE singleton = 1;
        END
    """)
    cur.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_cleanup_messages_update
        AFTER UPDATE OF pk, chat_id, content, content_norm ON messages
        WHEN old.pk IS NOT new.pk OR old.chat_id IS NOT new.chat_id
          OR old.content IS NOT new.content OR old.content_norm IS NOT new.content_norm
        BEGIN
            UPDATE admin_cleanup_scan_state
            SET generation = generation + 1,
                max_inserted_pk = MAX(max_inserted_pk, new.pk)
            WHERE singleton = 1;
        END
    """)


@dataclass(frozen=True)
class CleanupScanWindow:
    scope_key: str
    patterns: tuple[str, str]
    generation: int
    after_pk: int | None
    upper_pk: int


def load_cleanup_scan_window(
    cur: sqlite3.Cursor, scope_key: str, patterns: tuple[str, str]
) -> CleanupScanWindow:
    row = cur.execute(
        """
        SELECT state.generation, state.max_inserted_pk, scans.upper_pk
        FROM admin_cleanup_scan_state AS state
        LEFT JOIN admin_cleanup_scans AS scans
          ON scans.scope_key = ? AND scans.norm_pattern = ? AND scans.raw_pattern = ?
         AND scans.generation = state.generation
        WHERE state.singleton = 1
        """,
        (scope_key, *patterns),
    ).fetchone()
    return CleanupScanWindow(
        scope_key=scope_key,
        patterns=patterns,
        generation=int(row[0]),
        after_pk=None if row[2] is None else int(row[2]),
        upper_pk=int(row[1]),
    )


@synchronized_write
def save_cleanup_scan_window(
    conn: sqlite3.Connection, window: CleanupScanWindow
) -> None:
    # Save only after all candidate deletions and summary repairs succeeded.
    # Keep the captured generation: edits during a scan must invalidate it too.
    try:
        conn.execute(
            """
            INSERT INTO admin_cleanup_scans(
                scope_key, norm_pattern, raw_pattern, generation, upper_pk
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(scope_key, norm_pattern, raw_pattern) DO UPDATE SET
                generation = excluded.generation,
                upper_pk = excluded.upper_pk
            """,
            (window.scope_key, *window.patterns, window.generation, window.upper_pk),
        )
        conn.execute(
            """
            DELETE FROM admin_cleanup_scans WHERE rowid IN (
                SELECT rowid FROM admin_cleanup_scans ORDER BY rowid DESC LIMIT -1 OFFSET ?
            )
            """,
            (_MAX_SAVED_SCANS,),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
