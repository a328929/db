import sqlite3
from datetime import UTC, datetime

OUTBOX_TABLE = "manticore_search_outbox"
META_TABLE = "manticore_search_meta"

_TRIGGER_NAMES = (
    "trg_manticore_messages_insert",
    "trg_manticore_messages_update",
    "trg_manticore_messages_delete",
    "trg_manticore_media_insert",
    "trg_manticore_media_update",
    "trg_manticore_media_update_old",
    "trg_manticore_media_delete",
)


def create_manticore_outbox_table(
    cur: sqlite3.Cursor, strict_suffix: str = ""
) -> None:
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {OUTBOX_TABLE} (
        pk          INTEGER PRIMARY KEY,
        operation   TEXT NOT NULL CHECK(operation IN ('upsert', 'delete')),
        revision    INTEGER NOT NULL DEFAULT 1,
        attempts    INTEGER NOT NULL DEFAULT 0,
        last_error  TEXT NOT NULL DEFAULT '',
        queued_at   TEXT NOT NULL DEFAULT (datetime('now'))
    ){strict_suffix}
    """)
    cur.execute(f"PRAGMA table_info({OUTBOX_TABLE})")
    existing_columns = {str(row[1]) for row in cur.fetchall()}
    for column_name, column_sql in (
        ("operation", "operation TEXT NOT NULL DEFAULT 'upsert'"),
        ("revision", "revision INTEGER NOT NULL DEFAULT 1"),
        ("attempts", "attempts INTEGER NOT NULL DEFAULT 0"),
        ("last_error", "last_error TEXT NOT NULL DEFAULT ''"),
        ("queued_at", "queued_at TEXT NOT NULL DEFAULT ''"),
    ):
        if column_name not in existing_columns:
            cur.execute(f"ALTER TABLE {OUTBOX_TABLE} ADD COLUMN {column_sql}")
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_manticore_outbox_order "
        f"ON {OUTBOX_TABLE}(queued_at, pk)"
    )
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {META_TABLE} (
        key    TEXT PRIMARY KEY,
        value  TEXT NOT NULL
    ){strict_suffix}
    """)


def _status_key(table: str) -> str:
    return f"index_status:{str(table or '').strip()}"


def set_manticore_index_status(
    conn: sqlite3.Connection, table: str, status: str
) -> None:
    conn.execute(
        f"""
        INSERT INTO {META_TABLE}(key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (_status_key(table), str(status or "unknown")),
    )
    conn.commit()


def get_manticore_index_status(conn: sqlite3.Connection, table: str) -> str:
    row = conn.execute(
        f"SELECT value FROM {META_TABLE} WHERE key = ?", (_status_key(table),)
    ).fetchone()
    return str(row[0] or "") if row is not None else ""


def manticore_index_is_ready(conn: sqlite3.Connection, table: str) -> bool:
    """Return true only when a validated index has no pending source changes.

    SQLite triggers enqueue Manticore updates in the same transaction as a
    message or media mutation. The persisted status may still read ``ready``
    until the worker drains that queue, so checking it alone can expose a
    short window of stale full-text results.
    """
    try:
        if get_manticore_index_status(conn, table) != "ready":
            return False
        return conn.execute(f"SELECT 1 FROM {OUTBOX_TABLE} LIMIT 1").fetchone() is None
    except sqlite3.Error:
        # Missing or unreadable synchronization metadata must never enable a
        # backend whose freshness cannot be established.
        return False


def set_manticore_meta(conn: sqlite3.Connection, key: str, value: object) -> None:
    conn.execute(
        f"""
        INSERT INTO {META_TABLE}(key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (str(key), str(value if value is not None else "")),
    )
    conn.commit()


def get_manticore_meta(conn: sqlite3.Connection, key: str) -> str:
    row = conn.execute(f"SELECT value FROM {META_TABLE} WHERE key = ?", (str(key),)).fetchone()
    return str(row[0] or "") if row is not None else ""


def record_manticore_validation(
    conn: sqlite3.Connection,
    *,
    table: str,
    status: str,
    sqlite_count: int | None,
    manticore_count: int | None,
    outbox_pending: int | None,
    error: str = "",
    source_sample_digest: str = "",
    manticore_sample_digest: str = "",
) -> None:
    now = datetime.now(UTC).replace(microsecond=0).isoformat()
    values = {
        _status_key(table): status,
        f"manticore:{table}:last_validated_at": now,
        f"manticore:{table}:sqlite_document_count": (
            "" if sqlite_count is None else int(sqlite_count)
        ),
        f"manticore:{table}:manticore_document_count": (
            "" if manticore_count is None else int(manticore_count)
        ),
        f"manticore:{table}:outbox_pending": (
            "" if outbox_pending is None else int(outbox_pending)
        ),
        f"manticore:{table}:last_validation_error": error[:500],
        f"manticore:{table}:source_sample_digest": source_sample_digest,
        f"manticore:{table}:manticore_sample_digest": manticore_sample_digest,
    }
    conn.executemany(
        f"""
        INSERT INTO {META_TABLE}(key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        [(key, str(value)) for key, value in values.items()],
    )
    conn.commit()


def _drop_manticore_outbox_triggers(cur: sqlite3.Cursor) -> None:
    for name in _TRIGGER_NAMES:
        cur.execute(f"DROP TRIGGER IF EXISTS {name}")


def _queue_upsert_sql(pk_sql: str, operation: str) -> str:
    return f"""
        INSERT INTO {OUTBOX_TABLE}(pk, operation, revision, attempts, last_error, queued_at)
        VALUES ({pk_sql}, '{operation}', 1, 0, '', datetime('now'))
        ON CONFLICT(pk) DO UPDATE SET
            operation = excluded.operation,
            revision = {OUTBOX_TABLE}.revision + 1,
            attempts = 0,
            last_error = '',
            queued_at = excluded.queued_at;
    """


def configure_manticore_outbox_triggers(
    cur: sqlite3.Cursor, *, enabled: bool
) -> None:
    _drop_manticore_outbox_triggers(cur)
    if not enabled:
        return

    cur.execute(f"""
    CREATE TRIGGER trg_manticore_messages_insert
    AFTER INSERT ON messages BEGIN
        {_queue_upsert_sql('new.pk', 'upsert')}
    END
    """)
    cur.execute(f"""
    CREATE TRIGGER trg_manticore_messages_update
    AFTER UPDATE OF content, content_norm, chat_id, message_id, msg_date_ts,
                    msg_type, grouped_id, is_promo
    ON messages BEGIN
        {_queue_upsert_sql('new.pk', 'upsert')}
    END
    """)
    cur.execute(f"""
    CREATE TRIGGER trg_manticore_messages_delete
    AFTER DELETE ON messages BEGIN
        {_queue_upsert_sql('old.pk', 'delete')}
    END
    """)

    media_pk = (
        "(SELECT pk FROM messages "
        "WHERE chat_id = new.chat_id AND message_id = new.message_id)"
    )
    cur.execute(f"""
    CREATE TRIGGER trg_manticore_media_insert
    AFTER INSERT ON message_media
    WHEN EXISTS (
        SELECT 1 FROM messages
        WHERE chat_id = new.chat_id AND message_id = new.message_id
    ) BEGIN
        {_queue_upsert_sql(media_pk, 'upsert')}
    END
    """)
    cur.execute(f"""
    CREATE TRIGGER trg_manticore_media_update
    AFTER UPDATE OF file_size, duration_sec, media_kind, grouped_id,
                    chat_id, message_id
    ON message_media
    WHEN EXISTS (
        SELECT 1 FROM messages
        WHERE chat_id = new.chat_id AND message_id = new.message_id
    ) BEGIN
        {_queue_upsert_sql(media_pk, 'upsert')}
    END
    """)
    cur.execute(f"""
    CREATE TRIGGER trg_manticore_media_update_old
    AFTER UPDATE OF file_size, duration_sec, media_kind, grouped_id,
                    chat_id, message_id
    ON message_media
    WHEN (old.chat_id IS NOT new.chat_id OR old.message_id IS NOT new.message_id)
     AND EXISTS (
        SELECT 1 FROM messages
        WHERE chat_id = old.chat_id AND message_id = old.message_id
    ) BEGIN
        {_queue_upsert_sql("(SELECT pk FROM messages WHERE chat_id = old.chat_id AND message_id = old.message_id)", 'upsert')}
    END
    """)
    cur.execute(f"""
    CREATE TRIGGER trg_manticore_media_delete
    AFTER DELETE ON message_media
    WHEN EXISTS (
        SELECT 1 FROM messages
        WHERE chat_id = old.chat_id AND message_id = old.message_id
    ) BEGIN
        {_queue_upsert_sql("(SELECT pk FROM messages WHERE chat_id = old.chat_id AND message_id = old.message_id)", 'upsert')}
    END
    """)


def enqueue_all_messages(conn: sqlite3.Connection) -> int:
    cur = conn.cursor()
    try:
        cur.execute(f"""
            INSERT INTO {OUTBOX_TABLE}(
                pk, operation, revision, attempts, last_error, queued_at
            )
            SELECT pk, 'upsert', 1, 0, '', datetime('now')
            FROM messages
            WHERE 1
            ON CONFLICT(pk) DO UPDATE SET
                operation = 'upsert',
                revision = {OUTBOX_TABLE}.revision + 1,
                attempts = 0,
                last_error = '',
                queued_at = excluded.queued_at
        """)
        changed = max(0, int(cur.rowcount or 0))
        conn.commit()
        return changed
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
