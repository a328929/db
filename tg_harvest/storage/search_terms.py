import logging
import re
import sqlite3
from contextlib import suppress
from datetime import UTC, datetime

from tg_harvest.storage.connection import synchronized_write
from tg_harvest.storage.search_text_state import search_text_expression


def _table_exists(cur: sqlite3.Cursor, table_name: str) -> bool:
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    )
    return cur.fetchone() is not None


_CJK_CHAR_RE = re.compile(
    r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\U00020000-\U0002ceaf]"
)


def extract_cjk_bigrams(text: str) -> list[str]:
    raw = str(text or "")
    compact = "".join(raw.split())
    if len(compact) < 2:
        return []

    seen = set()
    out: list[str] = []
    for idx in range(len(compact) - 1):
        token = compact[idx : idx + 2]
        if len(token) != 2:
            continue
        if not (_CJK_CHAR_RE.fullmatch(token[0]) and _CJK_CHAR_RE.fullmatch(token[1])):
            continue
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def extract_cjk_search_terms(text: str) -> list[str]:
    raw = str(text or "")
    compact = "".join(raw.split())
    if not compact:
        return []

    seen = set()
    out: list[str] = []

    for ch in compact:
        if not _CJK_CHAR_RE.fullmatch(ch):
            continue
        if ch in seen:
            continue
        seen.add(ch)
        out.append(ch)

    for token in extract_cjk_bigrams(compact):
        if token in seen:
            continue
        seen.add(token)
        out.append(token)

    return out


_MESSAGE_SEARCH_TERMS_VERSION_KEY = "cjk_terms_version"
_MESSAGE_SEARCH_TERMS_VERSION = "3"
_MESSAGE_SEARCH_TERMS_PREVIOUS_VERSION = "2"
_MESSAGE_SEARCH_TERMS_REBUILD_STATE_KEY = "cjk_terms_rebuild_state"
_MESSAGE_SEARCH_TERMS_LEGACY_BACKFILL_MODE_KEY = "cjk_terms_backfill_mode"
_MESSAGE_SEARCH_TERMS_LEGACY_BACKFILL_LAST_PK_KEY = "cjk_terms_backfill_last_pk"
_MESSAGE_SEARCH_TERMS_HAS_TERMS_KEY = "cjk_terms_has_terms"
_MESSAGE_SEARCH_TERMS_LAST_MAINTENANCE_AT_KEY = "cjk_terms_last_maintenance_at"
_MESSAGE_SEARCH_TERMS_LAST_MAINTENANCE_RESULT_KEY = "cjk_terms_last_maintenance_result"
_MESSAGE_SEARCH_TERMS_LAST_REBUILD_AT_KEY = "cjk_terms_last_rebuild_at"
_MESSAGE_SEARCH_TERMS_QUEUE_LENGTH_KEY = "cjk_terms_queue_length"
_MESSAGE_SEARCH_TEXT_SQL = search_text_expression()


def _read_message_search_terms_meta(cur: sqlite3.Cursor, key: str) -> str:
    if not _table_exists(cur, "message_search_terms_meta"):
        return ""
    cur.execute(
        "SELECT value FROM message_search_terms_meta WHERE key = ? LIMIT 1",
        (key,),
    )
    row = cur.fetchone()
    if row is None:
        return ""
    return str(row["value"] if isinstance(row, sqlite3.Row) else row[0] or "")


def _write_message_search_terms_meta(
    cur: sqlite3.Cursor, key: str, value: str
) -> None:
    cur.execute(
        """
        INSERT INTO message_search_terms_meta(key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (key, value),
    )


def _delete_message_search_terms_meta(cur: sqlite3.Cursor, key: str) -> None:
    if not _table_exists(cur, "message_search_terms_meta"):
        return
    cur.execute("DELETE FROM message_search_terms_meta WHERE key = ?", (key,))


def _read_message_search_terms_version(cur: sqlite3.Cursor) -> str:
    return _read_message_search_terms_meta(cur, _MESSAGE_SEARCH_TERMS_VERSION_KEY)


def _write_message_search_terms_version(cur: sqlite3.Cursor) -> None:
    _write_message_search_terms_meta(
        cur,
        _MESSAGE_SEARCH_TERMS_VERSION_KEY,
        _MESSAGE_SEARCH_TERMS_VERSION,
    )


def _message_search_terms_rebuild_state(cur: sqlite3.Cursor) -> str:
    state = _read_message_search_terms_meta(
        cur, _MESSAGE_SEARCH_TERMS_REBUILD_STATE_KEY
    )
    if state:
        return state
    if (
        _read_message_search_terms_meta(
            cur, _MESSAGE_SEARCH_TERMS_LEGACY_BACKFILL_MODE_KEY
        )
        or _read_message_search_terms_meta(
            cur, _MESSAGE_SEARCH_TERMS_LEGACY_BACKFILL_LAST_PK_KEY
        )
    ):
        return "legacy"
    return ""


def _mark_message_search_terms_rebuilding(cur: sqlite3.Cursor) -> None:
    _write_message_search_terms_meta(
        cur, _MESSAGE_SEARCH_TERMS_REBUILD_STATE_KEY, "full"
    )


def _write_message_search_terms_presence(cur: sqlite3.Cursor) -> None:
    cur.execute("SELECT 1 FROM message_search_terms LIMIT 1")
    _write_message_search_terms_meta(
        cur,
        _MESSAGE_SEARCH_TERMS_HAS_TERMS_KEY,
        "1" if cur.fetchone() is not None else "0",
    )


def _message_search_terms_are_known_empty(cur: sqlite3.Cursor) -> bool:
    return (
        _read_message_search_terms_meta(cur, _MESSAGE_SEARCH_TERMS_HAS_TERMS_KEY)
        == "0"
    )


def _finish_message_search_terms_rebuild(cur: sqlite3.Cursor) -> None:
    _write_message_search_terms_version(cur)
    _write_message_search_terms_presence(cur)
    finished_at = datetime.now(UTC).replace(microsecond=0).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    _write_message_search_terms_meta(
        cur, _MESSAGE_SEARCH_TERMS_LAST_REBUILD_AT_KEY, finished_at
    )
    _write_message_search_terms_meta(
        cur, _MESSAGE_SEARCH_TERMS_LAST_MAINTENANCE_AT_KEY, finished_at
    )
    _write_message_search_terms_meta(
        cur, _MESSAGE_SEARCH_TERMS_LAST_MAINTENANCE_RESULT_KEY, "rebuilt"
    )
    for key in (
        _MESSAGE_SEARCH_TERMS_REBUILD_STATE_KEY,
        _MESSAGE_SEARCH_TERMS_LEGACY_BACKFILL_MODE_KEY,
        _MESSAGE_SEARCH_TERMS_LEGACY_BACKFILL_LAST_PK_KEY,
    ):
        _delete_message_search_terms_meta(cur, key)


def message_search_terms_are_current(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()
    try:
        if not _table_exists(cur, "message_search_terms_meta"):
            return False
        keys = (
            _MESSAGE_SEARCH_TERMS_VERSION_KEY,
            _MESSAGE_SEARCH_TERMS_REBUILD_STATE_KEY,
            _MESSAGE_SEARCH_TERMS_LEGACY_BACKFILL_MODE_KEY,
            _MESSAGE_SEARCH_TERMS_LEGACY_BACKFILL_LAST_PK_KEY,
        )
        placeholders = ", ".join("?" for _ in keys)
        cur.execute(
            f"SELECT key, value FROM message_search_terms_meta WHERE key IN ({placeholders})",
            keys,
        )
        values = {
            str(row["key"] if isinstance(row, sqlite3.Row) else row[0]): str(
                row["value"] if isinstance(row, sqlite3.Row) else row[1]
            )
            for row in cur.fetchall()
        }
        return (
            values.get(_MESSAGE_SEARCH_TERMS_VERSION_KEY) == _MESSAGE_SEARCH_TERMS_VERSION
            and not values.get(_MESSAGE_SEARCH_TERMS_REBUILD_STATE_KEY)
            and not values.get(_MESSAGE_SEARCH_TERMS_LEGACY_BACKFILL_MODE_KEY)
            and not values.get(_MESSAGE_SEARCH_TERMS_LEGACY_BACKFILL_LAST_PK_KEY)
        )
    finally:
        cur.close()


def _sync_message_search_terms_from_scratch(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    try:
        # Persist this before deleting anything. A later startup must never
        # mistake a partially committed batch for a complete index.
        _mark_message_search_terms_rebuilding(cur)
        conn.commit()
        cur.execute("DELETE FROM message_search_terms")

        last_pk = 0
        batch_size = 5000
        while True:
            cur.execute(
                f"""
                SELECT pk, {_MESSAGE_SEARCH_TEXT_SQL} AS search_text
                FROM messages
                WHERE pk > ?
                ORDER BY pk ASC
                LIMIT ?
                """,
                (last_pk, batch_size),
            )
            rows = cur.fetchall()
            if not rows:
                break

            inserts: list[tuple[str, int]] = []
            for row in rows:
                pk = int(row["pk"])
                last_pk = pk
                for token in extract_cjk_search_terms(str(row["search_text"] or "")):
                    inserts.append((token, pk))

            if inserts:
                cur.executemany(
                    "INSERT OR IGNORE INTO message_search_terms(term, pk) VALUES (?, ?)",
                    inserts,
                )
            conn.commit()
        _finish_message_search_terms_rebuild(cur)
        conn.commit()
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        cur.close()


def _migrate_v2_message_search_terms(conn: sqlite3.Connection) -> int:
    """Remove terms left behind by v2 when a message's text was cleared."""
    cur = conn.cursor()
    try:
        if not conn.in_transaction:
            cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            """
            DELETE FROM message_search_terms
            WHERE pk IN (
                SELECT pk
                FROM messages
                WHERE search_text_present = 0
            )
            """
        )
        removed_count = max(0, int(cur.rowcount or 0))
        conn.commit()
        return removed_count
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        cur.close()


def _v2_message_search_terms_may_be_incomplete(conn: sqlite3.Connection) -> bool:
    """Detect the prefix left by an interrupted pre-v3 full rebuild."""
    cur = conn.cursor()
    try:
        cur.execute("SELECT COALESCE(MAX(pk), 0) AS last_pk FROM message_search_terms")
        row = cur.fetchone()
        last_indexed_pk = int(
            row["last_pk"] if isinstance(row, sqlite3.Row) else row[0]
        )
        queue_filter = ""
        if _table_exists(cur, "message_search_terms_rebuild_queue"):
            queue_filter = """
                AND NOT EXISTS (
                    SELECT 1
                    FROM message_search_terms_rebuild_queue q
                    WHERE q.pk = m.pk
                )
            """
        cur.execute(
            f"""
            SELECT m.pk, {search_text_expression('m')} AS search_text
            FROM messages m
            WHERE m.pk > ?
            {queue_filter}
            ORDER BY m.pk ASC
            """,
            (last_indexed_pk,),
        )
        while True:
            rows = cur.fetchmany(5000)
            if not rows:
                return False
            for row in rows:
                if extract_cjk_search_terms(str(row["search_text"] or "")):
                    return True
    finally:
        cur.close()


def _finalize_v2_message_search_terms_migration(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    try:
        _finish_message_search_terms_rebuild(cur)
        conn.commit()
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        cur.close()


def _heal_message_search_terms_if_needed(
    conn: sqlite3.Connection, *, force_heal: bool = False
) -> None:
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM message_search_terms LIMIT 1")
        has_data = cur.fetchone() is not None
        cur.execute("SELECT 1 FROM messages LIMIT 1")
        has_messages = cur.fetchone() is not None
        index_version = _read_message_search_terms_version(cur)
        rebuild_state = _message_search_terms_rebuild_state(cur)
        known_empty = _message_search_terms_are_known_empty(cur)
    finally:
        cur.close()

    if not has_messages:
        cur = conn.cursor()
        try:
            _finish_message_search_terms_rebuild(cur)
            conn.commit()
        finally:
            cur.close()
        return
    if (
        has_data
        and not force_heal
        and not rebuild_state
        and index_version == _MESSAGE_SEARCH_TERMS_VERSION
    ):
        return

    # An index with no CJK terms is valid for a Latin-only message corpus. The
    # marker is written only after a complete rebuild or queue drain, so old or
    # interrupted databases still use the conservative full recovery path.
    if (
        not has_data
        and known_empty
        and not force_heal
        and not rebuild_state
        and index_version == _MESSAGE_SEARCH_TERMS_VERSION
    ):
        return

    if rebuild_state:
        logging.warning("检测到未完成的中文短词索引重建，正在从头恢复...")
        _sync_message_search_terms_from_scratch(conn)
        return

    if (
        has_data
        and not force_heal
        and index_version == _MESSAGE_SEARCH_TERMS_PREVIOUS_VERSION
        and _MESSAGE_SEARCH_TERMS_VERSION == "3"
    ):
        removed_count = _migrate_v2_message_search_terms(conn)
        if _v2_message_search_terms_may_be_incomplete(conn):
            logging.warning("检测到旧版中文短词索引可能中断，正在完整重建")
            _sync_message_search_terms_from_scratch(conn)
            return
        _finalize_v2_message_search_terms_migration(conn)
        logging.info(
            "已清理 %s 条由旧版触发器遗留的中文短词索引记录",
            removed_count,
        )
        return

    if force_heal:
        logging.warning("配置强制开启中文短词辅助索引重建...")
    elif has_data:
        logging.info("检测到中文短词辅助索引版本不一致，正在重建当前版本索引")
    else:
        logging.info("检测到中文短词辅助索引为空，正在同步当前版本索引")

    _sync_message_search_terms_from_scratch(conn)


@synchronized_write
def drain_message_search_terms_rebuild_queue(
    conn: sqlite3.Connection, *, batch_size: int = 5000
) -> int:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='message_search_terms_rebuild_queue' LIMIT 1"
        )
        if cur.fetchone() is None:
            return 0

        cur.execute(
            "SELECT pk FROM message_search_terms_rebuild_queue ORDER BY queued_at ASC, pk ASC LIMIT ?",
            (max(1, int(batch_size)),),
        )
        queued_rows = cur.fetchall()
        if not queued_rows:
            if _table_exists(cur, "message_search_terms_meta"):
                _write_message_search_terms_meta(
                    cur, _MESSAGE_SEARCH_TERMS_QUEUE_LENGTH_KEY, "0"
                )
                conn.commit()
            return 0

        pks = [int(row["pk"]) for row in queued_rows]
        placeholders = ",".join(["?"] * len(pks))

        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            f"""
            SELECT pk, {_MESSAGE_SEARCH_TEXT_SQL} AS search_text
            FROM messages
            WHERE pk IN ({placeholders})
            """,
            pks,
        )
        rows = cur.fetchall()

        cur.execute(
            f"DELETE FROM message_search_terms WHERE pk IN ({placeholders})",
            pks,
        )

        inserts: list[tuple[str, int]] = []
        for row in rows:
            pk = int(row["pk"])
            for token in extract_cjk_search_terms(str(row["search_text"] or "")):
                inserts.append((token, pk))
        if inserts:
            cur.executemany(
                "INSERT OR IGNORE INTO message_search_terms(term, pk) VALUES (?, ?)",
                inserts,
            )

        cur.execute(
            f"DELETE FROM message_search_terms_rebuild_queue WHERE pk IN ({placeholders})",
            pks,
        )
        cur.execute("SELECT 1 FROM message_search_terms_rebuild_queue LIMIT 1")
        if cur.fetchone() is None:
            _write_message_search_terms_meta(
                cur, _MESSAGE_SEARCH_TERMS_QUEUE_LENGTH_KEY, "0"
            )
        _write_message_search_terms_presence(cur)
        maintained_at = datetime.now(UTC).replace(microsecond=0).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        _write_message_search_terms_meta(
            cur,
            _MESSAGE_SEARCH_TERMS_LAST_MAINTENANCE_AT_KEY,
            maintained_at,
        )
        _write_message_search_terms_meta(
            cur,
            _MESSAGE_SEARCH_TERMS_LAST_MAINTENANCE_RESULT_KEY,
            f"drained:{len(pks)}",
        )
        conn.commit()
        return len(pks)
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        cur.close()


def _create_message_search_terms_queue_triggers(cur: sqlite3.Cursor) -> None:
    for trigger_name in (
        "trg_message_terms_queue_insert",
        "trg_message_terms_queue_update",
        "trg_message_terms_delete",
        "trg_message_terms_queue_count_insert",
        "trg_message_terms_queue_count_delete",
    ):
        cur.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")

    cur.execute("""
    CREATE TRIGGER trg_message_terms_queue_insert
    AFTER INSERT ON messages
    WHEN new.search_text_present = 1 BEGIN
        INSERT INTO message_search_terms_rebuild_queue(pk, reason, queued_at)
        VALUES (new.pk, 'insert', datetime('now'))
        ON CONFLICT(pk) DO UPDATE SET
            reason = excluded.reason,
            queued_at = excluded.queued_at;
        DELETE FROM message_search_terms_meta
        WHERE key = 'cjk_terms_has_terms';
    END;
    """)

    cur.execute("""
    CREATE TRIGGER trg_message_terms_queue_update
    AFTER UPDATE OF content, content_norm ON messages
    WHEN old.search_text_present = 1 OR new.search_text_present = 1 BEGIN
        INSERT INTO message_search_terms_rebuild_queue(pk, reason, queued_at)
        VALUES (new.pk, 'update', datetime('now'))
        ON CONFLICT(pk) DO UPDATE SET
            reason = excluded.reason,
            queued_at = excluded.queued_at;
        DELETE FROM message_search_terms_meta
        WHERE key = 'cjk_terms_has_terms';
    END;
    """)

    cur.execute("""
    CREATE TRIGGER trg_message_terms_delete
    AFTER DELETE ON messages BEGIN
        DELETE FROM message_search_terms WHERE pk = old.pk;
        DELETE FROM message_search_terms_rebuild_queue WHERE pk = old.pk;
    END;
    """)

    cur.execute("""
    CREATE TRIGGER trg_message_terms_queue_count_insert
    AFTER INSERT ON message_search_terms_rebuild_queue BEGIN
        INSERT INTO message_search_terms_meta(key, value)
        VALUES ('cjk_terms_queue_length', '1')
        ON CONFLICT(key) DO UPDATE SET
            value = CASE
                WHEN message_search_terms_meta.value = 'unknown' THEN 'unknown'
                ELSE CAST(CAST(message_search_terms_meta.value AS INTEGER) + 1 AS TEXT)
            END;
    END;
    """)

    cur.execute("""
    CREATE TRIGGER trg_message_terms_queue_count_delete
    AFTER DELETE ON message_search_terms_rebuild_queue BEGIN
        INSERT INTO message_search_terms_meta(key, value)
        VALUES ('cjk_terms_queue_length', '0')
        ON CONFLICT(key) DO UPDATE SET
            value = CASE
                WHEN message_search_terms_meta.value = 'unknown' THEN 'unknown'
                ELSE CAST(MAX(0, CAST(message_search_terms_meta.value AS INTEGER) - 1) AS TEXT)
            END;
    END;
    """)

    cur.execute("SELECT 1 FROM message_search_terms_rebuild_queue LIMIT 1")
    if cur.fetchone() is None:
        _write_message_search_terms_meta(
            cur, _MESSAGE_SEARCH_TERMS_QUEUE_LENGTH_KEY, "0"
        )
    elif not _read_message_search_terms_meta(
        cur, _MESSAGE_SEARCH_TERMS_QUEUE_LENGTH_KEY
    ):
        _write_message_search_terms_meta(
            cur, _MESSAGE_SEARCH_TERMS_QUEUE_LENGTH_KEY, "unknown"
        )
