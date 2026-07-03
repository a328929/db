import logging
import re
import sqlite3
from contextlib import suppress

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
_MESSAGE_SEARCH_TERMS_VERSION = "2"
_MESSAGE_SEARCH_TEXT_SQL = search_text_expression()


def _read_message_search_terms_version(cur: sqlite3.Cursor) -> str:
    if not _table_exists(cur, "message_search_terms_meta"):
        return ""
    cur.execute(
        "SELECT value FROM message_search_terms_meta WHERE key = ? LIMIT 1",
        (_MESSAGE_SEARCH_TERMS_VERSION_KEY,),
    )
    row = cur.fetchone()
    if row is None:
        return ""
    return str(row["value"] if isinstance(row, sqlite3.Row) else row[0] or "")


def _write_message_search_terms_version(cur: sqlite3.Cursor) -> None:
    cur.execute(
        """
        INSERT INTO message_search_terms_meta(key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (_MESSAGE_SEARCH_TERMS_VERSION_KEY, _MESSAGE_SEARCH_TERMS_VERSION),
    )


def message_search_terms_are_current(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()
    try:
        return _read_message_search_terms_version(cur) == _MESSAGE_SEARCH_TERMS_VERSION
    finally:
        cur.close()


def _sync_message_search_terms_from_scratch(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    try:
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
        _write_message_search_terms_version(cur)
        conn.commit()
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
    finally:
        cur.close()

    if not has_messages:
        cur = conn.cursor()
        try:
            _write_message_search_terms_version(cur)
            conn.commit()
        finally:
            cur.close()
        return
    if (
        has_data
        and not force_heal
        and index_version == _MESSAGE_SEARCH_TERMS_VERSION
    ):
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
    END;
    """)

    cur.execute("""
    CREATE TRIGGER trg_message_terms_queue_update
    AFTER UPDATE OF content, content_norm ON messages
    WHEN new.search_text_present = 1 BEGIN
        INSERT INTO message_search_terms_rebuild_queue(pk, reason, queued_at)
        VALUES (new.pk, 'update', datetime('now'))
        ON CONFLICT(pk) DO UPDATE SET
            reason = excluded.reason,
            queued_at = excluded.queued_at;
    END;
    """)

    cur.execute("""
    CREATE TRIGGER trg_message_terms_delete
    AFTER DELETE ON messages BEGIN
        DELETE FROM message_search_terms WHERE pk = old.pk;
        DELETE FROM message_search_terms_rebuild_queue WHERE pk = old.pk;
    END;
    """)
