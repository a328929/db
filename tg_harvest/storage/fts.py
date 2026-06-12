import logging
import sqlite3

_FTS_INDEX_STATUS_KEY = "fts_index_status"
_FTS_INDEX_STATUS_READY = "ready"
_FTS_INDEX_STATUS_INCOMPLETE = "incomplete"

_EXPECTED_FTS_TRIGGER_MARKERS = {
    "trg_messages_fts_insert": (
        "nullif(new.content_norm, '')",
        "new.content",
    ),
    "trg_messages_fts_delete": (
        "'delete'",
        "old.pk",
        "nullif(old.content_norm, '')",
        "old.content",
    ),
    "trg_messages_fts_update": (
        "'delete'",
        "old.pk",
        "new.pk",
        "nullif(old.content_norm, '')",
        "nullif(new.content_norm, '')",
    ),
}


def _message_search_terms_meta_exists(cur: sqlite3.Cursor) -> bool:
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='message_search_terms_meta' LIMIT 1"
    )
    return cur.fetchone() is not None


def _write_fts_index_status(cur: sqlite3.Cursor, *, ready: bool) -> None:
    if not _message_search_terms_meta_exists(cur):
        return
    cur.execute(
        """
        INSERT INTO message_search_terms_meta(key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """,
        (
            _FTS_INDEX_STATUS_KEY,
            _FTS_INDEX_STATUS_READY if ready else _FTS_INDEX_STATUS_INCOMPLETE,
        ),
    )


def fts_index_is_marked_ready(conn: sqlite3.Connection) -> bool:
    cur = conn.cursor()
    try:
        if not _message_search_terms_meta_exists(cur):
            return False
        cur.execute(
            """
            SELECT value
            FROM message_search_terms_meta
            WHERE key = ?
            LIMIT 1
            """,
            (_FTS_INDEX_STATUS_KEY,),
        )
        row = cur.fetchone()
        if row is None:
            return False
        value = row["value"] if isinstance(row, sqlite3.Row) else row[0]
        return str(value or "") == _FTS_INDEX_STATUS_READY
    finally:
        cur.close()


def _create_fts_table(cur: sqlite3.Cursor) -> None:
    cur.execute("""
    CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts
    USING fts5(
        content,
        content='messages',
        content_rowid='pk',
        tokenize='trigram'
    )
    """)


def _fts_triggers_are_current(cur: sqlite3.Cursor) -> bool:
    for trigger_name, markers in _EXPECTED_FTS_TRIGGER_MARKERS.items():
        cur.execute(
            """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'trigger' AND name = ?
            LIMIT 1
            """,
            (trigger_name,),
        )
        row = cur.fetchone()
        if row is None:
            return False
        trigger_sql = str(row["sql"] if isinstance(row, sqlite3.Row) else row[0] or "")
        normalized_sql = trigger_sql.lower()
        if not all(marker in normalized_sql for marker in markers):
            return False
    return True


def _drop_fts_triggers(cur: sqlite3.Cursor) -> None:
    for trigger_name in (
        "trg_messages_fts_insert",
        "trg_messages_fts_delete",
        "trg_messages_fts_update",
    ):
        cur.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")


def _create_fts_triggers(cur: sqlite3.Cursor) -> None:
    """
    重构 FTS5 触发器，确保数据在增删改时绝对同步。
    逻辑：同步内容时优先取标准化字段 content_norm。
    """
    _drop_fts_triggers(cur)

    cur.execute("""
    CREATE TRIGGER trg_messages_fts_insert AFTER INSERT ON messages BEGIN
        INSERT INTO messages_fts(rowid, content)
        VALUES (new.pk, COALESCE(NULLIF(new.content_norm, ''), new.content, ''));
    END;
    """)

    cur.execute("""
    CREATE TRIGGER trg_messages_fts_delete AFTER DELETE ON messages BEGIN
        INSERT INTO messages_fts(messages_fts, rowid, content)
        VALUES ('delete', old.pk, COALESCE(NULLIF(old.content_norm, ''), old.content, ''));
    END;
    """)

    cur.execute("""
    CREATE TRIGGER trg_messages_fts_update AFTER UPDATE OF content, content_norm ON messages BEGIN
        INSERT INTO messages_fts(messages_fts, rowid, content)
        VALUES ('delete', old.pk, COALESCE(NULLIF(old.content_norm, ''), old.content, ''));
        INSERT INTO messages_fts(rowid, content)
        VALUES (new.pk, COALESCE(NULLIF(new.content_norm, ''), new.content, ''));
    END;
    """)


def _create_fts_schema(cur: sqlite3.Cursor) -> None:
    _create_fts_table(cur)
    _create_fts_triggers(cur)


def _sync_fts_from_scratch(cur: sqlite3.Cursor, *, batch_size: int = 50000) -> None:
    """从 messages 表全量同步数据到 FTS 表。"""
    _drop_fts_triggers(cur)
    cur.execute("DROP TABLE IF EXISTS messages_fts")
    _create_fts_table(cur)
    _write_fts_index_status(cur, ready=False)
    # Keep triggers installed while historical rows are backfilled. The rebuild
    # commits in batches, so this prevents a crash or external writer from
    # leaving new message changes outside the FTS index mid-rebuild.
    _create_fts_triggers(cur)

    conn = cur.connection
    last_pk = 0
    batch_size = max(1, int(batch_size))
    while True:
        cur.execute(
            """
            SELECT pk
            FROM messages
            WHERE pk > ?
            ORDER BY pk ASC
            LIMIT ?
            """,
            (last_pk, batch_size),
        )
        pk_rows = cur.fetchall()
        if not pk_rows:
            break

        next_last_pk = int(
            pk_rows[-1]["pk"] if isinstance(pk_rows[-1], sqlite3.Row) else pk_rows[-1][0]
        )
        cur.execute(
            """
            INSERT INTO messages_fts(rowid, content)
            SELECT pk, COALESCE(NULLIF(content_norm, ''), content, '')
            FROM messages
            WHERE pk > ? AND pk <= ?
              AND NOT EXISTS (
                  SELECT 1 FROM messages_fts_docsize d WHERE d.id = messages.pk
              )
            ORDER BY pk ASC
            """,
            (last_pk, next_last_pk),
        )
        conn.commit()
        try:
            cur.execute("PRAGMA wal_checkpoint(PASSIVE)")
        except sqlite3.Error:
            logging.debug("FTS 分批重建期间 WAL checkpoint 跳过", exc_info=True)
        last_pk = next_last_pk
    _write_fts_index_status(cur, ready=True)


def _count_table_rows(cur: sqlite3.Cursor, table_name: str) -> int:
    cur.execute(f"SELECT COUNT(*) AS c FROM {table_name}")
    row = cur.fetchone()
    return int(row["c"] if isinstance(row, sqlite3.Row) else row[0] or 0)


def _fts_index_row_count_matches_messages(cur: sqlite3.Cursor) -> bool:
    message_count = _count_table_rows(cur, "messages")
    indexed_count = _count_table_rows(cur, "messages_fts_docsize")
    return indexed_count == message_count


def _heal_fts_if_needed(
    cur: sqlite3.Cursor,
    force_heal: bool = False,
    rebuild_reason: str = "",
) -> None:
    """
    检查 FTS5 外部内容索引是否真实完整。

    注意：messages_fts 是 external-content 虚表，普通 SELECT/COUNT 会回读
    messages 内容表，即使倒排索引完全为空也会“看起来有数据”。因此这里必须
    检查 shadow docsize 表，它才代表已经写入 FTS 索引的 rowid 数。
    """
    try:
        index_matches_messages = _fts_index_row_count_matches_messages(cur)
        if force_heal or not index_matches_messages:
            if force_heal:
                if rebuild_reason:
                    logging.warning("检测到 %s，正在全量重建 FTS 索引", rebuild_reason)
                else:
                    logging.warning("配置强制开启 FTS 索引修复...")
            else:
                logging.warning("检测到 FTS 索引与 messages 表不一致，正在全量重建")

            _sync_fts_from_scratch(cur)
            logging.info("FTS 索引同步成功完成")
        else:
            _write_fts_index_status(cur, ready=True)
            logging.debug("FTS 索引已存在且行数一致，跳过全量重建")

    except sqlite3.Error as exc:
        _write_fts_index_status(cur, ready=False)
        logging.warning("FTS 检查阶段遇到数据库错误，正在尝试全量重建: %s", exc)
        _sync_fts_from_scratch(cur)
