# -*- coding: utf-8 -*-
import logging
import sqlite3


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


def _sync_fts_from_scratch(cur: sqlite3.Cursor) -> None:
    """从 messages 表全量同步数据到 FTS 表。"""
    _drop_fts_triggers(cur)
    cur.execute("DROP TABLE IF EXISTS messages_fts")
    _create_fts_table(cur)
    cur.execute("""
        INSERT INTO messages_fts(rowid, content)
        SELECT pk, COALESCE(NULLIF(content_norm, ''), content, '')
        FROM messages
    """)
    _create_fts_triggers(cur)


def _heal_fts_if_needed(cur: sqlite3.Cursor, force_heal: bool = False) -> None:
    """
    优化后的 FTS 检查逻辑：
    不再每次启动都执行耗时的全表 COUNT(*)。
    仅在 FTS 表完全为空，或明确设置了 force_heal=True 时才启动全量同步。
    """
    try:
        cur.execute("SELECT 1 FROM messages_fts LIMIT 1")
        has_data = cur.fetchone() is not None

        if not has_data or force_heal:
            if force_heal:
                logging.warning("配置强制开启 FTS 索引修复...")
            else:
                logging.info(
                    "检测到 FTS 索引为空，正在执行首次同步（大数据库可能耗时几秒）..."
                )

            _sync_fts_from_scratch(cur)
            logging.info("FTS 索引同步成功完成")
        else:
            logging.debug("FTS 索引已存在，跳过耗时的全量计数校验")

    except sqlite3.Error as exc:
        logging.error(f"FTS 检查阶段遇到数据库错误: {exc}")
