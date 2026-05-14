# -*- coding: utf-8 -*-
import logging
import sys
import os

# 增加对上级目录的引用，以便导入 tg_harvest
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tg_harvest.storage.connection import ensure_configured_db
from tg_harvest.config import CFG


def fix_missing_filenames():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s"
    )

    db_name = CFG.db_name
    logging.info(f"正在连接数据库: {db_name}")
    conn, feats = ensure_configured_db(cfg=CFG)
    cur = conn.cursor()

    try:
        # 查找内容为空但有文件名的消息
        cur.execute("""
            SELECT m.chat_id, m.message_id, mm.file_name
            FROM messages m
            JOIN message_media mm ON m.chat_id = mm.chat_id AND m.message_id = mm.message_id
            WHERE (m.content IS NULL OR m.content = '')
              AND (mm.file_name IS NOT NULL AND mm.file_name <> '')
        """)

        rows = cur.fetchall()
        if not rows:
            logging.info("未发现需要修复的消息。")
            return

        logging.info(f"发现 {len(rows)} 条消息需要修复。")

        for row in rows:
            chat_id = row["chat_id"]
            msg_id = row["message_id"]
            file_name = row["file_name"]

            # 更新 content 和 content_norm (为了简单起见，这里直接更新 messages 表)
            # 这里的更新会通过数据库触发器同步到 FTS（如果触发器存在）
            # 或者我们之后手动重建 FTS
            cur.execute(
                """
                UPDATE messages 
                SET content = ?, content_norm = ? 
                WHERE chat_id = ? AND message_id = ?
            """,
                (file_name, file_name, chat_id, msg_id),
            )

        conn.commit()
        logging.info("消息内容已更新。")

        # 重建 FTS 索引
        if feats.supports_fts5:
            logging.info("正在重建 FTS 索引...")
            cur.execute("DELETE FROM messages_fts")
            cur.execute("""
                INSERT INTO messages_fts(rowid, content)
                SELECT pk, COALESCE(NULLIF(content_norm, ''), content, '')
                FROM messages
            """)
            conn.commit()
            logging.info("FTS 索引重建完成。")

    except Exception as e:
        logging.error(f"修复过程中出错: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    fix_missing_filenames()
