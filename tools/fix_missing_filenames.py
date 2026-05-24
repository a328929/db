# -*- coding: utf-8 -*-
import logging
import sys
import os

# 增加对上级目录的引用，以便导入 tg_harvest
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tg_harvest.storage.connection import ensure_configured_db
from tg_harvest.config import CFG
from tg_harvest.ingest.store import backfill_message_search_text_from_filenames


def fix_missing_filenames():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s"
    )

    db_name = CFG.db_name
    logging.info(f"正在连接数据库: {db_name}")
    conn, _ = ensure_configured_db(cfg=CFG)

    try:
        updated = backfill_message_search_text_from_filenames(
            conn,
            batch_size=5000,
            log_fn=logging.info,
            cfg=CFG,
        )
        if updated <= 0:
            logging.info("未发现需要修复的消息。")
            return

        logging.info("消息内容已更新: %s", updated)

    except Exception as e:
        logging.error(f"修复过程中出错: {e}")
        conn.rollback()
    finally:
        conn.close()


if __name__ == "__main__":
    fix_missing_filenames()
