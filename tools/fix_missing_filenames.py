import logging
import os
import sys
from collections.abc import Sequence

# 增加对上级目录的引用，以便导入 tg_harvest
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def main(_argv: Sequence[str] | None = None) -> int:
    from tg_harvest.config import CFG
    from tg_harvest.ingest.store import backfill_message_search_text_from_filenames
    from tg_harvest.storage.connection import ensure_configured_db

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s"
    )

    db_name = CFG.db_name
    logging.info("正在连接数据库: %s", db_name)
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
            return 0

        logging.info("消息内容已更新: %s", updated)
        return 0

    except Exception:
        logging.exception("修复过程中出错")
        conn.rollback()
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
