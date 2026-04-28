#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import logging
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from tg_harvest.config import CFG
from tg_harvest.storage.schema import ensure_configured_db
from tg_harvest.ingest.store import backfill_missing_message_media_placeholders


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="为缺失的 message_media 记录快速回填本地占位数据"
    )
    parser.add_argument("--chat-id", type=int, default=None, help="仅回填指定 chat_id")
    parser.add_argument(
        "--batch-size", type=int, default=50000, help="每批处理消息数，默认 50000"
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    logging.info("连接数据库: %s", CFG.db_name)
    conn, _ = ensure_configured_db(cfg=CFG)
    try:
        cur = conn.cursor()
        try:
            where_sql = ""
            params = []
            if args.chat_id is not None:
                where_sql = " AND m.chat_id = ?"
                params.append(int(args.chat_id))

            cur.execute(
                f"""
                SELECT COUNT(*) AS c
                FROM messages m
                LEFT JOIN message_media mm
                  ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id
                WHERE m.has_media = 1 AND mm.chat_id IS NULL{where_sql}
                """,
                params,
            )
            missing_before = int(cur.fetchone()["c"] or 0)
        finally:
            cur.close()

        if missing_before <= 0:
            logging.info("未发现需要回填的 message_media 占位数据")
            return 0

        logging.info("待回填缺失 message_media 记录: %s", missing_before)
        inserted = backfill_missing_message_media_placeholders(
            conn,
            chat_id=args.chat_id,
            batch_size=args.batch_size,
            log_fn=logging.info,
        )
        logging.info("回填完成，本次新增占位 message_media 记录: %s", inserted)
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
