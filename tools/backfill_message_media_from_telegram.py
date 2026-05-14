#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import asyncio
import logging
import os
import sys
from typing import Dict, List, Optional

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from telethon import TelegramClient
from telethon.errors import FloodWaitError

from tg_harvest.config import CFG
from tg_harvest.domain.chat_ids import candidate_chat_entity_ids
from tg_harvest.storage.connection import ensure_configured_db
from tg_harvest.ingest.parse import MessageParser
from tg_harvest.ingest.runner import _prepare_db_rows
from tg_harvest.ingest.store import _batch_upsert_media
from tg_harvest.ingest.store import backfill_message_search_text_from_filenames


def _flush_write_batch(conn, write_batch: List[tuple]) -> int:
    if not write_batch:
        return 0
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        _batch_upsert_media(cur, write_batch)
        conn.commit()
        return len(write_batch)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="从 Telegram 定向补抓缺失的媒体元数据"
    )
    parser.add_argument("--chat-id", type=int, default=None, help="仅处理指定 chat_id")
    parser.add_argument(
        "--scan-limit",
        type=int,
        default=50000,
        help="单次扫描最多装载多少条缺失媒体元数据的消息，默认 50000",
    )
    parser.add_argument(
        "--fetch-batch-size",
        type=int,
        default=100,
        help="每次向 Telegram 拉取多少个 message_id，默认 100",
    )
    parser.add_argument(
        "--write-batch-size",
        type=int,
        default=500,
        help="本地累计多少条 media_row 后写库，默认 500",
    )
    parser.add_argument(
        "--chunk-retries",
        type=int,
        default=3,
        help="单个消息块拉取失败后的最大重试次数，默认 3",
    )
    return parser


def _load_missing_targets(
    conn,
    *,
    chat_id: Optional[int],
    scan_limit: int,
) -> Dict[int, Dict[str, object]]:
    cur = conn.cursor()
    try:
        where_sql = "m.has_media = 1 AND COALESCE(mm.file_unique_id, '') = ''"
        params: List[int] = []
        if chat_id is not None:
            where_sql += " AND m.chat_id = ?"
            params.append(int(chat_id))
        params.append(int(scan_limit))
        cur.execute(
            f"""
            SELECT
                m.chat_id,
                m.message_id,
                c.chat_username
            FROM messages m
            LEFT JOIN message_media mm
              ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id
            LEFT JOIN chats c
              ON c.chat_id = m.chat_id
            WHERE {where_sql}
            ORDER BY m.chat_id ASC, m.message_id DESC
            LIMIT ?
            """,
            params,
        )

        out: Dict[int, Dict[str, object]] = {}
        for row in cur.fetchall():
            current_chat_id = int(row["chat_id"])
            bucket = out.setdefault(
                current_chat_id,
                {
                    "chat_username": (row["chat_username"] or "").strip() or None,
                    "message_ids": [],
                },
            )
            bucket["message_ids"].append(int(row["message_id"]))
        return out
    finally:
        cur.close()


async def _resolve_entity(client, chat_id: int, chat_username: Optional[str] = None):
    try:
        return await client.get_entity(chat_id)
    except Exception as e:
        err_msg = str(e).lower()
        if "could not find the input entity" not in err_msg:
            raise
        for candidate_id in candidate_chat_entity_ids(chat_id):
            if candidate_id == int(chat_id):
                continue
            try:
                return await client.get_entity(candidate_id)
            except Exception:
                pass
        if chat_username:
            return await client.get_entity(chat_username)
        raise


async def _fetch_chunk_messages(
    client,
    entity,
    chunk_ids: List[int],
    *,
    chat_id: int,
    max_retries: int,
) -> List[object]:
    attempt = 0
    safe_retries = max(1, int(max_retries))

    while attempt < safe_retries:
        attempt += 1
        try:
            return await client.get_messages(entity, ids=chunk_ids)
        except FloodWaitError as exc:
            wait_seconds = max(1, int(getattr(exc, "seconds", 0) or 0))
            logging.warning(
                "补抓限流 chat_id=%s chunk=%s..%s attempt=%s/%s，等待 %ss 后重试",
                chat_id,
                chunk_ids[0],
                chunk_ids[-1],
                attempt,
                safe_retries,
                wait_seconds,
            )
            await asyncio.sleep(wait_seconds + 1)
        except Exception as exc:
            if attempt >= safe_retries:
                raise
            backoff_seconds = min(10, attempt * 2)
            logging.warning(
                "补抓失败 chat_id=%s chunk=%s..%s attempt=%s/%s: %s；%ss 后重试",
                chat_id,
                chunk_ids[0],
                chunk_ids[-1],
                attempt,
                safe_retries,
                exc,
                backoff_seconds,
            )
            await asyncio.sleep(backoff_seconds)

    raise RuntimeError(
        f"补抓重试耗尽 chat_id={chat_id} chunk={chunk_ids[0]}..{chunk_ids[-1]}"
    )


async def _run() -> int:
    args = _build_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    conn, _ = ensure_configured_db(cfg=CFG)
    try:
        safe_scan_limit = max(1, int(args.scan_limit))
        targets = _load_missing_targets(conn, chat_id=args.chat_id, scan_limit=safe_scan_limit)
        if not targets:
            logging.info("未发现需要向 Telegram 补抓的缺失媒体元数据")
            return 0

        safe_fetch_batch_size = max(1, int(args.fetch_batch_size))
        safe_write_batch_size = max(50, int(args.write_batch_size))
        safe_chunk_retries = max(1, int(args.chunk_retries))
        total_targets = sum(len(v["message_ids"]) for v in targets.values())
        logging.info("待补抓消息数: %s，涉及群聊数: %s", total_targets, len(targets))

        client = TelegramClient(
            CFG.session_name,
            CFG.api_id,
            CFG.api_hash,
            request_retries=5,
            connection_retries=5,
            timeout=15,
            receive_updates=False,
        )
        await client.start()
        try:
            total_written = 0
            write_batch: List[tuple] = []
            failed_chunks: List[str] = []

            for current_chat_id, payload in targets.items():
                ids = payload["message_ids"]
                chat_username = payload["chat_username"]
                try:
                    entity = await _resolve_entity(
                        client, current_chat_id, chat_username=chat_username
                    )
                except Exception as exc:
                    logging.warning("跳过 chat_id=%s，实体解析失败: %s", current_chat_id, exc)
                    continue

                chat_written_before = total_written
                for start in range(0, len(ids), safe_fetch_batch_size):
                    chunk_ids = ids[start : start + safe_fetch_batch_size]
                    try:
                        messages = await _fetch_chunk_messages(
                            client,
                            entity,
                            chunk_ids,
                            chat_id=current_chat_id,
                            max_retries=safe_chunk_retries,
                        )
                    except Exception as exc:
                        logging.warning(
                            "补抓失败且已放弃 chat_id=%s chunk=%s..%s: %s",
                            current_chat_id,
                            chunk_ids[0],
                            chunk_ids[-1],
                            exc,
                        )
                        failed_chunks.append(
                            f"chat_id={current_chat_id}:{chunk_ids[0]}..{chunk_ids[-1]}"
                        )
                        continue

                    for msg in messages:
                        if not msg or not getattr(msg, "id", None):
                            continue
                        parsed = MessageParser.parse(msg)
                        if not parsed or not parsed.has_media:
                            continue
                        _, media_row = _prepare_db_rows(entity, current_chat_id, parsed)
                        if media_row:
                            write_batch.append(media_row)

                    if len(write_batch) >= safe_write_batch_size:
                        flushed = _flush_write_batch(conn, write_batch)
                        total_written += flushed
                        logging.info("已补写 message_media: %s", total_written)
                        write_batch = []

                    await asyncio.sleep(1.0)

                if write_batch:
                    flushed = _flush_write_batch(conn, write_batch)
                    total_written += flushed
                    write_batch = []

                text_backfilled = backfill_message_search_text_from_filenames(
                    conn,
                    chat_id=current_chat_id,
                    batch_size=max(safe_write_batch_size, 500),
                )
                if text_backfilled > 0:
                    logging.info(
                        "chat_id=%s 已补齐可搜索文件名文本: %s",
                        current_chat_id,
                        text_backfilled,
                    )

                logging.info(
                    "chat_id=%s 补抓完成，本轮新增 media_rows: %s / 目标消息: %s",
                    current_chat_id,
                    total_written - chat_written_before,
                    len(ids),
                )

            logging.info("Telegram 补抓完成，本次补写 media_rows: %s", total_written)
            if failed_chunks:
                logging.error(
                    "存在补抓失败的消息块，共 %s 个：%s",
                    len(failed_chunks),
                    ", ".join(failed_chunks[:20]),
                )
                if len(failed_chunks) > 20:
                    logging.error("其余失败块省略显示，合计仍有 %s 个", len(failed_chunks))
                return 1
            return 0
        finally:
            await client.disconnect()
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_run()))
