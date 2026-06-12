#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from collections.abc import Iterator, Sequence
from dataclasses import dataclass

from telethon import TelegramClient
from telethon.errors import FloodWaitError

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


@dataclass(frozen=True)
class ChatGapSummary:
    chat_id: int
    chat_title: str
    chat_username: str | None
    max_message_id: int
    existing_count: int
    missing_count: int


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "扫描数据库消息 ID 空洞，并可从 Telegram 定向补回仍存在的历史消息。"
            "默认只预览，不联网、不写库。"
        )
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="真实连接 Telegram 并写入数据库；不加时只打印空洞计划",
    )
    parser.add_argument("--chat-id", type=int, default=None, help="仅处理指定 chat_id")
    parser.add_argument(
        "--max-chats",
        type=int,
        default=0,
        help="最多处理多少个包含空洞的群组，默认 0 表示不限制",
    )
    parser.add_argument(
        "--min-missing",
        type=int,
        default=1,
        help="历史空洞数至少达到该值才列入计划，默认 1",
    )
    parser.add_argument(
        "--max-missing-per-chat",
        type=int,
        default=0,
        help="每个群组最多向 Telegram 查询多少个缺失 ID，默认 0 表示不限制",
    )
    parser.add_argument(
        "--fetch-batch-size",
        type=int,
        default=100,
        help="每次向 Telegram 查询多少个 message_id，默认 100，最大 200",
    )
    parser.add_argument(
        "--write-batch-size",
        type=int,
        default=500,
        help="累计多少条消息后写库，默认 500",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=1.0,
        help="每次 Telegram 请求后的等待秒数，默认 1.0",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="遇到单个群组或消息块失败后停止，默认记录失败后继续",
    )
    return parser


def _row_value(row, key: str, index: int):
    if hasattr(row, "keys"):
        return row[key]
    return row[index]


def load_chat_gap_summaries(
    conn,
    *,
    chat_id: int | None = None,
    min_missing: int = 1,
    max_chats: int = 0,
) -> list[ChatGapSummary]:
    cur = conn.cursor()
    try:
        where_sql = ""
        params: list[int] = []
        if chat_id is not None:
            where_sql = "WHERE m.chat_id = ?"
            params.append(int(chat_id))

        params.append(max(1, int(min_missing)))
        limit_sql = ""
        if int(max_chats) > 0:
            limit_sql = "LIMIT ?"
            params.append(int(max_chats))

        cur.execute(
            f"""
            WITH grouped AS (
                SELECT
                    m.chat_id,
                    COALESCE(NULLIF(TRIM(c.chat_title), ''), 'Chat ' || m.chat_id) AS chat_title,
                    c.chat_username,
                    MAX(m.message_id) AS max_message_id,
                    COUNT(DISTINCT m.message_id) AS existing_count
                FROM messages m
                LEFT JOIN chats c
                  ON c.chat_id = m.chat_id
                {where_sql}
                GROUP BY m.chat_id
            ),
            gaps AS (
                SELECT
                    chat_id,
                    chat_title,
                    chat_username,
                    max_message_id,
                    existing_count,
                    max_message_id - existing_count AS missing_count
                FROM grouped
                WHERE max_message_id > existing_count
            )
            SELECT
                chat_id,
                chat_title,
                chat_username,
                max_message_id,
                existing_count,
                missing_count
            FROM gaps
            WHERE missing_count >= ?
            ORDER BY missing_count DESC, max_message_id DESC, chat_id ASC
            {limit_sql}
            """,
            params,
        )
        summaries: list[ChatGapSummary] = []
        for row in cur.fetchall():
            username = _row_value(row, "chat_username", 2)
            summaries.append(
                ChatGapSummary(
                    chat_id=int(_row_value(row, "chat_id", 0)),
                    chat_title=str(_row_value(row, "chat_title", 1)),
                    chat_username=str(username).strip() if username else None,
                    max_message_id=int(_row_value(row, "max_message_id", 3) or 0),
                    existing_count=int(_row_value(row, "existing_count", 4) or 0),
                    missing_count=int(_row_value(row, "missing_count", 5) or 0),
                )
            )
        return summaries
    finally:
        cur.close()


def iter_missing_message_id_chunks(
    conn,
    *,
    chat_id: int,
    last_id: int,
    chunk_size: int,
    limit: int = 0,
) -> Iterator[list[int]]:
    safe_chunk_size = max(1, min(200, int(chunk_size)))
    remaining = int(limit) if int(limit) > 0 else None
    next_missing_id = int(last_id)
    chunk: list[int] = []

    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT DISTINCT message_id
            FROM messages
            WHERE chat_id = ?
              AND message_id BETWEEN 1 AND ?
            ORDER BY message_id DESC
            """,
            (int(chat_id), int(last_id)),
        )

        for row in cur:
            existing_id = int(_row_value(row, "message_id", 0))
            while next_missing_id > existing_id:
                if remaining is not None and remaining <= 0:
                    if chunk:
                        yield chunk
                    return
                chunk.append(next_missing_id)
                if remaining is not None:
                    remaining -= 1
                if len(chunk) >= safe_chunk_size:
                    yield chunk
                    chunk = []
                next_missing_id -= 1

            if next_missing_id == existing_id:
                next_missing_id -= 1

        while next_missing_id >= 1:
            if remaining is not None and remaining <= 0:
                break
            chunk.append(next_missing_id)
            if remaining is not None:
                remaining -= 1
            if len(chunk) >= safe_chunk_size:
                yield chunk
                chunk = []
            next_missing_id -= 1

        if chunk:
            yield chunk
    finally:
        cur.close()


async def _resolve_entity(client, chat_id: int, chat_username: str | None = None):
    from tg_harvest.domain.chat_ids import candidate_chat_entity_ids

    try:
        return await client.get_entity(chat_id)
    except Exception as exc:
        if "could not find the input entity" not in str(exc).lower():
            raise
        last_error: Exception = exc

    for candidate_id in candidate_chat_entity_ids(chat_id):
        if candidate_id == int(chat_id):
            continue
        try:
            return await client.get_entity(candidate_id)
        except Exception as exc:
            last_error = exc

    if chat_username:
        try:
            return await client.get_entity(chat_username)
        except Exception as exc:
            last_error = exc

    raise last_error


async def _fetch_messages_with_retry(
    client,
    entity,
    chunk_ids: Sequence[int],
    *,
    chat_id: int,
    stop_on_error: bool,
) -> list[object]:
    try:
        return await client.get_messages(entity, ids=list(chunk_ids))
    except FloodWaitError as exc:
        wait_seconds = max(1, int(getattr(exc, "seconds", 0) or 0))
        logging.warning(
            "触发 Telegram 限流 chat_id=%s chunk=%s..%s，等待 %ss 后重试",
            chat_id,
            chunk_ids[0],
            chunk_ids[-1],
            wait_seconds,
        )
        await asyncio.sleep(wait_seconds + 1)
        return await client.get_messages(entity, ids=list(chunk_ids))
    except Exception:
        if stop_on_error:
            raise
        logging.exception(
            "跳过失败消息块 chat_id=%s chunk=%s..%s",
            chat_id,
            chunk_ids[0],
            chunk_ids[-1],
        )
        return []


def _flush_write_batch(conn, msg_rows: list[tuple], media_rows: list[tuple]) -> tuple[int, int]:
    if not msg_rows and not media_rows:
        return 0, 0

    from tg_harvest.ingest.store import _batch_upsert_media, _batch_upsert_messages

    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        if msg_rows:
            _batch_upsert_messages(cur, msg_rows)
        if media_rows:
            _batch_upsert_media(cur, media_rows)
        conn.commit()
        return len(msg_rows), len(media_rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def _log_plan(summaries: list[ChatGapSummary], *, execute: bool) -> None:
    total_missing = sum(summary.missing_count for summary in summaries)
    logging.info(
        "发现 %s 个存在历史 message_id 空洞的群组，空洞合计 %s 个",
        len(summaries),
        total_missing,
    )
    for idx, summary in enumerate(summaries, start=1):
        username = f" @{summary.chat_username}" if summary.chat_username else ""
        logging.info(
            "[%s/%s] missing=%s max_id=%s existing=%s chat_id=%s%s title=%s",
            idx,
            len(summaries),
            summary.missing_count,
            summary.max_message_id,
            summary.existing_count,
            summary.chat_id,
            username,
            summary.chat_title,
        )
    if not execute:
        logging.info("当前为 dry-run：未连接 Telegram，未写数据库。确认计划后加 --execute 执行。")


async def _recover_one_chat(
    conn,
    client,
    summary: ChatGapSummary,
    *,
    fetch_batch_size: int,
    write_batch_size: int,
    request_delay: float,
    max_missing_per_chat: int,
    stop_on_error: bool,
) -> tuple[int, int, int]:
    from tg_harvest.ingest.parse import MessageParser
    from tg_harvest.ingest.runner import _prepare_db_rows

    try:
        entity = await _resolve_entity(
            client,
            summary.chat_id,
            chat_username=summary.chat_username,
        )
    except Exception:
        if stop_on_error:
            raise
        logging.exception("跳过实体解析失败的群组 chat_id=%s", summary.chat_id)
        return 0, 0, 1

    msg_batch: list[tuple] = []
    media_batch: list[tuple] = []
    written_messages = 0
    written_media = 0
    failed_chunks = 0
    scanned_missing_ids = 0

    for chunk_ids in iter_missing_message_id_chunks(
        conn,
        chat_id=summary.chat_id,
        last_id=summary.max_message_id,
        chunk_size=fetch_batch_size,
        limit=max_missing_per_chat,
    ):
        scanned_missing_ids += len(chunk_ids)
        messages = await _fetch_messages_with_retry(
            client,
            entity,
            chunk_ids,
            chat_id=summary.chat_id,
            stop_on_error=stop_on_error,
        )
        if not messages:
            failed_chunks += 1
            continue

        for msg in messages:
            if not msg or not getattr(msg, "id", None):
                continue
            parsed = MessageParser.parse(msg)
            if not parsed:
                continue
            msg_row, media_row = _prepare_db_rows(entity, summary.chat_id, parsed)
            if msg_row:
                msg_batch.append(msg_row)
            if media_row:
                media_batch.append(media_row)

        if len(msg_batch) >= write_batch_size or len(media_batch) >= write_batch_size:
            msg_count, media_count = _flush_write_batch(conn, msg_batch, media_batch)
            written_messages += msg_count
            written_media += media_count
            msg_batch = []
            media_batch = []
            logging.info(
                "chat_id=%s 已写入 messages=%s media=%s",
                summary.chat_id,
                written_messages,
                written_media,
            )

        safe_delay = max(0.0, float(request_delay))
        if safe_delay > 0:
            await asyncio.sleep(safe_delay)

    if msg_batch or media_batch:
        msg_count, media_count = _flush_write_batch(conn, msg_batch, media_batch)
        written_messages += msg_count
        written_media += media_count

    logging.info(
        "chat_id=%s 补回完成，查询缺失 ID=%s，写入 messages=%s media=%s，失败块=%s",
        summary.chat_id,
        scanned_missing_ids,
        written_messages,
        written_media,
        failed_chunks,
    )
    return written_messages, written_media, failed_chunks


async def _run_async(args: argparse.Namespace) -> int:
    from tg_harvest.config import CFG
    from tg_harvest.storage.connection import ensure_configured_db

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

    logging.info("连接数据库: %s", CFG.db_name)
    conn, _ = ensure_configured_db(cfg=CFG)
    try:
        summaries = load_chat_gap_summaries(
            conn,
            chat_id=args.chat_id,
            min_missing=args.min_missing,
            max_chats=args.max_chats,
        )
        if not summaries:
            logging.info("未发现需要处理的历史 message_id 空洞")
            return 0

        _log_plan(summaries, execute=bool(args.execute))
        if not args.execute:
            return 0

        safe_fetch_batch_size = max(1, min(200, int(args.fetch_batch_size)))
        safe_write_batch_size = max(1, int(args.write_batch_size))
        client = TelegramClient(CFG.session_name, CFG.api_id, CFG.api_hash)
        await client.start()
        try:
            total_messages = 0
            total_media = 0
            total_failed_chunks = 0
            for summary in summaries:
                msg_count, media_count, failed_chunks = await _recover_one_chat(
                    conn,
                    client,
                    summary,
                    fetch_batch_size=safe_fetch_batch_size,
                    write_batch_size=safe_write_batch_size,
                    request_delay=args.request_delay,
                    max_missing_per_chat=max(0, int(args.max_missing_per_chat)),
                    stop_on_error=bool(args.stop_on_error),
                )
                total_messages += msg_count
                total_media += media_count
                total_failed_chunks += failed_chunks

            logging.info(
                "历史空洞补回结束，本次写入 messages=%s media=%s，失败块=%s",
                total_messages,
                total_media,
                total_failed_chunks,
            )
            return 1 if total_failed_chunks else 0
        finally:
            await client.disconnect()
    finally:
        conn.close()


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    return asyncio.run(_run_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
