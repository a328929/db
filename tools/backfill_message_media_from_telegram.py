#!/usr/bin/env python3
import argparse
import asyncio
import hashlib
import logging
import os
import sys
import time

from telethon import TelegramClient
from telethon.errors import FloodWaitError

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from tg_harvest.storage.introspection import table_columns as _table_columns  # noqa: E402,I001

FILE_MEDIA_MESSAGE_TYPES = (
    "PHOTO",
    "VIDEO",
    "VIDEO_NOTE",
    "AUDIO",
    "VOICE",
    "GIF",
    "FILE",
    "STICKER",
)
TIMED_MEDIA_MESSAGE_TYPES = ("VIDEO", "VIDEO_NOTE", "AUDIO", "VOICE", "GIF")
SIZED_DOCUMENT_MESSAGE_TYPES = ("FILE", "STICKER")
TARGET_MODE_FULL = "full"
TARGET_MODE_IDENTITY = "identity"


class MessageParser:
    @staticmethod
    def parse(message):
        from tg_harvest.ingest.parse import MessageParser as _MessageParser

        return _MessageParser.parse(message)


def candidate_chat_entity_ids(chat_id: int):
    from tg_harvest.domain.chat_ids import candidate_chat_entity_ids as _candidate_ids

    return _candidate_ids(chat_id)


def refresh_media_groups_for_chat(*args, **kwargs):
    from tg_harvest.ingest.media_groups import (
        refresh_media_groups_for_chat as _refresh_media_groups_for_chat,
    )

    return _refresh_media_groups_for_chat(*args, **kwargs)


def _prepare_db_rows(*args, **kwargs):
    from tg_harvest.ingest.runner import _prepare_db_rows as _prepare_rows

    return _prepare_rows(*args, **kwargs)


def backfill_message_search_text_from_filenames(*args, **kwargs):
    from tg_harvest.ingest.store import (
        backfill_message_search_text_from_filenames as _backfill_text,
    )

    return _backfill_text(*args, **kwargs)


def batch_upsert(*args, **kwargs):
    from tg_harvest.ingest.store import batch_upsert as _batch_upsert

    return _batch_upsert(*args, **kwargs)


def load_grouped_ids_for_messages(*args, **kwargs):
    from tg_harvest.ingest.store import (
        load_grouped_ids_for_messages as _load_grouped_ids_for_messages,
    )

    return _load_grouped_ids_for_messages(*args, **kwargs)


def ensure_configured_db(*args, **kwargs):
    from tg_harvest.storage.connection import ensure_configured_db as _ensure_db

    return _ensure_db(*args, **kwargs)


def indexed_messages_from_clause(*args, **kwargs):
    from tg_harvest.storage.search_text_state import (
        indexed_messages_from_clause as _indexed_messages_from_clause,
    )

    return _indexed_messages_from_clause(*args, **kwargs)


def indexed_unsearchable_message_predicate(*args, **kwargs):
    from tg_harvest.storage.search_text_state import (
        indexed_unsearchable_message_predicate as _indexed_unsearchable_predicate,
    )

    return _indexed_unsearchable_predicate(*args, **kwargs)


def _get_cfg():
    from tg_harvest.config import CFG

    return CFG


def _sql_text_list(values: tuple[str, ...]) -> str:
    return ", ".join([f"'{value}'" for value in values])


def _flush_write_batch(
    conn,
    write_batch: list[tuple],
    media_rows: list[tuple] | None = None,
) -> int:
    if media_rows is None:
        msg_rows: list[tuple] = []
        media_rows = write_batch
    else:
        msg_rows = write_batch

    if not msg_rows and not media_rows:
        return 0
    batch_upsert(conn, msg_rows, media_rows)
    return len(media_rows)


def _collect_media_row_keys(media_rows: list[tuple]) -> list[tuple[int, int]]:
    return [(int(row[0]), int(row[1])) for row in media_rows]


def _collect_message_row_keys(msg_rows: list[tuple]) -> list[tuple[int, int]]:
    return [(int(row[0]), int(row[1])) for row in msg_rows]


def _collect_message_row_grouped_ids(msg_rows: list[tuple]) -> set[int]:
    grouped_ids: set[int] = set()
    for row in msg_rows:
        if len(row) <= 10 or row[10] is None:
            continue
        grouped_ids.add(int(row[10]))
    return grouped_ids


def _flush_parsed_write_batch(
    conn,
    msg_rows: list[tuple],
    media_rows: list[tuple],
    touched_groups: set[int],
) -> tuple[int, int]:
    if not msg_rows and not media_rows:
        return 0, 0

    message_keys = _collect_message_row_keys(msg_rows)
    if not message_keys:
        message_keys = _collect_media_row_keys(media_rows)

    touched_groups.update(load_grouped_ids_for_messages(conn, message_keys))
    touched_groups.update(_collect_message_row_grouped_ids(msg_rows))

    media_count = _flush_write_batch(conn, msg_rows, media_rows)
    return len(msg_rows), media_count


def _load_pending_filename_backfill_grouped_ids(conn, chat_id: int) -> set[int]:
    cur = conn.cursor()
    try:
        message_columns = _table_columns(cur, "messages")
        media_columns = _table_columns(cur, "message_media")
        if "grouped_id" not in message_columns or "file_name" not in media_columns:
            return set()

        unsearchable_predicate = indexed_unsearchable_message_predicate(cur, alias="m")
        messages_from_sql = indexed_messages_from_clause(
            cur,
            alias="m",
            chat_scoped=True,
        )
        cur.execute(
            f"""
            SELECT DISTINCT m.grouped_id
            FROM {messages_from_sql}
            JOIN message_media mm
              ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id
            WHERE m.chat_id = ?
              AND m.has_media = 1
              AND {unsearchable_predicate}
              AND COALESCE(NULLIF(TRIM(mm.file_name), ''), '') <> ''
              AND m.grouped_id IS NOT NULL
            """,
            (int(chat_id),),
        )
        return {int(row["grouped_id"]) for row in cur.fetchall()}
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
        default=200000,
        help="单轮扫描最多装载多少条缺失媒体元数据的消息，默认 200000",
    )
    parser.add_argument(
        "--fetch-batch-size",
        type=int,
        default=200,
        help="每次向 Telegram 拉取多少个 message_id，默认 200",
    )
    parser.add_argument(
        "--write-batch-size",
        type=int,
        default=2000,
        help="本地累计多少条消息/媒体记录后写库，默认 2000",
    )
    parser.add_argument(
        "--chunk-retries",
        type=int,
        default=3,
        help="单个消息块拉取失败后的最大重试次数，默认 3",
    )
    parser.add_argument(
        "--request-delay",
        type=float,
        default=0.1,
        help="每次 Telegram 批量请求后的等待秒数，默认 0.1；触发 FloodWait 时仍会按 Telegram 要求等待",
    )
    parser.add_argument(
        "--chat-delay",
        type=float,
        default=0.0,
        help="每个 chat 处理完成后的等待秒数，默认 0",
    )
    parser.add_argument(
        "--max-passes",
        type=int,
        default=1,
        help="最多重复扫描/补抓多少轮，默认 1；设为 0 表示一直跑到没有目标或失败",
    )
    parser.add_argument(
        "--max-chats",
        type=int,
        default=0,
        help="每轮最多处理多少个 chat，默认 0 表示不限制",
    )
    parser.add_argument(
        "--target-mode",
        choices=(TARGET_MODE_FULL, TARGET_MODE_IDENTITY),
        default=TARGET_MODE_FULL,
        help=(
            "目标筛选模式：full 补齐身份、尺寸、时长等字段；"
            "identity 只补 file_unique_id/media_fingerprint/media_kind/grouped_id，适合百万级占位数据快速修复"
        ),
    )
    parser.add_argument(
        "--skip-filename-backfill",
        action="store_true",
        help="跳过每个 chat 末尾的文件名搜索文本回填；当前库已经回填过时可加速",
    )
    return parser


def _load_missing_targets(
    conn,
    *,
    chat_id: int | None,
    scan_limit: int,
    target_mode: str = TARGET_MODE_FULL,
    max_chats: int = 0,
) -> dict[int, dict[str, object]]:
    cur = conn.cursor()
    try:
        message_columns = _table_columns(cur, "messages")
        media_columns = _table_columns(cur, "message_media")
        file_media_types_sql = _sql_text_list(FILE_MEDIA_MESSAGE_TYPES)
        timed_media_types_sql = _sql_text_list(TIMED_MEDIA_MESSAGE_TYPES)
        sized_document_types_sql = _sql_text_list(SIZED_DOCUMENT_MESSAGE_TYPES)

        missing_predicates = ["mm.chat_id IS NULL"]
        if "file_unique_id" in media_columns:
            missing_predicates.append("COALESCE(mm.file_unique_id, '') = ''")
        if "media_fingerprint" in media_columns:
            missing_predicates.append("COALESCE(mm.media_fingerprint, '') = ''")
        if "media_kind" in media_columns:
            missing_predicates.append("COALESCE(mm.media_kind, '') = ''")
        if "grouped_id" in message_columns and "grouped_id" in media_columns:
            missing_predicates.append(
                "(m.grouped_id IS NOT NULL AND m.grouped_id IS NOT mm.grouped_id)"
            )
        if target_mode == TARGET_MODE_FULL:
            if "file_size" in media_columns:
                missing_predicates.append(
                    f"(m.msg_type IN ({sized_document_types_sql}) AND mm.file_size IS NULL)"
                )
            if "file_size" in media_columns and "duration_sec" in media_columns:
                timed_meta_conditions = [
                    f"m.msg_type IN ({timed_media_types_sql})",
                ]
                if "mime_type" in media_columns:
                    timed_meta_conditions.extend(
                        [
                            "COALESCE(mm.mime_type, '') LIKE 'video/%'",
                            "COALESCE(mm.mime_type, '') LIKE 'audio/%'",
                        ]
                    )
                missing_predicates.append(
                    f"""
                    (
                        ({" OR ".join(timed_meta_conditions)})
                        AND (
                            mm.file_size IS NULL
                            OR mm.duration_sec IS NULL
                        )
                    )
                    """
                )
            if {"file_size", "width", "height"}.issubset(media_columns):
                missing_predicates.append(
                    """
                    (
                        m.msg_type = 'PHOTO'
                        AND (
                            mm.file_size IS NULL
                            OR mm.width IS NULL
                            OR mm.height IS NULL
                        )
                    )
                    """
                )

        where_sql = f"""
            m.has_media = 1
            AND m.msg_type IN ({file_media_types_sql})
            AND (
                {" OR ".join(missing_predicates)}
            )
        """
        params: list[int] = []
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

        out: dict[int, dict[str, object]] = {}
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
        if max_chats > 0:
            return {
                current_chat_id: out[current_chat_id]
                for current_chat_id in list(out.keys())[: int(max_chats)]
            }
        return out
    finally:
        cur.close()


def _max_passes_reached(pass_index: int, max_passes: int) -> bool:
    return int(max_passes) > 0 and int(pass_index) >= int(max_passes)


def _targets_signature(targets: dict[int, dict[str, object]]) -> str:
    digest = hashlib.sha1()
    for chat_id, payload in targets.items():
        digest.update(str(int(chat_id)).encode("ascii"))
        digest.update(b":")
        for message_id in payload["message_ids"]:
            digest.update(str(int(message_id)).encode("ascii"))
            digest.update(b",")
        digest.update(b";")
    return digest.hexdigest()


async def _sleep_after_request(delay_seconds: float) -> None:
    safe_delay = max(0.0, float(delay_seconds))
    if safe_delay <= 0:
        return
    await asyncio.sleep(safe_delay)


async def _sleep_after_chat(delay_seconds: float) -> None:
    safe_delay = max(0.0, float(delay_seconds))
    if safe_delay <= 0:
        return
    await asyncio.sleep(safe_delay)


async def _resolve_entity(client, chat_id: int, chat_username: str | None = None):
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
    chunk_ids: list[int],
    *,
    chat_id: int,
    max_retries: int,
) -> list[object]:
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
    cfg = _get_cfg()

    conn, _ = ensure_configured_db(cfg=cfg)
    try:
        safe_scan_limit = max(1, int(args.scan_limit))
        requested_fetch_batch_size = max(1, int(args.fetch_batch_size))
        safe_fetch_batch_size = min(200, requested_fetch_batch_size)
        if safe_fetch_batch_size != requested_fetch_batch_size:
            logging.info(
                "Telegram get_messages 单次 ids 上限按 200 处理，已将 fetch_batch_size=%s 调整为 %s",
                requested_fetch_batch_size,
                safe_fetch_batch_size,
            )
        safe_write_batch_size = max(50, int(args.write_batch_size))
        safe_chunk_retries = max(1, int(args.chunk_retries))
        safe_request_delay = max(0.0, float(args.request_delay))
        safe_chat_delay = max(0.0, float(args.chat_delay))
        safe_max_passes = max(0, int(args.max_passes))
        safe_max_chats = max(0, int(args.max_chats))
        target_mode = str(args.target_mode or TARGET_MODE_FULL)

        client = TelegramClient(
            cfg.session_name,
            cfg.api_id,
            cfg.api_hash,
            request_retries=5,
            connection_retries=5,
            timeout=15,
            receive_updates=False,
        )
        await client.start()
        try:
            total_message_written = 0
            total_media_written = 0
            failed_chunks: list[str] = []
            failed_messages: list[str] = []
            pass_index = 0
            previous_target_signature: str | None = None
            stalled = False

            while True:
                pass_index += 1
                targets = _load_missing_targets(
                    conn,
                    chat_id=args.chat_id,
                    scan_limit=safe_scan_limit,
                    target_mode=target_mode,
                    max_chats=safe_max_chats,
                )
                if not targets:
                    logging.info("未发现需要向 Telegram 补抓的缺失媒体元数据")
                    return 0 if not failed_chunks and not failed_messages else 1

                pass_total_targets = sum(len(v["message_ids"]) for v in targets.values())
                target_signature = _targets_signature(targets)
                if previous_target_signature == target_signature:
                    stalled = True
                    logging.warning(
                        "第 %s 轮目标集合与上一轮完全相同，说明上一轮没有减少待补抓目标；停止以避免无限重复请求，剩余目标数=%s",
                        pass_index,
                        pass_total_targets,
                    )
                    break
                previous_target_signature = target_signature
                pass_started_at = time.perf_counter()
                logging.info(
                    "第 %s 轮待补抓消息数: %s，涉及群聊数: %s，target_mode=%s，fetch_batch=%s，request_delay=%.3fs",
                    pass_index,
                    pass_total_targets,
                    len(targets),
                    target_mode,
                    safe_fetch_batch_size,
                    safe_request_delay,
                )

                pass_message_written_before = total_message_written
                pass_media_written_before = total_media_written
                pass_seen_chunks = 0
                pass_total_chunks = sum(
                    (len(v["message_ids"]) + safe_fetch_batch_size - 1)
                    // safe_fetch_batch_size
                    for v in targets.values()
                )

                for current_chat_id, payload in targets.items():
                    ids = payload["message_ids"]
                    chat_username = payload["chat_username"]
                    touched_groups: set[int] = set()
                    msg_batch: list[tuple] = []
                    media_batch: list[tuple] = []
                    try:
                        entity = await _resolve_entity(
                            client, current_chat_id, chat_username=chat_username
                        )
                    except Exception as exc:
                        logging.warning("跳过 chat_id=%s，实体解析失败: %s", current_chat_id, exc)
                        continue

                    chat_message_written_before = total_message_written
                    chat_media_written_before = total_media_written
                    chat_started_at = time.perf_counter()
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
                            try:
                                parsed = MessageParser.parse(msg)
                            except Exception as exc:
                                message_id = getattr(msg, "id", "?")
                                logging.warning(
                                    "跳过解析失败的消息 chat_id=%s message_id=%s: %s",
                                    current_chat_id,
                                    message_id,
                                    exc,
                                )
                                failed_messages.append(
                                    f"chat_id={current_chat_id}:message_id={message_id}"
                                )
                                continue
                            if not parsed:
                                continue

                            msg_row, media_row = _prepare_db_rows(
                                entity, current_chat_id, parsed
                            )
                            msg_batch.append(msg_row)
                            if media_row:
                                media_batch.append(media_row)

                        if (
                            len(msg_batch) >= safe_write_batch_size
                            or len(media_batch) >= safe_write_batch_size
                        ):
                            flushed_messages, flushed_media = _flush_parsed_write_batch(
                                conn,
                                msg_batch,
                                media_batch,
                                touched_groups,
                            )
                            total_message_written += flushed_messages
                            total_media_written += flushed_media
                            logging.info(
                                "已补写 messages=%s message_media=%s",
                                total_message_written,
                                total_media_written,
                            )
                            msg_batch = []
                            media_batch = []

                        pass_seen_chunks += 1
                        if pass_seen_chunks % 20 == 0 or pass_seen_chunks == pass_total_chunks:
                            elapsed = max(time.perf_counter() - pass_started_at, 0.001)
                            rate = pass_seen_chunks / elapsed
                            logging.info(
                                "第 %s 轮进度 chunks=%s/%s，速度=%.2f chunks/s",
                                pass_index,
                                pass_seen_chunks,
                                pass_total_chunks,
                                rate,
                            )

                        await _sleep_after_request(safe_request_delay)

                    if msg_batch or media_batch:
                        flushed_messages, flushed_media = _flush_parsed_write_batch(
                            conn,
                            msg_batch,
                            media_batch,
                            touched_groups,
                        )
                        total_message_written += flushed_messages
                        total_media_written += flushed_media
                        msg_batch = []
                        media_batch = []

                    if not args.skip_filename_backfill:
                        touched_groups.update(
                            _load_pending_filename_backfill_grouped_ids(conn, current_chat_id)
                        )
                        text_backfilled = backfill_message_search_text_from_filenames(
                            conn,
                            chat_id=current_chat_id,
                            batch_size=max(safe_write_batch_size, 500),
                            cfg=cfg,
                        )
                    else:
                        text_backfilled = 0
                    if text_backfilled > 0:
                        logging.info(
                            "chat_id=%s 已补齐可搜索文件名文本: %s",
                            current_chat_id,
                            text_backfilled,
                        )
                        touched_groups.update(
                            load_grouped_ids_for_messages(
                                conn,
                                [
                                    (current_chat_id, int(message_id))
                                    for message_id in ids
                                ],
                            )
                        )

                    if touched_groups:
                        refresh_media_groups_for_chat(
                            conn,
                            current_chat_id,
                            cfg=cfg,
                            grouped_ids=touched_groups,
                        )

                    chat_elapsed = time.perf_counter() - chat_started_at
                    logging.info(
                        "chat_id=%s 补抓完成，本轮同步 messages: %s，media_rows: %s / 目标消息: %s，耗时 %.2fs",
                        current_chat_id,
                        total_message_written - chat_message_written_before,
                        total_media_written - chat_media_written_before,
                        len(ids),
                        chat_elapsed,
                    )
                    await _sleep_after_chat(safe_chat_delay)

                pass_elapsed = time.perf_counter() - pass_started_at
                logging.info(
                    "第 %s 轮完成，同步 messages=%s message_media=%s / 目标消息=%s，耗时 %.2fs",
                    pass_index,
                    total_message_written - pass_message_written_before,
                    total_media_written - pass_media_written_before,
                    pass_total_targets,
                    pass_elapsed,
                )
                if failed_chunks or failed_messages:
                    break
                if _max_passes_reached(pass_index, safe_max_passes):
                    break

            logging.info(
                "Telegram 补抓完成，本次同步 messages=%s message_media=%s",
                total_message_written,
                total_media_written,
            )
            if failed_chunks:
                logging.error(
                    "存在补抓失败的消息块，共 %s 个：%s",
                    len(failed_chunks),
                    ", ".join(failed_chunks[:20]),
                )
                if len(failed_chunks) > 20:
                    logging.error("其余失败块省略显示，合计仍有 %s 个", len(failed_chunks))
                return 1
            if failed_messages:
                logging.error(
                    "存在解析失败的消息，共 %s 条：%s",
                    len(failed_messages),
                    ", ".join(failed_messages[:20]),
                )
                if len(failed_messages) > 20:
                    logging.error("其余解析失败消息省略显示，合计仍有 %s 条", len(failed_messages))
                return 1
            if stalled:
                logging.error("补抓已停止：待补抓目标连续两轮没有变化，请检查删帖、账号权限或不可解析媒体。")
                return 1
            return 0
        finally:
            await client.disconnect()
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_run()))
