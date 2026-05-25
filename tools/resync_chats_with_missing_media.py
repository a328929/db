#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass
from typing import List, Optional, Set, Tuple

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from tg_harvest.admin_jobs.common import admin_error_message
from tg_harvest.admin_jobs.common import resolve_chat_entity
from tg_harvest.admin_jobs.core import job_context
from tg_harvest.admin_jobs.runners import _admin_process_single_chat_update
from tg_harvest.admin_jobs.runners import _delete_chat_data
from tg_harvest.admin_jobs.sessions import _cleanup_isolated_worker_session
from tg_harvest.admin_jobs.sessions import _create_isolated_worker_client
from tg_harvest.admin_jobs.sessions import _disconnect_worker_client
from tg_harvest.admin_jobs.sessions import _ensure_base_session_valid
from tg_harvest.config import CFG
from tg_harvest.ingest.parse import setup_logging
from tg_harvest.storage.connection import ensure_configured_db


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
MISSING_IDENTITY_INDEX_NAME = "idx_media_missing_identity_rank"


@dataclass(frozen=True)
class MissingMediaChat:
    chat_id: int
    chat_title: str
    chat_username: Optional[str]
    message_count: int
    missing_count: int


@dataclass
class ResyncResult:
    chat: MissingMediaChat
    status: str
    deleted_messages: int = 0
    after_messages: int = 0
    missing_after: int = 0
    elapsed_sec: float = 0.0
    error: Optional[str] = None


def _sql_text_list(values: Tuple[str, ...]) -> str:
    return ", ".join([f"'{value}'" for value in values])


def _table_columns(cur, table_name: str) -> Set[str]:
    try:
        cur.execute(f"PRAGMA table_xinfo({table_name})")
    except Exception:
        cur.execute(f"PRAGMA table_info({table_name})")
    return {str(row["name"] if hasattr(row, "keys") else row[1]) for row in cur.fetchall()}


def _index_exists(cur, index_name: str) -> bool:
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",
        (str(index_name),),
    )
    return cur.fetchone() is not None


def _identity_missing_predicate(media_columns: Set[str], alias: str = "mm") -> str:
    predicates = []
    for column in ("file_unique_id", "media_fingerprint", "media_kind"):
        if column in media_columns:
            predicates.append(f"{alias}.{column} IS NULL")
            predicates.append(f"{alias}.{column} = ''")
    if not predicates:
        return "0"
    return "(" + " OR ".join(predicates) + ")"


def ensure_missing_identity_ranking_index(conn) -> bool:
    cur = conn.cursor()
    try:
        media_columns = _table_columns(cur, "message_media")
        predicate = _identity_missing_predicate(media_columns, alias="")
        if predicate == "0":
            return False
        predicate = predicate.replace(".file_unique_id", "file_unique_id")
        predicate = predicate.replace(".media_fingerprint", "media_fingerprint")
        predicate = predicate.replace(".media_kind", "media_kind")
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS {MISSING_IDENTITY_INDEX_NAME}
            ON message_media(chat_id, message_id)
            WHERE {predicate}
            """
        )
        conn.commit()
        return True
    finally:
        cur.close()


def _missing_media_where_sql(cur, *, target_mode: str) -> str:
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
            timed_meta_conditions = [f"m.msg_type IN ({timed_media_types_sql})"]
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

    return f"""
        m.has_media = 1
        AND m.msg_type IN ({file_media_types_sql})
        AND (
            {" OR ".join(missing_predicates)}
        )
    """


def _identity_target_sql(
    cur,
    *,
    chat_id: Optional[int],
) -> Tuple[str, List[int]]:
    message_columns = _table_columns(cur, "messages")
    media_columns = _table_columns(cur, "message_media")
    file_media_types_sql = _sql_text_list(FILE_MEDIA_MESSAGE_TYPES)
    has_rank_index = _index_exists(cur, MISSING_IDENTITY_INDEX_NAME)
    media_index_sql = (
        f"INDEXED BY {MISSING_IDENTITY_INDEX_NAME}" if has_rank_index else ""
    )
    message_pk_index_sql = (
        "INDEXED BY sqlite_autoindex_messages_1"
        if _index_exists(cur, "sqlite_autoindex_messages_1")
        else ""
    )
    message_type_index_sql = (
        "INDEXED BY idx_messages_type"
        if chat_id is not None and _index_exists(cur, "idx_messages_type")
        else "INDEXED BY idx_messages_type_global"
        if _index_exists(cur, "idx_messages_type_global")
        else ""
    )

    parts: List[str] = []
    params: List[int] = []
    identity_predicate = _identity_missing_predicate(media_columns, alias="mm")
    if identity_predicate != "0":
        chat_filter = ""
        if chat_id is not None:
            chat_filter = "AND mm.chat_id = ?"
            params.append(int(chat_id))
        parts.append(
            f"""
            SELECT mm.chat_id, mm.message_id
            FROM message_media mm {media_index_sql}
            CROSS JOIN messages m {message_pk_index_sql}
              ON m.chat_id = mm.chat_id AND m.message_id = mm.message_id
            WHERE m.has_media = 1
              AND m.msg_type IN ({file_media_types_sql})
              AND {identity_predicate}
              {chat_filter}
            """
        )

    if "grouped_id" in message_columns and "grouped_id" in media_columns:
        chat_filter = ""
        if chat_id is not None:
            chat_filter = "AND mm.chat_id = ?"
            params.append(int(chat_id))
        parts.append(
            f"""
            SELECT mm.chat_id, mm.message_id
            FROM message_media mm
            CROSS JOIN messages m {message_pk_index_sql}
              ON m.chat_id = mm.chat_id AND m.message_id = mm.message_id
            WHERE m.has_media = 1
              AND m.msg_type IN ({file_media_types_sql})
              AND m.grouped_id IS NOT NULL
              AND m.grouped_id IS NOT mm.grouped_id
              {chat_filter}
            """
        )

    chat_filter = ""
    if chat_id is not None:
        chat_filter = "AND m.chat_id = ?"
        params.append(int(chat_id))
    parts.append(
        f"""
        SELECT m.chat_id, m.message_id
        FROM messages m {message_type_index_sql}
        LEFT JOIN message_media mm
          ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id
        WHERE m.has_media = 1
          AND m.msg_type IN ({file_media_types_sql})
          AND mm.chat_id IS NULL
          {chat_filter}
        """
    )

    return "\nUNION\n".join(parts), params


def load_missing_media_chats(
    conn,
    *,
    target_mode: str = TARGET_MODE_IDENTITY,
    min_missing: int = 1,
    limit: int = 0,
    chat_id: Optional[int] = None,
) -> List[MissingMediaChat]:
    cur = conn.cursor()
    try:
        if target_mode == TARGET_MODE_IDENTITY:
            target_sql, target_params = _identity_target_sql(cur, chat_id=chat_id)
            params = list(target_params)
            params.append(max(1, int(min_missing)))
            limit_sql = ""
            if int(limit) > 0:
                limit_sql = "LIMIT ?"
                params.append(int(limit))

            cur.execute(
                f"""
                WITH target AS (
                    {target_sql}
                ),
                missing AS (
                    SELECT chat_id, COUNT(*) AS missing_count
                    FROM target
                    GROUP BY chat_id
                    HAVING COUNT(*) >= ?
                )
                SELECT
                    missing.chat_id,
                    COALESCE(NULLIF(TRIM(c.chat_title), ''), 'Chat ' || missing.chat_id) AS chat_title,
                    c.chat_username,
                    COALESCE(c.message_count, 0) AS message_count,
                    missing.missing_count
                FROM missing
                LEFT JOIN chats c
                  ON c.chat_id = missing.chat_id
                ORDER BY missing.missing_count DESC, COALESCE(c.message_count, 0) DESC, missing.chat_id ASC
                {limit_sql}
                """,
                params,
            )
            rows = []
            for row in cur.fetchall():
                username = row["chat_username"]
                rows.append(
                    MissingMediaChat(
                        chat_id=int(row["chat_id"]),
                        chat_title=str(row["chat_title"] or row["chat_id"]),
                        chat_username=str(username).strip() if username else None,
                        message_count=int(row["message_count"] or 0),
                        missing_count=int(row["missing_count"] or 0),
                    )
                )
            return rows

        where_sql = _missing_media_where_sql(cur, target_mode=target_mode)
        params: List[int] = []
        if chat_id is not None:
            where_sql += " AND m.chat_id = ?"
            params.append(int(chat_id))
        params.append(max(1, int(min_missing)))

        limit_sql = ""
        if int(limit) > 0:
            limit_sql = "LIMIT ?"
            params.append(int(limit))

        cur.execute(
            f"""
            WITH missing AS (
                SELECT m.chat_id, COUNT(*) AS missing_count
                FROM messages m
                LEFT JOIN message_media mm
                  ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id
                WHERE {where_sql}
                GROUP BY m.chat_id
                HAVING COUNT(*) >= ?
            )
            SELECT
                missing.chat_id,
                COALESCE(NULLIF(TRIM(c.chat_title), ''), 'Chat ' || missing.chat_id) AS chat_title,
                c.chat_username,
                COALESCE(c.message_count, 0) AS message_count,
                missing.missing_count
            FROM missing
            LEFT JOIN chats c
              ON c.chat_id = missing.chat_id
            ORDER BY missing.missing_count DESC, COALESCE(c.message_count, 0) DESC, missing.chat_id ASC
            {limit_sql}
            """,
            params,
        )
        rows = []
        for row in cur.fetchall():
            username = row["chat_username"]
            rows.append(
                MissingMediaChat(
                    chat_id=int(row["chat_id"]),
                    chat_title=str(row["chat_title"] or row["chat_id"]),
                    chat_username=str(username).strip() if username else None,
                    message_count=int(row["message_count"] or 0),
                    missing_count=int(row["missing_count"] or 0),
                )
            )
        return rows
    finally:
        cur.close()


def count_missing_media_messages(
    conn,
    chat_id: int,
    *,
    target_mode: str = TARGET_MODE_IDENTITY,
) -> int:
    cur = conn.cursor()
    try:
        if target_mode == TARGET_MODE_IDENTITY:
            target_sql, params = _identity_target_sql(cur, chat_id=chat_id)
            cur.execute(
                f"""
                WITH target AS (
                    {target_sql}
                )
                SELECT COUNT(*) AS c
                FROM target
                """,
                params,
            )
            row = cur.fetchone()
            return int(row["c"] or 0) if row else 0

        where_sql = _missing_media_where_sql(cur, target_mode=target_mode)
        cur.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM messages m
            LEFT JOIN message_media mm
              ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id
            WHERE {where_sql}
              AND m.chat_id = ?
            """,
            (int(chat_id),),
        )
        row = cur.fetchone()
        return int(row["c"] or 0) if row else 0
    finally:
        cur.close()


def count_messages(conn, chat_id: int) -> int:
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT COUNT(*) AS c FROM messages WHERE chat_id = ?",
            (int(chat_id),),
        )
        row = cur.fetchone()
        return int(row["c"] or 0) if row else 0
    finally:
        cur.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="按缺失媒体信息数量排序，逐群删除并全量重新采集"
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="真实执行删除和重新采集；不加时只打印计划",
    )
    parser.add_argument("--chat-id", type=int, default=None, help="仅处理指定 chat_id")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="最多处理多少个群组，默认 0 表示不限制",
    )
    parser.add_argument(
        "--min-missing",
        type=int,
        default=1,
        help="缺失媒体消息数量至少达到该值才处理，默认 1",
    )
    parser.add_argument(
        "--target-mode",
        choices=(TARGET_MODE_IDENTITY, TARGET_MODE_FULL),
        default=TARGET_MODE_IDENTITY,
        help="identity 只统计身份/指纹缺失；full 还统计尺寸、时长缺失，默认 identity",
    )
    parser.add_argument(
        "--create-ranking-index",
        action="store_true",
        help="创建缺失身份字段排行用的部分索引，可加速后续 dry-run/排序；会改库结构但不删除数据",
    )
    parser.add_argument(
        "--sleep-between-chats",
        type=float,
        default=0.0,
        help="每个群组重拉完成后的等待秒数，默认 0",
    )
    parser.add_argument(
        "--stop-on-error",
        action="store_true",
        help="任一群组失败后立即停止，默认记录失败后继续下一个",
    )
    return parser


def _open_conn():
    conn, _ = ensure_configured_db(cfg=CFG)
    return conn


def _log_plan(chats: List[MissingMediaChat], *, execute: bool) -> None:
    total_missing = sum(chat.missing_count for chat in chats)
    logging.info(
        "发现 %s 个包含缺失媒体信息的群组，缺失媒体消息合计 %s 条",
        len(chats),
        total_missing,
    )
    for idx, chat in enumerate(chats, start=1):
        username = f" @{chat.chat_username}" if chat.chat_username else ""
        logging.info(
            "[%s/%s] missing=%s messages=%s chat_id=%s%s title=%s",
            idx,
            len(chats),
            chat.missing_count,
            chat.message_count,
            chat.chat_id,
            username,
            chat.chat_title,
        )
    if not execute:
        logging.info("当前为 dry-run：未删除、未采集。确认计划后加 --execute 执行。")


def _append_job_log(_job_id: str, message: str) -> None:
    logging.info("%s", message)


def _delete_chat(chat_id: int) -> int:
    conn = _open_conn()
    try:
        return _delete_chat_data(conn, int(chat_id))
    finally:
        conn.close()


def _resync_one_chat(client, job_id: str, chat: MissingMediaChat, idx: int, total: int, *, target_mode: str) -> ResyncResult:
    started_at = time.perf_counter()
    try:
        resolve_chat_entity(client, chat.chat_id, chat.chat_username)
    except Exception as exc:
        return ResyncResult(
            chat=chat,
            status="skipped",
            elapsed_sec=time.perf_counter() - started_at,
            error=f"删除前实体解析失败：{admin_error_message(exc)}",
        )

    try:
        logging.info(
            "[%s/%s] 开始重建 chat_id=%s title=%s missing=%s",
            idx,
            total,
            chat.chat_id,
            chat.chat_title,
            chat.missing_count,
        )
        deleted_messages = _delete_chat(chat.chat_id)
        logging.info(
            "[%s/%s] 删除完成 chat_id=%s deleted_messages=%s，开始全量重新采集",
            idx,
            total,
            chat.chat_id,
            deleted_messages,
        )
        _admin_process_single_chat_update(
            job_id=job_id,
            client=client,
            get_conn_fn=_open_conn,
            admin_job_append_log_fn=_append_job_log,
            chat_id=chat.chat_id,
            chat_title=chat.chat_title,
            chat_username=chat.chat_username,
            idx=idx,
            total=total,
        )

        conn = _open_conn()
        try:
            after_messages = count_messages(conn, chat.chat_id)
            missing_after = count_missing_media_messages(
                conn,
                chat.chat_id,
                target_mode=target_mode,
            )
        finally:
            conn.close()

        elapsed = time.perf_counter() - started_at
        logging.info(
            "[%s/%s] 重建完成 chat_id=%s messages=%s missing_after=%s 耗时 %.2fs",
            idx,
            total,
            chat.chat_id,
            after_messages,
            missing_after,
            elapsed,
        )
        return ResyncResult(
            chat=chat,
            status="success",
            deleted_messages=deleted_messages,
            after_messages=after_messages,
            missing_after=missing_after,
            elapsed_sec=elapsed,
        )
    except Exception as exc:
        elapsed = time.perf_counter() - started_at
        logging.exception("重建失败 chat_id=%s", chat.chat_id)
        return ResyncResult(
            chat=chat,
            status="error",
            elapsed_sec=elapsed,
            error=admin_error_message(exc),
        )


def _log_summary(results: List[ResyncResult]) -> None:
    success = [r for r in results if r.status == "success"]
    skipped = [r for r in results if r.status == "skipped"]
    failed = [r for r in results if r.status == "error"]
    deleted_messages = sum(r.deleted_messages for r in success)
    after_messages = sum(r.after_messages for r in success)
    missing_before = sum(r.chat.missing_count for r in results)
    missing_after = sum(r.missing_after for r in success)
    elapsed = sum(r.elapsed_sec for r in results)

    logging.info(
        "重建总结：目标群组=%s，成功=%s，跳过=%s，失败=%s，删除旧消息=%s，重拉后消息=%s，缺失前=%s，成功群重拉后仍缺失=%s，总耗时 %.2fs",
        len(results),
        len(success),
        len(skipped),
        len(failed),
        deleted_messages,
        after_messages,
        missing_before,
        missing_after,
        elapsed,
    )
    if skipped:
        logging.warning(
            "跳过列表：%s",
            "; ".join(
                f"chat_id={r.chat.chat_id} missing={r.chat.missing_count} reason={r.error}"
                for r in skipped[:20]
            ),
        )
    if failed:
        logging.error(
            "失败列表：%s",
            "; ".join(
                f"chat_id={r.chat.chat_id} missing={r.chat.missing_count} reason={r.error}"
                for r in failed[:20]
            ),
        )


def _run() -> int:
    args = _build_parser().parse_args()
    setup_logging()

    conn = _open_conn()
    try:
        if args.create_ranking_index:
            created = ensure_missing_identity_ranking_index(conn)
            logging.info(
                "缺失身份字段排行索引%s：%s",
                "已创建或已存在" if created else "未创建",
                MISSING_IDENTITY_INDEX_NAME,
            )
        chats = load_missing_media_chats(
            conn,
            target_mode=args.target_mode,
            min_missing=args.min_missing,
            limit=args.limit,
            chat_id=args.chat_id,
        )
    finally:
        conn.close()

    if not chats:
        logging.info("未发现包含缺失媒体信息的群组")
        return 0

    _log_plan(chats, execute=bool(args.execute))
    if not args.execute:
        return 0

    job_id = f"resync-missing-media-{int(time.time())}"
    context_token = job_context.set(job_id)
    client = None
    worker_id = f"{job_id}_main"
    results: List[ResyncResult] = []
    try:
        if not _ensure_base_session_valid(CFG, job_id, _append_job_log):
            return 1
        client = _create_isolated_worker_client(CFG, worker_id)

        for idx, chat in enumerate(chats, start=1):
            result = _resync_one_chat(
                client,
                job_id,
                chat,
                idx,
                len(chats),
                target_mode=args.target_mode,
            )
            results.append(result)
            if result.status == "error" and args.stop_on_error:
                logging.error("因 --stop-on-error 已停止后续群组")
                break
            delay = max(0.0, float(args.sleep_between_chats))
            if delay > 0 and idx < len(chats):
                time.sleep(delay)
    finally:
        if client is not None:
            try:
                _disconnect_worker_client(client)
            except Exception:
                pass
        _cleanup_isolated_worker_session(CFG, worker_id)
        job_context.reset(context_token)

    _log_summary(results)
    return 1 if any(r.status == "error" for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(_run())
