import time
from collections import defaultdict
from collections.abc import Callable, Iterable
from contextlib import suppress
from typing import Any

from tg_harvest.config import CFG
from tg_harvest.domain.normalize import normalize_search_term
from tg_harvest.ingest.media_groups import refresh_media_groups_for_chat
from tg_harvest.storage.connection import synchronized_write
from tg_harvest.storage.schema import refresh_chat_message_counts
from tg_harvest.storage.search_text_state import (
    indexed_messages_from_clause,
    indexed_unsearchable_message_predicate,
)

CLEANUP_DELETE_BATCH_SIZE = 2000
MEDIA_GROUP_SYNC_BATCH_SIZE = 500
LIKE_ESCAPE_CHAR = "\\"


def _escape_like_literal(value: str) -> str:
    return (
        str(value or "")
        .replace(LIKE_ESCAPE_CHAR, LIKE_ESCAPE_CHAR + LIKE_ESCAPE_CHAR)
        .replace("%", LIKE_ESCAPE_CHAR + "%")
        .replace("_", LIKE_ESCAPE_CHAR + "_")
    )


def _build_cleanup_like_patterns(keyword: str) -> tuple[str, str]:
    raw_keyword = str(keyword or "")
    normalized_keyword = normalize_search_term(raw_keyword)
    return (
        f"%{_escape_like_literal(normalized_keyword)}%",
        f"%{_escape_like_literal(raw_keyword)}%",
    )


def _coerce_cleanup_like_patterns(like_pattern: Any) -> tuple[str, str]:
    if isinstance(like_pattern, (tuple, list)) and len(like_pattern) == 2:
        return str(like_pattern[0]), str(like_pattern[1])
    return str(like_pattern or ""), str(like_pattern or "")


def _build_cleanup_targets_table(
    cur,
    mode,
    scope_filter_sql,
    scope_filter_params,
    like_pattern,
):
    cur.execute("DROP TABLE IF EXISTS temp_cleanup_targets")
    cur.execute(
        """
        CREATE TEMP TABLE temp_cleanup_targets (
            chat_id INTEGER,
            pk INTEGER,
            message_id INTEGER,
            grouped_id INTEGER,
            PRIMARY KEY (chat_id, pk)
        )
        """
    )
    if mode == "empty_media":
        # 搜索统一只看 messages.content_norm/content。执行任务前会先把
        # message_media.file_name 回填到这两个字段，因此这里删除的就是
        # 最终仍没有任何可搜索文本的消息。
        unsearchable_predicate = indexed_unsearchable_message_predicate(cur, alias="m")
        messages_from_sql = indexed_messages_from_clause(
            cur,
            alias="m",
            chat_scoped="m.chat_id = ?" in scope_filter_sql,
        )
        cur.execute(
            f"""
            INSERT INTO temp_cleanup_targets
            SELECT m.chat_id, m.pk, m.message_id, m.grouped_id
            FROM {messages_from_sql}
            WHERE {unsearchable_predicate}
              {scope_filter_sql}
            """,
            scope_filter_params,
        )
    else:
        norm_like_pattern, raw_like_pattern = _coerce_cleanup_like_patterns(
            like_pattern
        )
        cur.execute(
            f"""
            INSERT INTO temp_cleanup_targets
            SELECT chat_id, pk, message_id, grouped_id
            FROM messages m
            WHERE (
                COALESCE(content_norm, '') LIKE ? ESCAPE '{LIKE_ESCAPE_CHAR}'
                OR COALESCE(content, '') LIKE ? ESCAPE '{LIKE_ESCAPE_CHAR}'
            )
            {scope_filter_sql}
            """,
            (norm_like_pattern, raw_like_pattern, *scope_filter_params),
        )
    return int(
        cur.execute("SELECT COUNT(*) FROM temp_cleanup_targets").fetchone()[0] or 0
    )


def _collect_cleanup_affected_state(cur) -> tuple[set[int], dict[int, set[int]]]:
    affected_chats: set[int] = set()
    affected_groups_by_chat: dict[int, set[int]] = defaultdict(set)

    cur.execute("SELECT DISTINCT chat_id, grouped_id FROM temp_cleanup_targets")
    for row in cur.fetchall():
        chat_id = int(row[0])
        affected_chats.add(chat_id)
        grouped_id = row[1]
        if grouped_id is not None:
            affected_groups_by_chat[chat_id].add(int(grouped_id))

    return affected_chats, affected_groups_by_chat


def _chunked(values: list[int], size: int):
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _load_remaining_grouped_ids(cur, chat_id: int, grouped_ids: set[int]) -> set[int]:
    normalized_ids = sorted({int(grouped_id) for grouped_id in grouped_ids})
    if not normalized_ids:
        return set()

    remaining_ids: set[int] = set()
    for part in _chunked(normalized_ids, MEDIA_GROUP_SYNC_BATCH_SIZE):
        placeholders = ",".join(["?"] * len(part))
        cur.execute(
            f"""
            SELECT DISTINCT grouped_id
            FROM messages
            WHERE chat_id = ?
              AND grouped_id IN ({placeholders})
            """,
            [int(chat_id), *part],
        )
        remaining_ids.update(
            int(row[0] if not hasattr(row, "keys") else row["grouped_id"])
            for row in cur.fetchall()
            if (row[0] if not hasattr(row, "keys") else row["grouped_id"]) is not None
        )
    return remaining_ids


@synchronized_write
def _delete_empty_media_groups(
    conn,
    chat_id: int,
    grouped_ids: set[int],
) -> int:
    normalized_ids = sorted({int(grouped_id) for grouped_id in grouped_ids})
    if not normalized_ids:
        return 0

    cur = conn.cursor()
    try:
        deleted = 0
        cur.execute("BEGIN IMMEDIATE")
        for part in _chunked(normalized_ids, MEDIA_GROUP_SYNC_BATCH_SIZE):
            placeholders = ",".join(["?"] * len(part))
            cur.execute(
                f"""
                DELETE FROM media_groups
                WHERE chat_id = ?
                  AND grouped_id IN ({placeholders})
                """,
                [int(chat_id), *part],
            )
            deleted += int(cur.rowcount or 0)
        conn.commit()
        return deleted
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        cur.close()


def _split_remaining_and_deleted_groups(
    conn,
    chat_id: int,
    grouped_ids: set[int],
) -> tuple[set[int], set[int]]:
    cur = conn.cursor()
    try:
        remaining_ids = _load_remaining_grouped_ids(cur, chat_id, grouped_ids)
    finally:
        cur.close()
    deleted_ids = {int(grouped_id) for grouped_id in grouped_ids} - remaining_ids
    return remaining_ids, deleted_ids


def _load_chats_without_messages(conn, chat_ids: Iterable[int]) -> set[int]:
    normalized_ids = sorted({int(chat_id) for chat_id in chat_ids})
    if not normalized_ids:
        return set()

    empty_chat_ids: set[int] = set()
    cur = conn.cursor()
    try:
        for part in _chunked(normalized_ids, MEDIA_GROUP_SYNC_BATCH_SIZE):
            placeholders = ",".join(["?"] * len(part))
            cur.execute(
                f"""
                SELECT c.chat_id
                FROM chats c
                WHERE c.chat_id IN ({placeholders})
                  AND NOT EXISTS (
                      SELECT 1
                      FROM messages m
                      WHERE m.chat_id = c.chat_id
                  )
                """,
                part,
            )
            empty_chat_ids.update(int(row[0]) for row in cur.fetchall())
    finally:
        cur.close()
    return empty_chat_ids


@synchronized_write
def _delete_media_groups_for_chats(conn, chat_ids: Iterable[int]) -> int:
    normalized_ids = sorted({int(chat_id) for chat_id in chat_ids})
    if not normalized_ids:
        return 0

    cur = conn.cursor()
    try:
        deleted = 0
        cur.execute("BEGIN IMMEDIATE")
        for part in _chunked(normalized_ids, MEDIA_GROUP_SYNC_BATCH_SIZE):
            placeholders = ",".join(["?"] * len(part))
            cur.execute(
                f"""
                DELETE FROM media_groups
                WHERE chat_id IN ({placeholders})
                """,
                part,
            )
            deleted += int(cur.rowcount or 0)
        conn.commit()
        return deleted
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        cur.close()


def _refresh_cleanup_denormalized_state(
    conn,
    job_id: str,
    affected_chats: Iterable[int],
    affected_groups_by_chat: dict[int, set[int]],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    normalized_chats = sorted({int(chat_id) for chat_id in affected_chats})
    if not normalized_chats:
        return

    started_at = time.perf_counter()
    affected_group_count = 0
    deleted_group_count = 0
    rebuilt_group_count = 0
    admin_job_append_log_fn(job_id, "正在同步清理关联媒体组信息...")
    emptied_chats = _load_chats_without_messages(conn, normalized_chats)
    if emptied_chats:
        deleted_group_count += _delete_media_groups_for_chats(conn, emptied_chats)

    for chat_id in normalized_chats:
        grouped_ids = affected_groups_by_chat.get(chat_id, set())
        if not grouped_ids:
            continue

        affected_group_count += len(grouped_ids)
        if chat_id in emptied_chats:
            continue

        remaining_grouped_ids, deleted_grouped_ids = _split_remaining_and_deleted_groups(
            conn,
            chat_id,
            grouped_ids,
        )
        if deleted_grouped_ids:
            _delete_empty_media_groups(conn, chat_id, deleted_grouped_ids)
            deleted_group_count += len(deleted_grouped_ids)
        if remaining_grouped_ids:
            refresh_media_groups_for_chat(
                conn,
                chat_id,
                cfg=CFG,
                grouped_ids=remaining_grouped_ids,
            )
            rebuilt_group_count += len(remaining_grouped_ids)

    refresh_chat_message_counts(conn, normalized_chats)
    elapsed = time.perf_counter() - started_at
    admin_job_append_log_fn(
        job_id,
        "关联数据同步完成："
        f"涉及媒体组 {affected_group_count} 个，"
        f"直接移除空组 {deleted_group_count} 个，"
        f"重建 {rebuilt_group_count} 个，"
        f"耗时 {elapsed:.2f}s",
    )


@synchronized_write
def _delete_cleanup_batch(conn, cur, pks: list[int]) -> int:
    if not pks:
        return 0

    placeholders = ",".join(["?"] * len(pks))
    try:
        cur.execute(
            f"""
            DELETE FROM message_media
            WHERE (chat_id, message_id) IN (
                SELECT chat_id, message_id
                FROM messages
                WHERE pk IN ({placeholders})
            )
            """,
            pks,
        )
        cur.execute(f"DELETE FROM messages WHERE pk IN ({placeholders})", pks)
        count = int(cur.rowcount or 0)
        cur.execute(f"DELETE FROM temp_cleanup_targets WHERE pk IN ({placeholders})", pks)
        conn.commit()
        return count
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise


def _execute_cleanup_deletion_batches(
    conn,
    cur,
    job_id: str,
    target_count: int,
    admin_job_append_log_fn: Callable[[str, str], Any],
):
    deleted = 0
    affected_chats, affected_groups_by_chat = _collect_cleanup_affected_state(cur)

    while True:
        cur.execute(f"SELECT pk FROM temp_cleanup_targets LIMIT {CLEANUP_DELETE_BATCH_SIZE}")
        rows = cur.fetchall()
        if not rows:
            break

        pks = [r[0] for r in rows]
        count = _delete_cleanup_batch(conn, cur, pks)
        deleted += count
        admin_job_append_log_fn(job_id, f"进度：已清理 {deleted}/{target_count}")
        time.sleep(0.02)

    _refresh_cleanup_denormalized_state(
        conn,
        job_id,
        affected_chats,
        affected_groups_by_chat,
        admin_job_append_log_fn,
    )

    return deleted
