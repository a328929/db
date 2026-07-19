import logging
import time
from collections import defaultdict
from collections.abc import Callable, Iterable
from contextlib import suppress
from typing import Any

from tg_harvest.config import CFG
from tg_harvest.domain.normalize import normalize_search_term
from tg_harvest.ingest.media_groups import _refresh_media_groups_for_cursor
from tg_harvest.storage.connection import synchronized_write
from tg_harvest.storage.schema import (
    _refresh_chat_message_counts,
    refresh_chat_message_counts,
)
from tg_harvest.storage.search_text_state import (
    indexed_messages_from_clause,
    indexed_unsearchable_message_predicate,
)

CLEANUP_DELETE_BATCH_SIZE = 2000
MEDIA_GROUP_SYNC_BATCH_SIZE = 500
# Keep every DELETE statement below SQLite's common 999-variable limit. The
# outer cleanup batch can remain larger; it is split inside one transaction.
CLEANUP_SQL_BATCH_SIZE = 400
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

    # ``messages.grouped_id`` is the canonical source for rebuilding an
    # aggregate, but old/partially migrated rows can retain a different
    # grouped_id in ``message_media``.  Include both values so deleting the
    # message also removes any stale aggregate keyed by the media metadata.
    cur.execute(
        """
        SELECT DISTINCT t.chat_id, t.grouped_id
        FROM temp_cleanup_targets AS t
        UNION
        SELECT DISTINCT t.chat_id, mm.grouped_id
        FROM temp_cleanup_targets AS t
        JOIN message_media AS mm
          ON mm.chat_id = t.chat_id
         AND mm.message_id = t.message_id
        WHERE mm.grouped_id IS NOT NULL
        """
    )
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


def _load_chats_without_messages_from_cursor(
    cur, chat_ids: Iterable[int]
) -> set[int]:
    normalized_ids = sorted({int(chat_id) for chat_id in chat_ids})
    if not normalized_ids:
        return set()

    empty_chat_ids: set[int] = set()
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
    return empty_chat_ids


@synchronized_write
def _refresh_cleanup_denormalized_state_locked(
    conn,
    affected_chats: Iterable[int],
    affected_groups_by_chat: dict[int, set[int]],
) -> tuple[int, int, int]:
    normalized_chats = sorted({int(chat_id) for chat_id in affected_chats})
    if not normalized_chats:
        return 0, 0, 0

    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        emptied_chats = _load_chats_without_messages_from_cursor(cur, normalized_chats)
        affected_group_count = sum(
            len(affected_groups_by_chat.get(chat_id, set()))
            for chat_id in normalized_chats
        )
        deleted_group_count = 0
        rebuilt_group_count = 0

        for chat_id in normalized_chats:
            if chat_id in emptied_chats:
                cur.execute("DELETE FROM media_groups WHERE chat_id = ?", (chat_id,))
                deleted_group_count += int(cur.rowcount or 0)
                continue

            grouped_ids = sorted(
                {
                    int(grouped_id)
                    for grouped_id in affected_groups_by_chat.get(chat_id, set())
                }
            )
            for grouped_id_part in _chunked(
                grouped_ids, MEDIA_GROUP_SYNC_BATCH_SIZE
            ):
                placeholders = ",".join("?" for _ in grouped_id_part)
                cur.execute(
                    f"""
                    SELECT COUNT(DISTINCT grouped_id)
                    FROM messages
                    WHERE chat_id = ? AND grouped_id IN ({placeholders})
                    """,
                    [chat_id, *grouped_id_part],
                )
                remaining_group_count = int(cur.fetchone()[0] or 0)
                if remaining_group_count == 0:
                    cur.execute(
                        f"""
                        DELETE FROM media_groups
                        WHERE chat_id = ? AND grouped_id IN ({placeholders})
                        """,
                        [chat_id, *grouped_id_part],
                    )
                    deleted_group_count += int(cur.rowcount or 0)
                    continue
                rebuilt_group_count += remaining_group_count
                cur.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM media_groups
                    WHERE chat_id = ? AND grouped_id IN ({placeholders})
                    """,
                    [chat_id, *grouped_id_part],
                )
                before_count = int(cur.fetchone()[0] or 0)
                _refresh_media_groups_for_cursor(
                    cur,
                    chat_id,
                    CFG,
                    grouped_ids=set(grouped_id_part),
                )
                cur.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM media_groups
                    WHERE chat_id = ? AND grouped_id IN ({placeholders})
                    """,
                    [chat_id, *grouped_id_part],
                )
                after_count = int(cur.fetchone()[0] or 0)
                deleted_group_count += max(0, before_count - after_count)

        _refresh_chat_message_counts(cur, normalized_chats)
        conn.commit()
        return affected_group_count, deleted_group_count, rebuilt_group_count
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
    try:
        admin_job_append_log_fn(job_id, "正在同步清理关联媒体组信息...")
    except Exception:
        logging.exception("记录清理关联数据同步进度失败: job_id=%s", job_id)
    try:
        affected_group_count, deleted_group_count, rebuilt_group_count = (
            _refresh_cleanup_denormalized_state_locked(
                conn,
                normalized_chats,
                affected_groups_by_chat,
            )
        )
    except Exception:
        # Physical deletion is committed in batches. If a media-group trigger
        # or rebuild fails during the final transaction, repair chat summaries
        # independently so the next admin view still reports the real count.
        logging.exception("清理关联媒体组收尾失败，尝试单独修复群聊摘要: job_id=%s", job_id)
        try:
            refresh_chat_message_counts(conn, normalized_chats)
        except Exception:
            logging.exception("清理收尾单独修复群聊摘要也失败: job_id=%s", job_id)
        raise
    elapsed = time.perf_counter() - started_at
    try:
        admin_job_append_log_fn(
            job_id,
            "关联数据同步完成："
            f"涉及媒体组 {affected_group_count} 个，"
            f"直接移除空组 {deleted_group_count} 个，"
            f"重建 {rebuilt_group_count} 个，"
            f"耗时 {elapsed:.2f}s",
        )
    except Exception:
        logging.exception("记录清理关联数据同步结果失败: job_id=%s", job_id)


@synchronized_write
def _delete_cleanup_batch(conn, cur, pks: list[int]) -> int:
    if not pks:
        return 0

    try:
        count = 0
        for part in _chunked(pks, CLEANUP_SQL_BATCH_SIZE):
            placeholders = ",".join("?" for _ in part)
            cur.execute(
                f"""
                DELETE FROM message_media
                WHERE (chat_id, message_id) IN (
                    SELECT chat_id, message_id
                    FROM messages
                    WHERE pk IN ({placeholders})
                )
                """,
                part,
            )
            cur.execute(f"DELETE FROM messages WHERE pk IN ({placeholders})", part)
            count += int(cur.rowcount or 0)
            cur.execute(
                f"DELETE FROM temp_cleanup_targets WHERE pk IN ({placeholders})",
                part,
            )
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
    deletion_error: Exception | None = None
    try:
        while True:
            cur.execute(
                f"SELECT pk FROM temp_cleanup_targets LIMIT {CLEANUP_DELETE_BATCH_SIZE}"
            )
            rows = cur.fetchall()
            if not rows:
                break

            pks = [r[0] for r in rows]
            count = _delete_cleanup_batch(conn, cur, pks)
            deleted += count
            try:
                admin_job_append_log_fn(
                    job_id, f"进度：已清理 {deleted}/{target_count}"
                )
            except Exception:
                logging.exception("记录清理任务进度失败: job_id=%s", job_id)
            time.sleep(0.02)
    except Exception as exc:
        deletion_error = exc
        raise
    finally:
        try:
            _refresh_cleanup_denormalized_state(
                conn,
                job_id,
                affected_chats,
                affected_groups_by_chat,
                admin_job_append_log_fn,
            )
        except Exception:
            if deletion_error is None:
                raise
            logging.exception(
                "清理异常收尾时同步关联数据失败，保留原始清理异常: job_id=%s",
                job_id,
            )

    return deleted
