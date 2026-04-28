# -*- coding: utf-8 -*-
import time
from collections import defaultdict
from typing import Any, Callable, Dict, Iterable, Tuple

from tg_harvest.config import CFG
from tg_harvest.domain.normalize import normalize_search_term
from tg_harvest.storage.schema import refresh_chat_message_counts
from tg_harvest.ingest.store import refresh_media_groups_for_chat


CLEANUP_DELETE_BATCH_SIZE = 2000
LIKE_ESCAPE_CHAR = "\\"


def _escape_like_literal(value: str) -> str:
    return (
        str(value or "")
        .replace(LIKE_ESCAPE_CHAR, LIKE_ESCAPE_CHAR + LIKE_ESCAPE_CHAR)
        .replace("%", LIKE_ESCAPE_CHAR + "%")
        .replace("_", LIKE_ESCAPE_CHAR + "_")
    )


def _build_cleanup_like_patterns(keyword: str) -> Tuple[str, str]:
    raw_keyword = str(keyword or "")
    normalized_keyword = normalize_search_term(raw_keyword)
    return (
        f"%{_escape_like_literal(normalized_keyword)}%",
        f"%{_escape_like_literal(raw_keyword)}%",
    )


def _coerce_cleanup_like_patterns(like_pattern: Any) -> Tuple[str, str]:
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
        "CREATE TEMP TABLE temp_cleanup_targets (chat_id INTEGER, pk INTEGER, message_id INTEGER, grouped_id INTEGER, PRIMARY KEY (chat_id, pk))"
    )
    if mode == "empty_media":
        cur.execute(
            f"""
            INSERT INTO temp_cleanup_targets
            SELECT m.chat_id, m.pk, m.message_id, m.grouped_id
            FROM messages m
            WHERE COALESCE(NULLIF(m.content_norm, ''), NULLIF(m.content, ''), '') = ''
              AND m.has_media = 1
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


def _collect_cleanup_affected_state(cur) -> Tuple[set[int], Dict[int, set[int]]]:
    affected_chats: set[int] = set()
    affected_groups_by_chat: Dict[int, set[int]] = defaultdict(set)

    cur.execute("SELECT DISTINCT chat_id, grouped_id FROM temp_cleanup_targets")
    for row in cur.fetchall():
        chat_id = int(row[0])
        affected_chats.add(chat_id)
        grouped_id = row[1]
        if grouped_id is not None:
            affected_groups_by_chat[chat_id].add(int(grouped_id))

    return affected_chats, affected_groups_by_chat


def _refresh_cleanup_denormalized_state(
    conn,
    job_id: str,
    affected_chats: Iterable[int],
    affected_groups_by_chat: Dict[int, set[int]],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    normalized_chats = sorted({int(chat_id) for chat_id in affected_chats})
    if not normalized_chats:
        return

    admin_job_append_log_fn(job_id, "正在同步清理关联媒体组信息...")
    for chat_id in normalized_chats:
        grouped_ids = affected_groups_by_chat.get(chat_id, set())
        if grouped_ids:
            refresh_media_groups_for_chat(
                conn,
                chat_id,
                cfg=CFG,
                grouped_ids=set(grouped_ids),
            )

    refresh_chat_message_counts(conn, normalized_chats)
    admin_job_append_log_fn(job_id, "关联数据同步完成")


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
        placeholders = ",".join(["?"] * len(pks))

        cur.execute(f"DELETE FROM messages WHERE pk IN ({placeholders})", pks)
        count = cur.rowcount
        cur.execute(f"DELETE FROM temp_cleanup_targets WHERE pk IN ({placeholders})", pks)

        deleted += count
        conn.commit()
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
