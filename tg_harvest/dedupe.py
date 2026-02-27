# -*- coding: utf-8 -*-
import sqlite3
from typing import List, Optional, Tuple, Set
from datetime import datetime, timezone
import uuid
from .normalize import make_hash


def build_media_fingerprint(file_unique_id: Optional[str],
                            mime_type: Optional[str],
                            file_size: Optional[int],
                            width: Optional[int],
                            height: Optional[int],
                            duration_sec: Optional[int]) -> str:
    if file_unique_id:
        return "fid:" + str(file_unique_id)
    parts = [
        f"mime={mime_type or ''}",
        f"size={file_size or 0}",
        f"w={width or 0}",
        f"h={height or 0}",
        f"d={duration_sec or 0}",
    ]
    return "meta:" + make_hash("|".join(parts))


def make_media_group_signature(media_fingerprints: List[str], msg_types: List[str], item_count: int) -> str:
    fps = [x for x in media_fingerprints if x]
    if not fps:
        return ""
    raw = f"n={item_count}|types={','.join(sorted([t for t in msg_types if t]))}|fps={'|'.join(sorted(fps))}"
    return make_hash(raw)


def build_message_dedupe_hash(text_pure_hash: str,
                              has_media: bool,
                              media_fingerprint: Optional[str]) -> str:
    if text_pure_hash:
        return text_pure_hash
    if has_media and media_fingerprint:
        return "m:" + make_hash(media_fingerprint)
    return ""


def _create_temp_dup_hashes_solo(cur: sqlite3.Cursor, chat_id: int, threshold: int):
    cur.execute("DROP TABLE IF EXISTS temp_dup_hashes_solo")
    cur.execute(
        """
        CREATE TEMP TABLE temp_dup_hashes_solo AS
        SELECT dedupe_hash
        FROM messages
        WHERE chat_id = ? AND grouped_id IS NULL AND is_promo = 1 AND dedupe_eligible = 1 AND dedupe_hash <> ''
        GROUP BY dedupe_hash
        HAVING COUNT(*) >= ?
        """,
        (chat_id, threshold),
    )


def _create_temp_dup_hashes_group_txt(cur: sqlite3.Cursor, chat_id: int, threshold: int):
    cur.execute("DROP TABLE IF EXISTS temp_dup_hashes_group_txt")
    cur.execute(
        """
        CREATE TEMP TABLE temp_dup_hashes_group_txt AS
        SELECT pure_hash
        FROM media_groups
        WHERE chat_id = ? AND item_count >= 2 AND is_promo = 1 AND dedupe_eligible = 1 AND pure_hash <> ''
        GROUP BY pure_hash
        HAVING COUNT(*) >= ?
        """,
        (chat_id, threshold),
    )


def _create_temp_dup_hashes_group_med(cur: sqlite3.Cursor, chat_id: int, threshold: int):
    cur.execute("DROP TABLE IF EXISTS temp_dup_hashes_group_med")
    cur.execute(
        """
        CREATE TEMP TABLE temp_dup_hashes_group_med AS
        SELECT media_sig_hash
        FROM media_groups
        WHERE chat_id = ? AND item_count >= 2 AND is_promo = 1 AND dedupe_eligible = 1 AND media_sig_hash <> ''
        GROUP BY media_sig_hash
        HAVING COUNT(*) >= ?
        """,
        (chat_id, threshold),
    )


def _count_rows(cur: sqlite3.Cursor, table_name: str) -> int:
    cur.execute(f"SELECT COUNT(*) AS c FROM {table_name}")
    return int(cur.fetchone()["c"] or 0)


def _create_dup_hash_tables(cur: sqlite3.Cursor, chat_id: int, threshold: int) -> Tuple[int, int, int]:
    _create_temp_dup_hashes_solo(cur, chat_id, threshold)
    _create_temp_dup_hashes_group_txt(cur, chat_id, threshold)
    _create_temp_dup_hashes_group_med(cur, chat_id, threshold)

    solo = _count_rows(cur, "temp_dup_hashes_solo")
    group_txt = _count_rows(cur, "temp_dup_hashes_group_txt")
    group_med = _count_rows(cur, "temp_dup_hashes_group_med")
    return solo, group_txt, group_med


def _init_temp_targets(cur: sqlite3.Cursor):
    cur.execute("DROP TABLE IF EXISTS temp_targets")
    cur.execute("CREATE TEMP TABLE temp_targets(pk INTEGER PRIMARY KEY)")


def _insert_targets_from_solo_hashes(cur: sqlite3.Cursor, chat_id: int):
    cur.execute(
        """
        INSERT OR IGNORE INTO temp_targets(pk)
        SELECT pk FROM messages
        WHERE chat_id = ? AND grouped_id IS NULL AND is_promo = 1 AND dedupe_eligible = 1
          AND dedupe_hash IN (SELECT dedupe_hash FROM temp_dup_hashes_solo)
        """,
        (chat_id,),
    )


def _create_temp_target_groups_txt(cur: sqlite3.Cursor, chat_id: int):
    cur.execute("DROP TABLE IF EXISTS temp_target_groups_txt")
    cur.execute(
        """
        CREATE TEMP TABLE temp_target_groups_txt AS
        SELECT DISTINCT grouped_id
        FROM media_groups
        WHERE chat_id = ? AND item_count >= 2 AND is_promo = 1 AND dedupe_eligible = 1
          AND pure_hash IN (SELECT pure_hash FROM temp_dup_hashes_group_txt)
        """,
        (chat_id,),
    )


def _create_temp_target_groups_med(cur: sqlite3.Cursor, chat_id: int):
    cur.execute("DROP TABLE IF EXISTS temp_target_groups_med")
    cur.execute(
        """
        CREATE TEMP TABLE temp_target_groups_med AS
        SELECT DISTINCT grouped_id
        FROM media_groups
        WHERE chat_id = ? AND item_count >= 2 AND is_promo = 1 AND dedupe_eligible = 1
          AND media_sig_hash IN (SELECT media_sig_hash FROM temp_dup_hashes_group_med)
        """,
        (chat_id,),
    )


def _merge_temp_target_groups(cur: sqlite3.Cursor):
    cur.execute("DROP TABLE IF EXISTS temp_target_groups")
    cur.execute(
        """
        CREATE TEMP TABLE temp_target_groups AS
        SELECT grouped_id FROM temp_target_groups_txt
        UNION
        SELECT grouped_id FROM temp_target_groups_med
        """
    )


def _insert_targets_from_group_hashes(cur: sqlite3.Cursor, chat_id: int):
    cur.execute(
        """
        INSERT OR IGNORE INTO temp_targets(pk)
        SELECT m.pk FROM messages m
        WHERE m.chat_id = ? AND m.grouped_id IN (SELECT grouped_id FROM temp_target_groups)
        """,
        (chat_id,),
    )


def _fill_temp_targets(cur: sqlite3.Cursor, chat_id: int):
    _init_temp_targets(cur)
    _insert_targets_from_solo_hashes(cur, chat_id)
    _create_temp_target_groups_txt(cur, chat_id)
    _create_temp_target_groups_med(cur, chat_id)
    _merge_temp_target_groups(cur)
    _insert_targets_from_group_hashes(cur, chat_id)


def _build_keep_first_solo(cur: sqlite3.Cursor, chat_id: int):
    cur.execute("DROP TABLE IF EXISTS temp_keep_solo")
    try:
        cur.execute(
            """
            CREATE TEMP TABLE temp_keep_solo AS
            SELECT pk FROM (
                SELECT pk,
                       ROW_NUMBER() OVER (PARTITION BY dedupe_hash ORDER BY msg_date_ts ASC, message_id ASC, pk ASC) AS rn
                FROM messages
                WHERE chat_id = ? AND grouped_id IS NULL AND is_promo = 1 AND dedupe_eligible = 1
                  AND dedupe_hash IN (SELECT dedupe_hash FROM temp_dup_hashes_solo)
            ) WHERE rn = 1
            """,
            (chat_id,),
        )
    except sqlite3.Error as e:
        msg = str(e).lower()
        if "row_number" in msg or "over" in msg or "window" in msg:
            raise sqlite3.OperationalError(
                "SQLite 版本过低或未启用窗口函数支持，无法执行去重 KEEP_FIRST（ROW_NUMBER/OVER）。"
            ) from e
        raise


def _build_keep_first_groups_txt(cur: sqlite3.Cursor, chat_id: int):
    cur.execute("DROP TABLE IF EXISTS temp_keep_groups_txt")
    cur.execute(
        """
        CREATE TEMP TABLE temp_keep_groups_txt AS
        SELECT mg.grouped_id
        FROM media_groups mg
        JOIN (
            SELECT pure_hash, MIN(first_message_id) AS min_msgid
            FROM media_groups
            WHERE chat_id = ? AND item_count >= 2 AND is_promo = 1 AND dedupe_eligible = 1
              AND pure_hash IN (SELECT pure_hash FROM temp_dup_hashes_group_txt)
            GROUP BY pure_hash
        ) k ON mg.pure_hash = k.pure_hash AND mg.first_message_id = k.min_msgid
        WHERE mg.chat_id = ?
        GROUP BY mg.pure_hash
        """,
        (chat_id, chat_id),
    )


def _build_keep_first_groups_med(cur: sqlite3.Cursor, chat_id: int):
    cur.execute("DROP TABLE IF EXISTS temp_keep_groups_med")
    cur.execute(
        """
        CREATE TEMP TABLE temp_keep_groups_med AS
        SELECT mg.grouped_id
        FROM media_groups mg
        JOIN (
            SELECT media_sig_hash, MIN(first_message_id) AS min_msgid
            FROM media_groups
            WHERE chat_id = ? AND item_count >= 2 AND is_promo = 1 AND dedupe_eligible = 1
              AND media_sig_hash IN (SELECT media_sig_hash FROM temp_dup_hashes_group_med)
            GROUP BY media_sig_hash
        ) k ON mg.media_sig_hash = k.media_sig_hash AND mg.first_message_id = k.min_msgid
        WHERE mg.chat_id = ?
        GROUP BY mg.media_sig_hash
        """,
        (chat_id, chat_id),
    )


def _build_keep_first_groups_final(cur: sqlite3.Cursor):
    cur.execute("DROP TABLE IF EXISTS temp_keep_groups_final")
    cur.execute(
        """
        CREATE TEMP TABLE temp_keep_groups_final AS
        SELECT grouped_id FROM temp_keep_groups_txt
        UNION
        SELECT grouped_id FROM temp_keep_groups_med
        """
    )


def _prune_temp_targets_with_keep_first(cur: sqlite3.Cursor, chat_id: int):
    cur.execute("DELETE FROM temp_targets WHERE pk IN (SELECT pk FROM temp_keep_solo)")
    cur.execute(
        """
        DELETE FROM temp_targets
        WHERE pk IN (
            SELECT m.pk FROM messages m
            WHERE m.chat_id = ?
              AND m.grouped_id IN (SELECT grouped_id FROM temp_keep_groups_final)
        )
        """,
        (chat_id,),
    )


def _apply_keep_first(cur: sqlite3.Cursor, chat_id: int):
    _build_keep_first_solo(cur, chat_id)
    _build_keep_first_groups_txt(cur, chat_id)
    _build_keep_first_groups_med(cur, chat_id)
    _build_keep_first_groups_final(cur)
    _prune_temp_targets_with_keep_first(cur, chat_id)


def _insert_dedupe_run(
    cur: sqlite3.Cursor,
    batch_id: str,
    chat_id: int,
    mode: str,
    threshold: int,
    promo_score_threshold: int,
):
    cur.execute(
        """
        INSERT INTO dedupe_runs(batch_id, chat_id, mode, threshold, promo_threshold, started_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
        """,
        (batch_id, chat_id, mode, int(threshold), int(promo_score_threshold)),
    )


def _prepare_dedupe_targets(
    cur: sqlite3.Cursor,
    chat_id: int,
    mode: str,
    threshold: int,
) -> Tuple[int, int, int, int]:
    dup_hash_count_solo, dup_hash_count_group_txt, dup_hash_count_group_med = _create_dup_hash_tables(cur, chat_id, threshold)
    _fill_temp_targets(cur, chat_id)
    if mode == "KEEP_FIRST":
        _apply_keep_first(cur, chat_id)

    cur.execute("SELECT COUNT(*) AS c FROM temp_targets")
    target_count = int(cur.fetchone()["c"] or 0)
    return target_count, dup_hash_count_solo, dup_hash_count_group_txt, dup_hash_count_group_med


def _insert_dedupe_actions(cur: sqlite3.Cursor, batch_id: str):
    cur.execute(
        """
        INSERT INTO dedupe_actions(batch_id, chat_id, pk, message_id, grouped_id, dedupe_hash, pure_hash, action, reason)
        SELECT ?, m.chat_id, m.pk, m.message_id, m.grouped_id, m.dedupe_hash, m.pure_hash, 'HARD_DELETE',
               'DEDUPE_PROMO_HASH_OR_MEDIA_GROUP'
        FROM messages m
        WHERE m.pk IN (SELECT pk FROM temp_targets)
        """,
        (batch_id,),
    )


def _collect_affected_group_ids(cur: sqlite3.Cursor) -> Set[int]:
    cur.execute(
        """
        SELECT DISTINCT grouped_id
        FROM messages
        WHERE pk IN (SELECT pk FROM temp_targets)
          AND grouped_id IS NOT NULL
        """
    )
    return {int(r["grouped_id"]) for r in cur.fetchall()}


def _delete_target_messages(cur: sqlite3.Cursor):
    cur.execute("DELETE FROM messages WHERE pk IN (SELECT pk FROM temp_targets)")


def _record_actions_and_delete_targets(cur: sqlite3.Cursor, batch_id: str) -> Set[int]:
    _insert_dedupe_actions(cur, batch_id)
    affected_group_ids = _collect_affected_group_ids(cur)
    _delete_target_messages(cur)
    return affected_group_ids


def _finish_dedupe_run(
    cur: sqlite3.Cursor,
    batch_id: str,
    dup_hash_count_solo: int,
    dup_hash_count_group_txt: int,
    dup_hash_count_group_med: int,
    target_count: int,
):
    cur.execute(
        """
        UPDATE dedupe_runs
        SET dup_hash_count_solo=?, dup_hash_count_group_txt=?, dup_hash_count_group_med=?, target_count=?,
            finished_at=datetime('now')
        WHERE batch_id=?
        """,
        (dup_hash_count_solo, dup_hash_count_group_txt, dup_hash_count_group_med, target_count, batch_id),
    )


def dedupe_promotional_duplicates(
    conn: sqlite3.Connection,
    chat_id: int,
    mode: str = "PURGE_ALL",
    threshold: int = 2,
    promo_score_threshold: int = 3,
) -> Tuple[int, int, int, int, Set[int]]:
    """promo_score_threshold 仅用于审计记录，实际筛选依赖 is_promo/dedupe_eligible。"""
    batch_id = f"dedupe_{chat_id}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S_%f')}_{uuid.uuid4().hex[:8]}"
    mode = (mode or "PURGE_ALL").upper()
    cur = conn.cursor()

    try:
        # 去重主流程放在同一事务里，避免出现 run/action/delete/finished_at 的半状态。
        cur.execute("BEGIN IMMEDIATE")

        _insert_dedupe_run(cur, batch_id, chat_id, mode, threshold, promo_score_threshold)
        target_count, dup_hash_count_solo, dup_hash_count_group_txt, dup_hash_count_group_med = _prepare_dedupe_targets(
            cur, chat_id, mode, threshold
        )
        if target_count == 0:
            _finish_dedupe_run(
                cur,
                batch_id,
                dup_hash_count_solo,
                dup_hash_count_group_txt,
                dup_hash_count_group_med,
                0,
            )
            conn.commit()
            return 0, dup_hash_count_solo, dup_hash_count_group_txt, dup_hash_count_group_med, set()

        affected_group_ids = _record_actions_and_delete_targets(cur, batch_id)
        _finish_dedupe_run(
            cur,
            batch_id,
            dup_hash_count_solo,
            dup_hash_count_group_txt,
            dup_hash_count_group_med,
            target_count,
        )
        conn.commit()
        return target_count, dup_hash_count_solo, dup_hash_count_group_txt, dup_hash_count_group_med, affected_group_ids
    except Exception:
        conn.rollback()
        raise
