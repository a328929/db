# -*- coding: utf-8 -*-
import sqlite3
from typing import List, Optional, Tuple, Set, Dict
from datetime import datetime, timezone
from .normalize import make_hash

# =========================
# 去重逻辑
# =========================

def build_media_fingerprint(file_unique_id: Optional[str],
                            mime_type: Optional[str],
                            file_size: Optional[int],
                            width: Optional[int],
                            height: Optional[int],
                            duration_sec: Optional[int]) -> str:
    """
    不下载文件的情况下做“媒体指纹”：
    - 优先 file_unique_id（最稳）
    - 否则退化为 mime/size/wh/dur
    """
    if file_unique_id:
        return "fid:" + str(file_unique_id)

    parts = [
        f"mime={mime_type or ''}",
        f"size={file_size or 0}",
        f"w={width or 0}",
        f"h={height or 0}",
        f"d={duration_sec or 0}",
    ]
    raw = "|".join(parts)
    return "meta:" + make_hash(raw)


def make_media_group_signature(media_fingerprints: List[str], msg_types: List[str], item_count: int) -> str:
    """
    媒体组签名（用于“相册广告”二次去重通道）
    - 指纹排序后签名，抗 caption 随机扰动
    """
    fps = [x for x in media_fingerprints if x]
    if not fps:
        return ""

    # 排序提升稳定性（即使顺序偶发有变，也尽量不影响）
    fps_sorted = sorted(fps)
    types_sorted = sorted([t for t in msg_types if t])
    raw = f"n={item_count}|types={','.join(types_sorted)}|fps={'|'.join(fps_sorted)}"
    return make_hash(raw)


def build_message_dedupe_hash(text_pure_hash: str,
                              has_media: bool,
                              media_fingerprint: Optional[str]) -> str:
    """
    单条消息去重键：
    - 优先文本模板 hash（原逻辑兼容）
    - 文本为空时，媒体消息回退到媒体指纹签名
    """
    if text_pure_hash:
        return text_pure_hash
    if has_media and media_fingerprint:
        return "m:" + make_hash(media_fingerprint)
    return ""


def dedupe_promotional_duplicates(
    conn: sqlite3.Connection,
    chat_id: int,
    mode: str = "PURGE_ALL",
    threshold: int = 2,
    promo_score_threshold: int = 3,
) -> Tuple[int, int, int, int, Set[int]]:
    """
    双通道去重（硬删除）：
    A) 单条消息（非媒体组）按 dedupe_hash 去重（文本优先 / 空文本回退媒体指纹）
    B) 媒体组（grouped_id）去重
       - 文本模板通道（pure_hash）
       - 媒体签名通道（media_sig_hash）【抗随机乱码文案】

    返回：
    (处理条数, 单条重复模板数, 媒体组文本模板数, 媒体组媒体签名数, 受影响grouped_id集合)
    """
    batch_id = datetime.now(timezone.utc).strftime("dedupe_%Y%m%d_%H%M%S")
    cur = conn.cursor()
    mode = (mode or "PURGE_ALL").upper()

    cur.execute("""
        INSERT OR REPLACE INTO dedupe_runs(batch_id, chat_id, mode, threshold, promo_threshold, started_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
    """, (batch_id, chat_id, mode, int(threshold), int(promo_score_threshold)))
    conn.commit()

    # ========== A) 单条消息（非媒体组）重复模板 ==========
    cur.execute("DROP TABLE IF EXISTS temp_dup_hashes_solo")
    cur.execute("""
        CREATE TEMP TABLE temp_dup_hashes_solo AS
        SELECT dedupe_hash
        FROM messages
        WHERE chat_id = ?
          AND grouped_id IS NULL
          AND is_promo = 1
          AND dedupe_eligible = 1
          AND dedupe_hash <> ''
        GROUP BY dedupe_hash
        HAVING COUNT(*) >= ?
    """, (chat_id, threshold))
    cur.execute("SELECT COUNT(*) AS c FROM temp_dup_hashes_solo")
    dup_hash_count_solo = int(cur.fetchone()["c"] or 0)

    # ========== B1) 媒体组文本模板重复 ==========
    cur.execute("DROP TABLE IF EXISTS temp_dup_hashes_group_txt")
    cur.execute("""
        CREATE TEMP TABLE temp_dup_hashes_group_txt AS
        SELECT pure_hash
        FROM media_groups
        WHERE chat_id = ?
          AND item_count >= 2
          AND is_promo = 1
          AND dedupe_eligible = 1
          AND pure_hash <> ''
        GROUP BY pure_hash
        HAVING COUNT(*) >= ?
    """, (chat_id, threshold))
    cur.execute("SELECT COUNT(*) AS c FROM temp_dup_hashes_group_txt")
    dup_hash_count_group_txt = int(cur.fetchone()["c"] or 0)

    # ========== B2) 媒体组媒体签名重复（抗随机文案） ==========
    cur.execute("DROP TABLE IF EXISTS temp_dup_hashes_group_med")
    cur.execute("""
        CREATE TEMP TABLE temp_dup_hashes_group_med AS
        SELECT media_sig_hash
        FROM media_groups
        WHERE chat_id = ?
          AND item_count >= 2
          AND is_promo = 1
          AND dedupe_eligible = 1
          AND media_sig_hash <> ''
        GROUP BY media_sig_hash
        HAVING COUNT(*) >= ?
    """, (chat_id, threshold))
    cur.execute("SELECT COUNT(*) AS c FROM temp_dup_hashes_group_med")
    dup_hash_count_group_med = int(cur.fetchone()["c"] or 0)

    # 汇总目标消息
    cur.execute("DROP TABLE IF EXISTS temp_targets")
    cur.execute("CREATE TEMP TABLE temp_targets(pk INTEGER PRIMARY KEY)")

    # A: 单条目标
    cur.execute("""
        INSERT OR IGNORE INTO temp_targets(pk)
        SELECT pk
        FROM messages
        WHERE chat_id = ?
          AND grouped_id IS NULL
          AND is_promo = 1
          AND dedupe_eligible = 1
          AND dedupe_hash IN (SELECT dedupe_hash FROM temp_dup_hashes_solo)
    """, (chat_id,))

    # B: 命中的媒体组（文本 OR 媒体签名）
    cur.execute("DROP TABLE IF EXISTS temp_target_groups")
    cur.execute("""
        CREATE TEMP TABLE temp_target_groups AS
        SELECT DISTINCT grouped_id
        FROM media_groups
        WHERE chat_id = ?
          AND item_count >= 2
          AND is_promo = 1
          AND dedupe_eligible = 1
          AND (
            pure_hash IN (SELECT pure_hash FROM temp_dup_hashes_group_txt)
            OR media_sig_hash IN (SELECT media_sig_hash FROM temp_dup_hashes_group_med)
          )
    """, (chat_id,))

    cur.execute("""
        INSERT OR IGNORE INTO temp_targets(pk)
        SELECT m.pk
        FROM messages m
        WHERE m.chat_id = ?
          AND m.grouped_id IN (SELECT grouped_id FROM temp_target_groups)
    """, (chat_id,))

    # KEEP_FIRST：每个模板保留最早单条 / 每个组模板保留最早组
    if mode == "KEEP_FIRST":
        # 单条
        cur.execute("DROP TABLE IF EXISTS temp_keep_solo")
        cur.execute("""
            CREATE TEMP TABLE temp_keep_solo AS
            SELECT pk
            FROM (
                SELECT pk,
                       ROW_NUMBER() OVER (
                           PARTITION BY dedupe_hash
                           ORDER BY msg_date_ts ASC, message_id ASC, pk ASC
                       ) AS rn
                FROM messages
                WHERE chat_id = ?
                  AND grouped_id IS NULL
                  AND is_promo = 1
                  AND dedupe_eligible = 1
                  AND dedupe_hash IN (SELECT dedupe_hash FROM temp_dup_hashes_solo)
            )
            WHERE rn = 1
        """, (chat_id,))
        cur.execute("DELETE FROM temp_targets WHERE pk IN (SELECT pk FROM temp_keep_solo)")

        # 组（文本模板保留）
        cur.execute("DROP TABLE IF EXISTS temp_keep_groups_txt")
        cur.execute("""
            CREATE TEMP TABLE temp_keep_groups_txt AS
            SELECT mg.grouped_id
            FROM media_groups mg
            JOIN (
                SELECT pure_hash, MIN(first_message_id) AS min_msgid
                FROM media_groups
                WHERE chat_id = ?
                  AND item_count >= 2
                  AND is_promo = 1
                  AND dedupe_eligible = 1
                  AND pure_hash IN (SELECT pure_hash FROM temp_dup_hashes_group_txt)
                GROUP BY pure_hash
            ) k
              ON mg.pure_hash = k.pure_hash AND mg.first_message_id = k.min_msgid
            WHERE mg.chat_id = ?
            GROUP BY mg.pure_hash
        """, (chat_id, chat_id))

        # 组（媒体签名保留）
        cur.execute("DROP TABLE IF EXISTS temp_keep_groups_med")
        cur.execute("""
            CREATE TEMP TABLE temp_keep_groups_med AS
            SELECT mg.grouped_id
            FROM media_groups mg
            JOIN (
                SELECT media_sig_hash, MIN(first_message_id) AS min_msgid
                FROM media_groups
                WHERE chat_id = ?
                  AND item_count >= 2
                  AND is_promo = 1
                  AND dedupe_eligible = 1
                  AND media_sig_hash IN (SELECT media_sig_hash FROM temp_dup_hashes_group_med)
                GROUP BY media_sig_hash
            ) k
              ON mg.media_sig_hash = k.media_sig_hash AND mg.first_message_id = k.min_msgid
            WHERE mg.chat_id = ?
            GROUP BY mg.media_sig_hash
        """, (chat_id, chat_id))

        # 合并保留组（被任一通道保留即可）
        cur.execute("DROP TABLE IF EXISTS temp_keep_groups_final")
        cur.execute("""
            CREATE TEMP TABLE temp_keep_groups_final AS
            SELECT grouped_id FROM temp_keep_groups_txt
            UNION
            SELECT grouped_id FROM temp_keep_groups_med
        """)

        cur.execute("""
            DELETE FROM temp_targets
            WHERE pk IN (
                SELECT m.pk
                FROM messages m
                WHERE m.chat_id = ?
                  AND m.grouped_id IN (SELECT grouped_id FROM temp_keep_groups_final)
            )
        """, (chat_id,))

    cur.execute("SELECT COUNT(*) AS c FROM temp_targets")
    target_count = int(cur.fetchone()["c"] or 0)

    if target_count == 0:
        cur.execute("""
            UPDATE dedupe_runs
            SET dup_hash_count_solo=?,
                dup_hash_count_group_txt=?,
                dup_hash_count_group_med=?,
                target_count=0,
                finished_at=datetime('now')
            WHERE batch_id=?
        """, (dup_hash_count_solo, dup_hash_count_group_txt, dup_hash_count_group_med, batch_id))
        conn.commit()
        return 0, dup_hash_count_solo, dup_hash_count_group_txt, dup_hash_count_group_med, set()

    # 审计记录
    cur.execute("""
        INSERT INTO dedupe_actions(batch_id, chat_id, pk, message_id, grouped_id, dedupe_hash, pure_hash, action, reason)
        SELECT ?, m.chat_id, m.pk, m.message_id, m.grouped_id, m.dedupe_hash, m.pure_hash, 'HARD_DELETE',
               'DEDUPE_PROMO_HASH_OR_MEDIA_GROUP'
        FROM messages m
        WHERE m.pk IN (SELECT pk FROM temp_targets)
    """, (batch_id,))

    cur.execute("""
        SELECT DISTINCT grouped_id
        FROM messages
        WHERE pk IN (SELECT pk FROM temp_targets)
          AND grouped_id IS NOT NULL
    """)
    affected_group_ids = {int(r["grouped_id"]) for r in cur.fetchall()}

    # 硬删除（级联清理 message_media，FTS 触发器同步）
    cur.execute("DELETE FROM messages WHERE pk IN (SELECT pk FROM temp_targets)")

    cur.execute("""
        UPDATE dedupe_runs
        SET dup_hash_count_solo=?,
            dup_hash_count_group_txt=?,
            dup_hash_count_group_med=?,
            target_count=?,
            finished_at=datetime('now')
        WHERE batch_id=?
    """, (dup_hash_count_solo, dup_hash_count_group_txt, dup_hash_count_group_med, target_count, batch_id))

    conn.commit()
    return target_count, dup_hash_count_solo, dup_hash_count_group_txt, dup_hash_count_group_med, affected_group_ids
