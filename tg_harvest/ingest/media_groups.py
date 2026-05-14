# -*- coding: utf-8 -*-
import logging
import sqlite3
import time
from typing import Any, Dict, Iterable, List, Optional, Set

from tg_harvest.config import AppConfig
from tg_harvest.domain.dedupe import make_media_group_signature
from tg_harvest.domain.normalize import _safe_json
from tg_harvest.domain.promo import build_group_promo_features
from tg_harvest.storage.connection import synchronized_write


UPSERT_MEDIA_GROUP_SQL = """
INSERT INTO media_groups(
    chat_id, grouped_id,
    first_message_id, first_msg_date_ts, last_message_id, last_msg_date_ts,
    item_count, active_items, types_csv,
    captions_concat, caption_norm, pure_hash, media_sig_hash, dedupe_hash,
    is_promo, promo_score, promo_reasons, dedupe_eligible, guard_reason,
    created_at, updated_at
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, datetime('now'), datetime('now'))
ON CONFLICT(chat_id, grouped_id) DO UPDATE SET
    first_message_id=excluded.first_message_id,
    first_msg_date_ts=excluded.first_msg_date_ts,
    last_message_id=excluded.last_message_id,
    last_msg_date_ts=excluded.last_msg_date_ts,
    item_count=excluded.item_count,
    active_items=excluded.active_items,
    types_csv=excluded.types_csv,
    captions_concat=excluded.captions_concat,
    caption_norm=excluded.caption_norm,
    pure_hash=excluded.pure_hash,
    media_sig_hash=excluded.media_sig_hash,
    dedupe_hash=excluded.dedupe_hash,
    is_promo=excluded.is_promo,
    promo_score=excluded.promo_score,
    promo_reasons=excluded.promo_reasons,
    dedupe_eligible=excluded.dedupe_eligible,
    guard_reason=excluded.guard_reason,
    updated_at=datetime('now')
"""


def chunked(seq: List[Any], n: int) -> Iterable[List[Any]]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def _normalize_grouped_ids(grouped_ids: Optional[Set[int]]) -> List[int]:
    if grouped_ids is None:
        return []
    return sorted({int(x) for x in grouped_ids if x is not None})


def _load_all_grouped_ids(cur: sqlite3.Cursor, chat_id: int) -> List[int]:
    cur.execute(
        "SELECT DISTINCT grouped_id FROM messages WHERE chat_id=? AND grouped_id IS NOT NULL",
        (chat_id,),
    )
    return [int(r["grouped_id"]) for r in cur.fetchall() if r["grouped_id"] is not None]


def _delete_media_groups(
    cur: sqlite3.Cursor, chat_id: int, grouped_ids: Optional[List[int]] = None
):
    if grouped_ids is None:
        cur.execute("DELETE FROM media_groups WHERE chat_id=?", (chat_id,))
        return
    if not grouped_ids:
        return
    for part in chunked(grouped_ids, 500):
        placeholders = ",".join(["?"] * len(part))
        cur.execute(
            f"DELETE FROM media_groups WHERE chat_id=? AND grouped_id IN ({placeholders})",
            [chat_id] + part,
        )


def _query_media_group_rows(
    cur: sqlite3.Cursor, chat_id: int, grouped_ids_part: List[int]
):
    placeholders = ",".join(["?"] * len(grouped_ids_part))
    cur.execute(
        f"""
        SELECT
            m.grouped_id,
            m.message_id,
            m.msg_date_ts,
            m.msg_type,
            COALESCE(m.content, '') AS content,
            mm.media_fingerprint
        FROM messages m
        LEFT JOIN message_media mm ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id
        WHERE m.chat_id = ? AND m.grouped_id IN ({placeholders})
        ORDER BY m.grouped_id ASC, m.message_id ASC
    """,
        [chat_id] + grouped_ids_part,
    )
    return cur.fetchall()


def _build_media_group_upsert_rows(
    rows: List[sqlite3.Row], chat_id: int, cfg: AppConfig
) -> List[tuple]:
    bucket: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        gid = int(r["grouped_id"])
        b = bucket.setdefault(
            gid,
            {
                "first_message_id": None,
                "first_msg_date_ts": None,
                "last_message_id": None,
                "last_msg_date_ts": None,
                "item_count": 0,
                "types": [],
                "captions": [],
                "media_fingerprints": [],
            },
        )
        mid = int(r["message_id"])
        ts = int(r["msg_date_ts"])
        b["first_message_id"] = (
            mid
            if b["first_message_id"] is None or mid < b["first_message_id"]
            else b["first_message_id"]
        )
        b["first_msg_date_ts"] = (
            ts
            if b["first_msg_date_ts"] is None or ts < b["first_msg_date_ts"]
            else b["first_msg_date_ts"]
        )
        b["last_message_id"] = (
            mid
            if b["last_message_id"] is None or mid > b["last_message_id"]
            else b["last_message_id"]
        )
        b["last_msg_date_ts"] = (
            ts
            if b["last_msg_date_ts"] is None or ts > b["last_msg_date_ts"]
            else b["last_msg_date_ts"]
        )
        b["item_count"] += 1
        b["types"].append(r["msg_type"] or "")
        if r["content"]:
            b["captions"].append(str(r["content"]))
        if r["media_fingerprint"]:
            b["media_fingerprints"].append(str(r["media_fingerprint"]))

    up_rows = []
    for gid, b in bucket.items():
        types_csv = ",".join(sorted(set([x for x in b["types"] if x])))
        captions_concat = "\n".join([c for c in b["captions"] if c]).strip()
        media_sig_hash = make_media_group_signature(
            b["media_fingerprints"], b["types"], int(b["item_count"])
        )
        features = build_group_promo_features(
            captions_concat, int(b["item_count"]), media_sig_hash, cfg
        )
        up_rows.append(
            (
                chat_id,
                gid,
                b["first_message_id"],
                b["first_msg_date_ts"],
                b["last_message_id"],
                b["last_msg_date_ts"],
                int(b["item_count"]),
                int(b["item_count"]),
                types_csv,
                captions_concat,
                features["caption_norm"],
                features["pure_hash"],
                media_sig_hash,
                features["dedupe_hash"],
                int(features["is_promo"]),
                int(features["promo_score"]),
                _safe_json(features["promo_reasons"]),
                int(features["dedupe_eligible"]),
                features["guard_reason"],
            )
        )
    return up_rows


def _upsert_media_group_rows(cur: sqlite3.Cursor, up_rows: List[tuple]):
    if not up_rows:
        return
    cur.executemany(UPSERT_MEDIA_GROUP_SQL, up_rows)


def _rebuild_media_groups_for_ids(
    cur: sqlite3.Cursor, chat_id: int, grouped_ids: List[int], cfg: AppConfig
):
    if not grouped_ids:
        return
    for part in chunked(sorted(set(grouped_ids)), 500):
        _delete_media_groups(cur, chat_id, grouped_ids=part)
        rows = _query_media_group_rows(cur, chat_id, part)
        up_rows = _build_media_group_upsert_rows(rows, chat_id, cfg)
        _upsert_media_group_rows(cur, up_rows)


def _resolve_refresh_grouped_ids(
    cur: sqlite3.Cursor, chat_id: int, grouped_ids: Optional[Set[int]]
):
    if grouped_ids is None:
        return _load_all_grouped_ids(cur, chat_id), True
    return _normalize_grouped_ids(grouped_ids), False


def _execute_media_group_refresh(
    cur: sqlite3.Cursor,
    chat_id: int,
    cfg: AppConfig,
    target_ids: List[int],
    full_refresh: bool,
):
    if full_refresh:
        _delete_media_groups(cur, chat_id, grouped_ids=None)
        _rebuild_media_groups_for_ids(cur, chat_id, target_ids, cfg)
        return
    if target_ids:
        _rebuild_media_groups_for_ids(cur, chat_id, target_ids, cfg)


@synchronized_write
def refresh_media_groups_for_chat(
    conn: sqlite3.Connection,
    chat_id: int,
    cfg: AppConfig,
    grouped_ids: Optional[Set[int]] = None,
):
    """
    grouped_ids=None: 全量刷新（删除 chat 全部聚合，再按 messages 全量重建）
    grouped_ids=set(...): 按组刷新（仅删除并重建指定 grouped_id）
    """
    started_at = time.perf_counter()
    cur = conn.cursor()
    try:
        target_ids, full_refresh = _resolve_refresh_grouped_ids(
            cur, chat_id, grouped_ids
        )
        target_count = len(target_ids)
        mode_label = "full" if full_refresh else "partial"
        logging.info(
            f"media_groups 刷新开始: chat_id={chat_id} mode={mode_label} grouped_ids={target_count}"
        )
        cur.execute("BEGIN IMMEDIATE")
        _execute_media_group_refresh(cur, chat_id, cfg, target_ids, full_refresh)
        conn.commit()
        elapsed = time.perf_counter() - started_at
        logging.info(
            f"media_groups 刷新完成: chat_id={chat_id} mode={mode_label} grouped_ids={target_count} 耗时={elapsed:.2f}s"
        )
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        cur.close()
