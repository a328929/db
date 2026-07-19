import logging
import sqlite3
import time
from collections.abc import Iterable
from contextlib import suppress
from typing import Any

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
WHERE media_groups.first_message_id IS NOT excluded.first_message_id
   OR media_groups.first_msg_date_ts IS NOT excluded.first_msg_date_ts
   OR media_groups.last_message_id IS NOT excluded.last_message_id
   OR media_groups.last_msg_date_ts IS NOT excluded.last_msg_date_ts
   OR media_groups.item_count IS NOT excluded.item_count
   OR media_groups.active_items IS NOT excluded.active_items
   OR media_groups.types_csv IS NOT excluded.types_csv
   OR media_groups.captions_concat IS NOT excluded.captions_concat
   OR media_groups.caption_norm IS NOT excluded.caption_norm
   OR media_groups.pure_hash IS NOT excluded.pure_hash
   OR media_groups.media_sig_hash IS NOT excluded.media_sig_hash
   OR media_groups.dedupe_hash IS NOT excluded.dedupe_hash
   OR media_groups.is_promo IS NOT excluded.is_promo
   OR media_groups.promo_score IS NOT excluded.promo_score
   OR media_groups.promo_reasons IS NOT excluded.promo_reasons
   OR media_groups.dedupe_eligible IS NOT excluded.dedupe_eligible
   OR media_groups.guard_reason IS NOT excluded.guard_reason
"""


def chunked(seq: list[Any], n: int) -> Iterable[list[Any]]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def _normalize_grouped_ids(grouped_ids: set[int] | None) -> list[int]:
    if grouped_ids is None:
        return []
    return sorted({int(x) for x in grouped_ids if x is not None})


def _load_all_grouped_ids(cur: sqlite3.Cursor, chat_id: int) -> list[int]:
    cur.execute(
        "SELECT DISTINCT grouped_id FROM messages WHERE chat_id=? AND grouped_id IS NOT NULL",
        (chat_id,),
    )
    return [int(r["grouped_id"]) for r in cur.fetchall() if r["grouped_id"] is not None]


def _delete_media_groups(
    cur: sqlite3.Cursor, chat_id: int, grouped_ids: list[int]
):
    if not grouped_ids:
        return
    for part in chunked(grouped_ids, 500):
        placeholders = ",".join(["?"] * len(part))
        cur.execute(
            f"DELETE FROM media_groups WHERE chat_id=? AND grouped_id IN ({placeholders})",
            [chat_id] + part,
        )


def _query_media_group_rows(
    cur: sqlite3.Cursor, chat_id: int, grouped_ids_part: list[int]
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
    rows: list[sqlite3.Row], chat_id: int, cfg: AppConfig
) -> list[tuple]:
    bucket: dict[int, dict[str, Any]] = {}
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


def _upsert_media_group_rows(cur: sqlite3.Cursor, up_rows: list[tuple]):
    if not up_rows:
        return
    cur.executemany(UPSERT_MEDIA_GROUP_SQL, up_rows)


def _rebuild_media_groups_for_ids(
    cur: sqlite3.Cursor, chat_id: int, grouped_ids: list[int], cfg: AppConfig
):
    if not grouped_ids:
        return
    for part in chunked(sorted(set(grouped_ids)), 500):
        rows = _query_media_group_rows(cur, chat_id, part)
        up_rows = _build_media_group_upsert_rows(rows, chat_id, cfg)
        _upsert_media_group_rows(cur, up_rows)
        present_ids = {int(row[1]) for row in up_rows}
        missing_ids = [grouped_id for grouped_id in part if grouped_id not in present_ids]
        _delete_media_groups(cur, chat_id, grouped_ids=missing_ids)


def _delete_stale_media_groups_for_chat(cur: sqlite3.Cursor, chat_id: int) -> None:
    cur.execute(
        """
        DELETE FROM media_groups
        WHERE chat_id = ?
          AND NOT EXISTS (
              SELECT 1
              FROM messages m
              WHERE m.chat_id = media_groups.chat_id
                AND m.grouped_id = media_groups.grouped_id
          )
        """,
        (int(chat_id),),
    )


def _resolve_refresh_grouped_ids(
    cur: sqlite3.Cursor, chat_id: int, grouped_ids: set[int] | None
):
    if grouped_ids is None:
        return _load_all_grouped_ids(cur, chat_id), True
    return _normalize_grouped_ids(grouped_ids), False


def _execute_media_group_refresh(
    cur: sqlite3.Cursor,
    chat_id: int,
    cfg: AppConfig,
    target_ids: list[int],
    full_refresh: bool,
):
    if full_refresh:
        _rebuild_media_groups_for_ids(cur, chat_id, target_ids, cfg)
        _delete_stale_media_groups_for_chat(cur, chat_id)
        return
    if target_ids:
        _rebuild_media_groups_for_ids(cur, chat_id, target_ids, cfg)


def _refresh_media_groups_for_cursor(
    cur: sqlite3.Cursor,
    chat_id: int,
    cfg: AppConfig,
    grouped_ids: set[int] | None = None,
) -> tuple[int, str]:
    """Refresh media groups on an already-open transaction."""
    target_ids, full_refresh = _resolve_refresh_grouped_ids(cur, chat_id, grouped_ids)
    _execute_media_group_refresh(cur, chat_id, cfg, target_ids, full_refresh)
    return len(target_ids), "full" if full_refresh else "partial"


@synchronized_write
def refresh_media_groups_for_chat(
    conn: sqlite3.Connection,
    chat_id: int,
    cfg: AppConfig,
    grouped_ids: set[int] | None = None,
):
    """
    grouped_ids=None: 在一次写事务中按 messages 全量同步，并清除孤立聚合。
    grouped_ids=set(...): 只同步指定 grouped_id，删除已不存在的目标聚合。
    """
    started_at = time.perf_counter()
    cur = conn.cursor()
    try:
        requested_mode = "full" if grouped_ids is None else "partial"
        logging.info(
            "media_groups 刷新开始: chat_id=%s mode=%s",
            chat_id,
            requested_mode,
        )
        cur.execute("BEGIN IMMEDIATE")
        target_count, mode_label = _refresh_media_groups_for_cursor(
            cur,
            chat_id,
            cfg,
            grouped_ids,
        )
        conn.commit()
        elapsed = time.perf_counter() - started_at
        logging.info(
            f"media_groups 刷新完成: chat_id={chat_id} mode={mode_label} grouped_ids={target_count} 耗时={elapsed:.2f}s"
        )
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        cur.close()
