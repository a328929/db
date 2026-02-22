# -*- coding: utf-8 -*-
import os
import re
import html
import sqlite3
import time
import json
import hashlib
import logging
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any, Iterable, Set

from .db import SqliteFeatures, connect_db, create_schema, resolve_db_path
from .config import AppConfig, CFG, _is_enabled
from .normalize import _safe_json, make_hash
from .promo import build_single_promo_features, build_group_promo_features
from .dedupe import dedupe_promotional_duplicates, build_message_dedupe_hash, build_media_fingerprint, make_media_group_signature








# =========================
# 日志
# =========================

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )











# =========================
# Telegram 消息解析
# =========================

def classify_msg_type(message) -> str:
    try:
        if getattr(message, "sticker", None):
            return "STICKER"
        if getattr(message, "gif", None):
            return "GIF"
        if getattr(message, "voice", None):
            return "VOICE"
        if getattr(message, "video_note", None):
            return "VIDEO_NOTE"
        if getattr(message, "audio", None):
            return "AUDIO"
        if getattr(message, "video", None):
            return "VIDEO"
        if getattr(message, "photo", None):
            return "PHOTO"
        if getattr(message, "document", None):
            return "FILE"
        if getattr(message, "poll", None):
            return "POLL"
        if getattr(message, "contact", None):
            return "CONTACT"
        if getattr(message, "geo", None):
            return "GEO"
        return "TEXT"
    except Exception:
        return "TEXT"


def extract_message_text(message) -> str:
    for attr in ("raw_text", "message", "text"):
        try:
            v = getattr(message, attr, None)
            if v:
                return str(v).strip()
        except Exception:
            continue
    return ""


def extract_media_meta(message, msg_type: str) -> Dict[str, Any]:
    out = {
        "media_kind": msg_type if msg_type != "TEXT" else None,
        "file_unique_id": None,
        "file_name": None,
        "file_ext": None,
        "mime_type": None,
        "file_size": None,
        "width": None,
        "height": None,
        "duration_sec": None,
        "media_fingerprint": None,
        "meta_json": None,
    }
    if msg_type == "TEXT":
        return out

    extra = {}

    try:
        f = getattr(message, "file", None)
        if f is not None:
            for k in ("id", "name", "ext", "mime_type", "size", "width", "height", "duration", "title", "performer", "emoji"):
                try:
                    v = getattr(f, k, None)
                except Exception:
                    v = None
                if v is None:
                    continue

                if k == "id":
                    out["file_unique_id"] = str(v)
                elif k == "name":
                    out["file_name"] = str(v)
                elif k == "ext":
                    out["file_ext"] = str(v)
                elif k == "mime_type":
                    out["mime_type"] = str(v)
                elif k == "size":
                    try:
                        out["file_size"] = int(v)
                    except Exception:
                        pass
                elif k == "width":
                    try:
                        out["width"] = int(v)
                    except Exception:
                        pass
                elif k == "height":
                    try:
                        out["height"] = int(v)
                    except Exception:
                        pass
                elif k == "duration":
                    try:
                        out["duration_sec"] = int(v)
                    except Exception:
                        pass
                else:
                    extra[k] = v
    except Exception as e:
        extra["file_wrapper_error"] = str(e)

    # 兜底取媒体 ID
    if not out["file_unique_id"]:
        try:
            p = getattr(message, "photo", None)
            if p is not None and hasattr(p, "id"):
                out["file_unique_id"] = str(getattr(p, "id"))
        except Exception:
            pass

    if not out["file_unique_id"]:
        try:
            d = getattr(message, "document", None)
            if d is not None and hasattr(d, "id"):
                out["file_unique_id"] = str(getattr(d, "id"))
        except Exception:
            pass

    try:
        extra["views"] = getattr(message, "views", None)
        extra["forwards"] = getattr(message, "forwards", None)
        extra["edit_date"] = str(getattr(message, "edit_date", None)) if getattr(message, "edit_date", None) else None
    except Exception:
        pass

    extra = {k: v for k, v in extra.items() if v is not None}
    out["meta_json"] = _safe_json(extra) if extra else None

    out["media_fingerprint"] = build_media_fingerprint(
        file_unique_id=out["file_unique_id"],
        mime_type=out["mime_type"],
        file_size=out["file_size"],
        width=out["width"],
        height=out["height"],
        duration_sec=out["duration_sec"],
    )
    return out


def resolve_target_entity(client: Any, target: str):
    """
    优先 username / 链接 / id；失败再扫 dialogs 标题
    """
    t = (target or "").strip()

    try:
        cleaned = t.replace("https://t.me/", "").replace("http://t.me/", "").strip("/")
        if cleaned.startswith("@"):
            cleaned = cleaned.lstrip("@")
        if cleaned and (cleaned != t or t.startswith("@") or re.fullmatch(r"-?\d+", t)):
            return client.get_entity(cleaned)
    except Exception:
        pass

    try:
        exact_match = None
        partial_match = None
        for d in client.get_dialogs():
            title = (d.title or "")
            title_stripped = title.strip()
            if title_stripped == t:
                exact_match = d.entity
                break
            if t and partial_match is None and t in title:
                partial_match = d.entity
        if exact_match is not None:
            return exact_match
        if partial_match is not None:
            return partial_match
    except Exception:
        pass

    return None


def get_existing_chat_ids(conn: sqlite3.Connection) -> List[int]:
    cur = conn.cursor()
    cur.execute("SELECT chat_id FROM chats ORDER BY last_seen_at DESC, first_seen_at DESC")
    out: List[int] = []
    for row in cur.fetchall():
        try:
            out.append(int(row["chat_id"]))
        except Exception:
            continue
    return out


def collect_target_entities(conn: sqlite3.Connection, client: Any, cfg: AppConfig) -> List[Any]:
    entities: List[Any] = []
    seen_chat_ids: Set[int] = set()

    def _append_entity(ent: Any):
        if not ent:
            return
        try:
            cid = int(getattr(ent, "id", 0))
        except Exception:
            cid = 0
        if cid and cid in seen_chat_ids:
            return
        if cid:
            seen_chat_ids.add(cid)
        entities.append(ent)

    if _is_enabled(cfg.scan_existing_chats):
        chat_ids = get_existing_chat_ids(conn)
        logging.info(f"参数 TG_SCAN_DB_CHATS=1，尝试扫描数据库已有会话数: {len(chat_ids)}")
        for cid in chat_ids:
            try:
                _append_entity(client.get_entity(cid))
            except Exception as e:
                logging.warning(f"跳过 chat_id={cid}（无法解析实体）: {e}")

    if cfg.target_group.strip():
        entity = resolve_target_entity(client, cfg.target_group)
        if entity:
            _append_entity(entity)
        elif not entities:
            logging.error("❌ 未找到该群组/频道，请检查名称 / 用户名 / 链接")

    return entities


def build_msg_link(entity, msg_id: int) -> str:
    username = getattr(entity, "username", None)
    if username:
        return f"https://t.me/{username}/{msg_id}"

    raw_id = str(getattr(entity, "id", ""))
    if raw_id.startswith("-100"):
        raw_id = raw_id[4:]
    else:
        raw_id = raw_id.lstrip("-")
    return f"https://t.me/c/{raw_id}/{msg_id}"


# =========================
# UPSERT SQL
# =========================

UPSERT_CHAT_SQL = """
INSERT INTO chats(chat_id, chat_title, chat_username, is_public, chat_type, last_seen_at)
VALUES (?, ?, ?, ?, ?, datetime('now'))
ON CONFLICT(chat_id) DO UPDATE SET
    chat_title = excluded.chat_title,
    chat_username = excluded.chat_username,
    is_public = excluded.is_public,
    chat_type = excluded.chat_type,
    last_seen_at = datetime('now')
"""

UPSERT_MESSAGE_SQL = """
INSERT INTO messages(
    chat_id, message_id, msg_date_text, msg_date_ts, sender_id,
    content, content_norm, pure_hash, dedupe_hash,
    msg_type, grouped_id, link, has_media,
    is_promo, promo_score, promo_reasons, dedupe_eligible, guard_reason, text_len,
    updated_at
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, datetime('now'))
ON CONFLICT(chat_id, message_id) DO UPDATE SET
    msg_date_text     = excluded.msg_date_text,
    msg_date_ts       = excluded.msg_date_ts,
    sender_id         = excluded.sender_id,
    content           = excluded.content,
    content_norm      = excluded.content_norm,
    pure_hash         = excluded.pure_hash,
    dedupe_hash       = excluded.dedupe_hash,
    msg_type          = excluded.msg_type,
    grouped_id        = excluded.grouped_id,
    link              = excluded.link,
    has_media         = excluded.has_media,
    is_promo          = excluded.is_promo,
    promo_score       = excluded.promo_score,
    promo_reasons     = excluded.promo_reasons,
    dedupe_eligible   = excluded.dedupe_eligible,
    guard_reason      = excluded.guard_reason,
    text_len          = excluded.text_len,
    updated_at        = datetime('now')
"""

UPSERT_MEDIA_SQL = """
INSERT INTO message_media(
    chat_id, message_id, media_kind, file_unique_id, file_name, file_ext, mime_type,
    file_size, width, height, duration_sec, grouped_id, media_fingerprint, meta_json, updated_at
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?, datetime('now'))
ON CONFLICT(chat_id, message_id) DO UPDATE SET
    media_kind        = excluded.media_kind,
    file_unique_id    = excluded.file_unique_id,
    file_name         = excluded.file_name,
    file_ext          = excluded.file_ext,
    mime_type         = excluded.mime_type,
    file_size         = excluded.file_size,
    width             = excluded.width,
    height            = excluded.height,
    duration_sec      = excluded.duration_sec,
    grouped_id        = excluded.grouped_id,
    media_fingerprint = excluded.media_fingerprint,
    meta_json         = excluded.meta_json,
    updated_at        = datetime('now')
"""

UPSERT_MEDIA_GROUP_SQL = """
INSERT INTO media_groups(
    chat_id, grouped_id,
    first_message_id, first_msg_date_ts, last_message_id, last_msg_date_ts,
    item_count, active_items, types_csv,
    captions_concat, caption_norm, pure_hash, media_sig_hash, dedupe_hash,
    is_promo, promo_score, promo_reasons, dedupe_eligible, guard_reason,
    updated_at
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, datetime('now'))
ON CONFLICT(chat_id, grouped_id) DO UPDATE SET
    first_message_id = excluded.first_message_id,
    first_msg_date_ts = excluded.first_msg_date_ts,
    last_message_id = excluded.last_message_id,
    last_msg_date_ts = excluded.last_msg_date_ts,
    item_count = excluded.item_count,
    active_items = excluded.active_items,
    types_csv = excluded.types_csv,
    captions_concat = excluded.captions_concat,
    caption_norm = excluded.caption_norm,
    pure_hash = excluded.pure_hash,
    media_sig_hash = excluded.media_sig_hash,
    dedupe_hash = excluded.dedupe_hash,
    is_promo = excluded.is_promo,
    promo_score = excluded.promo_score,
    promo_reasons = excluded.promo_reasons,
    dedupe_eligible = excluded.dedupe_eligible,
    guard_reason = excluded.guard_reason,
    updated_at = datetime('now')
"""


# =========================
# DB 写入 / 查询辅助
# =========================

def get_last_message_id(conn: sqlite3.Connection, chat_id: int) -> int:
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(message_id), 0) AS m FROM messages WHERE chat_id=?", (chat_id,))
    return int(cur.fetchone()["m"])


def upsert_chat(conn: sqlite3.Connection, row: tuple):
    # 使用独立游标并在提交前显式关闭，避免 "SQL statements in progress"
    # （某些 SQLite/Python 组合在连接级 execute + 立刻 commit 时会触发）。
    cur = conn.cursor()
    try:
        cur.execute(UPSERT_CHAT_SQL, row)
    finally:
        cur.close()
    conn.commit()


def batch_upsert(conn: sqlite3.Connection, msg_rows: List[tuple], media_rows: List[tuple]):
    if not msg_rows and not media_rows:
        return
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        if msg_rows:
            cur.executemany(UPSERT_MESSAGE_SQL, msg_rows)
        if media_rows:
            cur.executemany(UPSERT_MEDIA_SQL, media_rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def chunked(seq: List[Any], n: int) -> Iterable[List[Any]]:
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _rebuild_media_groups_for_ids(conn: sqlite3.Connection, chat_id: int, grouped_ids: List[int], cfg: AppConfig):
    if not grouped_ids:
        return

    cur = conn.cursor()

    for part in chunked(sorted(set(grouped_ids)), 500):
        placeholders = ",".join(["?"] * len(part))

        # 先删旧聚合
        cur.execute(f"DELETE FROM media_groups WHERE chat_id=? AND grouped_id IN ({placeholders})", [chat_id] + part)

        # 拉明细，Python 聚合，保证顺序和签名稳定
        cur.execute(f"""
            SELECT
                m.grouped_id,
                m.message_id,
                m.msg_date_ts,
                m.msg_type,
                COALESCE(m.content, '') AS content,
                mm.media_fingerprint
            FROM messages m
            LEFT JOIN message_media mm
              ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id
            WHERE m.chat_id = ?
              AND m.grouped_id IN ({placeholders})
            ORDER BY m.grouped_id ASC, m.message_id ASC
        """, [chat_id] + part)
        rows = cur.fetchall()

        bucket: Dict[int, Dict[str, Any]] = {}
        for r in rows:
            gid = int(r["grouped_id"])
            if gid not in bucket:
                bucket[gid] = {
                    "first_message_id": None,
                    "first_msg_date_ts": None,
                    "last_message_id": None,
                    "last_msg_date_ts": None,
                    "item_count": 0,
                    "types": [],
                    "captions": [],
                    "media_fingerprints": [],
                }

            b = bucket[gid]
            mid = int(r["message_id"])
            ts = int(r["msg_date_ts"])

            if b["first_message_id"] is None or mid < b["first_message_id"]:
                b["first_message_id"] = mid
            if b["first_msg_date_ts"] is None or ts < b["first_msg_date_ts"]:
                b["first_msg_date_ts"] = ts
            if b["last_message_id"] is None or mid > b["last_message_id"]:
                b["last_message_id"] = mid
            if b["last_msg_date_ts"] is None or ts > b["last_msg_date_ts"]:
                b["last_msg_date_ts"] = ts

            b["item_count"] += 1
            b["types"].append(r["msg_type"] or "")
            if r["content"]:
                b["captions"].append(str(r["content"]))
            if r["media_fingerprint"]:
                b["media_fingerprints"].append(str(r["media_fingerprint"]))

        up_rows = []
        for gid, b in bucket.items():
            item_count = int(b["item_count"])
            active_items = item_count
            types_csv = ",".join(sorted(set([x for x in b["types"] if x])))
            captions_concat = "\n".join([c for c in b["captions"] if c]).strip()

            media_sig_hash = make_media_group_signature(
                media_fingerprints=b["media_fingerprints"],
                msg_types=b["types"],
                item_count=item_count
            )

            features = build_group_promo_features(
                captions_concat=captions_concat,
                item_count=item_count,
                types_csv=types_csv,
                media_sig_hash=media_sig_hash,
                cfg=cfg
            )

            up_rows.append((
                chat_id, gid,
                b["first_message_id"], b["first_msg_date_ts"],
                b["last_message_id"], b["last_msg_date_ts"],
                item_count, active_items, types_csv,
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
            ))

        if up_rows:
            cur.executemany(UPSERT_MEDIA_GROUP_SQL, up_rows)

    conn.commit()


def refresh_media_groups_for_chat(conn: sqlite3.Connection, chat_id: int, cfg: AppConfig, grouped_ids: Optional[Set[int]] = None):
    """
    重建/增量刷新 media_groups（增强版）
    - 全量：先取所有 grouped_id，再分批聚合
    - 增量：只刷新本轮 touched grouped_id
    """
    cur = conn.cursor()

    if grouped_ids is None:
        cur.execute("SELECT DISTINCT grouped_id FROM messages WHERE chat_id=? AND grouped_id IS NOT NULL", (chat_id,))
        gids = [int(r["grouped_id"]) for r in cur.fetchall() if r["grouped_id"] is not None]
        cur.execute("DELETE FROM media_groups WHERE chat_id=?", (chat_id,))
        conn.commit()
        _rebuild_media_groups_for_ids(conn, chat_id, gids, cfg)
        return

    gids = [int(x) for x in grouped_ids if x is not None]
    if not gids:
        return
    _rebuild_media_groups_for_ids(conn, chat_id, gids, cfg)





# =========================
# 检索函数（后续做 UI / API 复用）
# =========================



@dataclass
class HarvestCounters:
    seen: int = 0
    written: int = 0
    parse_failures: int = 0
    parse_failure_samples: List[str] = None
    parse_failures_by_type: Dict[str, int] = None

    def __post_init__(self):
        if self.parse_failure_samples is None:
            self.parse_failure_samples = []
        if self.parse_failures_by_type is None:
            self.parse_failures_by_type = {}

    def note_parse_failure(self, err: Exception, message: Any = None):
        self.parse_failures += 1
        key = err.__class__.__name__
        self.parse_failures_by_type[key] = self.parse_failures_by_type.get(key, 0) + 1
        if len(self.parse_failure_samples) >= 5:
            return
        msg_id = getattr(message, "id", None)
        self.parse_failure_samples.append(f"id={msg_id}, err={key}: {err}")


def _log_parse_failure_summary(counters: HarvestCounters):
    if counters.parse_failures == 0:
        return
    logging.warning(
        "解析失败统计: total=%s by_type=%s",
        counters.parse_failures,
        counters.parse_failures_by_type,
    )
    for sample in counters.parse_failure_samples:
        logging.warning("解析失败样例: %s", sample)

def get_chat_stats(conn: sqlite3.Connection, chat_id: int) -> Dict[str, int]:
    cur = conn.cursor()
    out = {}

    cur.execute("SELECT COUNT(*) AS c FROM messages WHERE chat_id=?", (chat_id,))
    out["total_messages"] = int(cur.fetchone()["c"] or 0)

    cur.execute("SELECT COUNT(*) AS c FROM messages WHERE chat_id=? AND has_media=1", (chat_id,))
    out["media_messages"] = int(cur.fetchone()["c"] or 0)

    cur.execute("SELECT COUNT(*) AS c FROM messages WHERE chat_id=? AND is_promo=1", (chat_id,))
    out["promo_messages"] = int(cur.fetchone()["c"] or 0)

    cur.execute("SELECT COUNT(*) AS c FROM messages WHERE chat_id=? AND is_promo=1 AND dedupe_eligible=1", (chat_id,))
    out["promo_dedupe_eligible_messages"] = int(cur.fetchone()["c"] or 0)

    cur.execute("SELECT COUNT(*) AS c FROM messages WHERE chat_id=? AND is_promo=1 AND dedupe_eligible=0", (chat_id,))
    out["promo_guarded_messages"] = int(cur.fetchone()["c"] or 0)

    cur.execute("SELECT COUNT(*) AS c FROM media_groups WHERE chat_id=?", (chat_id,))
    out["media_groups"] = int(cur.fetchone()["c"] or 0)

    cur.execute("SELECT COUNT(*) AS c FROM media_groups WHERE chat_id=? AND is_promo=1", (chat_id,))
    out["promo_media_groups"] = int(cur.fetchone()["c"] or 0)

    cur.execute("SELECT COUNT(*) AS c FROM media_groups WHERE chat_id=? AND is_promo=1 AND dedupe_eligible=0", (chat_id,))
    out["guarded_media_groups"] = int(cur.fetchone()["c"] or 0)

    return out


# =========================
# 主流程
# =========================

def run_harvest():
    from telethon.sync import TelegramClient
    from telethon.errors import FloodWaitError, RPCError

    setup_logging()
    conn, feats = connect_db(CFG.db_name)
    create_schema(conn, feats)

    try:
        with TelegramClient(CFG.session_name, CFG.api_id, CFG.api_hash) as client:
            entities = collect_target_entities(conn, client, CFG)
            if not entities:
                logging.error("❌ 无可处理的群组/频道（可检查 TG_TARGET_GROUP 或将 TG_SCAN_DB_CHATS 设为 1）")
                return

            logging.info(f"本轮待处理群组/频道数: {len(entities)}")

            for idx, entity in enumerate(entities, start=1):
                chat_id = int(getattr(entity, "id", 0))
                chat_title = getattr(entity, "title", CFG.target_group) or CFG.target_group
                chat_username = getattr(entity, "username", None)
                is_public = 1 if chat_username else 0
                chat_type = entity.__class__.__name__

                logging.info(f"[{idx}/{len(entities)}] 正在处理: {chat_title} (chat_id={chat_id})")
                upsert_chat(conn, (chat_id, chat_title, chat_username, is_public, chat_type))

                last_id = get_last_message_id(conn, chat_id)
                first_sync = (last_id == 0)
                scan_from_id = max(last_id - CFG.rescan_tail_ids, 0)
                logging.info("首次同步，开始全量抓取..." if first_sync else f"增量同步：last_id={last_id}，回扫到 > {scan_from_id}")

                counters = HarvestCounters()
                msg_rows: List[tuple] = []
                media_rows: List[tuple] = []
                touched_group_ids: Set[int] = set()
                iterator = iter(client.iter_messages(entity, min_id=scan_from_id, reverse=True))

                while True:
                    try:
                        message = next(iterator)
                    except StopIteration:
                        break
                    except FloodWaitError as e:
                        wait_s = int(getattr(e, "seconds", 5))
                        logging.warning(f"⏳ FloodWait，等待 {wait_s}s")
                        time.sleep(wait_s)
                        continue
                    except RPCError as e:
                        logging.warning(f"Telegram RPC 错误：{e}")
                        time.sleep(2)
                        continue
                    except Exception as e:
                        logging.warning(f"消息迭代异常：{e}")
                        time.sleep(1)
                        continue

                    counters.seen += 1
                    try:
                        dt = getattr(message, "date", None)
                        if dt is None:
                            continue
                        msg_date_text = dt.strftime("%Y-%m-%d %H:%M:%S")
                        msg_date_ts = int(dt.timestamp())
                        msg_id = int(getattr(message, "id", 0) or 0)
                        sender_id = int(getattr(message, "sender_id", 0) or 0)
                        msg_type = classify_msg_type(message)
                        content = extract_message_text(message)
                        has_media = 0 if msg_type == "TEXT" else 1

                        grouped_id = getattr(message, "grouped_id", None)
                        try:
                            grouped_id = int(grouped_id) if grouped_id is not None else None
                        except Exception:
                            grouped_id = None
                        if grouped_id is not None:
                            touched_group_ids.add(grouped_id)

                        link = build_msg_link(entity, msg_id)
                        mmeta = extract_media_meta(message, msg_type) if has_media else None
                        features = build_single_promo_features(content, msg_type=msg_type, has_media=bool(has_media), cfg=CFG)
                        message_dedupe_hash = build_message_dedupe_hash(
                            text_pure_hash=features["pure_hash"],
                            has_media=bool(has_media),
                            media_fingerprint=(mmeta or {}).get("media_fingerprint"),
                        )

                        msg_rows.append((
                            chat_id, msg_id, msg_date_text, msg_date_ts, sender_id,
                            content, features["content_norm"], features["pure_hash"], message_dedupe_hash,
                            msg_type, grouped_id, link, has_media,
                            int(features["is_promo"]), int(features["promo_score"]), _safe_json(features["promo_reasons"]),
                            int(features["dedupe_eligible"]), features["guard_reason"], int(features["text_len"])
                        ))

                        if has_media and mmeta is not None:
                            media_rows.append((
                                chat_id, msg_id,
                                mmeta["media_kind"], mmeta["file_unique_id"], mmeta["file_name"], mmeta["file_ext"],
                                mmeta["mime_type"], mmeta["file_size"], mmeta["width"], mmeta["height"], mmeta["duration_sec"],
                                grouped_id, mmeta["media_fingerprint"], mmeta["meta_json"]
                            ))

                        if len(msg_rows) >= CFG.batch_size:
                            batch_upsert(conn, msg_rows, media_rows)
                            counters.written += len(msg_rows)
                            msg_rows.clear()
                            media_rows.clear()
                            if counters.seen % CFG.log_every == 0:
                                logging.info(f"扫描 {counters.seen} | 写入/更新 {counters.written}")
                    except Exception as e:
                        counters.note_parse_failure(e, message)
                        logging.warning(f"⚠️ 跳过一条消息（解析失败）: {e}")

                if msg_rows or media_rows:
                    batch_upsert(conn, msg_rows, media_rows)
                    counters.written += len(msg_rows)

                if first_sync:
                    logging.info("刷新 media_groups（全量）...")
                    refresh_media_groups_for_chat(conn, chat_id, cfg=CFG, grouped_ids=None)
                else:
                    logging.info(f"刷新 media_groups（增量，涉及组数={len(touched_group_ids)}）...")
                    refresh_media_groups_for_chat(conn, chat_id, cfg=CFG, grouped_ids=touched_group_ids)

                logging.info("执行数据库级去重（硬删除，含媒体组广告去重）...")
                deduped_count, dup_hash_solo, dup_hash_group_txt, dup_hash_group_med, affected_group_ids = dedupe_promotional_duplicates(
                    conn,
                    chat_id=chat_id,
                    mode=CFG.dedup_mode,
                    threshold=CFG.dedup_threshold,
                    promo_score_threshold=CFG.promo_score_threshold,
                )
                if affected_group_ids:
                    refresh_media_groups_for_chat(conn, chat_id, cfg=CFG, grouped_ids=affected_group_ids)

                stats = get_chat_stats(conn, chat_id)
                try:
                    conn.execute("PRAGMA optimize;").fetchall()
                except Exception:
                    pass
                try:
                    conn.execute("PRAGMA wal_checkpoint(TRUNCATE);").fetchall()
                except Exception:
                    pass

                logging.info("✅ 处理完成")
                logging.info(f"群组: {chat_title} (chat_id={chat_id})")
                logging.info(f"本轮扫描消息: {counters.seen}")
                logging.info(f"本轮写入/更新: {counters.written}")
                _log_parse_failure_summary(counters)
                logging.info(f"命中重复模板(单条): {dup_hash_solo}")
                logging.info(f"命中重复模板(媒体组-文案): {dup_hash_group_txt}")
                logging.info(f"命中重复模板(媒体组-媒体签名): {dup_hash_group_med}")
                logging.info(f"本轮硬删除条数: {deduped_count}")
                logging.info(f"数据库总记录: {stats['total_messages']}")
                logging.info(f"媒体消息: {stats['media_messages']}")
                logging.info(
                    f"引流候选消息: {stats['promo_messages']}（可自动去重: {stats['promo_dedupe_eligible_messages']} | 受保护: {stats['promo_guarded_messages']}）"
                )
                logging.info(f"媒体组总数: {stats['media_groups']}")
                logging.info(f"引流候选媒体组: {stats['promo_media_groups']}（受保护: {stats['guarded_media_groups']}）")
    finally:
        conn.close()


if __name__ == "__main__":
    run_harvest()
