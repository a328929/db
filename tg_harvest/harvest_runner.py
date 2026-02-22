# -*- coding: utf-8 -*-
import logging
import sqlite3
import time
from typing import Any, List, Set, Tuple

from .config import AppConfig, CFG, _is_enabled
from .db import connect_db, create_schema
from .dedupe import build_message_dedupe_hash, dedupe_promotional_duplicates
from .harvest_parse import (
    HarvestCounters,
    build_msg_link,
    classify_msg_type,
    extract_media_meta,
    extract_message_text,
    log_parse_failure_summary,
    resolve_target_entity,
    setup_logging,
)
from .harvest_store import (
    batch_upsert,
    get_chat_stats,
    get_last_message_id,
    refresh_media_groups_for_chat,
    upsert_chat,
)
from .normalize import _safe_json
from .promo import build_single_promo_features


def get_existing_chat_ids(conn: sqlite3.Connection) -> List[int]:
    cur = conn.cursor()
    try:
        cur.execute("SELECT chat_id FROM chats ORDER BY last_seen_at DESC, first_seen_at DESC")
        out: List[int] = []
        for row in cur.fetchall():
            try:
                out.append(int(row["chat_id"]))
            except Exception:
                continue
        return out
    finally:
        cur.close()


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


def _refresh_groups_before_dedupe(conn: sqlite3.Connection, chat_id: int, first_sync: bool, touched_group_ids: Set[int]):
    """阶段2：先刷新 media_groups，给 dedupe 提供稳定的组视图。"""
    if first_sync:
        refresh_media_groups_for_chat(conn, chat_id, cfg=CFG, grouped_ids=None)
        return
    refresh_media_groups_for_chat(conn, chat_id, cfg=CFG, grouped_ids=touched_group_ids)


def _run_dedupe_phase(conn: sqlite3.Connection, chat_id: int) -> Tuple[int, int, int, int, Set[int]]:
    """阶段3：执行 dedupe，返回删除统计与受影响 grouped_id。"""
    return dedupe_promotional_duplicates(
        conn,
        chat_id=chat_id,
        mode=CFG.dedup_mode,
        threshold=CFG.dedup_threshold,
        promo_score_threshold=CFG.promo_score_threshold,
    )


def _refresh_groups_after_dedupe(conn: sqlite3.Connection, chat_id: int, affected_group_ids: Set[int]):
    """阶段4：去重后仅刷新受影响组，确保 media_groups 一致性。"""
    if not affected_group_ids:
        return
    refresh_media_groups_for_chat(conn, chat_id, cfg=CFG, grouped_ids=affected_group_ids)


def _run_group_refresh_and_dedupe_pipeline(
    conn: sqlite3.Connection,
    chat_id: int,
    first_sync: bool,
    touched_group_ids: Set[int],
) -> Tuple[int, int, int, int, Set[int]]:
    """固定流程：首次聚合 -> dedupe -> 去重后一致性修复。"""
    _refresh_groups_before_dedupe(conn, chat_id, first_sync=first_sync, touched_group_ids=touched_group_ids)
    dedupe_result = _run_dedupe_phase(conn, chat_id)
    _refresh_groups_after_dedupe(conn, chat_id, dedupe_result[4])
    return dedupe_result


def run_harvest():
    from telethon.errors import FloodWaitError, RPCError
    from telethon.sync import TelegramClient

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
                            int(features["dedupe_eligible"]), features["guard_reason"], int(features["text_len"]),
                        ))
                        if has_media and mmeta is not None:
                            media_rows.append((
                                chat_id, msg_id,
                                mmeta["media_kind"], mmeta["file_unique_id"], mmeta["file_name"], mmeta["file_ext"],
                                mmeta["mime_type"], mmeta["file_size"], mmeta["width"], mmeta["height"], mmeta["duration_sec"],
                                grouped_id, mmeta["media_fingerprint"], mmeta["meta_json"],
                            ))

                    except Exception as e:
                        counters.note_parse_failure(e, message)
                        logging.warning(f"⚠️ 跳过一条消息（解析失败）: {e}")
                        continue

                    if len(msg_rows) >= CFG.batch_size:
                        try:
                            batch_upsert(conn, msg_rows, media_rows)
                        except Exception as e:
                            logging.exception(f"批量落库失败（chat_id={chat_id}, batch_size={len(msg_rows)}）: {e}")
                            raise
                        counters.written += len(msg_rows)
                        msg_rows.clear()
                        media_rows.clear()

                # 阶段1：批量落库
                if msg_rows or media_rows:
                    try:
                        batch_upsert(conn, msg_rows, media_rows)
                    except Exception as e:
                        logging.exception(f"收尾批量落库失败（chat_id={chat_id}, remain={len(msg_rows)}）: {e}")
                        raise
                    counters.written += len(msg_rows)

                # 阶段2~4：首次聚合 -> dedupe -> 去重后一致性修复
                try:
                    deduped_count, dup_hash_solo, dup_hash_group_txt, dup_hash_group_med, _ = _run_group_refresh_and_dedupe_pipeline(
                        conn,
                        chat_id=chat_id,
                        first_sync=first_sync,
                        touched_group_ids=touched_group_ids,
                    )
                except Exception as e:
                    logging.exception(f"群组处理失败（chat_id={chat_id}，阶段=media_groups/dedupe）: {e}")
                    raise

                # 阶段5：汇总统计/日志输出
                stats = get_chat_stats(conn, chat_id)
                log_parse_failure_summary(counters)
                logging.info(f"群组: {chat_title} (chat_id={chat_id}) | 扫描={counters.seen} 写入={counters.written} 删除={deduped_count}")
                logging.info(f"模板命中: 单条={dup_hash_solo} 组文案={dup_hash_group_txt} 组媒体={dup_hash_group_med}")
                logging.info(f"库存: 总={stats['total_messages']} 媒体={stats['media_messages']} 广告候选={stats['promo_messages']}")
    finally:
        conn.close()
