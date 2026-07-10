import logging
import sqlite3
import time
from collections.abc import Callable
from typing import Any

from tg_harvest.config import CFG, AppConfig, _is_enabled
from tg_harvest.domain.chat_ids import candidate_chat_entity_ids
from tg_harvest.domain.coerce import clean_username
from tg_harvest.domain.dedupe import (
    build_message_dedupe_hash,
    dedupe_promotional_duplicates,
)
from tg_harvest.domain.normalize import _safe_json
from tg_harvest.domain.promo import build_single_promo_features
from tg_harvest.ingest.flood_wait import (
    AccountFloodWaitError,
    bounded_retry_count,
    exponential_backoff_seconds,
    flood_sleep_threshold_kwargs,
    flood_wait_seconds,
    format_retry_context,
    is_transient_telegram_error,
    maybe_refresh_entity_cursor,
    raise_if_long_flood_wait,
    short_retry_sleep_seconds,
)
from tg_harvest.ingest.media_groups import refresh_media_groups_for_chat
from tg_harvest.ingest.parse import (
    HarvestCounters,
    MessageParseError,
    MessageParser,
    ParsedMessage,
    log_parse_failure_summary,
    resolve_target_entities,
    setup_logging,
)
from tg_harvest.ingest.store import (
    batch_upsert,
    get_last_message_id,
    upsert_chat,
)
from tg_harvest.runtime.paths import secure_session_artifacts
from tg_harvest.storage.connection import ensure_configured_db
from tg_harvest.storage.schema import refresh_chat_message_counts


def _format_harvest_progress_message(
    current: int, total: int | None = None, *, prefix: str = "正在采集"
) -> str:
    safe_current = max(int(current), 0)
    if isinstance(total, int) and total >= 0:
        safe_total = max(int(total), 0)
        display_current = min(safe_current, safe_total)
        return f"{prefix} {display_current}/{safe_total}"
    return f"{prefix} {safe_current}"


def _log_harvest_progress(
    current: int,
    total: int | None = None,
    *,
    prefix: str = "正在采集",
) -> None:
    logging.info(_format_harvest_progress_message(current, total, prefix=prefix))


def _build_iter_messages_kwargs(
    resume_from_id: int, *, history_wait_time: float | None = None
) -> dict[str, int | bool | float]:
    kwargs: dict[str, int | bool | float] = {"reverse": True}
    if resume_from_id > 0:
        kwargs["min_id"] = int(resume_from_id)
    if history_wait_time is not None:
        kwargs["wait_time"] = max(0.0, float(history_wait_time))
    return kwargs


def _last_message_id_in_rows(msg_rows: list[tuple]) -> int:
    if not msg_rows:
        return 0
    return max(int(row[1]) for row in msg_rows)


def _write_message_batch(
    conn: sqlite3.Connection,
    msg_rows: list[tuple],
    media_rows: list[tuple],
    *,
    write_batch_fn: Callable[[list[tuple], list[tuple]], None] | None = None,
) -> None:
    if write_batch_fn is not None:
        write_batch_fn(list(msg_rows), list(media_rows))
        return
    batch_upsert(conn, msg_rows, media_rows)


def get_existing_chat_ids(conn: sqlite3.Connection) -> list[int]:
    cur = conn.cursor()
    try:
        cur.execute("SELECT chat_id FROM chats ORDER BY last_seen_at DESC")
        return [int(row["chat_id"]) for row in cur.fetchall()]
    finally:
        cur.close()


def _resolve_entity_by_chat_id(client: Any, chat_id: int) -> Any:
    try:
        return client.get_entity(int(chat_id))
    except Exception as exc:
        for fallback_id in candidate_chat_entity_ids(int(chat_id)):
            if fallback_id == int(chat_id):
                continue
            try:
                return client.get_entity(fallback_id)
            except Exception:
                pass
        raise exc


def _resolve_same_entity_for_client(client: Any, entity: Any) -> Any:
    username = clean_username(getattr(entity, "username", ""))
    if username:
        try:
            return client.get_entity(username)
        except Exception:
            pass

    entity_id = int(getattr(entity, "id", 0) or 0)
    for candidate_id in candidate_chat_entity_ids(entity_id):
        try:
            return client.get_entity(candidate_id)
        except Exception:
            pass

    raise RuntimeError("备用账号无法解析到同一群组/频道")


def collect_target_entities(
    conn: sqlite3.Connection, client: Any, cfg: AppConfig
) -> list[Any]:
    entities = []
    seen_ids = set()
    to_resolve_ids = []

    if _is_enabled(cfg.scan_existing_chats):
        to_resolve_ids.extend(get_existing_chat_ids(conn))

    if to_resolve_ids:
        try:
            resolved = client.get_entity(to_resolve_ids)
            if not isinstance(resolved, list):
                resolved = [resolved]
            for ent in resolved:
                cid = int(getattr(ent, "id", 0))
                if cid and cid not in seen_ids:
                    seen_ids.add(cid)
                    entities.append(ent)
        except Exception as e:
            logging.warning(f"批量解析现有 chat_id 失败，退回到逐个解析模式: {e}")
            for cid in to_resolve_ids:
                try:
                    ent = _resolve_entity_by_chat_id(client, cid)
                    cid_actual = int(getattr(ent, "id", 0))
                    if cid_actual and cid_actual not in seen_ids:
                        seen_ids.add(cid_actual)
                        entities.append(ent)
                except Exception as e2:
                    logging.warning(f"无法获取 chat_id={cid}: {e2}")

    if cfg.target_group.strip():
        for ent in resolve_target_entities(client, cfg.target_group):
            cid = int(getattr(ent, "id", 0))
            if cid and cid not in seen_ids:
                seen_ids.add(cid)
                entities.append(ent)

    return entities


def _prepare_db_rows(
    entity: Any, chat_id: int, p: ParsedMessage
) -> tuple[tuple, tuple | None]:
    """构建存库所需的元组"""
    features = build_single_promo_features(
        p.content, msg_type=p.msg_type, has_media=p.has_media, cfg=CFG
    )

    dedupe_hash = build_message_dedupe_hash(
        text_pure_hash=features["pure_hash"],
        has_media=p.has_media,
        media_fingerprint=(p.media_meta or {}).get("media_fingerprint"),
    )

    msg_row = (
        chat_id,
        p.msg_id,
        p.date_text,
        p.date_ts,
        p.sender_id,
        p.content,
        features["content_norm"],
        features["pure_hash"],
        dedupe_hash,
        p.msg_type,
        p.grouped_id,
        1 if p.has_media else 0,
        int(features["is_promo"]),
        int(features["promo_score"]),
        _safe_json(features["promo_reasons"]),
        int(features["dedupe_eligible"]),
        features["guard_reason"],
        int(features["text_len"]),
    )

    media_row = None
    if p.has_media and p.media_meta:
        m = p.media_meta
        media_row = (
            chat_id,
            p.msg_id,
            m["media_kind"],
            m["file_unique_id"],
            m["file_name"],
            m["file_ext"],
            m["mime_type"],
            m["file_size"],
            m["width"],
            m["height"],
            m["duration_sec"],
            p.grouped_id,
            m["media_fingerprint"],
            m["meta_json"],
        )
    return msg_row, media_row


def _harvest_messages_for_entity(
    conn: sqlite3.Connection,
    client: Any,
    entity: Any,
    chat_id: int,
    *,
    write_batch_fn: Callable[[list[tuple], list[tuple]], None] | None = None,
    progress_total: int | None = None,
    progress_prefix: str = "正在采集",
) -> tuple[HarvestCounters, set[int], bool]:
    from telethon.errors import FloodWaitError, RPCError

    started_at = time.perf_counter()
    last_id = get_last_message_id(conn, chat_id)
    first_sync = last_id == 0
    # 允许少量重扫尾部以处理 Telegram 乱序或删除情况
    scan_from_id = max(last_id - CFG.rescan_tail_ids, 0)
    resume_from_id = scan_from_id
    progress_floor = max(last_id, 0)
    normalized_progress_total = (
        max(int(progress_total), progress_floor)
        if isinstance(progress_total, int) and progress_total >= 0
        else None
    )

    counters = HarvestCounters()
    msg_rows, media_rows = [], []
    touched_groups = set()

    logging.info(
        f"开始抓取 chat_id={chat_id}，{'全量' if first_sync else '增量'}同步，中止 ID <= {scan_from_id}"
    )
    _log_harvest_progress(
        progress_floor if normalized_progress_total is not None else 0,
        normalized_progress_total,
        prefix=progress_prefix,
    )

    max_retries = bounded_retry_count(3)
    retry_count = 0
    harvest_completed = False

    while retry_count < max_retries:
        try:
            iterator = client.iter_messages(
                entity,
                **_build_iter_messages_kwargs(
                    resume_from_id,
                    history_wait_time=CFG.history_wait_time,
                ),
            )
            for message in iterator:
                if message.id <= resume_from_id:
                    continue

                counters.seen += 1
                try:
                    p = MessageParser.parse(message)
                except MessageParseError as exc:
                    root_exc = (
                        exc.cause if isinstance(exc.cause, Exception) else exc
                    )
                    counters.note_parse_failure(root_exc, message)
                    logging.error(
                        "消息解析失败，为避免静默丢数已中止当前采集: chat_id=%s message_id=%s error=%s",
                        chat_id,
                        getattr(message, "id", "?"),
                        exc,
                    )
                    raise RuntimeError(
                        "消息解析失败并已中止当前采集 "
                        f"(chat_id={chat_id}, message_id={getattr(message, 'id', '?')}): {exc}"
                    ) from exc
                if not p:
                    continue

                if p.grouped_id:
                    touched_groups.add(p.grouped_id)

                m_row, med_row = _prepare_db_rows(entity, chat_id, p)
                msg_rows.append(m_row)
                if med_row:
                    media_rows.append(med_row)

                if len(msg_rows) >= CFG.batch_size:
                    _write_message_batch(
                        conn,
                        msg_rows,
                        media_rows,
                        write_batch_fn=write_batch_fn,
                    )
                    counters.written += len(msg_rows)
                    resume_from_id = max(resume_from_id, _last_message_id_in_rows(msg_rows))
                    msg_rows, media_rows = [], []
                    logging.info(f"批量写入完成：已写入={counters.written}")

                if counters.seen % CFG.log_every == 0:
                    progress_current = (
                        max(int(message.id or 0), progress_floor)
                        if normalized_progress_total is not None
                        else counters.seen
                    )
                    _log_harvest_progress(
                        progress_current,
                        normalized_progress_total,
                        prefix=progress_prefix,
                    )
                    logging.info(f"进度：扫描={counters.seen}，写入={counters.written}")

            harvest_completed = True
            break

        except FloodWaitError as e:
            msg_rows, media_rows = [], []
            raise_if_long_flood_wait(
                e,
                threshold_seconds=CFG.flood_wait_switch_threshold,
                scope=f"chat:{chat_id}",
            )
            wait_seconds = flood_wait_seconds(e)
            retry_count += 1
            if retry_count >= max_retries:
                raise RuntimeError(
                    f"采集触发 FloodWait 且重试耗尽 (chat_id={chat_id}, wait={wait_seconds}s)"
                ) from e
            sleep_seconds = exponential_backoff_seconds(
                retry_count,
                required_wait_seconds=wait_seconds,
            )
            logging.warning(
                "触发 FloodWait，准备重试: chat_id=%s %s raw_wait=%ss",
                chat_id,
                format_retry_context(
                    retry_index=retry_count,
                    max_retries=max_retries,
                    wait_seconds=sleep_seconds,
                ),
                wait_seconds,
            )
            time.sleep(sleep_seconds)
        except (RPCError, ConnectionError, OSError, TimeoutError) as e:
            msg_rows, media_rows = [], []
            # P0 级修复：专门处理 MsgidDecreaseRetryError 等位点冲突错误
            # 这种错误如果不做状态重置，单纯的重试是无效的
            err_msg = str(e)
            if "MsgidDecreaseRetryError" in err_msg or "msg_id" in err_msg.lower():
                logging.error(
                    f"检测到 Telegram 内部位点冲突 ({err_msg})，正在尝试重置位点..."
                )
                maybe_refresh_entity_cursor(client, entity)
                retry_count += 1
                if retry_count >= max_retries:
                    raise RuntimeError(
                        f"检测到 Telegram 位点冲突且重试耗尽 (chat_id={chat_id}): {e}"
                    ) from e
                sleep_seconds = short_retry_sleep_seconds(retry_count)
                logging.warning(
                    "位点冲突刷新后准备重试: chat_id=%s %s",
                    chat_id,
                    format_retry_context(
                        retry_index=retry_count,
                        max_retries=max_retries,
                        wait_seconds=sleep_seconds,
                    ),
                )
                time.sleep(sleep_seconds)
                continue

            retry_count += 1
            if retry_count >= max_retries:
                logging.error(
                    f"网络或 RPC 错误 (尝试 {retry_count}/{max_retries}): {e}。已达到重试上限。"
                )
                raise RuntimeError(
                    f"网络或 RPC 错误重试耗尽，采集中断 (chat_id={chat_id}): {e}"
                ) from e
            if is_transient_telegram_error(e) or isinstance(e, RPCError):
                sleep_seconds = short_retry_sleep_seconds(retry_count)
            else:
                sleep_seconds = short_retry_sleep_seconds(retry_count)
            logging.warning(
                "网络或 RPC 错误，准备重试: chat_id=%s %s error=%s",
                chat_id,
                format_retry_context(
                    retry_index=retry_count,
                    max_retries=max_retries,
                    wait_seconds=sleep_seconds,
                ),
                e,
            )
            time.sleep(sleep_seconds)
        except Exception as e:
            msg_rows, media_rows = [], []
            logging.error(f"采集发生未预期错误: {e}")
            raise RuntimeError(f"采集发生未预期错误 (chat_id={chat_id}): {e}") from e

    if not harvest_completed:
        raise RuntimeError(f"采集未完成即退出 (chat_id={chat_id})")

    if msg_rows:
        _write_message_batch(
            conn,
            msg_rows,
            media_rows,
            write_batch_fn=write_batch_fn,
        )
        counters.written += len(msg_rows)
        resume_from_id = max(resume_from_id, _last_message_id_in_rows(msg_rows))

    _log_harvest_progress(
        normalized_progress_total
        if normalized_progress_total is not None
        else counters.seen,
        normalized_progress_total,
        prefix=progress_prefix,
    )
    elapsed = time.perf_counter() - started_at
    logging.info(
        f"消息抓取阶段完成: chat_id={chat_id} 扫描={counters.seen} 写入={counters.written} 耗时={elapsed:.2f}s"
    )
    return counters, touched_groups, first_sync


def _finalize_entity_processing(
    conn: sqlite3.Connection,
    *,
    chat_id: int,
    chat_title: str,
    counters: HarvestCounters,
    touched_groups: set[int],
    first_sync: bool,
    total_started_at: float,
    skip_postprocess_if_unchanged: bool = False,
    enable_dedupe: bool = True,
) -> dict[str, Any]:
    media_groups_elapsed = 0.0
    refresh_counts_elapsed = 0.0
    dedupe_elapsed = 0.0
    dedupe_refresh_elapsed = 0.0
    del_count = 0
    affected_groups: set[int] = set()
    should_skip_postprocess = (
        skip_postprocess_if_unchanged
        and counters.written <= 0
        and not touched_groups
    )

    if should_skip_postprocess:
        total_elapsed = time.perf_counter() - total_started_at
        logging.info(
            f"完成: {chat_title} | 扫描={counters.seen} 写入={counters.written} 删除=0 "
            f"| 耗时 total={total_elapsed:.2f}s harvest={total_elapsed:.2f}s "
            f"postprocess=skipped(no_changes)"
        )
        log_parse_failure_summary(counters)
        return {
            "deleted_count": 0,
            "affected_groups": set(),
            "media_groups_elapsed": 0.0,
            "refresh_counts_elapsed": 0.0,
            "dedupe_elapsed": 0.0,
            "dedupe_refresh_elapsed": 0.0,
            "total_elapsed": total_elapsed,
        }

    if first_sync and counters.written > 0:
        media_groups_started_at = time.perf_counter()
        refresh_media_groups_for_chat(conn, chat_id, cfg=CFG, grouped_ids=None)
        media_groups_elapsed += time.perf_counter() - media_groups_started_at
    elif touched_groups:
        media_groups_started_at = time.perf_counter()
        refresh_media_groups_for_chat(
            conn, chat_id, cfg=CFG, grouped_ids=touched_groups
        )
        media_groups_elapsed += time.perf_counter() - media_groups_started_at

    if counters.written > 0:
        refresh_counts_started_at = time.perf_counter()
        refresh_chat_message_counts(conn, [chat_id])
        refresh_counts_elapsed += time.perf_counter() - refresh_counts_started_at

    if enable_dedupe:
        dedupe_started_at = time.perf_counter()
        del_count, _solo, _g_txt, _g_med, affected_groups = dedupe_promotional_duplicates(
            conn,
            chat_id=chat_id,
            mode=CFG.dedup_mode,
            threshold=CFG.dedup_threshold,
            promo_score_threshold=CFG.promo_score_threshold,
        )
        dedupe_elapsed += time.perf_counter() - dedupe_started_at

        if affected_groups:
            dedupe_refresh_started_at = time.perf_counter()
            refresh_media_groups_for_chat(
                conn, chat_id, cfg=CFG, grouped_ids=affected_groups
            )
            dedupe_refresh_elapsed += time.perf_counter() - dedupe_refresh_started_at
        if del_count > 0:
            refresh_counts_started_at = time.perf_counter()
            refresh_chat_message_counts(conn, [chat_id])
            refresh_counts_elapsed += time.perf_counter() - refresh_counts_started_at

    total_elapsed = time.perf_counter() - total_started_at
    dedupe_display = (
        f"{dedupe_elapsed:.2f}s" if enable_dedupe else "skipped(disabled)"
    )
    harvest_elapsed = max(
        total_elapsed
        - media_groups_elapsed
        - refresh_counts_elapsed
        - dedupe_elapsed
        - dedupe_refresh_elapsed,
        0,
    )
    logging.info(
        f"完成: {chat_title} | 扫描={counters.seen} 写入={counters.written} 删除={del_count} "
        f"| 耗时 total={total_elapsed:.2f}s harvest={harvest_elapsed:.2f}s "
        f"media_groups={media_groups_elapsed:.2f}s refresh_counts={refresh_counts_elapsed:.2f}s "
        f"dedupe={dedupe_display} dedupe_media_groups={dedupe_refresh_elapsed:.2f}s"
    )
    log_parse_failure_summary(counters)
    return {
        "deleted_count": del_count,
        "affected_groups": affected_groups,
        "media_groups_elapsed": media_groups_elapsed,
        "refresh_counts_elapsed": refresh_counts_elapsed,
        "dedupe_elapsed": dedupe_elapsed,
        "dedupe_refresh_elapsed": dedupe_refresh_elapsed,
        "total_elapsed": total_elapsed,
    }


def _process_entity(
    conn: sqlite3.Connection,
    client: Any,
    entity: Any,
    idx: int,
    total: int,
    *,
    skip_postprocess_if_unchanged: bool = False,
    enable_dedupe: bool = True,
):
    total_started_at = time.perf_counter()
    chat_id = int(getattr(entity, "id", 0))
    chat_title = getattr(entity, "title", "Unknown")
    chat_username = getattr(entity, "username", None)

    logging.info(f"[{idx}/{total}] 正在处理: {chat_title} (ID={chat_id})")
    upsert_chat(
        conn,
        (
            chat_id,
            chat_title,
            chat_username,
            1 if chat_username else 0,
            entity.__class__.__name__,
        ),
    )

    counters, touched_groups, first_sync = _harvest_messages_for_entity(
        conn, client, entity, chat_id
    )

    try:
        _finalize_entity_processing(
            conn,
            chat_id=chat_id,
            chat_title=chat_title,
            counters=counters,
            touched_groups=touched_groups,
            first_sync=first_sync,
            total_started_at=total_started_at,
            skip_postprocess_if_unchanged=skip_postprocess_if_unchanged,
            enable_dedupe=enable_dedupe,
        )

    except sqlite3.Error as e:
        logging.error(f"数据库操作失败 (chat_id={chat_id}): {e}")
        raise RuntimeError(f"数据库后处理失败 (chat_id={chat_id}): {e}") from e


def run_harvest():
    from telethon.sync import TelegramClient

    setup_logging()

    conn, _ = ensure_configured_db(cfg=CFG)

    try:
        from pathlib import Path

        Path(str(CFG.session_name)).parent.mkdir(parents=True, exist_ok=True)
        with TelegramClient(
            CFG.session_name,
            CFG.api_id,
            CFG.api_hash,
            receive_updates=False,
            **flood_sleep_threshold_kwargs(CFG),
        ) as client:
            secure_session_artifacts(CFG.session_name)
            entities = collect_target_entities(conn, client, CFG)
            if not entities:
                logging.error("没有可抓取的群组/频道。")
                return

            for idx, ent in enumerate(entities, 1):
                try:
                    _process_entity(conn, client, ent, idx, len(entities))
                except AccountFloodWaitError as exc:
                    secondary_session = str(CFG.secondary_session_name or "").strip()
                    if not secondary_session or secondary_session == str(CFG.session_name):
                        raise
                    logging.warning(
                        "主账号触发长时间 FloodWait，切换第二账号继续当前目标: wait=%ss threshold=%ss",
                        exc.seconds,
                        exc.threshold_seconds,
                    )
                    with TelegramClient(
                        secondary_session,
                        CFG.api_id,
                        CFG.api_hash,
                        receive_updates=False,
                        **flood_sleep_threshold_kwargs(CFG),
                    ) as secondary_client:
                        secure_session_artifacts(secondary_session)
                        secondary_entity = _resolve_same_entity_for_client(
                            secondary_client, ent
                        )
                        _process_entity(conn, secondary_client, secondary_entity, idx, len(entities))
    finally:
        conn.close()
