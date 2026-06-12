import logging
import time
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import suppress
from typing import Any

from tg_harvest.admin_jobs.cleanup import (
    _build_cleanup_like_patterns,
    _build_cleanup_targets_table,
    _execute_cleanup_deletion_batches,
)
from tg_harvest.admin_jobs.common import (
    admin_error_message,
    finish_job_heartbeat,
    read_chat_username,
    resolve_chat_entity,
    start_admin_job_thread,
)
from tg_harvest.admin_jobs.core import (
    _admin_job_heartbeat,
    _admin_job_stop_requested,
    _admin_job_update_progress,
    job_context,
    job_log_passthrough_enabled,
)
from tg_harvest.admin_jobs.sessions import (
    _cleanup_isolated_worker_session,
    _create_isolated_worker_client,
    _disconnect_worker_client,
    _ensure_base_session_valid,
    _start_job_heartbeat,
)
from tg_harvest.admin_jobs.streaming import stream_entity_harvest_to_writer
from tg_harvest.admin_jobs.update_writer import ChatUpdateWriteCoordinator
from tg_harvest.domain.chat_inventory import (
    filter_database_chats_to_joined,
    load_joined_chat_inventory,
)
from tg_harvest.ingest.store import backfill_message_search_text_from_filenames
from tg_harvest.storage import fts as _fts
from tg_harvest.storage import search_terms as _search_terms
from tg_harvest.storage.connection import synchronized_write

DELETE_CHAT_FAST_PATH_THRESHOLD = 50000


def _close_write_coordinator(
    write_coordinator: ChatUpdateWriteCoordinator, *, suppress_errors: bool = False
) -> None:
    try:
        write_coordinator.close()
    except Exception:
        if not suppress_errors:
            raise
        logging.exception("写入队列关闭失败，保留原始采集异常")


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        keys = row.keys()
    except Exception:
        keys = None
    if keys is not None and key not in keys:
        return default
    try:
        value = row[key]
    except Exception:
        return default
    return default if value is None else value


def _chat_title_fallback(chat_id: Any, chat_title: Any) -> str:
    title = str(chat_title or "").strip()
    if title:
        return title
    chat_id_text = str(chat_id or "").strip()
    return f"Chat {chat_id_text}" if chat_id_text else "未知群组"


def _chat_log_label(chat_id: Any, chat_title: Any) -> str:
    title = _chat_title_fallback(chat_id, chat_title)
    chat_id_text = str(chat_id or "").strip()
    if not chat_id_text:
        return title
    return f"{title} (ID={chat_id_text})"


def _chat_failure_item(chat_id: Any, chat_title: Any, reason: Any) -> str:
    return f"{_chat_log_label(chat_id, chat_title)}({str(reason or '').strip()})"


def _admin_update_all_chats(
    job_id, _ignored_client, get_conn_fn, admin_job_append_log_fn, cfg
):
    if not _ensure_base_session_valid(cfg, job_id, admin_job_append_log_fn):
        return False

    conn = get_conn_fn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT chat_id, chat_title, chat_username FROM chats ORDER BY chat_title COLLATE NOCASE ASC, chat_id ASC"
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        admin_job_append_log_fn(job_id, "当前无可更新群聊，任务结束")
        return True

    admin_job_append_log_fn(
        job_id,
        f"数据库中共有 {len(rows)} 个群组/频道，正在扫描当前账号已加入会话...",
    )
    inventory_client = None
    inventory_worker_id = f"{job_id}_inventory"
    try:
        inventory_client = _create_isolated_worker_client(cfg, inventory_worker_id)
        joined_rows = load_joined_chat_inventory(inventory_client.iter_dialogs())
    finally:
        if inventory_client:
            with suppress(Exception):
                _disconnect_worker_client(inventory_client)
        _cleanup_isolated_worker_session(cfg, inventory_worker_id)

    accessible_count = sum(
        1 for row in joined_rows if not str(row.unavailable_reason or "").strip()
    )
    rows = filter_database_chats_to_joined(rows, joined_rows)
    skipped_unjoined_count = max(0, len(joined_rows) - accessible_count)
    admin_job_append_log_fn(
        job_id,
        f"当前账号可访问 {accessible_count} 个群组/频道；"
        f"本次仅更新其中已入库的 {len(rows)} 个，跳过数据库中账号未加入或不可访问的群组",
    )
    if skipped_unjoined_count:
        admin_job_append_log_fn(
            job_id,
            f"另发现 {skipped_unjoined_count} 个已加入但 Telegram 返回不可访问的会话，已跳过",
        )

    if not rows:
        admin_job_append_log_fn(job_id, "当前账号没有可访问且已入库的群组，任务结束")
        _admin_job_update_progress(
            job_id,
            0,
            total=0,
            stage="done",
            log_step=0,
            auto_log=False,
        )
        return True

    total = len(rows)
    success_count, failed_count, total_added_messages = 0, 0, 0
    failed_chats = []

    try:
        concurrency = max(1, int(getattr(cfg, "admin_update_concurrency", 5)))
    except (TypeError, ValueError):
        concurrency = 5
    admin_job_append_log_fn(
        job_id, f"读取到 {total} 个群组，开始执行并发拉取 + 单线程写入（并发数：{concurrency}）"
    )
    _admin_job_update_progress(
        job_id,
        0,
        total=total,
        stage="updating",
        log_step=0,
    )

    write_coordinator = ChatUpdateWriteCoordinator(
        job_id=str(job_id),
        get_conn_fn=get_conn_fn,
        queue_maxsize=max(concurrency * 4, 16),
    )

    def _worker(idx, row):
        job_context.set(str(job_id))
        passthrough_token = job_log_passthrough_enabled.set(False)
        raw_chat_id = _row_value(row, "chat_id", "")
        current_chat_title = _chat_title_fallback(
            raw_chat_id, _row_value(row, "chat_title", "")
        )
        current_chat_label = _chat_log_label(raw_chat_id, current_chat_title)
        current_chat_username = _row_value(row, "chat_username", None)

        import random

        time.sleep(random.uniform(0.1, 0.5))

        local_client = None
        worker_id = f"{job_id}_{idx}"

        try:
            current_chat_id = int(raw_chat_id)
            if current_chat_id == 0:
                raise RuntimeError("无法识别群组/频道 ID")

            local_client = _create_isolated_worker_client(cfg, worker_id)
            before_count = _admin_get_chat_message_count(get_conn_fn, current_chat_id)
            entity = resolve_chat_entity(
                local_client, current_chat_id, current_chat_username
            )
            entity_title = (
                getattr(entity, "title", None)
                or getattr(entity, "username", None)
                or str(current_chat_id)
            )
            admin_job_append_log_fn(
                job_id,
                f"[{idx}/{total}] 群组连接成功：目标={current_chat_label}，名称={entity_title}",
            )

            stream_entity_harvest_to_writer(
                write_coordinator=write_coordinator,
                get_conn_fn=get_conn_fn,
                client=local_client,
                entity=entity,
                idx=idx,
                total=total,
                fallback_chat_id=current_chat_id,
                fallback_chat_title=current_chat_title,
                fallback_chat_username=current_chat_username,
                skip_postprocess_if_unchanged=True,
                enable_dedupe=False,
            )
            after_count = _admin_get_chat_message_count(get_conn_fn, current_chat_id)
            added_count = max(0, after_count - before_count)
            return current_chat_title, current_chat_id, True, added_count, None
        except Exception as chat_exc:
            logging.exception(f"Worker 执行群组 {current_chat_label} 失败")
            user_msg = admin_error_message(chat_exc)
            return (
                current_chat_title,
                raw_chat_id,
                False,
                0,
                user_msg,
            )
        finally:
            job_log_passthrough_enabled.reset(passthrough_token)
            if local_client:
                with suppress(Exception):
                    _disconnect_worker_client(local_client)
            _cleanup_isolated_worker_session(cfg, worker_id)

    stopped_early = False
    stop_logged = False
    row_iter = iter(enumerate(rows, start=1))

    def _log_stop_once() -> None:
        nonlocal stop_logged
        if stop_logged:
            return
        stop_logged = True
        admin_job_append_log_fn(
            job_id,
            "已收到停止请求：不再启动新的群组，等待当前并发中的群组完成后收尾",
        )

    def _should_stop_submitting() -> bool:
        nonlocal stopped_early
        if not _admin_job_stop_requested(str(job_id)):
            return False
        stopped_early = True
        _log_stop_once()
        return True

    def _submit_next(executor: ThreadPoolExecutor, futures: dict) -> bool:
        if _should_stop_submitting():
            return False
        try:
            idx, row = next(row_iter)
        except StopIteration:
            return False
        futures[executor.submit(_worker, idx, row)] = (idx, row)
        return True

    try:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {}
            while len(futures) < concurrency and _submit_next(executor, futures):
                pass

            while futures:
                done_futures, _pending_futures = wait(
                    futures.keys(), return_when=FIRST_COMPLETED
                )
                for future in done_futures:
                    idx, row = futures.pop(future)
                    try:
                        chat_title, chat_id, success, added, err_msg = future.result()
                        chat_label = _chat_log_label(chat_id, chat_title)
                        if success:
                            total_added_messages += added
                            success_count += 1
                            admin_job_append_log_fn(
                                job_id,
                                f"[{idx}/{total}] {chat_label} 新增 {added} 条消息",
                            )
                        else:
                            failed_count += 1
                            failed_chats.append(
                                _chat_failure_item(chat_id, chat_title, err_msg)
                            )
                            admin_job_append_log_fn(
                                job_id,
                                f"[{idx}/{total}] 增量采集失败：群组={chat_label}，错误={err_msg}",
                            )
                    except Exception as e:
                        failed_count += 1
                        raw_chat_id = _row_value(row, "chat_id", "")
                        chat_title = _chat_title_fallback(
                            raw_chat_id, _row_value(row, "chat_title", "")
                        )
                        chat_label = _chat_log_label(raw_chat_id, chat_title)
                        err_msg = admin_error_message(e)
                        failed_chats.append(
                            _chat_failure_item(raw_chat_id, chat_title, err_msg)
                        )
                        admin_job_append_log_fn(
                            job_id,
                            f"[{idx}/{total}] 线程执行异常：群组={chat_label}，错误={err_msg}",
                        )
                    finally:
                        _admin_job_update_progress(
                            job_id,
                            success_count + failed_count,
                            total=total,
                            stage="updating",
                            log_step=0,
                            auto_log=False,
                        )

                while len(futures) < concurrency and _submit_next(executor, futures):
                    pass
    finally:
        if success_count + failed_count >= total:
            _admin_job_update_progress(
                job_id,
                total,
                total=total,
                stage="finalizing",
                log_step=0,
                auto_log=False,
            )
        write_coordinator.close()

    processed_count = success_count + failed_count
    skipped_count = max(0, total - processed_count)
    if stopped_early:
        final_log_msg = (
            f"全部群组增量采集已按请求停止：成功 {success_count} 个，失败 {failed_count} 个，"
            f"未启动 {skipped_count} 个，总计 {total} 个，共新增 {total_added_messages} 条消息"
        )
    else:
        final_log_msg = (
            f"全部群组增量采集完成：成功 {success_count} 个，失败 {failed_count} 个，"
            f"总计 {total} 个，共新增 {total_added_messages} 条消息"
        )
    if failed_chats:
        final_log_msg += f"。失败列表：{', '.join(failed_chats)}"
    admin_job_append_log_fn(job_id, final_log_msg)
    _admin_job_update_progress(
        job_id,
        processed_count if stopped_early else total,
        total=total,
        stage="done" if failed_count == 0 else "error",
        log_step=0,
        auto_log=False,
    )
    return failed_count == 0


def _admin_process_single_chat_update(
    *,
    job_id: str,
    client: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_append_log_fn: Callable[[str, str], Any],
    chat_id: int,
    chat_title: str,
    chat_username: str | None = None,
    idx: int,
    total: int,
) -> None:
    entity = resolve_chat_entity(client, chat_id, chat_username)

    entity_title = (
        getattr(entity, "title", None)
        or getattr(entity, "username", None)
        or str(chat_id)
    )
    admin_job_append_log_fn(
        job_id, f"[{idx}/{total}] 群组连接成功：名称={entity_title}"
    )
    write_coordinator = ChatUpdateWriteCoordinator(
        job_id=str(job_id),
        get_conn_fn=get_conn_fn,
        queue_maxsize=16,
    )
    stream_failed = False
    try:
        _admin_job_update_progress(
            job_id,
            0,
            total=1,
            stage="updating",
            log_step=0,
            auto_log=False,
        )
        admin_job_append_log_fn(job_id, "启用边抓取边写入：抓取与数据库写入并行执行")
        stream_entity_harvest_to_writer(
            write_coordinator=write_coordinator,
            get_conn_fn=get_conn_fn,
            client=client,
            entity=entity,
            idx=idx,
            total=total,
            fallback_chat_id=chat_id,
            fallback_chat_title=chat_title,
            fallback_chat_username=chat_username,
            skip_postprocess_if_unchanged=True,
            enable_dedupe=False,
        )
        _admin_job_update_progress(
            job_id,
            1,
            total=1,
            stage="done",
            log_step=0,
            auto_log=False,
        )
    except Exception:
        stream_failed = True
        raise
    finally:
        _close_write_coordinator(write_coordinator, suppress_errors=stream_failed)


def _admin_get_chat_message_count(get_conn_fn: Callable[[], Any], chat_id: int) -> int:
    count_conn = get_conn_fn()
    try:
        cur = count_conn.cursor()
        cur.execute(
            "SELECT COALESCE(message_count, 0) AS cnt FROM chats WHERE chat_id = ?",
            (chat_id,),
        )
        row = cur.fetchone()
        return int(row["cnt"] or 0) if row else 0
    finally:
        count_conn.close()


def _admin_harvest_job_runner(
    job_id: str,
    target: str,
    *,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_make_job_log_handler_fn: Callable[[str], logging.Handler],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    root_logger = logging.getLogger()
    job_log_handler = admin_make_job_log_handler_fn(job_id)
    root_logger.addHandler(job_log_handler)

    heartbeat_stop, heartbeat_thread = _start_job_heartbeat(job_id, _admin_job_heartbeat)
    local_client = None
    worker_id = f"{job_id}_main"
    try:
        job_context.set(str(job_id))
        admin_job_set_status_fn(job_id, "running")
        admin_job_append_log_fn(job_id, f"开始新增数据采集：目标={target}")
        admin_job_append_log_fn(job_id, "正在验证 Telegram 会话...")

        if not _ensure_base_session_valid(cfg, job_id, admin_job_append_log_fn):
            admin_job_set_status_fn(job_id, "error")
            return

        admin_job_append_log_fn(job_id, "会话验证通过，正在建立 Telegram 连接...")
        local_client = _create_isolated_worker_client(cfg, worker_id)

        from tg_harvest.ingest.parse import resolve_target_entities

        entities = resolve_target_entities(local_client, target)

        if not entities:
            admin_job_append_log_fn(
                job_id,
                f"找不到对应目标：{target}。如果你输入的是标题，它可能不在该账号已有的群列表中。",
            )
            admin_job_set_status_fn(job_id, "error")
            return

        total = len(entities)
        admin_job_append_log_fn(job_id, f"目标解析成功：匹配到 {total} 个会话")
        admin_job_append_log_fn(job_id, "启用边抓取边写入：抓取与数据库写入并行执行")
        _admin_job_update_progress(
            job_id,
            0,
            total=total,
            stage="harvesting",
            log_step=0,
            auto_log=False,
        )
        write_coordinator = ChatUpdateWriteCoordinator(
            job_id=str(job_id),
            get_conn_fn=get_conn_fn,
            queue_maxsize=max(16, min(total, 4) * 4),
        )
        stream_failed = False
        try:
            for idx, entity in enumerate(entities, start=1):
                entity_title = (
                    getattr(entity, "title", None)
                    or getattr(entity, "username", None)
                    or str(target)
                )
                admin_job_append_log_fn(
                    job_id, f"[{idx}/{total}] 导入目标：名称={entity_title}"
                )
                stream_entity_harvest_to_writer(
                    write_coordinator=write_coordinator,
                    get_conn_fn=get_conn_fn,
                    client=local_client,
                    entity=entity,
                    idx=idx,
                    total=total,
                    fallback_chat_title=entity_title,
                    skip_postprocess_if_unchanged=False,
                    enable_dedupe=True,
                )
                _admin_job_update_progress(
                    job_id,
                    idx,
                    total=total,
                    stage="harvesting",
                    log_step=0,
                    auto_log=False,
                )
        except Exception:
            stream_failed = True
            raise
        finally:
            if not stream_failed:
                _admin_job_update_progress(
                    job_id,
                    total,
                    total=total,
                    stage="finalizing",
                    log_step=0,
                    auto_log=False,
                )
            _close_write_coordinator(write_coordinator, suppress_errors=stream_failed)
        _admin_job_update_progress(
            job_id,
            total,
            total=total,
            stage="done",
            log_step=0,
            auto_log=False,
        )
        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        user_msg = admin_error_message(exc)
        admin_job_append_log_fn(job_id, f"新增数据采集失败：{user_msg}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        if local_client:
            with suppress(Exception):
                _disconnect_worker_client(local_client)
        _cleanup_isolated_worker_session(cfg, worker_id)
        root_logger.removeHandler(job_log_handler)


def _admin_update_job_runner(
    job_id: str,
    chat_id: Any,
    chat_title: str,
    *,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_make_job_log_handler_fn: Callable[[str], logging.Handler],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    root_logger = logging.getLogger()
    job_log_handler = admin_make_job_log_handler_fn(job_id)
    root_logger.addHandler(job_log_handler)

    heartbeat_stop, heartbeat_thread = _start_job_heartbeat(job_id, _admin_job_heartbeat)
    local_client = None
    worker_id = f"{job_id}_main"
    try:
        job_context.set(str(job_id))
        admin_job_set_status_fn(job_id, "running")
        admin_job_append_log_fn(job_id, "正在验证 Telegram 会话...")

        is_all_scope = isinstance(chat_id, str) and chat_id.strip().lower() == "all"
        if not _ensure_base_session_valid(cfg, job_id, admin_job_append_log_fn):
            admin_job_set_status_fn(job_id, "error")
            return

        admin_job_append_log_fn(job_id, "会话验证通过，正在建立 Telegram 连接...")
        if is_all_scope:
            all_ok = _admin_update_all_chats(
                job_id, None, get_conn_fn, admin_job_append_log_fn, cfg
            )
            if not all_ok:
                admin_job_set_status_fn(job_id, "error")
                return
        else:
            chat_username = read_chat_username(get_conn_fn, int(chat_id))
            local_client = _create_isolated_worker_client(cfg, worker_id)
            _admin_process_single_chat_update(
                job_id=job_id,
                client=local_client,
                get_conn_fn=get_conn_fn,
                admin_job_append_log_fn=admin_job_append_log_fn,
                chat_id=int(chat_id),
                chat_title=chat_title,
                chat_username=chat_username,
                idx=1,
                total=1,
            )
        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        user_msg = admin_error_message(exc)
        admin_job_append_log_fn(job_id, f"采集失败：{user_msg}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        if local_client:
            with suppress(Exception):
                _disconnect_worker_client(local_client)
        _cleanup_isolated_worker_session(cfg, worker_id)
        root_logger.removeHandler(job_log_handler)


def _admin_delete_job_runner(
    job_id: str,
    chat_id: int,
    chat_title: str,
    *,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    job_context.set(str(job_id))
    heartbeat_stop, heartbeat_thread = _start_job_heartbeat(
        job_id, _admin_job_heartbeat
    )
    conn = None
    try:
        admin_job_set_status_fn(job_id, "running")
        admin_job_append_log_fn(
            job_id, f"开始删除数据：目标={chat_title}，群组ID={chat_id}"
        )
        conn = get_conn_fn()
        related_counts = _count_chat_related_rows(conn, int(chat_id))
        admin_job_append_log_fn(
            job_id,
            "待删除数据："
            f"消息 {related_counts['messages']} 条，"
            f"媒体记录 {related_counts['media_rows']} 条，"
            f"媒体组 {related_counts['media_groups']} 个",
        )
        if related_counts["messages"] >= DELETE_CHAT_FAST_PATH_THRESHOLD:
            admin_job_append_log_fn(
                job_id,
                "大型群组启用快速删除模式：批量同步搜索索引，暂停逐条关联索引触发器",
            )
        admin_job_append_log_fn(job_id, "清理关联数据...")
        deleted_messages = _delete_chat_data(conn, int(chat_id))
        admin_job_append_log_fn(
            job_id, f"删除完成：共清除 {deleted_messages} 条消息"
        )
        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        if conn:
            conn.rollback()
        admin_job_append_log_fn(job_id, f"删除失败：{exc}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        if conn:
            conn.close()


def _admin_delete_empty_chats_job_runner(
    job_id: str,
    *,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    job_context.set(str(job_id))
    heartbeat_stop, heartbeat_thread = _start_job_heartbeat(
        job_id, _admin_job_heartbeat
    )
    conn = None
    try:
        admin_job_set_status_fn(job_id, "running")
        admin_job_append_log_fn(job_id, "开始删除零消息群组")
        conn = get_conn_fn()
        stats = _delete_empty_chats_data(conn)
        deleted_chats = int(stats.get("deleted_chats", 0) or 0)
        if deleted_chats <= 0:
            admin_job_append_log_fn(job_id, "未发现消息数量为 0 的可删除群组")
        else:
            admin_job_append_log_fn(
                job_id,
                "零消息群组删除完成："
                f"删除群组 {deleted_chats} 个，"
                f"清理残留消息 {int(stats.get('deleted_messages', 0) or 0)} 条，"
                f"媒体记录 {int(stats.get('deleted_media_rows', 0) or 0)} 条，"
                f"媒体组 {int(stats.get('deleted_media_groups', 0) or 0)} 个",
            )
        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        if conn:
            conn.rollback()
        admin_job_append_log_fn(job_id, f"删除零消息群组失败：{exc}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        if conn:
            conn.close()


def _admin_cleanup_job_runner(
    job_id: str,
    keyword: str,
    scope: str,
    chat_id: int | None,
    target_label: str,
    *,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
    cleanup_mode: str = "keyword",
) -> None:
    job_context.set(str(job_id))
    heartbeat_stop, heartbeat_thread = _start_job_heartbeat(job_id, _admin_job_heartbeat)
    conn = None
    try:
        admin_job_set_status_fn(job_id, "running")
        conn = get_conn_fn()
        cur = conn.cursor()

        # 修正：根据 scope 和 chat_id 构建过滤条件
        scope_filter_sql = ""
        scope_filter_params = []
        if scope == "chat" and chat_id is not None:
            scope_filter_sql = " AND m.chat_id = ?"
            scope_filter_params = [chat_id]

        like_pattern = _build_cleanup_like_patterns(keyword)

        # 安全检查：如果是关键词模式且关键词为空，则直接结束，防止 LIKE '%%' 扫描全库导致崩溃
        if cleanup_mode == "keyword" and not keyword.strip():
            admin_job_append_log_fn(job_id, "关键词不能为空，清理任务取消")
            admin_job_set_status_fn(job_id, "done")
            return

        if cleanup_mode == "empty_media":
            synced = backfill_message_search_text_from_filenames(
                conn,
                chat_id=chat_id if scope == "chat" else None,
                batch_size=5000,
                log_fn=lambda message: admin_job_append_log_fn(job_id, str(message)),
            )
            if synced > 0:
                admin_job_append_log_fn(
                    job_id,
                    f"已先补齐 {synced} 条可搜索文件名文本，用于保留可通过文件名搜索的数据",
                )

        target_count = _build_cleanup_targets_table(
            cur,
            cleanup_mode,
            scope_filter_sql,
            scope_filter_params,
            like_pattern,
        )
        conn.commit()

        if target_count > 0:
            admin_job_append_log_fn(
                job_id, f"检索到待清理数据：{target_count} 条，开始执行物理删除..."
            )
            actual_deleted = _execute_cleanup_deletion_batches(
                conn,
                cur,
                job_id,
                target_count,
                admin_job_append_log_fn,
            )
            admin_job_append_log_fn(job_id, f"清理完成：共删除 {actual_deleted} 条数据")
        else:
            admin_job_append_log_fn(job_id, "未发现符合条件的数据，任务结束")

        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        logging.exception(f"清理任务异常 (job_id: {job_id})")
        admin_job_append_log_fn(job_id, f"清理失败：{exc}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        if conn:
            conn.close()


def _admin_start_harvest_job_thread(job_id, target, **kwargs):
    return start_admin_job_thread(_admin_harvest_job_runner, job_id, target, **kwargs)


def _admin_start_update_job_thread(job_id, chat_id, chat_title, **kwargs):
    return start_admin_job_thread(
        _admin_update_job_runner, job_id, chat_id, chat_title, **kwargs
    )


def _optional_table_exists(cur: Any, table_name: str) -> bool:
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    return cur.fetchone() is not None


def _delete_from_optional_chat_table(cur: Any, table_name: str, chat_id: int) -> None:
    if not _optional_table_exists(cur, table_name):
        return
    cur.execute(f"DELETE FROM {table_name} WHERE chat_id = ?", (chat_id,))


def _delete_from_optional_chat_targets_table(
    cur: Any, table_name: str, target_table: str
) -> int:
    if not _optional_table_exists(cur, table_name):
        return 0
    cur.execute(
        f"""
        DELETE FROM {table_name}
        WHERE chat_id IN (
            SELECT chat_id
            FROM {target_table}
        )
        """
    )
    return int(cur.rowcount or 0)


def _delete_from_optional_message_pk_targets_table(
    cur: Any, table_name: str, target_table: str
) -> int:
    if not _optional_table_exists(cur, table_name):
        return 0
    cur.execute(
        f"""
        DELETE FROM {table_name}
        WHERE pk IN (
            SELECT pk
            FROM {target_table}
        )
        """
    )
    return int(cur.rowcount or 0)


def _prepare_delete_chat_message_targets(cur: Any, chat_id: int) -> int:
    cur.execute("DROP TABLE IF EXISTS temp_delete_chat_messages")
    cur.execute(
        """
        CREATE TEMP TABLE temp_delete_chat_messages (
            pk INTEGER PRIMARY KEY,
            message_id INTEGER NOT NULL
        )
        """
    )
    cur.execute(
        """
        INSERT INTO temp_delete_chat_messages(pk, message_id)
        SELECT pk, message_id
        FROM messages
        WHERE chat_id = ?
        """,
        (int(chat_id),),
    )
    cur.execute("SELECT COUNT(*) FROM temp_delete_chat_messages")
    return int(cur.fetchone()[0] or 0)


def _prepare_empty_chat_targets(cur: Any) -> int:
    cur.execute("DROP TABLE IF EXISTS temp_delete_empty_chats")
    cur.execute(
        """
        CREATE TEMP TABLE temp_delete_empty_chats (
            chat_id INTEGER PRIMARY KEY
        )
        """
    )
    cur.execute(
        """
        INSERT INTO temp_delete_empty_chats(chat_id)
        SELECT c.chat_id
        FROM chats c
        WHERE COALESCE(c.message_count, 0) = 0
          AND NOT EXISTS (
              SELECT 1
              FROM messages m
              WHERE m.chat_id = c.chat_id
          )
        """
    )
    cur.execute("SELECT COUNT(*) FROM temp_delete_empty_chats")
    return int(cur.fetchone()[0] or 0)


def _sqlite_object_exists(cur: Any, object_type: str, name: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = ? AND name = ?
        LIMIT 1
        """,
        (object_type, name),
    )
    return cur.fetchone() is not None


def _chat_delete_has_fts_index(cur: Any) -> bool:
    return _sqlite_object_exists(cur, "table", "messages_fts")


def _drop_message_delete_triggers_for_bulk_delete(
    cur: Any, *, fts_enabled: bool
) -> None:
    if fts_enabled:
        cur.execute("DROP TRIGGER IF EXISTS trg_messages_fts_delete")
    cur.execute("DROP TRIGGER IF EXISTS trg_message_terms_delete")


def _restore_message_delete_triggers_after_bulk_delete(
    cur: Any, *, fts_enabled: bool
) -> None:
    if fts_enabled:
        _fts._create_fts_triggers(cur)
    _search_terms._create_message_search_terms_queue_triggers(cur)


def _delete_fts_entries_for_chat_targets(cur: Any) -> None:
    cur.execute(
        """
        INSERT INTO messages_fts(messages_fts, rowid, content)
        SELECT
            'delete',
            m.pk,
            COALESCE(NULLIF(m.content_norm, ''), m.content, '')
        FROM messages m
        JOIN temp_delete_chat_messages t ON t.pk = m.pk
        """
    )


@synchronized_write
def _delete_chat_data(conn: Any, chat_id: int) -> int:
    cur = conn.cursor()
    fts_enabled = False
    triggers_dropped = False
    try:
        cur.execute("BEGIN IMMEDIATE")
        deleted_messages = _prepare_delete_chat_message_targets(cur, chat_id)
        trigger_optimization_required = (
            int(deleted_messages) >= DELETE_CHAT_FAST_PATH_THRESHOLD
        )
        fts_enabled = trigger_optimization_required and _chat_delete_has_fts_index(cur)
        if trigger_optimization_required:
            _drop_message_delete_triggers_for_bulk_delete(
                cur,
                fts_enabled=fts_enabled,
            )
            triggers_dropped = True
            if fts_enabled:
                _delete_fts_entries_for_chat_targets(cur)
        _delete_from_optional_chat_table(cur, "admin_absent_chats", chat_id)
        _delete_from_optional_chat_table(cur, "admin_missing_chats", chat_id)
        _delete_from_optional_chat_table(cur, "admin_restricted_chats", chat_id)
        _delete_from_optional_message_pk_targets_table(
            cur, "message_search_terms", "temp_delete_chat_messages"
        )
        _delete_from_optional_message_pk_targets_table(
            cur, "message_search_terms_rebuild_queue", "temp_delete_chat_messages"
        )
        cur.execute("DELETE FROM dedupe_actions WHERE chat_id = ?", (chat_id,))
        cur.execute("DELETE FROM dedupe_runs WHERE chat_id = ?", (chat_id,))
        cur.execute("DELETE FROM media_groups WHERE chat_id = ?", (chat_id,))
        cur.execute("DELETE FROM message_media WHERE chat_id = ?", (chat_id,))
        cur.execute("DELETE FROM messages WHERE chat_id = ?", (chat_id,))
        cur.execute("DELETE FROM chats WHERE chat_id = ?", (chat_id,))
        if triggers_dropped:
            _restore_message_delete_triggers_after_bulk_delete(
                cur,
                fts_enabled=fts_enabled,
            )
            triggers_dropped = False
        cur.execute("DROP TABLE IF EXISTS temp_delete_chat_messages")
        conn.commit()
        return deleted_messages
    except Exception:
        with suppress(Exception):
            conn.rollback()
        if triggers_dropped:
            with suppress(Exception):
                _restore_message_delete_triggers_after_bulk_delete(
                    cur,
                    fts_enabled=fts_enabled,
                )
                conn.commit()
        raise
    finally:
        with suppress(Exception):
            cur.execute("DROP TABLE IF EXISTS temp_delete_chat_messages")
        cur.close()


@synchronized_write
def _delete_empty_chats_data(conn: Any) -> dict[str, int]:
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        target_count = _prepare_empty_chat_targets(cur)
        if target_count <= 0:
            cur.execute("DROP TABLE IF EXISTS temp_delete_empty_chats")
            conn.commit()
            return {
                "deleted_chats": 0,
                "deleted_messages": 0,
                "deleted_media_rows": 0,
                "deleted_media_groups": 0,
            }

        _delete_from_optional_chat_targets_table(
            cur, "admin_absent_chats", "temp_delete_empty_chats"
        )
        _delete_from_optional_chat_targets_table(
            cur, "admin_missing_chats", "temp_delete_empty_chats"
        )
        _delete_from_optional_chat_targets_table(
            cur, "admin_restricted_chats", "temp_delete_empty_chats"
        )
        cur.execute(
            """
            DELETE FROM dedupe_actions
            WHERE chat_id IN (SELECT chat_id FROM temp_delete_empty_chats)
            """
        )
        cur.execute(
            """
            DELETE FROM dedupe_runs
            WHERE chat_id IN (SELECT chat_id FROM temp_delete_empty_chats)
            """
        )
        cur.execute(
            """
            DELETE FROM media_groups
            WHERE chat_id IN (SELECT chat_id FROM temp_delete_empty_chats)
            """
        )
        deleted_media_groups = int(cur.rowcount or 0)
        cur.execute(
            """
            DELETE FROM message_media
            WHERE chat_id IN (SELECT chat_id FROM temp_delete_empty_chats)
            """
        )
        deleted_media_rows = int(cur.rowcount or 0)
        cur.execute(
            """
            DELETE FROM messages
            WHERE chat_id IN (SELECT chat_id FROM temp_delete_empty_chats)
            """
        )
        deleted_messages = int(cur.rowcount or 0)
        cur.execute(
            """
            DELETE FROM chats
            WHERE chat_id IN (SELECT chat_id FROM temp_delete_empty_chats)
            """
        )
        deleted_chats = int(cur.rowcount or 0)
        cur.execute("DROP TABLE IF EXISTS temp_delete_empty_chats")
        conn.commit()
        return {
            "deleted_chats": deleted_chats,
            "deleted_messages": deleted_messages,
            "deleted_media_rows": deleted_media_rows,
            "deleted_media_groups": deleted_media_groups,
        }
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        with suppress(Exception):
            cur.execute("DROP TABLE IF EXISTS temp_delete_empty_chats")
        cur.close()


def _count_chat_related_rows(conn: Any, chat_id: int) -> dict[str, int]:
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM messages WHERE chat_id = ?", (int(chat_id),))
        messages = int(cur.fetchone()[0] or 0)
        cur.execute(
            "SELECT COUNT(*) FROM message_media WHERE chat_id = ?", (int(chat_id),)
        )
        media_rows = int(cur.fetchone()[0] or 0)
        cur.execute(
            "SELECT COUNT(*) FROM media_groups WHERE chat_id = ?", (int(chat_id),)
        )
        media_groups = int(cur.fetchone()[0] or 0)
        return {
            "messages": messages,
            "media_rows": media_rows,
            "media_groups": media_groups,
        }
    finally:
        cur.close()


def _admin_start_delete_job_thread(job_id, chat_id, chat_title, **kwargs):
    return start_admin_job_thread(
        _admin_delete_job_runner, job_id, chat_id, chat_title, **kwargs
    )


def _admin_start_delete_empty_chats_job_thread(job_id, **kwargs):
    return start_admin_job_thread(_admin_delete_empty_chats_job_runner, job_id, **kwargs)


def _admin_start_cleanup_job_thread(
    job_id, keyword, scope, chat_id, target_label, **kwargs
):
    return start_admin_job_thread(
        _admin_cleanup_job_runner,
        job_id,
        keyword,
        scope,
        chat_id,
        target_label,
        **kwargs,
    )


def _admin_start_cleanup_empty_job_thread(
    job_id, scope, chat_id, target_label, **kwargs
):
    # 修正：必须明确指定 cleanup_mode="empty_media"
    return start_admin_job_thread(
        _admin_cleanup_job_runner,
        job_id,
        "",
        scope,
        chat_id,
        target_label,
        **{**kwargs, "cleanup_mode": "empty_media"},
    )
