# -*- coding: utf-8 -*-
import logging
import time
from typing import Any, Callable, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from tg_harvest.storage.connection import synchronized_write
from tg_harvest.storage.schema import refresh_chat_message_counts
from tg_harvest.admin_jobs.core import (
    job_context,
    job_log_passthrough_enabled,
    _admin_job_heartbeat,
    _admin_job_update_progress,
)
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
from tg_harvest.admin_jobs.sessions import (
    _cleanup_isolated_worker_session,
    _create_isolated_worker_client,
    _disconnect_worker_client,
    _ensure_base_session_valid,
    _start_job_heartbeat,
)
from tg_harvest.admin_jobs.streaming import stream_entity_harvest_to_writer
from tg_harvest.admin_jobs.update_writer import ChatUpdateWriteCoordinator
from tg_harvest.ingest.store import backfill_message_search_text_from_filenames


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

    total = len(rows)
    success_count, failed_count, total_added_messages = 0, 0, 0
    failed_chats = []

    concurrency = getattr(cfg, "admin_update_concurrency", 5)
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

    if not _ensure_base_session_valid(cfg, job_id, admin_job_append_log_fn):
        return False

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
                try:
                    _disconnect_worker_client(local_client)
                except Exception:
                    pass
            _cleanup_isolated_worker_session(cfg, worker_id)

    try:
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {
                executor.submit(_worker, idx, row): (idx, row)
                for idx, row in enumerate(rows, start=1)
            }
            for future in as_completed(futures):
                idx, row = futures[future]
                try:
                    chat_title, chat_id, success, added, err_msg = future.result()
                    chat_label = _chat_log_label(chat_id, chat_title)
                    if success:
                        total_added_messages += added
                        success_count += 1
                        admin_job_append_log_fn(
                            job_id, f"[{idx}/{total}] {chat_label} 新增 {added} 条消息"
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

    final_log_msg = (
        f"全部群组增量采集完成：成功 {success_count} 个，失败 {failed_count} 个，"
        f"总计 {total} 个，共新增 {total_added_messages} 条消息"
    )
    if failed_chats:
        final_log_msg += f"。失败列表：{', '.join(failed_chats)}"
    admin_job_append_log_fn(job_id, final_log_msg)
    _admin_job_update_progress(
        job_id,
        total,
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
    chat_username: Optional[str] = None,
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
            "SELECT COUNT(*) AS cnt FROM messages WHERE chat_id = ?", (chat_id,)
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
            try:
                _disconnect_worker_client(local_client)
            except Exception:
                pass
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
            try:
                _disconnect_worker_client(local_client)
            except Exception:
                pass
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


def _admin_cleanup_job_runner(
    job_id: str,
    keyword: str,
    scope: str,
    chat_id: Optional[int],
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


def _delete_from_optional_chat_table(cur: Any, table_name: str, chat_id: int) -> None:
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    if cur.fetchone() is None:
        return
    cur.execute(f"DELETE FROM {table_name} WHERE chat_id = ?", (chat_id,))


def _delete_from_optional_message_pk_table(
    cur: Any, table_name: str, chat_id: int
) -> None:
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    if cur.fetchone() is None:
        return
    cur.execute(
        f"""
        DELETE FROM {table_name}
        WHERE pk IN (
            SELECT pk
            FROM messages
            WHERE chat_id = ?
        )
        """,
        (chat_id,),
    )


@synchronized_write
def _delete_chat_data(conn: Any, chat_id: int) -> int:
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        _delete_from_optional_chat_table(cur, "admin_absent_chats", chat_id)
        _delete_from_optional_chat_table(cur, "admin_missing_chats", chat_id)
        _delete_from_optional_message_pk_table(cur, "message_search_terms", chat_id)
        _delete_from_optional_message_pk_table(
            cur, "message_search_terms_rebuild_queue", chat_id
        )
        cur.execute("DELETE FROM dedupe_actions WHERE chat_id = ?", (chat_id,))
        cur.execute("DELETE FROM dedupe_runs WHERE chat_id = ?", (chat_id,))
        cur.execute("DELETE FROM media_groups WHERE chat_id = ?", (chat_id,))
        cur.execute("DELETE FROM message_media WHERE chat_id = ?", (chat_id,))
        cur.execute("DELETE FROM messages WHERE chat_id = ?", (chat_id,))
        deleted_messages = int(cur.rowcount or 0)
        cur.execute("DELETE FROM chats WHERE chat_id = ?", (chat_id,))
        conn.commit()
        return deleted_messages
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        cur.close()


def _admin_start_delete_job_thread(job_id, chat_id, chat_title, **kwargs):
    return start_admin_job_thread(
        _admin_delete_job_runner, job_id, chat_id, chat_title, **kwargs
    )


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
