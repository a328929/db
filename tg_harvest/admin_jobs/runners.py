# -*- coding: utf-8 -*-
import threading
import logging
import time
from typing import Any, Callable, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Full, Queue

from tg_harvest.storage.schema import refresh_chat_message_counts, synchronized_write
from tg_harvest.admin_jobs.core import (
    job_context,
    job_log_passthrough_enabled,
    _admin_job_heartbeat,
    _admin_job_update_progress,
)
from tg_harvest.admin_jobs.cleanup import (
    _build_cleanup_like_patterns as _build_cleanup_like_patterns_impl,
    _build_cleanup_targets_table as _build_cleanup_targets_table_impl,
    _execute_cleanup_deletion_batches as _execute_cleanup_deletion_batches_impl,
)
from tg_harvest.admin_jobs.sessions import (
    _cleanup_isolated_worker_session,
    _create_isolated_worker_client,
    _ensure_base_session_valid,
    _start_job_heartbeat,
)
from tg_harvest.ingest.store import backfill_message_search_text_from_filenames
from tg_harvest.ingest.store import batch_upsert
from tg_harvest.ingest.store import upsert_chat

# =========================
# 任务运行器逻辑
# =========================


def _build_cleanup_targets_table(cur, *args):
    if len(args) == 4:
        mode, scope_filter_sql, scope_filter_params, like_pattern = args
    elif len(args) == 6:
        _job_id, mode, scope_filter_sql, scope_filter_params, like_pattern, _admin_job_append_log_fn = args
    else:
        raise TypeError("_build_cleanup_targets_table() received unexpected arguments")
    return _build_cleanup_targets_table_impl(
        cur,
        mode,
        scope_filter_sql,
        scope_filter_params,
        like_pattern,
    )


def _execute_cleanup_deletion_batches(conn, cur, job_id, target_count, admin_job_append_log_fn):
    return _execute_cleanup_deletion_batches_impl(
        conn,
        cur,
        job_id,
        target_count,
        admin_job_append_log_fn,
    )


def _admin_error_message(exc: Exception) -> str:
    err_str = str(exc).lower()
    if "channelprivate" in err_str:
        return "您已被踢出该群组，或该群组已转为私有且您不在其中"
    if "userbanned" in err_str:
        return "您的账号已被该群组/频道封禁"
    if "not exist" in err_str or "could not find the input entity" in err_str:
        return "该群组/频道已解散或不存在"
    if "chatrestrictd" in err_str or "chatwriteforbidden" in err_str:
        return "账号被限制或禁言"
    if "floodwait" in err_str:
        return "触发 Telegram 频控限制，请稍后再试"
    return f"{type(exc).__name__}: {exc}"


def _resolve_chat_entity(client: Any, chat_id: int, chat_username: Optional[str] = None) -> Any:
    try:
        return client.get_entity(chat_id)
    except Exception as exc:
        err_msg = str(exc).lower()
        if "could not find the input entity" not in err_msg:
            raise

        fallback_ids = (
            int(f"-100{chat_id}"),
            int(f"-{chat_id}"),
        )
        for fallback_id in fallback_ids:
            try:
                return client.get_entity(fallback_id)
            except Exception:
                pass

        if chat_username:
            return client.get_entity(chat_username)
        raise exc


def _finish_job_heartbeat(heartbeat_stop, heartbeat_thread) -> None:
    heartbeat_stop.set()
    heartbeat_thread.join(timeout=1.0)


def _start_admin_job_thread(target, *args, **kwargs):
    thread = threading.Thread(
        target=target,
        args=args,
        kwargs=kwargs,
        daemon=True,
    )
    thread.start()
    return thread


def _read_chat_username(get_conn_fn: Callable[[], Any], chat_id: int) -> Optional[str]:
    conn = None
    try:
        conn = get_conn_fn()
        cur = conn.cursor()
        cur.execute("SELECT chat_username FROM chats WHERE chat_id = ?", (chat_id,))
        row = cur.fetchone()
        if row is None:
            return None
        username = row["chat_username"]
        return str(username) if username else None
    except Exception:
        return None
    finally:
        if conn:
            conn.close()


class _ChatUpdateWriteCoordinator:
    def __init__(self, *, job_id: str, get_conn_fn: Callable[[], Any], queue_maxsize: int = 0):
        self._job_id = str(job_id)
        self._get_conn_fn = get_conn_fn
        self._queue: Queue[dict[str, Any]] = Queue(maxsize=max(0, int(queue_maxsize)))
        self._states_lock = threading.Lock()
        self._states: dict[int, dict[str, Any]] = {}
        self._error_lock = threading.Lock()
        self._error: Optional[BaseException] = None
        self._thread = threading.Thread(
            target=self._writer_loop,
            name=f"chat-update-writer-{self._job_id[:8]}",
            daemon=True,
        )
        self._thread.start()

    def _set_error(self, exc: BaseException) -> None:
        with self._error_lock:
            if self._error is None:
                self._error = exc
        with self._states_lock:
            for state in self._states.values():
                if state.get("error") is None:
                    state["error"] = exc
                state["done"].set()

    def _raise_if_error(self) -> None:
        with self._error_lock:
            if self._error is not None:
                raise RuntimeError(f"写入线程失败: {self._error}") from self._error

    def register_chat(self, chat_id: int) -> None:
        self._raise_if_error()
        with self._states_lock:
            self._states[int(chat_id)] = {
                "done": threading.Event(),
                "error": None,
            }

    def submit_chat_start(
        self,
        *,
        chat_id: int,
        chat_title: str,
        chat_username: Optional[str],
        chat_type: str,
    ) -> None:
        self._enqueue(
            {
                "kind": "chat_start",
                "chat_id": int(chat_id),
                "chat_title": str(chat_title),
                "chat_username": chat_username,
                "chat_type": str(chat_type),
            }
        )

    def submit_batch(self, *, chat_id: int, msg_rows: list[tuple], media_rows: list[tuple]) -> None:
        self._enqueue(
            {
                "kind": "batch",
                "chat_id": int(chat_id),
                "msg_rows": list(msg_rows),
                "media_rows": list(media_rows),
            }
        )

    def submit_finalize(
        self,
        *,
        chat_id: int,
        chat_title: str,
        counters: Any,
        touched_groups: set[int],
        first_sync: bool,
        total_started_at: float,
        skip_postprocess_if_unchanged: bool,
        enable_dedupe: bool,
    ) -> None:
        self._enqueue(
            {
                "kind": "finalize",
                "chat_id": int(chat_id),
                "chat_title": str(chat_title),
                "counters": counters,
                "touched_groups": set(touched_groups),
                "first_sync": bool(first_sync),
                "total_started_at": float(total_started_at),
                "skip_postprocess_if_unchanged": bool(skip_postprocess_if_unchanged),
                "enable_dedupe": bool(enable_dedupe),
            }
        )

    def wait_for_chat(self, chat_id: int) -> None:
        with self._states_lock:
            state = self._states.get(int(chat_id))
        if state is None:
            raise RuntimeError(f"未注册的 chat_id: {chat_id}")
        state["done"].wait()
        if state.get("error") is not None:
            raise RuntimeError(f"写入 chat_id={chat_id} 失败: {state['error']}") from state["error"]
        self._raise_if_error()

    def close(self) -> None:
        if self._thread.is_alive():
            try:
                self._enqueue({"kind": "stop"}, allow_error=True)
            except RuntimeError:
                pass
            self._thread.join(timeout=5.0)
        self._raise_if_error()

    def _enqueue(self, item: dict[str, Any], *, allow_error: bool = False) -> None:
        while True:
            if not allow_error:
                self._raise_if_error()
            elif not self._thread.is_alive():
                return
            try:
                self._queue.put(item, timeout=0.5)
                return
            except Full:
                self._raise_if_error()
                if not self._thread.is_alive():
                    raise RuntimeError("写入线程已退出，无法继续入队")

    def _mark_done(self, chat_id: int, exc: Optional[BaseException] = None) -> None:
        with self._states_lock:
            state = self._states.get(int(chat_id))
        if state is None:
            return
        if exc is not None and state.get("error") is None:
            state["error"] = exc
        state["done"].set()

    def _writer_loop(self) -> None:
        from tg_harvest.ingest.runner import _finalize_entity_processing

        conn = None
        try:
            job_context.set(self._job_id)
            passthrough_token = job_log_passthrough_enabled.set(False)
            conn = self._get_conn_fn()
            while True:
                item = self._queue.get()
                kind = str(item.get("kind") or "")
                if kind == "stop":
                    break
                chat_id = int(item.get("chat_id") or 0)
                try:
                    if kind == "chat_start":
                        upsert_chat(
                            conn,
                            (
                                chat_id,
                                str(item["chat_title"]),
                                item.get("chat_username"),
                                1 if item.get("chat_username") else 0,
                                str(item["chat_type"]),
                            ),
                        )
                    elif kind == "batch":
                        batch_upsert(conn, item["msg_rows"], item["media_rows"])
                    elif kind == "finalize":
                        _finalize_entity_processing(
                            conn,
                            chat_id=chat_id,
                            chat_title=str(item["chat_title"]),
                            counters=item["counters"],
                            touched_groups=set(item["touched_groups"]),
                            first_sync=bool(item["first_sync"]),
                            total_started_at=float(item["total_started_at"]),
                            skip_postprocess_if_unchanged=bool(item["skip_postprocess_if_unchanged"]),
                            enable_dedupe=bool(item["enable_dedupe"]),
                        )
                        self._mark_done(chat_id)
                except BaseException as exc:
                    if chat_id:
                        self._mark_done(chat_id, exc)
                    self._set_error(exc)
                    break
        except BaseException as exc:
            self._set_error(exc)
        finally:
            try:
                job_log_passthrough_enabled.reset(passthrough_token)
            except Exception:
                pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass


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

    write_coordinator = _ChatUpdateWriteCoordinator(
        job_id=str(job_id),
        get_conn_fn=get_conn_fn,
        queue_maxsize=max(concurrency * 4, 16),
    )

    def _worker(idx, row):
        job_context.set(str(job_id))
        passthrough_token = job_log_passthrough_enabled.set(False)
        current_chat_id = int(row["chat_id"])
        current_chat_title = str(row["chat_title"] or current_chat_id)
        current_chat_username = row["chat_username"]

        import random

        time.sleep(random.uniform(0.1, 0.5))

        local_client = None
        worker_id = f"{job_id}_{idx}"
        try:
            from tg_harvest.ingest.runner import _harvest_messages_for_entity

            local_client = _create_isolated_worker_client(cfg, worker_id)
            before_count = _admin_get_chat_message_count(get_conn_fn, current_chat_id)
            entity = _resolve_chat_entity(
                local_client, current_chat_id, current_chat_username
            )
            entity_title = (
                getattr(entity, "title", None)
                or getattr(entity, "username", None)
                or str(current_chat_id)
            )
            admin_job_append_log_fn(
                job_id, f"[{idx}/{total}] 群组连接成功：名称={entity_title}"
            )
            logging.info(f"[{idx}/{total}] 正在处理: {entity_title} (ID={current_chat_id})")

            write_coordinator.register_chat(current_chat_id)
            write_coordinator.submit_chat_start(
                chat_id=current_chat_id,
                chat_title=getattr(entity, "title", None) or current_chat_title,
                chat_username=getattr(entity, "username", None),
                chat_type=entity.__class__.__name__,
            )

            total_started_at = time.perf_counter()
            read_conn = get_conn_fn()
            try:
                counters, touched_groups, first_sync = _harvest_messages_for_entity(
                    read_conn,
                    local_client,
                    entity,
                    current_chat_id,
                    write_batch_fn=lambda msg_rows, media_rows: write_coordinator.submit_batch(
                        chat_id=current_chat_id,
                        msg_rows=msg_rows,
                        media_rows=media_rows,
                    ),
                )
            finally:
                read_conn.close()

            write_coordinator.submit_finalize(
                chat_id=current_chat_id,
                chat_title=getattr(entity, "title", None) or current_chat_title,
                counters=counters,
                touched_groups=touched_groups,
                first_sync=first_sync,
                total_started_at=total_started_at,
                skip_postprocess_if_unchanged=True,
                enable_dedupe=False,
            )
            write_coordinator.wait_for_chat(current_chat_id)
            after_count = _admin_get_chat_message_count(get_conn_fn, current_chat_id)
            added_count = max(0, after_count - before_count)
            return current_chat_title, current_chat_id, True, added_count, None
        except Exception as chat_exc:
            logging.exception(f"Worker 执行群组 {current_chat_title} 失败")
            user_msg = _admin_error_message(chat_exc)
            return (
                current_chat_title,
                current_chat_id,
                False,
                0,
                user_msg,
            )
        finally:
            job_log_passthrough_enabled.reset(passthrough_token)
            if local_client:
                try:
                    local_client.disconnect()
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
                    if success:
                        total_added_messages += added
                        success_count += 1
                        admin_job_append_log_fn(
                            job_id, f"[{idx}/{total}] {chat_title} 新增 {added} 条消息"
                        )
                    else:
                        failed_count += 1
                        failed_chats.append(f"{chat_title}({err_msg})")
                        admin_job_append_log_fn(
                            job_id,
                            f"[{idx}/{total}] 增量采集失败：群组={chat_title}，错误={err_msg}",
                        )
                except Exception as e:
                    failed_count += 1
                    admin_job_append_log_fn(job_id, f"[{idx}/{total}] 线程执行异常：{e}")
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

    final_log_msg = f"全部群组增量采集完成：成功 {success_count} 个，失败 {failed_count} 个，总计 {total} 个，共新增 {total_added_messages} 条消息"
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
    from tg_harvest.ingest.runner import _process_entity

    entity = _resolve_chat_entity(client, chat_id, chat_username)

    entity_title = (
        getattr(entity, "title", None)
        or getattr(entity, "username", None)
        or str(chat_id)
    )
    admin_job_append_log_fn(
        job_id, f"[{idx}/{total}] 群组连接成功：名称={entity_title}"
    )
    conn = get_conn_fn()
    try:
        _admin_job_update_progress(
            job_id,
            0,
            total=1,
            stage="updating",
            log_step=0,
            auto_log=False,
        )
        _process_entity(
            conn,
            client,
            entity,
            idx=idx,
            total=total,
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
    finally:
        conn.close()


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
        from tg_harvest.ingest.runner import _process_entity

        for idx, entity in enumerate(entities, start=1):
            entity_title = (
                getattr(entity, "title", None)
                or getattr(entity, "username", None)
                or str(target)
            )
            admin_job_append_log_fn(
                job_id, f"[{idx}/{total}] 导入目标：名称={entity_title}"
            )
            conn = get_conn_fn()
            try:
                _process_entity(conn, local_client, entity, idx=idx, total=total)
            finally:
                conn.close()
        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        user_msg = _admin_error_message(exc)
        admin_job_append_log_fn(job_id, f"新增数据采集失败：{user_msg}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        _finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        if local_client:
            try:
                local_client.disconnect()
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
            chat_username = _read_chat_username(get_conn_fn, int(chat_id))
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
        user_msg = _admin_error_message(exc)
        admin_job_append_log_fn(job_id, f"采集失败：{user_msg}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        _finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        if local_client:
            try:
                local_client.disconnect()
            except Exception:
                pass
        _cleanup_isolated_worker_session(cfg, worker_id)
        root_logger.removeHandler(job_log_handler)


@synchronized_write
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
        cur = conn.cursor()
        try:
            admin_job_append_log_fn(job_id, "清理关联数据...")
            cur.execute("DELETE FROM dedupe_actions WHERE chat_id = ?", (chat_id,))
            cur.execute("DELETE FROM dedupe_runs WHERE chat_id = ?", (chat_id,))
            cur.execute("DELETE FROM media_groups WHERE chat_id = ?", (chat_id,))
            cur.execute("DELETE FROM message_media WHERE chat_id = ?", (chat_id,))
            cur.execute("DELETE FROM messages WHERE chat_id = ?", (chat_id,))
            deleted_messages = int(cur.rowcount or 0)
            cur.execute("DELETE FROM chats WHERE chat_id = ?", (chat_id,))
            conn.commit()
            admin_job_append_log_fn(
                job_id, f"删除完成：共清除 {deleted_messages} 条消息"
            )
            admin_job_set_status_fn(job_id, "done")
        finally:
            cur.close()
    except Exception as exc:
        if conn:
            conn.rollback()
        admin_job_append_log_fn(job_id, f"删除失败：{exc}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        _finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        if conn:
            conn.close()


@synchronized_write
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

        like_pattern = _build_cleanup_like_patterns_impl(keyword)

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
                log_fn=admin_job_append_log_fn,
            )
            if synced > 0:
                admin_job_append_log_fn(
                    job_id, f"已先补齐 {synced} 条可搜索文件名文本，避免误删"
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
        _finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        if conn:
            conn.close()


def _admin_start_harvest_job_thread(job_id, target, **kwargs):
    return _start_admin_job_thread(_admin_harvest_job_runner, job_id, target, **kwargs)


def _admin_start_update_job_thread(job_id, chat_id, chat_title, **kwargs):
    return _start_admin_job_thread(
        _admin_update_job_runner, job_id, chat_id, chat_title, **kwargs
    )


def _admin_start_delete_job_thread(job_id, chat_id, chat_title, **kwargs):
    return _start_admin_job_thread(
        _admin_delete_job_runner, job_id, chat_id, chat_title, **kwargs
    )


def _admin_start_cleanup_job_thread(
    job_id, keyword, scope, chat_id, target_label, **kwargs
):
    return _start_admin_job_thread(
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
    return _start_admin_job_thread(
        _admin_cleanup_job_runner,
        job_id,
        "",
        scope,
        chat_id,
        target_label,
        **{**kwargs, "cleanup_mode": "empty_media"},
    )
