# -*- coding: utf-8 -*-
import threading
from queue import Full, Queue
from typing import Any, Callable, Optional

from tg_harvest.admin_jobs.core import job_context, job_log_passthrough_enabled
from tg_harvest.ingest.store import batch_upsert, upsert_chat


class ChatUpdateWriteCoordinator:
    def __init__(
        self,
        *,
        job_id: str,
        get_conn_fn: Callable[[], Any],
        queue_maxsize: int = 0,
    ):
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

    def submit_batch(
        self, *, chat_id: int, msg_rows: list[tuple], media_rows: list[tuple]
    ) -> None:
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
            raise RuntimeError(
                f"写入 chat_id={chat_id} 失败: {state['error']}"
            ) from state["error"]
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
        passthrough_token = None
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
                            skip_postprocess_if_unchanged=bool(
                                item["skip_postprocess_if_unchanged"]
                            ),
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
            if passthrough_token is not None:
                try:
                    job_log_passthrough_enabled.reset(passthrough_token)
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
