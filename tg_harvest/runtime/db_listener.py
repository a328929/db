import logging
import threading
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from queue import Empty, Queue
from typing import Any

from telethon import events

from tg_harvest.admin_jobs.common import admin_error_message, resolve_chat_entity
from tg_harvest.admin_jobs.runners import (
    _account_cooldown_remaining,
    _cfg_with_session_name,
    _create_isolated_worker_client,
    _ensure_base_session_valid,
    _read_session_cached_chat_ids,
    _remember_account_cooldown,
    _admin_process_single_chat_update,
)
from tg_harvest.admin_jobs.sessions import (
    _cleanup_isolated_worker_session,
    _create_isolated_worker_client_with_options,
    _disconnect_worker_client,
)
from tg_harvest.domain.chat_inventory import load_joined_chat_inventory
from tg_harvest.domain.chat_ids import stored_chat_id_from_entity_id
from tg_harvest.domain.coerce import clean_username, enabled_int, optional_int
from tg_harvest.ingest.flood_wait import AccountFloodWaitError
from tg_harvest.ingest.range_harvest import read_latest_message_id
from tg_harvest.ingest.store import get_last_message_id

_DEFAULT_EVENT_IDLE_SLEEP_SECONDS = 1.0
_DEFAULT_MISSED_CHAT_COOLDOWN_SECONDS = 600.0
_DEFAULT_QUEUE_MAXSIZE = 4096
_DEFAULT_PROBE_EVENT_REASON = "public_probe"
_DEFAULT_JOINED_SNAPSHOT_REFRESH_SECONDS = 1800.0
_DEFAULT_HOT_PROBE_SLOT_RATIO = 0.75
_DEFAULT_PUBLIC_PROBE_ACCOUNT_GAP_SECONDS = 6.0

_LISTENER_SINGLETON: "DatabaseChatListenerRuntime | None" = None
_LISTENER_SINGLETON_LOCK = threading.Lock()


def _listener_log(job_id: str, message: str) -> None:
    logging.info("[db-listener:%s] %s", job_id, message)


def _safe_int(value: Any) -> int:
    parsed = optional_int(value)
    return int(parsed or 0)


def _safe_timestamp_text_sort_key(value: Any) -> str:
    return str(value or "").strip()


def _load_database_chat_rows(conn: Any) -> list[dict[str, Any]]:
    cur = conn.cursor()
    try:
        has_messages = False
        try:
            cur.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'messages'"
            )
            has_messages = cur.fetchone() is not None
        except Exception:
            has_messages = False

        if has_messages:
            cur.execute(
                """
                SELECT
                    c.chat_id,
                    c.chat_title,
                    c.chat_username,
                    c.last_seen_at,
                    COALESCE(lm.message_id, 0) AS last_message_id
                FROM chats c
                LEFT JOIN messages lm
                  ON lm.chat_id = c.chat_id
                 AND lm.message_id = (
                        SELECT m.message_id
                        FROM messages m
                        WHERE m.chat_id = c.chat_id
                        ORDER BY m.msg_date_ts DESC, m.message_id DESC
                        LIMIT 1
                    )
                ORDER BY
                    CASE WHEN lm.msg_date_ts IS NULL THEN 1 ELSE 0 END ASC,
                    lm.msg_date_ts DESC,
                    c.last_seen_at DESC,
                    c.chat_id ASC
                """
            )
        else:
            cur.execute(
                """
                SELECT
                    c.chat_id,
                    c.chat_title,
                    c.chat_username,
                    c.last_seen_at,
                    0 AS last_message_id
                FROM chats c
                ORDER BY c.last_seen_at DESC, c.chat_id ASC
                """
            )
        rows = cur.fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            chat_id = _safe_int(row["chat_id"])
            if chat_id <= 0:
                continue
            result.append(
                {
                    "chat_id": chat_id,
                    "chat_title": str(row["chat_title"] or "").strip(),
                    "chat_username": clean_username(row["chat_username"]),
                    "last_seen_at": str(row["last_seen_at"] or "").strip(),
                    "last_message_id": _safe_int(row["last_message_id"]),
                }
            )
        return result
    finally:
        close = getattr(cur, "close", None)
        if callable(close):
            close()


def _load_database_chat_ids(conn: Any) -> set[int]:
    return {int(row["chat_id"]) for row in _load_database_chat_rows(conn)}


@dataclass(frozen=True)
class _QueuedChatUpdate:
    chat_id: int
    chat_title: str
    chat_username: str | None
    reason: str
    source_account: str


@dataclass(frozen=True)
class _ListenerAccount:
    key: str
    label: str
    cfg: Any
    session_name: str


@dataclass(frozen=True)
class _PublicProbeOutcome:
    status: str
    cooldown_seconds: int


class DatabaseChatListenerRuntime:
    def __init__(
        self,
        *,
        cfg: Any,
        get_conn_fn: Callable[[], Any],
    ) -> None:
        self._cfg = cfg
        self._get_conn_fn = get_conn_fn
        self._queue: Queue[_QueuedChatUpdate] = Queue(maxsize=_DEFAULT_QUEUE_MAXSIZE)
        self._queued_chat_ids_lock = threading.Lock()
        self._queued_chat_ids: set[int] = set()
        self._db_chat_rows_lock = threading.Lock()
        self._db_chat_rows_by_id: dict[int, dict[str, Any]] = {}
        self._joined_chat_ids_lock = threading.Lock()
        self._joined_chat_ids_by_account: dict[str, set[int]] = {}
        self._joined_chat_snapshot_updated_at = 0.0
        self._missed_chat_until_lock = threading.Lock()
        self._missed_chat_until: dict[int, float] = {}
        self._public_probe_next_allowed_lock = threading.Lock()
        self._public_probe_next_allowed: dict[int, float] = {}
        self._public_probe_account_next_allowed_lock = threading.Lock()
        self._public_probe_account_next_allowed: dict[str, float] = {}
        self._public_probe_cursor_lock = threading.Lock()
        self._public_probe_cursor = 0
        self._listener_clients_lock = threading.Lock()
        self._listener_clients: dict[str, Any] = {}
        self._worker_stop = threading.Event()
        self._watcher_stop = threading.Event()
        self._worker_thread = threading.Thread(
            target=self._worker_loop,
            name="db-chat-listener-worker",
            daemon=True,
        )
        self._refresh_thread = threading.Thread(
            target=self._refresh_loop,
            name="db-chat-listener-refresh",
            daemon=True,
        )
        self._public_probe_thread = threading.Thread(
            target=self._public_probe_loop,
            name="db-chat-listener-public-probe",
            daemon=True,
        )
        self._listener_threads: list[threading.Thread] = []
        self._started = False
        self._job_id = "db-listener"

    def start(self) -> None:
        if self._started:
            return
        if enabled_int(getattr(self._cfg, "db_listener_enabled", 1)) != 1:
            _listener_log(self._job_id, "数据库内群组监听已关闭")
            self._started = True
            return
        self._refresh_database_chat_cache()
        self._refresh_joined_chat_snapshot()
        self._started = True
        self._worker_thread.start()
        self._refresh_thread.start()
        if enabled_int(getattr(self._cfg, "db_listener_public_probe_enabled", 1)) == 1:
            self._public_probe_thread.start()
        self._start_listener_threads()
        _listener_log(
            self._job_id,
            f"数据库内群组监听已启动，当前追踪 {len(self._db_chat_rows_by_id)} 个已入库群组/频道",
        )

    def stop(self) -> None:
        self._worker_stop.set()
        self._watcher_stop.set()
        with self._listener_clients_lock:
            active_clients = list(self._listener_clients.values())
        for client in active_clients:
            with suppress(Exception):
                _disconnect_worker_client(client)
        self._worker_thread.join(timeout=2.0)
        self._refresh_thread.join(timeout=2.0)
        if self._public_probe_thread.is_alive():
            self._public_probe_thread.join(timeout=2.0)
        for thread in self._listener_threads:
            thread.join(timeout=2.0)

    def _remember_listener_client(self, account_key: str, client: Any | None) -> None:
        with self._listener_clients_lock:
            if client is None:
                self._listener_clients.pop(str(account_key), None)
            else:
                self._listener_clients[str(account_key)] = client

    def _refresh_database_chat_cache(self) -> None:
        conn = None
        try:
            conn = self._get_conn_fn()
            rows = _load_database_chat_rows(conn)
        except Exception:
            logging.exception("刷新数据库监听群组缓存失败")
            return
        finally:
            if conn is not None:
                with suppress(Exception):
                    conn.close()
        with self._db_chat_rows_lock:
            self._db_chat_rows_by_id = {
                int(row["chat_id"]): dict(row) for row in rows if int(row["chat_id"]) > 0
            }

    def _load_joined_chat_ids_for_account(self, account: _ListenerAccount) -> set[int]:
        worker_id = f"{self._job_id}_{account.key}_joined_snapshot"
        client = None
        try:
            if not _ensure_base_session_valid(account.cfg, self._job_id, _listener_log):
                return set()
            client = _create_isolated_worker_client(account.cfg, worker_id)
            joined_rows = load_joined_chat_inventory(client.iter_dialogs())
            return {
                int(row.chat_id)
                for row in joined_rows
                if int(row.chat_id) > 0 and not str(row.unavailable_reason or "").strip()
            }
        except Exception:
            logging.exception("刷新已加入群组快照失败: account=%s", account.key)
            return set()
        finally:
            if client is not None:
                with suppress(Exception):
                    _disconnect_worker_client(client)
            _cleanup_isolated_worker_session(account.cfg, worker_id)

    def _refresh_joined_chat_snapshot(self) -> None:
        snapshot: dict[str, set[int]] = {}
        for account in self._listener_accounts():
            snapshot[account.key] = self._load_joined_chat_ids_for_account(account)
        with self._joined_chat_ids_lock:
            self._joined_chat_ids_by_account = {
                str(account_key): set(chat_ids)
                for account_key, chat_ids in snapshot.items()
            }
            self._joined_chat_snapshot_updated_at = time.time()

    def _database_chat_rows(self) -> list[dict[str, Any]]:
        with self._db_chat_rows_lock:
            return [dict(item) for item in self._db_chat_rows_by_id.values()]

    def _database_chat_row(self, chat_id: int) -> dict[str, Any] | None:
        with self._db_chat_rows_lock:
            row = self._db_chat_rows_by_id.get(int(chat_id))
            return dict(row) if row is not None else None

    def _database_chat_ids(self) -> set[int]:
        with self._db_chat_rows_lock:
            return set(self._db_chat_rows_by_id)

    def _joined_chat_ids_by_account_snapshot(self) -> dict[str, set[int]]:
        with self._joined_chat_ids_lock:
            return {
                str(account_key): set(chat_ids)
                for account_key, chat_ids in self._joined_chat_ids_by_account.items()
            }

    def _set_joined_chat_ids_for_account(
        self, account_key: str, joined_chat_ids: set[int]
    ) -> None:
        with self._joined_chat_ids_lock:
            self._joined_chat_ids_by_account[str(account_key)] = {
                int(chat_id) for chat_id in joined_chat_ids if int(chat_id) > 0
            }
            self._joined_chat_snapshot_updated_at = time.time()

    def _is_database_chat(self, chat_id: int) -> bool:
        if chat_id <= 0:
            return False
        with self._db_chat_rows_lock:
            return int(chat_id) in self._db_chat_rows_by_id

    def _is_chat_temporarily_suppressed(self, chat_id: int) -> bool:
        now = time.time()
        with self._missed_chat_until_lock:
            expires_at = self._missed_chat_until.get(int(chat_id), 0.0)
            if expires_at <= now:
                self._missed_chat_until.pop(int(chat_id), None)
                return False
            return True

    def _suppress_chat_temporarily(self, chat_id: int, *, seconds: int) -> None:
        until = time.time() + max(1, int(seconds))
        with self._missed_chat_until_lock:
            self._missed_chat_until[int(chat_id)] = until

    def _set_public_probe_cooldown(self, chat_id: int, *, seconds: int) -> None:
        until = time.time() + max(1, int(seconds))
        with self._public_probe_next_allowed_lock:
            self._public_probe_next_allowed[int(chat_id)] = until

    def _public_probe_is_due(self, chat_id: int) -> bool:
        now = time.time()
        with self._public_probe_next_allowed_lock:
            expires_at = self._public_probe_next_allowed.get(int(chat_id), 0.0)
            if expires_at <= now:
                self._public_probe_next_allowed.pop(int(chat_id), None)
                return True
            return False

    def _public_probe_has_pending_updates(self) -> bool:
        with self._queued_chat_ids_lock:
            return bool(self._queued_chat_ids)

    def _wait_for_public_probe_account_slot(self, account_key: str) -> None:
        gap_seconds = float(_DEFAULT_PUBLIC_PROBE_ACCOUNT_GAP_SECONDS)
        if gap_seconds <= 0:
            return
        wait_seconds = 0.0
        with self._public_probe_account_next_allowed_lock:
            now = time.time()
            next_allowed_at = float(
                self._public_probe_account_next_allowed.get(str(account_key), 0.0)
            )
            wait_seconds = max(0.0, next_allowed_at - now)
            self._public_probe_account_next_allowed[str(account_key)] = max(
                next_allowed_at,
                now,
            ) + gap_seconds
        if wait_seconds > 0:
            time.sleep(wait_seconds)

    def _enqueue_chat_update(
        self,
        *,
        chat_id: int,
        chat_title: str,
        chat_username: str | None,
        reason: str,
        source_account: str,
    ) -> None:
        safe_chat_id = int(chat_id)
        if safe_chat_id <= 0:
            return
        if not self._is_database_chat(safe_chat_id):
            return
        if self._is_chat_temporarily_suppressed(safe_chat_id):
            return
        with self._queued_chat_ids_lock:
            if safe_chat_id in self._queued_chat_ids:
                return
            self._queued_chat_ids.add(safe_chat_id)
        try:
            self._queue.put_nowait(
                _QueuedChatUpdate(
                    chat_id=safe_chat_id,
                    chat_title=str(chat_title or "").strip() or f"Chat {safe_chat_id}",
                    chat_username=clean_username(chat_username) or None,
                    reason=str(reason or "").strip() or "event",
                    source_account=str(source_account or "").strip() or "primary",
                )
            )
        except Exception:
            with self._queued_chat_ids_lock:
                self._queued_chat_ids.discard(safe_chat_id)
            logging.exception("数据库监听更新队列入队失败: chat_id=%s", safe_chat_id)

    def _listener_accounts(self) -> list[_ListenerAccount]:
        accounts = [
            _ListenerAccount(
                key="primary",
                label="主账号",
                cfg=self._cfg,
                session_name=str(getattr(self._cfg, "session_name", "") or "").strip(),
            )
        ]
        secondary_session_name = str(
            getattr(self._cfg, "secondary_session_name", "") or ""
        ).strip()
        primary_session_name = accounts[0].session_name
        if secondary_session_name and secondary_session_name != primary_session_name:
            secondary_cfg = _cfg_with_session_name(self._cfg, secondary_session_name)
            accounts.append(
                _ListenerAccount(
                    key="secondary",
                    label="第二账号",
                    cfg=secondary_cfg,
                    session_name=secondary_session_name,
                )
            )
        return accounts

    def _start_listener_threads(self) -> None:
        for account in self._listener_accounts():
            if not _ensure_base_session_valid(account.cfg, self._job_id, _listener_log):
                continue
            thread = threading.Thread(
                target=self._listener_account_loop,
                kwargs={"account": account},
                name=f"db-chat-listener-{account.key}",
                daemon=True,
            )
            self._listener_threads.append(thread)
            thread.start()

    def _listener_account_loop(self, *, account: _ListenerAccount) -> None:
        worker_id = f"{self._job_id}_{account.key}_updates"
        client = None
        try:
            while not self._watcher_stop.is_set():
                try:
                    if client is None:
                        client = _create_isolated_worker_client_with_options(
                            account.cfg,
                            worker_id=worker_id,
                            receive_updates=True,
                        )
                        self._remember_listener_client(account.key, client)
                        self._register_client_event_handlers(client, account_key=account.key)
                        _listener_log(self._job_id, f"{account.label} 监听线程已连接")
                    client.run_until_disconnected()
                    if self._watcher_stop.is_set():
                        break
                    _listener_log(
                        self._job_id,
                        f"{account.label} 监听连接已断开，5 秒后尝试重连",
                    )
                except Exception as exc:
                    if self._watcher_stop.is_set():
                        break
                    logging.warning(
                        "数据库监听账号线程异常: account=%s error=%s",
                        account.key,
                        exc,
                    )
                if self._watcher_stop.wait(5.0):
                    break
                if client is not None:
                    with suppress(Exception):
                        _disconnect_worker_client(client)
                    self._remember_listener_client(account.key, None)
                    _cleanup_isolated_worker_session(account.cfg, worker_id)
                    client = None
        except Exception:
            logging.exception("数据库监听账号线程异常退出: account=%s", account.key)
        finally:
            self._remember_listener_client(account.key, None)
            if client is not None:
                with suppress(Exception):
                    _disconnect_worker_client(client)
            _cleanup_isolated_worker_session(account.cfg, worker_id)

    def _refresh_loop(self) -> None:
        refresh_seconds = max(
            30, int(getattr(self._cfg, "db_listener_refresh_seconds", 120) or 120)
        )
        joined_snapshot_refresh_seconds = max(
            refresh_seconds,
            _DEFAULT_JOINED_SNAPSHOT_REFRESH_SECONDS,
        )
        last_joined_snapshot_refresh_at = 0.0
        while not self._watcher_stop.wait(float(refresh_seconds)):
            self._refresh_database_chat_cache()
            now = time.time()
            if now - last_joined_snapshot_refresh_at >= joined_snapshot_refresh_seconds:
                self._refresh_joined_chat_snapshot()
                last_joined_snapshot_refresh_at = now

    def _register_client_event_handlers(self, client: Any, *, account_key: str) -> None:
        async def _on_new_message(event: Any) -> None:
            self._handle_message_event(event, reason="new_message", account_key=account_key)

        async def _on_message_edited(event: Any) -> None:
            self._handle_message_event(
                event,
                reason="message_edited",
                account_key=account_key,
            )

        async def _on_message_deleted(event: Any) -> None:
            self._handle_message_event(
                event,
                reason="message_deleted",
                account_key=account_key,
                allow_missing_message=True,
            )

        client.add_event_handler(_on_new_message, events.NewMessage)
        client.add_event_handler(_on_message_edited, events.MessageEdited)
        client.add_event_handler(_on_message_deleted, events.MessageDeleted)

    def _handle_message_event(
        self,
        event: Any,
        *,
        reason: str,
        account_key: str,
        allow_missing_message: bool = False,
    ) -> None:
        try:
            chat_id = self._event_chat_id(event)
            if chat_id <= 0 or not self._is_database_chat(chat_id):
                return
            message = getattr(event, "message", None)
            chat_title = ""
            chat_username = None
            if message is not None:
                chat = getattr(event, "chat", None)
                chat_title = str(getattr(chat, "title", "") or "").strip()
                chat_username = clean_username(getattr(chat, "username", None))
            elif not allow_missing_message:
                return
            row = self._database_chat_row(chat_id) or {}
            self._enqueue_chat_update(
                chat_id=chat_id,
                chat_title=chat_title or str(row.get("chat_title") or ""),
                chat_username=chat_username or row.get("chat_username"),
                reason=reason,
                source_account=account_key,
            )
        except Exception:
            logging.exception(
                "数据库监听事件处理失败: reason=%s account=%s",
                reason,
                account_key,
            )

    def _event_chat_id(self, event: Any) -> int:
        chat_id = _safe_int(getattr(event, "chat_id", None))
        if chat_id:
            return stored_chat_id_from_entity_id(chat_id)
        message = getattr(event, "message", None)
        peer_id = getattr(message, "peer_id", None)
        peer_channel_id = _safe_int(getattr(peer_id, "channel_id", None))
        if peer_channel_id:
            return stored_chat_id_from_entity_id(peer_channel_id)
        peer_chat_id = _safe_int(getattr(peer_id, "chat_id", None))
        if peer_chat_id:
            return stored_chat_id_from_entity_id(peer_chat_id)
        return 0

    def _worker_loop(self) -> None:
        while not self._worker_stop.is_set():
            try:
                item = self._queue.get(timeout=_DEFAULT_EVENT_IDLE_SLEEP_SECONDS)
            except Empty:
                continue
            try:
                self._process_queued_chat_update(item)
            finally:
                with self._queued_chat_ids_lock:
                    self._queued_chat_ids.discard(int(item.chat_id))

    def _account_priority_for_item(self, item: _QueuedChatUpdate) -> list[_ListenerAccount]:
        accounts = self._listener_accounts()
        preferred_key = str(item.source_account or "").strip()
        if preferred_key:
            accounts.sort(key=lambda account: 0 if account.key == preferred_key else 1)
        return accounts

    def _attempt_single_chat_update(
        self,
        *,
        account: _ListenerAccount,
        item: _QueuedChatUpdate,
    ) -> None:
        worker_id = f"{self._job_id}_{account.key}_single_{item.chat_id}"
        client = None
        try:
            client = _create_isolated_worker_client(account.cfg, worker_id)
            _admin_process_single_chat_update(
                job_id=self._job_id,
                client=client,
                cfg=account.cfg,
                get_conn_fn=self._get_conn_fn,
                admin_job_append_log_fn=_listener_log,
                chat_id=item.chat_id,
                chat_title=item.chat_title,
                chat_username=item.chat_username,
                idx=1,
                total=1,
                account_label=account.label,
                enable_progress_probe=False,
            )
        finally:
            if client is not None:
                with suppress(Exception):
                    _disconnect_worker_client(client)
            _cleanup_isolated_worker_session(account.cfg, worker_id)

    def _process_queued_chat_update(self, item: _QueuedChatUpdate) -> None:
        if not self._is_database_chat(item.chat_id):
            return
        accounts = self._account_priority_for_item(item)
        last_exc: Exception | None = None
        tried_any = False
        for account in accounts:
            if _account_cooldown_remaining(account) > 0:
                continue
            if not _ensure_base_session_valid(account.cfg, self._job_id, _listener_log):
                continue
            tried_any = True
            try:
                self._attempt_single_chat_update(account=account, item=item)
                return
            except Exception as exc:
                last_exc = exc
                if isinstance(exc, AccountFloodWaitError):
                    _remember_account_cooldown(account, exc)
                    continue
                message = admin_error_message(exc)
                if (
                    "本地实体缓存未命中" in message
                    or "不存在" in message
                    or "解散" in message
                ):
                    continue

        if isinstance(last_exc, AccountFloodWaitError):
            self._suppress_chat_temporarily(
                item.chat_id,
                seconds=max(60, int(last_exc.seconds)),
            )
            return

        if last_exc is None and not tried_any:
            self._suppress_chat_temporarily(item.chat_id, seconds=120)
            return

        if last_exc is not None:
            message = admin_error_message(last_exc)
            if (
                "本地实体缓存未命中" in message
                or "不存在" in message
                or "解散" in message
            ):
                self._suppress_chat_temporarily(
                    item.chat_id,
                    seconds=_DEFAULT_MISSED_CHAT_COOLDOWN_SECONDS,
                )
            logging.warning(
                "数据库监听增量更新失败: chat_id=%s reason=%s source_account=%s error=%s",
                item.chat_id,
                item.reason,
                item.source_account,
                message,
            )

    def _public_probe_candidate_rows(self) -> list[dict[str, Any]]:
        rows = self._database_chat_rows()
        if not rows:
            return []

        joined_by_account = self._joined_chat_ids_by_account_snapshot()
        use_joined_snapshot = any(joined_by_account.values())
        if not use_joined_snapshot:
            # 启动早期或异常场景下退回到 session 实体缓存近似判定，避免完全失效。
            joined_by_account = {
                account.key: _read_session_cached_chat_ids(account.session_name)
                for account in self._listener_accounts()
            }

        public_only: list[dict[str, Any]] = []
        for row in rows:
            chat_id = int(row["chat_id"])
            chat_username = clean_username(row.get("chat_username"))
            if not chat_username:
                continue
            if use_joined_snapshot:
                # 至少一个账号真实出现在 dialogs 中，说明它更适合交给事件监听。
                if any(chat_id in joined for joined in joined_by_account.values()):
                    continue
            else:
                # 仅在 joined 快照还不可用时，才用 session entities 作为保守近似。
                if any(chat_id in cached for cached in joined_by_account.values()):
                    continue
            public_only.append(dict(row))
        return public_only

    def _public_probe_hot_row_sort_key(self, row: dict[str, Any]) -> tuple[Any, ...]:
        return (
            -max(0, int(row.get("last_message_id", 0) or 0)),
            _safe_timestamp_text_sort_key(row.get("last_seen_at")),
            str(row.get("chat_title") or "").casefold(),
            int(row.get("chat_id") or 0),
        )

    def _split_public_probe_rows(
        self, rows: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        if not rows:
            return [], []
        sorted_rows = sorted(
            (dict(row) for row in rows),
            key=self._public_probe_hot_row_sort_key,
        )
        if len(sorted_rows) <= 2:
            return sorted_rows, []
        hot_count = max(1, (len(sorted_rows) + 1) // 2)
        return sorted_rows[:hot_count], sorted_rows[hot_count:]

    def _rotated_due_rows(
        self,
        rows: list[dict[str, Any]],
        *,
        start_index: int,
        limit: int,
    ) -> tuple[list[dict[str, Any]], int]:
        if not rows or limit <= 0:
            return [], start_index
        total = len(rows)
        due_rows: list[dict[str, Any]] = []
        inspected = 0
        index = start_index % total
        while inspected < total and len(due_rows) < limit:
            row = rows[index]
            chat_id = int(row["chat_id"])
            if (
                not self._is_chat_temporarily_suppressed(chat_id)
                and self._public_probe_is_due(chat_id)
            ):
                due_rows.append(row)
            index = (index + 1) % total
            inspected += 1
        return due_rows, index

    def _next_public_probe_batch(self) -> list[dict[str, Any]]:
        rows = self._public_probe_candidate_rows()
        if not rows:
            return []
        batch_size = max(
            1,
            int(getattr(self._cfg, "db_listener_public_probe_batch_size", 4) or 4),
        )
        hot_rows, cold_rows = self._split_public_probe_rows(rows)
        hot_slot_count = max(
            1,
            min(
                batch_size,
                int(round(batch_size * _DEFAULT_HOT_PROBE_SLOT_RATIO)),
            ),
        )
        cold_slot_count = max(0, batch_size - hot_slot_count)
        if cold_rows and cold_slot_count <= 0:
            cold_slot_count = 1
            hot_slot_count = max(0, batch_size - cold_slot_count)

        with self._public_probe_cursor_lock:
            total = len(rows)
            start = self._public_probe_cursor % total
            hot_start = min(start, len(hot_rows)) if hot_rows else 0
            cold_base = max(0, start - len(hot_rows))
            cold_start = min(cold_base, len(cold_rows)) if cold_rows else 0

            hot_due_rows, next_hot_index = self._rotated_due_rows(
                hot_rows,
                start_index=hot_start,
                limit=hot_slot_count,
            )
            cold_due_rows, next_cold_index = self._rotated_due_rows(
                cold_rows,
                start_index=cold_start,
                limit=cold_slot_count,
            )

            remaining_slots = max(
                0,
                batch_size - len(hot_due_rows) - len(cold_due_rows),
            )
            if remaining_slots > 0:
                if len(hot_due_rows) < hot_slot_count and cold_rows:
                    extra_cold_rows, next_cold_index = self._rotated_due_rows(
                        cold_rows,
                        start_index=next_cold_index,
                        limit=remaining_slots,
                    )
                    cold_due_rows.extend(extra_cold_rows)
                    remaining_slots = max(
                        0,
                        batch_size - len(hot_due_rows) - len(cold_due_rows),
                    )
                if remaining_slots > 0 and hot_rows:
                    extra_hot_rows, next_hot_index = self._rotated_due_rows(
                        hot_rows,
                        start_index=next_hot_index,
                        limit=remaining_slots,
                    )
                    seen_chat_ids = {
                        int(row["chat_id"]) for row in hot_due_rows + cold_due_rows
                    }
                    for row in extra_hot_rows:
                        if int(row["chat_id"]) in seen_chat_ids:
                            continue
                        hot_due_rows.append(row)
                        seen_chat_ids.add(int(row["chat_id"]))
                        if len(hot_due_rows) + len(cold_due_rows) >= batch_size:
                            break

            merged_rows: list[dict[str, Any]] = []
            hot_idx = 0
            cold_idx = 0
            while len(merged_rows) < batch_size:
                if hot_idx < len(hot_due_rows):
                    merged_rows.append(hot_due_rows[hot_idx])
                    hot_idx += 1
                    if len(merged_rows) >= batch_size:
                        break
                if cold_idx < len(cold_due_rows):
                    merged_rows.append(cold_due_rows[cold_idx])
                    cold_idx += 1
                    if len(merged_rows) >= batch_size:
                        break
                if hot_idx >= len(hot_due_rows) and cold_idx >= len(cold_due_rows):
                    break

            self._public_probe_cursor = next_hot_index + len(hot_rows) + next_cold_index
        return merged_rows

    def _load_local_last_message_id(self, chat_id: int) -> int:
        conn = None
        try:
            conn = self._get_conn_fn()
            return int(get_last_message_id(conn, int(chat_id)) or 0)
        except Exception:
            logging.exception("读取本地最后消息 ID 失败: chat_id=%s", chat_id)
            return 0
        finally:
            if conn is not None:
                with suppress(Exception):
                    conn.close()

    def _probe_public_row_with_account(
        self,
        *,
        row: dict[str, Any],
        account: _ListenerAccount,
    ) -> bool:
        self._wait_for_public_probe_account_slot(account.key)
        worker_id = f"{self._job_id}_{account.key}_probe_{row['chat_id']}"
        client = None
        try:
            client = _create_isolated_worker_client(account.cfg, worker_id)
            chat_username = clean_username(row.get("chat_username"))
            if not chat_username:
                return False
            entity = resolve_chat_entity(
                client,
                int(row["chat_id"]),
                chat_username,
                retry_scope="db-listener-public-probe",
            )
            remote_last_id = int(read_latest_message_id(client, entity) or 0)
            local_last_id = self._load_local_last_message_id(int(row["chat_id"]))
            if remote_last_id > max(0, local_last_id):
                self._enqueue_chat_update(
                    chat_id=int(row["chat_id"]),
                    chat_title=str(row.get("chat_title") or ""),
                    chat_username=chat_username,
                    reason=_DEFAULT_PROBE_EVENT_REASON,
                    source_account=account.key,
                )
                return True
            return False
        finally:
            if client is not None:
                with suppress(Exception):
                    _disconnect_worker_client(client)
            _cleanup_isolated_worker_session(account.cfg, worker_id)

    def _public_probe_success_cooldown_seconds(self, *, changed: bool) -> int:
        base_seconds = max(
            300,
            int(
                getattr(
                    self._cfg,
                    "db_listener_public_probe_chat_cooldown_seconds",
                    3600,
                )
                or 3600
            ),
        )
        if changed:
            return max(300, min(base_seconds, 900))
        return base_seconds

    def _public_probe_failure_cooldown_seconds(self, *, flood_wait_seconds: int = 0) -> int:
        if flood_wait_seconds > 0:
            return max(120, min(int(flood_wait_seconds), 900))
        return 180

    def _probe_public_row(self, row: dict[str, Any]) -> _PublicProbeOutcome:
        accounts = self._listener_accounts()
        if not accounts:
            return _PublicProbeOutcome(status="no_account", cooldown_seconds=300)
        last_exc: Exception | None = None
        for account in sorted(
            accounts,
            key=lambda item: _account_cooldown_remaining(item),
        ):
            if _account_cooldown_remaining(account) > 0:
                continue
            if not _ensure_base_session_valid(account.cfg, self._job_id, _listener_log):
                continue
            try:
                changed = self._probe_public_row_with_account(row=row, account=account)
                return _PublicProbeOutcome(
                    status="changed" if changed else "unchanged",
                    cooldown_seconds=self._public_probe_success_cooldown_seconds(
                        changed=changed
                    ),
                )
            except Exception as exc:
                last_exc = exc
                if isinstance(exc, AccountFloodWaitError):
                    _remember_account_cooldown(account, exc)
                    continue
                message = admin_error_message(exc)
                if "本地实体缓存未命中" in message:
                    continue
                if "不存在" in message or "解散" in message:
                    return _PublicProbeOutcome(
                        status="missing",
                        cooldown_seconds=int(_DEFAULT_MISSED_CHAT_COOLDOWN_SECONDS),
                    )
                return _PublicProbeOutcome(
                    status="failed",
                    cooldown_seconds=self._public_probe_failure_cooldown_seconds(),
                )
        if isinstance(last_exc, AccountFloodWaitError):
            return _PublicProbeOutcome(
                status="flood_wait",
                cooldown_seconds=self._public_probe_failure_cooldown_seconds(
                    flood_wait_seconds=int(last_exc.seconds)
                ),
            )
        if last_exc is not None:
            return _PublicProbeOutcome(
                status="failed",
                cooldown_seconds=self._public_probe_failure_cooldown_seconds(),
            )
        return _PublicProbeOutcome(status="no_account", cooldown_seconds=300)

    def _public_probe_loop(self) -> None:
        interval_seconds = max(
            60,
            int(
                getattr(
                    self._cfg,
                    "db_listener_public_probe_interval_seconds",
                    180,
                )
                or 180
            ),
        )
        while not self._watcher_stop.wait(float(interval_seconds)):
            if self._public_probe_has_pending_updates():
                continue
            for row in self._next_public_probe_batch():
                if self._public_probe_has_pending_updates():
                    break
                chat_id = int(row["chat_id"])
                try:
                    outcome = self._probe_public_row(row)
                    if outcome.status == "missing":
                        self._suppress_chat_temporarily(
                            chat_id,
                            seconds=max(
                                int(_DEFAULT_MISSED_CHAT_COOLDOWN_SECONDS),
                                int(outcome.cooldown_seconds),
                            ),
                        )
                    self._set_public_probe_cooldown(
                        chat_id,
                        seconds=max(60, int(outcome.cooldown_seconds)),
                    )
                except Exception:
                    logging.exception("公开群低频定向探测失败: chat_id=%s", chat_id)
                    self._set_public_probe_cooldown(
                        chat_id,
                        seconds=self._public_probe_failure_cooldown_seconds(),
                    )


def ensure_database_chat_listener_runtime(
    *,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
) -> DatabaseChatListenerRuntime:
    global _LISTENER_SINGLETON
    with _LISTENER_SINGLETON_LOCK:
        if _LISTENER_SINGLETON is None:
            runtime = DatabaseChatListenerRuntime(
                cfg=cfg,
                get_conn_fn=get_conn_fn,
            )
            runtime.start()
            _LISTENER_SINGLETON = runtime
        return _LISTENER_SINGLETON
