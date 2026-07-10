import logging
import os
import socket
import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from queue import Empty, Queue
from typing import Any

from telethon import events

from tg_harvest.admin_jobs.common import admin_error_message, resolve_chat_entity
from tg_harvest.admin_jobs.runners import (
    _account_cooldown_remaining,
    _admin_process_single_chat_update,
    _cfg_with_session_name,
    _create_isolated_worker_client,
    _ensure_base_session_valid,
    _read_session_cached_chat_ids,
    _remember_account_cooldown,
)
from tg_harvest.admin_jobs.sessions import (
    _cleanup_isolated_worker_session,
    _create_isolated_worker_client_with_options,
    _disconnect_worker_client,
)
from tg_harvest.domain.chat_ids import stored_chat_id_from_entity_id
from tg_harvest.domain.chat_inventory import load_joined_chat_inventory
from tg_harvest.domain.coerce import clean_username, enabled_int, optional_int
from tg_harvest.ingest.flood_wait import AccountFloodWaitError
from tg_harvest.ingest.range_harvest import read_latest_message_id
from tg_harvest.ingest.store import get_last_message_id
from tg_harvest.storage import sync_scheduler
from tg_harvest.storage.sync_scheduler import (
    SyncObservation,
    SyncPendingTask,
    SyncUpdateResult,
)

_DEFAULT_EVENT_IDLE_SLEEP_SECONDS = 1.0
_DEFAULT_MISSED_CHAT_COOLDOWN_SECONDS = 600.0
_DEFAULT_QUEUE_MAXSIZE = 4096
_DEFAULT_PROBE_EVENT_REASON = "public_probe"
_DEFAULT_JOINED_SNAPSHOT_REFRESH_SECONDS = 1800.0
_DEFAULT_HOT_PROBE_SLOT_RATIO = 0.75
_DEFAULT_PUBLIC_PROBE_ACCOUNT_GAP_SECONDS = 6.0
_DEFAULT_JOINED_PROBE_CHANGED_COOLDOWN_SECONDS = 1800
_DEFAULT_PUBLIC_PROBE_CHANGED_COOLDOWN_SECONDS = 900
_DEFAULT_INACTIVE_PROBE_AGE_SECONDS = 7 * 24 * 60 * 60

_LISTENER_SINGLETON: "DatabaseChatListenerRuntime | None" = None
_LISTENER_SINGLETON_LOCK = threading.Lock()


def _listener_log(job_id: str, message: str) -> None:
    logging.info("[db-listener:%s] %s", job_id, message)


def _safe_int(value: Any) -> int:
    parsed = optional_int(value)
    return int(parsed or 0)


def _parse_utc_text_timestamp(value: Any) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return (
            datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
            .replace(tzinfo=UTC)
            .timestamp()
        )
    except Exception:
        return 0.0


def _utc_now_ts() -> float:
    return time.time()


def _format_utc_timestamp(value: float) -> str:
    if float(value or 0) <= 0:
        return ""
    return (
        datetime.fromtimestamp(float(value), tz=UTC)
        .replace(microsecond=0)
        .strftime("%Y-%m-%d %H:%M:%S")
    )


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
                    COALESCE(lm.message_id, 0) AS last_message_id,
                    COALESCE(lm.msg_date_ts, 0) AS last_message_ts
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
                    0 AS last_message_id,
                    0 AS last_message_ts
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
                    "last_message_ts": _safe_int(row["last_message_ts"]),
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
    source_account: str = ""
    remote_last_id: int = 0
    local_last_id: int = 0


@dataclass(frozen=True)
class _PublicProbeRead:
    changed: bool
    remote_last_id: int
    local_last_id: int


class AccountRuntimeCoordinator:
    def __init__(
        self,
        *,
        cfg: Any,
        get_conn_fn: Callable[[], Any],
        account_loader: Callable[[], list[_ListenerAccount]],
    ) -> None:
        self._cfg = cfg
        self._get_conn_fn = get_conn_fn
        self._account_loader = account_loader
        self._locks_lock = threading.Lock()
        self._locks: dict[str, threading.Lock] = {}

    def scheduler_concurrency(self) -> int:
        accounts = self._account_loader()
        configured = max(
            1,
            int(getattr(self._cfg, "sync_scheduler_concurrency", 2) or 2),
        )
        return max(1, min(configured, max(1, len(accounts))))

    def account_lock(self, account_key: str) -> threading.Lock:
        key = str(account_key or "").strip() or "primary"
        with self._locks_lock:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock
            return lock

    def sync_configured_accounts(self) -> None:
        for account in self._account_loader():
            self._write_account_state(
                account_key=account.key,
                session_name=account.session_name,
                label=account.label,
            )

    def restore_cooldowns(self) -> None:
        conn = None
        try:
            conn = self._get_conn_fn()
            rows = sync_scheduler.list_account_runtime_states(conn)
        except Exception:
            logging.exception("恢复账号运行态冷却失败")
            return
        finally:
            if conn is not None:
                with suppress(Exception):
                    conn.close()
        accounts_by_key = {account.key: account for account in self._account_loader()}
        now = time.time()
        for row in rows:
            account = accounts_by_key.get(str(row.get("account_key") or ""))
            if account is None:
                continue
            until_ts = _parse_utc_text_timestamp(row.get("cooldown_until"))
            remaining = int(until_ts - now)
            if remaining <= 0:
                continue
            _remember_account_cooldown(
                account,
                AccountFloodWaitError(
                    seconds=remaining,
                    threshold_seconds=1,
                    account_label=account.key,
                    scope="runtime-restore",
                ),
            )

    def mark_cooldown(self, account: _ListenerAccount, seconds: int) -> None:
        cooldown_until = _format_utc_timestamp(time.time() + max(1, int(seconds or 0)))
        self._write_account_state(
            account_key=account.key,
            session_name=account.session_name,
            label=account.label,
            cooldown_until=cooldown_until,
            success=False,
            failure_message=f"FloodWait {max(1, int(seconds or 0))}s",
        )

    def mark_update_start(self, account: _ListenerAccount) -> None:
        self._write_account_state(
            account_key=account.key,
            session_name=account.session_name,
            label=account.label,
            in_flight_delta=1,
        )

    def mark_update_finish(
        self,
        account: _ListenerAccount,
        *,
        success: bool,
        duration_seconds: float,
        failure_message: str = "",
    ) -> None:
        self._write_account_state(
            account_key=account.key,
            session_name=account.session_name,
            label=account.label,
            success=success,
            duration_seconds=duration_seconds,
            failure_message=failure_message,
            in_flight_delta=-1,
        )

    def _write_account_state(self, **kwargs: Any) -> None:
        conn = None
        try:
            conn = self._get_conn_fn()
            sync_scheduler.upsert_account_runtime_state(conn, **kwargs)
        except Exception:
            logging.exception("写入账号运行态失败: account=%s", kwargs.get("account_key"))
        finally:
            if conn is not None:
                with suppress(Exception):
                    conn.close()


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
        self._public_probe_cursor: dict[str, int] = {}
        self._listener_clients_lock = threading.Lock()
        self._listener_clients: dict[str, Any] = {}
        self._listener_connected_at_lock = threading.Lock()
        self._listener_connected_at: dict[str, float] = {}
        self._listener_last_error_lock = threading.Lock()
        self._listener_last_error: dict[str, str] = {}
        self._listener_last_error_at_lock = threading.Lock()
        self._listener_last_error_at: dict[str, float] = {}
        self._last_event_at_lock = threading.Lock()
        self._last_event_at = 0.0
        self._last_event_reason_lock = threading.Lock()
        self._last_event_reason = ""
        self._last_event_chat_id_lock = threading.Lock()
        self._last_event_chat_id = 0
        self._last_update_attempt_at_lock = threading.Lock()
        self._last_update_attempt_at = 0.0
        self._last_update_success_at_lock = threading.Lock()
        self._last_update_success_at = 0.0
        self._last_update_failure_at_lock = threading.Lock()
        self._last_update_failure_at = 0.0
        self._last_update_failure_message_lock = threading.Lock()
        self._last_update_failure_message = ""
        self._last_update_success_chat_id_lock = threading.Lock()
        self._last_update_success_chat_id = 0
        self._last_update_failure_chat_id_lock = threading.Lock()
        self._last_update_failure_chat_id = 0
        self._last_probe_attempt_at_lock = threading.Lock()
        self._last_probe_attempt_at = 0.0
        self._last_probe_result_at_lock = threading.Lock()
        self._last_probe_result_at = 0.0
        self._last_probe_status_lock = threading.Lock()
        self._last_probe_status = ""
        self._last_probe_chat_id_lock = threading.Lock()
        self._last_probe_chat_id = 0
        self._manual_probe_requested_at_lock = threading.Lock()
        self._manual_probe_requested_at = 0.0
        self._manual_probe_completed_at_lock = threading.Lock()
        self._manual_probe_completed_at = 0.0
        self._manual_probe_last_result_lock = threading.Lock()
        self._manual_probe_last_result = ""
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
        self._model_stop = threading.Event()
        self._model_thread = threading.Thread(
            target=self._model_training_loop,
            name="db-chat-sync-model-trainer",
            daemon=True,
        )
        self._listener_threads: list[threading.Thread] = []
        self._started = False
        self._job_id = "db-listener"
        self._runtime_instance_id = f"pid-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        self._runtime_host = socket.gethostname()
        self._account_runtime = AccountRuntimeCoordinator(
            cfg=cfg,
            get_conn_fn=get_conn_fn,
            account_loader=self._listener_accounts,
        )

    def _mark_listener_connected(self, account_key: str) -> None:
        now = _utc_now_ts()
        with self._listener_connected_at_lock:
            self._listener_connected_at[str(account_key)] = now
        with self._listener_last_error_lock:
            self._listener_last_error.pop(str(account_key), None)
        with self._listener_last_error_at_lock:
            self._listener_last_error_at.pop(str(account_key), None)

    def _mark_listener_error(self, account_key: str, message: str) -> None:
        now = _utc_now_ts()
        with self._listener_last_error_lock:
            self._listener_last_error[str(account_key)] = str(message or "").strip()
        with self._listener_last_error_at_lock:
            self._listener_last_error_at[str(account_key)] = now

    def _record_event_observed(self, *, reason: str, chat_id: int) -> None:
        now = _utc_now_ts()
        with self._last_event_at_lock:
            self._last_event_at = now
        with self._last_event_reason_lock:
            self._last_event_reason = str(reason or "").strip()
        with self._last_event_chat_id_lock:
            self._last_event_chat_id = int(chat_id or 0)

    def _record_update_attempt(self) -> None:
        with self._last_update_attempt_at_lock:
            self._last_update_attempt_at = _utc_now_ts()

    def _record_update_success(self, *, chat_id: int) -> None:
        now = _utc_now_ts()
        with self._last_update_success_at_lock:
            self._last_update_success_at = now
        with self._last_update_success_chat_id_lock:
            self._last_update_success_chat_id = int(chat_id or 0)
        with self._last_update_failure_message_lock:
            self._last_update_failure_message = ""

    def _record_update_failure(self, *, chat_id: int, message: str) -> None:
        now = _utc_now_ts()
        with self._last_update_failure_at_lock:
            self._last_update_failure_at = now
        with self._last_update_failure_chat_id_lock:
            self._last_update_failure_chat_id = int(chat_id or 0)
        with self._last_update_failure_message_lock:
            self._last_update_failure_message = str(message or "").strip()

    def _record_probe_attempt(self) -> None:
        with self._last_probe_attempt_at_lock:
            self._last_probe_attempt_at = _utc_now_ts()

    def _record_probe_result(self, *, status: str, chat_id: int) -> None:
        now = _utc_now_ts()
        with self._last_probe_result_at_lock:
            self._last_probe_result_at = now
        with self._last_probe_status_lock:
            self._last_probe_status = str(status or "").strip()
        with self._last_probe_chat_id_lock:
            self._last_probe_chat_id = int(chat_id or 0)

    def _record_manual_probe_requested(self) -> None:
        with self._manual_probe_requested_at_lock:
            self._manual_probe_requested_at = _utc_now_ts()

    def _record_manual_probe_completed(self, result: str) -> None:
        now = _utc_now_ts()
        with self._manual_probe_completed_at_lock:
            self._manual_probe_completed_at = now
        with self._manual_probe_last_result_lock:
            self._manual_probe_last_result = str(result or "").strip()

    def _snapshot_listener_clients(self) -> dict[str, Any]:
        with self._listener_clients_lock:
            return dict(self._listener_clients)

    def _snapshot_listener_connected_at(self) -> dict[str, float]:
        with self._listener_connected_at_lock:
            return dict(self._listener_connected_at)

    def _snapshot_listener_last_error(self) -> dict[str, str]:
        with self._listener_last_error_lock:
            return dict(self._listener_last_error)

    def _snapshot_listener_last_error_at(self) -> dict[str, float]:
        with self._listener_last_error_at_lock:
            return dict(self._listener_last_error_at)

    def _queue_size(self) -> int:
        try:
            return int(self._queue.qsize())
        except Exception:
            return len(self._queued_chat_ids_snapshot())

    def _queued_chat_ids_snapshot(self) -> set[int]:
        with self._queued_chat_ids_lock:
            return set(self._queued_chat_ids)

    def _sync_scheduler_enabled(self) -> bool:
        return sync_scheduler.scheduler_enabled(self._cfg)

    def _sync_ai_enabled(self) -> bool:
        return sync_scheduler.ai_enabled(self._cfg)

    def _sync_ai_shadow_enabled(self) -> bool:
        return sync_scheduler.ai_shadow_enabled(self._cfg)

    def _sync_ai_auto_promote_enabled(self) -> bool:
        return sync_scheduler.ai_auto_promote_enabled(self._cfg)

    def _sync_model_training_enabled(self) -> bool:
        return self._sync_scheduler_enabled() and self._sync_ai_enabled()

    def _pending_update_counts(self) -> dict[str, int]:
        conn = None
        try:
            conn = self._get_conn_fn()
            summary = sync_scheduler.build_scheduler_summary(
                conn,
                health_snapshot={
                    "scheduler_enabled": self._sync_scheduler_enabled(),
                    "ai_enabled": self._sync_ai_enabled(),
                    "ai_shadow": self._sync_ai_shadow_enabled(),
                    "ai_auto_promote_enabled": self._sync_ai_auto_promote_enabled(),
                    "scheduler_concurrency": self._account_runtime.scheduler_concurrency(),
                },
            )
            return {
                "pending": int(summary.get("pending_count") or 0),
                "due": int(summary.get("due_count") or 0),
                "in_flight": int(summary.get("in_flight_count") or 0),
            }
        except Exception:
            logging.exception("读取同步调度 pending 统计失败")
            return {"pending": 0, "due": 0, "in_flight": 0}
        finally:
            if conn is not None:
                with suppress(Exception):
                    conn.close()

    def _scheduler_backpressure_snapshot(
        self,
        pending_counts: dict[str, int] | None = None,
    ) -> dict[str, Any]:
        if not self._sync_scheduler_enabled():
            queue_size = self._queue_size()
            active = queue_size > 0
            return {
                "active": active,
                "reason": "legacy_queue_pending" if active else "",
                "pending_threshold": 1,
                "due_threshold": 0,
            }
        counts = pending_counts or self._pending_update_counts()
        scheduler_concurrency = self._account_runtime.scheduler_concurrency()
        pending_threshold = max(20, scheduler_concurrency * 10)
        due_threshold = max(4, scheduler_concurrency * 3)
        pending = int(counts.get("pending") or 0)
        due = int(counts.get("due") or 0)
        in_flight = int(counts.get("in_flight") or 0)
        active = (
            pending >= pending_threshold
            or due >= due_threshold
            or (due > 0 and in_flight >= scheduler_concurrency)
        )
        reason = ""
        if active:
            if pending >= pending_threshold:
                reason = "pending_backlog"
            elif due >= due_threshold:
                reason = "due_backlog"
            else:
                reason = "scheduler_capacity_full"
        return {
            "active": active,
            "reason": reason,
            "pending_threshold": pending_threshold,
            "due_threshold": due_threshold,
            "pending": pending,
            "due": due,
            "in_flight": in_flight,
        }

    def health_snapshot(self) -> dict[str, Any]:
        accounts = self._listener_accounts()
        clients = self._snapshot_listener_clients()
        connected_at = self._snapshot_listener_connected_at()
        last_errors = self._snapshot_listener_last_error()
        last_error_at = self._snapshot_listener_last_error_at()
        with self._last_event_at_lock:
            last_event_at = float(self._last_event_at)
        with self._last_event_reason_lock:
            last_event_reason = str(self._last_event_reason or "")
        with self._last_event_chat_id_lock:
            last_event_chat_id = int(self._last_event_chat_id or 0)
        with self._last_update_attempt_at_lock:
            last_update_attempt_at = float(self._last_update_attempt_at)
        with self._last_update_success_at_lock:
            last_update_success_at = float(self._last_update_success_at)
        with self._last_update_failure_at_lock:
            last_update_failure_at = float(self._last_update_failure_at)
        with self._last_update_failure_message_lock:
            last_update_failure_message = str(self._last_update_failure_message or "")
        with self._last_update_success_chat_id_lock:
            last_update_success_chat_id = int(self._last_update_success_chat_id or 0)
        with self._last_update_failure_chat_id_lock:
            last_update_failure_chat_id = int(self._last_update_failure_chat_id or 0)
        with self._last_probe_attempt_at_lock:
            last_probe_attempt_at = float(self._last_probe_attempt_at)
        with self._last_probe_result_at_lock:
            last_probe_result_at = float(self._last_probe_result_at)
        with self._last_probe_status_lock:
            last_probe_status = str(self._last_probe_status or "")
        with self._last_probe_chat_id_lock:
            last_probe_chat_id = int(self._last_probe_chat_id or 0)
        with self._manual_probe_requested_at_lock:
            manual_probe_requested_at = float(self._manual_probe_requested_at)
        with self._manual_probe_completed_at_lock:
            manual_probe_completed_at = float(self._manual_probe_completed_at)
        with self._manual_probe_last_result_lock:
            manual_probe_last_result = str(self._manual_probe_last_result or "")
        now = _utc_now_ts()
        account_snapshots = []
        active_listener_count = 0
        for account in accounts:
            is_connected = str(account.key) in clients
            if is_connected:
                active_listener_count += 1
            cooldown_seconds = _account_cooldown_remaining(account)
            account_snapshots.append(
                {
                    "key": str(account.key),
                    "label": str(account.label),
                    "connected": is_connected,
                    "connected_at": _format_utc_timestamp(connected_at.get(str(account.key), 0.0)),
                    "last_error": str(last_errors.get(str(account.key), "") or ""),
                    "last_error_at": _format_utc_timestamp(last_error_at.get(str(account.key), 0.0)),
                    "cooldown_seconds": int(cooldown_seconds or 0),
                }
            )
        pending_counts = self._pending_update_counts() if self._sync_scheduler_enabled() else {
            "pending": len(self._queued_chat_ids_snapshot()),
            "due": 0,
            "in_flight": 0,
        }
        backpressure = self._scheduler_backpressure_snapshot(pending_counts)
        queue_size = int(pending_counts.get("pending") or 0)
        return {
            "started": bool(self._started),
            "listener_enabled": enabled_int(getattr(self._cfg, "db_listener_enabled", 1)) == 1,
            "scheduler_enabled": self._sync_scheduler_enabled(),
            "ai_enabled": self._sync_ai_enabled(),
            "ai_shadow": self._sync_ai_shadow_enabled(),
            "ai_auto_promote_enabled": self._sync_ai_auto_promote_enabled(),
            "public_probe_enabled": enabled_int(
                getattr(self._cfg, "db_listener_public_probe_enabled", 1)
            )
            == 1,
            "tracked_chat_count": len(self._database_chat_ids()),
            "queued_chat_count": queue_size,
            "queue_size": queue_size,
            "pending_update_count": int(pending_counts.get("pending") or 0),
            "due_update_count": int(pending_counts.get("due") or 0),
            "in_flight_update_count": int(pending_counts.get("in_flight") or 0),
            "scheduler_concurrency": self._account_runtime.scheduler_concurrency(),
            "backpressure": backpressure,
            "active_listener_count": active_listener_count,
            "configured_listener_count": len(accounts),
            "worker_thread_alive": self._worker_thread.is_alive(),
            "refresh_thread_alive": self._refresh_thread.is_alive(),
            "public_probe_thread_alive": self._public_probe_thread.is_alive(),
            "model_thread_alive": self._model_thread.is_alive(),
            "joined_snapshot_updated_at": _format_utc_timestamp(
                float(self._joined_chat_snapshot_updated_at or 0.0)
            ),
            "last_event_at": _format_utc_timestamp(last_event_at),
            "last_event_age_seconds": max(0, int(now - last_event_at)) if last_event_at > 0 else None,
            "last_event_reason": last_event_reason,
            "last_event_chat_id": last_event_chat_id,
            "last_update_attempt_at": _format_utc_timestamp(last_update_attempt_at),
            "last_update_success_at": _format_utc_timestamp(last_update_success_at),
            "last_update_success_age_seconds": max(0, int(now - last_update_success_at))
            if last_update_success_at > 0
            else None,
            "last_update_success_chat_id": last_update_success_chat_id,
            "last_update_failure_at": _format_utc_timestamp(last_update_failure_at),
            "last_update_failure_age_seconds": max(0, int(now - last_update_failure_at))
            if last_update_failure_at > 0
            else None,
            "last_update_failure_chat_id": last_update_failure_chat_id,
            "last_update_failure_message": last_update_failure_message,
            "last_probe_attempt_at": _format_utc_timestamp(last_probe_attempt_at),
            "last_probe_result_at": _format_utc_timestamp(last_probe_result_at),
            "last_probe_result_age_seconds": max(0, int(now - last_probe_result_at))
            if last_probe_result_at > 0
            else None,
            "last_probe_status": last_probe_status,
            "last_probe_chat_id": last_probe_chat_id,
            "manual_probe_requested_at": _format_utc_timestamp(manual_probe_requested_at),
            "manual_probe_completed_at": _format_utc_timestamp(manual_probe_completed_at),
            "manual_probe_last_result": manual_probe_last_result,
            "accounts": account_snapshots,
        }

    def trigger_manual_probe(self, *, limit: int = 3) -> dict[str, Any]:
        self._record_manual_probe_requested()
        rows = self._next_public_probe_batch()
        if not rows:
            result = {
                "ok": True,
                "triggered": 0,
                "message": "当前没有可立即探测的轮巡目标",
                "items": [],
            }
            self._record_manual_probe_completed("no_due_public_probe_rows")
            return result

        items = []
        triggered = 0
        for row in rows[: max(1, int(limit or 1))]:
            chat_id = int(row["chat_id"])
            self._record_probe_attempt()
            try:
                outcome = self._probe_public_row(row)
                self._record_probe_result(status=outcome.status, chat_id=chat_id)
                self._set_public_probe_cooldown(
                    chat_id,
                    seconds=max(60, int(outcome.cooldown_seconds)),
                )
                if outcome.status == "changed":
                    triggered += 1
                items.append(
                    {
                        "chat_id": chat_id,
                        "chat_title": str(row.get("chat_title") or ""),
                        "status": str(outcome.status),
                        "cooldown_seconds": int(outcome.cooldown_seconds),
                    }
                )
            except Exception as exc:
                message = admin_error_message(exc)
                self._record_probe_result(status="failed", chat_id=chat_id)
                self._set_public_probe_cooldown(
                    chat_id,
                    seconds=self._public_probe_failure_cooldown_seconds(),
                )
                items.append(
                    {
                        "chat_id": chat_id,
                        "chat_title": str(row.get("chat_title") or ""),
                        "status": "failed",
                        "error": message,
                    }
                )
        result_key = "triggered_updates" if triggered > 0 else "completed_without_changes"
        self._record_manual_probe_completed(result_key)
        return {
            "ok": True,
            "triggered": triggered,
            "message": "已完成即时轮巡探测",
            "items": items,
        }

    def trigger_manual_chat_probe(self, chat_id: int) -> dict[str, Any]:
        safe_chat_id = int(chat_id or 0)
        if safe_chat_id <= 0:
            return {"ok": False, "message": "chat_id 参数非法", "items": []}
        self._record_manual_probe_requested()
        rows = [
            row
            for row in self._public_probe_candidate_rows()
            if int(row.get("chat_id") or 0) == safe_chat_id
        ]
        if not rows:
            row = self._database_chat_row(safe_chat_id)
            if row is None:
                self._record_manual_probe_completed("chat_not_found")
                return {"ok": False, "message": "chat_id 不存在", "items": []}
            conn = None
            try:
                conn = self._get_conn_fn()
                sync_scheduler.record_probe_result(
                    conn,
                    chat_id=safe_chat_id,
                    chat_title=str(row.get("chat_title") or ""),
                    chat_username=clean_username(row.get("chat_username")),
                    status="unobservable",
                    cooldown_seconds=int(
                        getattr(
                            self._cfg,
                            "db_listener_inactive_probe_chat_cooldown_seconds",
                            43200,
                        )
                        or 43200
                    ),
                    reason="manual_probe",
                )
            except Exception:
                logging.exception("写入单群不可观察 probe 结果失败: chat_id=%s", safe_chat_id)
            finally:
                if conn is not None:
                    with suppress(Exception):
                        conn.close()
            self._record_manual_probe_completed("unobservable")
            return {
                "ok": True,
                "message": "该群组当前没有可用的 joined/public/cached 探测通道",
                "triggered": 0,
                "items": [
                    {
                        "chat_id": safe_chat_id,
                        "chat_title": str(row.get("chat_title") or ""),
                        "status": "unobservable",
                    }
                ],
            }

        row = rows[0]
        try:
            outcome = self._probe_public_row(row)
            self._set_public_probe_cooldown(
                safe_chat_id,
                seconds=max(60, int(outcome.cooldown_seconds)),
            )
            self._record_manual_probe_completed(outcome.status)
            return {
                "ok": True,
                "message": "已完成单群即时调度诊断",
                "triggered": 1 if outcome.status == "changed" else 0,
                "items": [
                    {
                        "chat_id": safe_chat_id,
                        "chat_title": str(row.get("chat_title") or ""),
                        "status": outcome.status,
                        "remote_last_id": int(outcome.remote_last_id or 0),
                        "local_last_id": int(outcome.local_last_id or 0),
                        "cooldown_seconds": int(outcome.cooldown_seconds or 0),
                    }
                ],
            }
        except Exception as exc:
            message = admin_error_message(exc)
            self._record_probe_result(status="failed", chat_id=safe_chat_id)
            self._record_manual_probe_completed("failed")
            return {
                "ok": False,
                "message": "单群即时调度诊断失败：" + message,
                "triggered": 0,
                "items": [],
            }

    def start(self) -> None:
        if self._started:
            return
        if enabled_int(getattr(self._cfg, "db_listener_enabled", 1)) != 1:
            _listener_log(self._job_id, "数据库内群组监听已关闭")
            self._started = True
            return
        self._account_runtime.sync_configured_accounts()
        self._account_runtime.restore_cooldowns()
        self._recover_scheduler_in_flight_updates()
        self._refresh_database_chat_cache()
        self._refresh_joined_chat_snapshot()
        self._started = True
        self._worker_thread.start()
        self._refresh_thread.start()
        if enabled_int(getattr(self._cfg, "db_listener_public_probe_enabled", 1)) == 1:
            self._public_probe_thread.start()
        if self._sync_model_training_enabled():
            self._model_thread.start()
        self._start_listener_threads()
        _listener_log(
            self._job_id,
            f"数据库内群组监听已启动，当前追踪 {len(self._db_chat_rows_by_id)} 个已入库群组/频道",
        )

    def _recover_scheduler_in_flight_updates(self) -> int:
        conn = None
        try:
            conn = self._get_conn_fn()
            recovered = sync_scheduler.recover_in_flight_pending_updates(
                conn,
                local_host=self._runtime_host,
            )
        except Exception:
            logging.exception("恢复中断的同步调度任务失败")
            return 0
        finally:
            if conn is not None:
                with suppress(Exception):
                    conn.close()

        if recovered > 0:
            _listener_log(
                self._job_id,
                f"已恢复 {recovered} 个因进程中断而遗留的同步调度任务",
            )
        return recovered

    def stop(self) -> None:
        self._worker_stop.set()
        self._watcher_stop.set()
        self._model_stop.set()
        with self._listener_clients_lock:
            active_clients = list(self._listener_clients.values())
        for client in active_clients:
            with suppress(Exception):
                _disconnect_worker_client(client)
        self._worker_thread.join(timeout=2.0)
        self._refresh_thread.join(timeout=2.0)
        if self._public_probe_thread.is_alive():
            self._public_probe_thread.join(timeout=2.0)
        if self._model_thread.is_alive():
            self._model_thread.join(timeout=2.0)
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
        self._refresh_sync_scheduler_state()

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
        self._refresh_sync_scheduler_state()

    def _refresh_sync_scheduler_state(self) -> None:
        conn = None
        try:
            rows = self._database_chat_rows()
            joined_by_account = self._joined_chat_ids_by_account_snapshot()
            cached_by_account = self._session_cached_chat_ids_by_account()
            account_keys = [account.key for account in self._listener_accounts()]
            conn = self._get_conn_fn()
            sync_scheduler.refresh_chat_states(
                conn,
                chat_rows=rows,
                joined_by_account=joined_by_account,
                cached_by_account=cached_by_account,
                account_keys=account_keys,
            )
        except Exception:
            logging.exception("刷新同步调度群组状态失败")
        finally:
            if conn is not None:
                with suppress(Exception):
                    conn.close()

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
        if self._sync_scheduler_enabled():
            return bool(self._scheduler_backpressure_snapshot().get("active"))
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

    def _session_cached_chat_ids_by_account(self) -> dict[str, set[int]]:
        return {
            account.key: _read_session_cached_chat_ids(account.session_name)
            for account in self._listener_accounts()
        }

    def _probe_row_last_activity_ts(self, row: dict[str, Any]) -> float:
        return max(
            float(_safe_int(row.get("last_message_ts"))),
            float(_parse_utc_text_timestamp(row.get("last_seen_at"))),
        )

    def _probe_row_is_inactive(self, row: dict[str, Any]) -> bool:
        last_activity_ts = self._probe_row_last_activity_ts(row)
        if last_activity_ts <= 0:
            return True
        return (_utc_now_ts() - last_activity_ts) >= float(
            _DEFAULT_INACTIVE_PROBE_AGE_SECONDS
        )

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
        if self._sync_scheduler_enabled():
            conn = None
            try:
                conn = self._get_conn_fn()
                sync_scheduler.enqueue_observation(
                    conn,
                    cfg=self._cfg,
                    observation=SyncObservation(
                        chat_id=safe_chat_id,
                        chat_title=str(chat_title or "").strip()
                        or f"Chat {safe_chat_id}",
                        chat_username=clean_username(chat_username) or None,
                        reason=str(reason or "").strip() or "event",
                        source_account=str(source_account or "").strip() or "primary",
                    ),
                )
            except Exception:
                logging.exception("同步调度 observation 写入失败: chat_id=%s", safe_chat_id)
            finally:
                if conn is not None:
                    with suppress(Exception):
                        conn.close()
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
                        self._mark_listener_connected(account.key)
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
                    self._mark_listener_error(account.key, str(exc))
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
            self._record_event_observed(reason=reason, chat_id=chat_id)
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
        if self._sync_scheduler_enabled():
            self._scheduler_worker_loop()
            return
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

    def _claim_due_scheduler_tasks(
        self,
        *,
        limit: int,
        exclude_preferred_accounts: set[str] | None = None,
    ) -> list[SyncPendingTask]:
        conn = None
        try:
            conn = self._get_conn_fn()
            return sync_scheduler.claim_due_pending_updates(
                conn,
                limit=limit,
                exclude_preferred_accounts=exclude_preferred_accounts or set(),
                owner_instance_id=self._runtime_instance_id,
                owner_pid=os.getpid(),
                owner_host=self._runtime_host,
            )
        except Exception:
            logging.exception("领取同步调度任务失败")
            return []
        finally:
            if conn is not None:
                with suppress(Exception):
                    conn.close()

    def _claim_due_scheduler_task(self) -> SyncPendingTask | None:
        tasks = self._claim_due_scheduler_tasks(limit=1)
        return tasks[0] if tasks else None

    def _scheduler_task_to_queue_item(self, task: SyncPendingTask) -> _QueuedChatUpdate:
        preferred_account = str(task.preferred_source_account or "").strip()
        if not preferred_account:
            source_accounts = [
                part.strip()
                for part in str(task.source_accounts or "").split(",")
                if part.strip()
            ]
            preferred_account = source_accounts[0] if source_accounts else "primary"
        return _QueuedChatUpdate(
            chat_id=int(task.chat_id),
            chat_title=str(task.chat_title or "").strip() or f"Chat {int(task.chat_id)}",
            chat_username=clean_username(task.chat_username) or None,
            reason=str(task.reason or "").strip() or "event",
            source_account=preferred_account,
        )

    def _finish_scheduler_task(
        self,
        *,
        task: SyncPendingTask,
        result: SyncUpdateResult | None,
    ) -> None:
        conn = None
        try:
            conn = self._get_conn_fn()
            effective_result = result or SyncUpdateResult(
                chat_id=int(task.chat_id),
                chat_title=task.chat_title,
                chat_username=task.chat_username,
                failure_type="failed",
                failure_message="调度任务未返回结果",
            )
            if effective_result.failure_type == "deleted":
                sync_scheduler.deactivate_chat(
                    conn,
                    int(task.chat_id),
                    task=task,
                )
                return
            if effective_result.failure_type:
                sync_scheduler.fail_pending_update(
                    conn,
                    cfg=self._cfg,
                    task=task,
                    result=effective_result,
                )
            else:
                sync_scheduler.complete_pending_update(
                    conn,
                    task=task,
                    result=effective_result,
                )
        except Exception:
            logging.exception("写入同步调度任务结果失败: chat_id=%s", task.chat_id)
        finally:
            if conn is not None:
                with suppress(Exception):
                    conn.close()

    def _scheduler_worker_loop(self) -> None:
        concurrency = self._account_runtime.scheduler_concurrency()
        active_futures = {}
        while not self._worker_stop.is_set():
            while len(active_futures) < concurrency and not self._worker_stop.is_set():
                active_accounts = {
                    self._scheduler_task_to_queue_item(task).source_account
                    for task in active_futures.values()
                }
                tasks = self._claim_due_scheduler_tasks(
                    limit=max(1, concurrency - len(active_futures)),
                    exclude_preferred_accounts=active_accounts,
                )
                if not tasks:
                    if not active_accounts:
                        break
                    tasks = self._claim_due_scheduler_tasks(
                        limit=max(1, concurrency - len(active_futures)),
                    )
                if not tasks:
                    break
                if not hasattr(self, "_scheduler_executor"):
                    self._scheduler_executor = ThreadPoolExecutor(
                        max_workers=concurrency,
                        thread_name_prefix="db-chat-sync-worker",
                    )
                for task in tasks:
                    if len(active_futures) >= concurrency:
                        break
                    future = self._scheduler_executor.submit(
                        self._execute_scheduler_task,
                        task,
                    )
                    active_futures[future] = task
            if not active_futures:
                self._worker_stop.wait(_DEFAULT_EVENT_IDLE_SLEEP_SECONDS)
                continue
            done_futures, _pending = wait(
                active_futures.keys(),
                timeout=_DEFAULT_EVENT_IDLE_SLEEP_SECONDS,
                return_when=FIRST_COMPLETED,
            )
            for future in done_futures:
                task = active_futures.pop(future)
                try:
                    result = future.result()
                except Exception as exc:
                    logging.exception("同步调度任务执行异常: chat_id=%s", task.chat_id)
                    result = SyncUpdateResult(
                        chat_id=int(task.chat_id),
                        chat_title=task.chat_title,
                        chat_username=task.chat_username,
                        failure_type="failed",
                        failure_message=admin_error_message(exc),
                    )
                self._finish_scheduler_task(task=task, result=result)
        for future, task in list(active_futures.items()):
            try:
                result = future.result()
            except Exception:
                result = SyncUpdateResult(
                    chat_id=int(task.chat_id),
                    chat_title=task.chat_title,
                    chat_username=task.chat_username,
                    failure_type="failed",
                    failure_message="服务停止，调度任务未完成",
                )
            self._finish_scheduler_task(task=task, result=result)
        executor = getattr(self, "_scheduler_executor", None)
        if executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)

    def _execute_scheduler_task(self, task: SyncPendingTask) -> SyncUpdateResult | None:
        item = self._scheduler_task_to_queue_item(task)
        return self._process_queued_chat_update(item)

    def _run_sync_model_training_once(self) -> dict[str, Any]:
        conn = None
        try:
            from tg_harvest.ml.sync_predictor import train_sync_model

            conn = self._get_conn_fn()
            sync_scheduler.prune_learning_events(conn, self._cfg)
            result = train_sync_model(conn, self._cfg)
            backend = str(result.get("backend") or "")
            if bool(result.get("trained")):
                logging.info(
                    "同步调度模型训练完成: backend=%s sample_count=%s",
                    backend,
                    result.get("sample_count"),
                )
            elif backend and backend not in {"disabled", "waiting_for_samples"}:
                logging.info(
                    "同步调度模型训练未执行: backend=%s sample_count=%s",
                    backend,
                    result.get("sample_count"),
                )
            return result
        except Exception:
            logging.exception("同步调度模型训练失败")
            return {"ok": False, "trained": False, "backend": "failed"}
        finally:
            if conn is not None:
                with suppress(Exception):
                    conn.close()

    def _model_training_loop(self) -> None:
        if self._model_stop.wait(5.0):
            return
        while not self._model_stop.is_set():
            self._run_sync_model_training_once()
            interval_seconds = max(
                60,
                int(
                    getattr(
                        self._cfg,
                        "sync_model_train_interval_seconds",
                        1800,
                    )
                    or 1800
                ),
            )
            if self._model_stop.wait(float(interval_seconds)):
                break

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
    ) -> SyncUpdateResult:
        self._record_update_attempt()
        worker_id = f"{self._job_id}_{account.key}_single_{item.chat_id}"
        client = None
        account_lock = self._account_runtime.account_lock(account.key)
        with account_lock:
            started_at = time.perf_counter()
            self._account_runtime.mark_update_start(account)
            try:
                client = _create_isolated_worker_client(account.cfg, worker_id)
                harvest_result = _admin_process_single_chat_update(
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
                counters = getattr(harvest_result, "counters", None)
                scanned_count = int(getattr(counters, "seen", 0) or 0)
                added_count = int(getattr(counters, "written", 0) or 0)
                remote_last_id = 0
                local_last_id = self._load_local_last_message_id(item.chat_id)
                duration_seconds = max(0.0, time.perf_counter() - started_at)
                self._record_update_success(chat_id=item.chat_id)
                self._account_runtime.mark_update_finish(
                    account,
                    success=True,
                    duration_seconds=duration_seconds,
                )
                return SyncUpdateResult(
                    chat_id=int(item.chat_id),
                    chat_title=item.chat_title,
                    chat_username=item.chat_username,
                    source_account=account.key,
                    added_message_count=added_count,
                    scanned_message_count=scanned_count,
                    local_last_id=local_last_id,
                    remote_last_id=remote_last_id,
                    duration_seconds=duration_seconds,
                    api_cost=max(1.0, float(scanned_count or added_count or 1)),
                )
            except Exception as exc:
                self._account_runtime.mark_update_finish(
                    account,
                    success=False,
                    duration_seconds=max(0.0, time.perf_counter() - started_at),
                    failure_message=admin_error_message(exc),
                )
                raise
            finally:
                if client is not None:
                    with suppress(Exception):
                        _disconnect_worker_client(client)
                _cleanup_isolated_worker_session(account.cfg, worker_id)

    def _process_queued_chat_update(
        self, item: _QueuedChatUpdate
    ) -> SyncUpdateResult | None:
        if not self._is_database_chat(item.chat_id):
            return SyncUpdateResult(
                chat_id=int(item.chat_id),
                chat_title=item.chat_title,
                chat_username=item.chat_username,
                source_account=item.source_account,
                failure_type="deleted",
                failure_message="群组已不在数据库中",
            )
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
                return self._attempt_single_chat_update(account=account, item=item)
            except Exception as exc:
                last_exc = exc
                if isinstance(exc, AccountFloodWaitError):
                    _remember_account_cooldown(account, exc)
                    self._account_runtime.mark_cooldown(account, int(exc.seconds))
                    continue
                message = admin_error_message(exc)
                self._record_update_failure(chat_id=item.chat_id, message=message)
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
            return SyncUpdateResult(
                chat_id=int(item.chat_id),
                chat_title=item.chat_title,
                chat_username=item.chat_username,
                source_account=item.source_account,
                failure_type="flood_wait",
                failure_message=f"FloodWait {int(last_exc.seconds)}s",
                retry_after_seconds=max(60, int(last_exc.seconds)),
            )

        if last_exc is None and not tried_any:
            self._suppress_chat_temporarily(item.chat_id, seconds=120)
            return SyncUpdateResult(
                chat_id=int(item.chat_id),
                chat_title=item.chat_title,
                chat_username=item.chat_username,
                source_account=item.source_account,
                failure_type="no_account",
                failure_message="没有可用账号执行更新",
                retry_after_seconds=120,
            )

        if last_exc is not None:
            message = admin_error_message(last_exc)
            failure_type = "failed"
            if (
                "本地实体缓存未命中" in message
                or "不存在" in message
                or "解散" in message
            ):
                failure_type = "unavailable"
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
            return SyncUpdateResult(
                chat_id=int(item.chat_id),
                chat_title=item.chat_title,
                chat_username=item.chat_username,
                source_account=item.source_account,
                failure_type=failure_type,
                failure_message=message,
                retry_after_seconds=int(_DEFAULT_MISSED_CHAT_COOLDOWN_SECONDS)
                if failure_type == "unavailable"
                else 0,
            )
        return None

    def _public_probe_candidate_rows(self) -> list[dict[str, Any]]:
        rows = self._database_chat_rows()
        if not rows:
            return []

        accounts = self._listener_accounts()
        if not accounts:
            return []
        all_account_keys = [account.key for account in accounts]
        joined_by_account = self._joined_chat_ids_by_account_snapshot()
        cached_by_account = self._session_cached_chat_ids_by_account()
        use_joined_snapshot = any(joined_by_account.values())

        probe_rows: list[dict[str, Any]] = []
        for row in rows:
            chat_id = int(row["chat_id"])
            chat_username = clean_username(row.get("chat_username"))
            joined_account_keys = [
                account_key
                for account_key, joined_chat_ids in joined_by_account.items()
                if chat_id in joined_chat_ids
            ]
            cached_account_keys = [
                account_key
                for account_key, cached_chat_ids in cached_by_account.items()
                if chat_id in cached_chat_ids
            ]

            probe_scope = ""
            preferred_account_keys: list[str] = []
            probe_reason = _DEFAULT_PROBE_EVENT_REASON

            if joined_account_keys:
                probe_scope = "joined"
                preferred_account_keys = joined_account_keys
                probe_reason = "joined_probe"
            elif chat_username:
                probe_scope = "public"
                preferred_account_keys = cached_account_keys or all_account_keys
                probe_reason = _DEFAULT_PROBE_EVENT_REASON
            elif cached_account_keys:
                probe_scope = "cached"
                preferred_account_keys = cached_account_keys
                probe_reason = "cached_probe"
            elif not use_joined_snapshot:
                # 启动早期或 joined 快照异常时，尽量只保留明确能解析的公开群。
                continue
            else:
                continue

            candidate_row = dict(row)
            candidate_row["chat_username"] = chat_username or ""
            candidate_row["probe_scope"] = probe_scope
            candidate_row["probe_reason"] = probe_reason
            candidate_row["probe_account_keys"] = tuple(preferred_account_keys)
            candidate_row["probe_inactive"] = self._probe_row_is_inactive(candidate_row)
            probe_rows.append(candidate_row)
        return probe_rows

    def _public_probe_hot_row_sort_key(self, row: dict[str, Any]) -> tuple[Any, ...]:
        scope_priority = {"public": 0, "cached": 1, "joined": 2}.get(
            str(row.get("probe_scope") or "").strip(),
            3,
        )
        return (
            scope_priority,
            -max(0, int(row.get("last_message_ts", 0) or 0)),
            -max(0, int(self._probe_row_last_activity_ts(row)) or 0),
            -max(0, int(row.get("last_message_id", 0) or 0)),
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

    def _next_due_public_probe_rows(
        self,
        rows: list[dict[str, Any]],
        *,
        batch_size: int,
        cursor_key: str,
    ) -> list[dict[str, Any]]:
        if not rows or batch_size <= 0:
            return []
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
            start = int(self._public_probe_cursor.get(str(cursor_key), 0) or 0) % total
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

            self._public_probe_cursor[str(cursor_key)] = (
                next_hot_index + len(hot_rows) + next_cold_index
            )
        return merged_rows

    def _merge_public_probe_batches(
        self,
        *batches: list[dict[str, Any]],
        batch_size: int,
    ) -> list[dict[str, Any]]:
        if batch_size <= 0:
            return []
        pending = [list(batch) for batch in batches if batch]
        if not pending:
            return []
        merged_rows: list[dict[str, Any]] = []
        seen_chat_ids: set[int] = set()
        while pending and len(merged_rows) < batch_size:
            next_pending: list[list[dict[str, Any]]] = []
            for batch in pending:
                while batch and int(batch[0].get("chat_id") or 0) in seen_chat_ids:
                    batch.pop(0)
                if not batch:
                    continue
                row = batch.pop(0)
                chat_id = int(row.get("chat_id") or 0)
                if chat_id > 0 and chat_id not in seen_chat_ids:
                    merged_rows.append(row)
                    seen_chat_ids.add(chat_id)
                if batch:
                    next_pending.append(batch)
                if len(merged_rows) >= batch_size:
                    break
            pending = next_pending
        return merged_rows

    def _next_public_probe_batch(self) -> list[dict[str, Any]]:
        rows = self._public_probe_candidate_rows()
        if not rows:
            return []
        batch_size = max(
            1,
            int(getattr(self._cfg, "db_listener_public_probe_batch_size", 4) or 4),
        )
        joined_rows = [
            dict(row)
            for row in rows
            if str(row.get("probe_scope") or "").strip() == "joined"
        ]
        unjoined_rows = [
            dict(row)
            for row in rows
            if str(row.get("probe_scope") or "").strip() != "joined"
        ]

        if joined_rows and unjoined_rows and batch_size > 1:
            unjoined_quota = max(1, min(batch_size - 1, (batch_size + 1) // 2))
            joined_quota = max(1, batch_size - unjoined_quota)
        elif unjoined_rows:
            unjoined_quota = batch_size
            joined_quota = 0
        else:
            unjoined_quota = 0
            joined_quota = batch_size

        unjoined_batch = self._next_due_public_probe_rows(
            unjoined_rows,
            batch_size=unjoined_quota,
            cursor_key="unjoined",
        )
        joined_batch = self._next_due_public_probe_rows(
            joined_rows,
            batch_size=joined_quota,
            cursor_key="joined",
        )
        merged_rows = self._merge_public_probe_batches(
            unjoined_batch,
            joined_batch,
            batch_size=batch_size,
        )
        seen_chat_ids = {int(row.get("chat_id") or 0) for row in merged_rows}
        remaining_slots = max(0, batch_size - len(merged_rows))
        if remaining_slots > 0 and unjoined_rows:
            extra_unjoined_rows = self._next_due_public_probe_rows(
                unjoined_rows,
                batch_size=remaining_slots,
                cursor_key="unjoined",
            )
            for row in extra_unjoined_rows:
                chat_id = int(row.get("chat_id") or 0)
                if chat_id <= 0 or chat_id in seen_chat_ids:
                    continue
                merged_rows.append(row)
                seen_chat_ids.add(chat_id)
                if len(merged_rows) >= batch_size:
                    return merged_rows
        remaining_slots = max(0, batch_size - len(merged_rows))
        if remaining_slots > 0 and joined_rows:
            extra_joined_rows = self._next_due_public_probe_rows(
                joined_rows,
                batch_size=remaining_slots,
                cursor_key="joined",
            )
            for row in extra_joined_rows:
                chat_id = int(row.get("chat_id") or 0)
                if chat_id <= 0 or chat_id in seen_chat_ids:
                    continue
                merged_rows.append(row)
                seen_chat_ids.add(chat_id)
                if len(merged_rows) >= batch_size:
                    break
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

    def _probe_account_priority_for_row(self, row: dict[str, Any]) -> list[_ListenerAccount]:
        accounts = self._listener_accounts()
        preferred_account_keys = [
            str(account_key or "").strip()
            for account_key in (row.get("probe_account_keys") or ())
            if str(account_key or "").strip()
        ]
        preferred_order = {
            account_key: index for index, account_key in enumerate(preferred_account_keys)
        }
        accounts.sort(
            key=lambda account: (
                0 if account.key in preferred_order else 1,
                preferred_order.get(account.key, len(preferred_order)),
                _account_cooldown_remaining(account),
                account.key,
            )
        )
        return accounts

    def _probe_public_row_with_account(
        self,
        *,
        row: dict[str, Any],
        account: _ListenerAccount,
    ) -> _PublicProbeRead:
        self._wait_for_public_probe_account_slot(account.key)
        worker_id = f"{self._job_id}_{account.key}_probe_{row['chat_id']}"
        client = None
        try:
            client = _create_isolated_worker_client(account.cfg, worker_id)
            chat_username = clean_username(row.get("chat_username"))
            entity = resolve_chat_entity(
                client,
                int(row["chat_id"]),
                chat_username,
                retry_scope="db-listener-probe",
            )
            remote_last_id = int(read_latest_message_id(client, entity) or 0)
            local_last_id = self._load_local_last_message_id(int(row["chat_id"]))
            if remote_last_id > max(0, local_last_id):
                self._enqueue_chat_update(
                    chat_id=int(row["chat_id"]),
                    chat_title=str(row.get("chat_title") or ""),
                    chat_username=chat_username,
                    reason=str(row.get("probe_reason") or _DEFAULT_PROBE_EVENT_REASON),
                    source_account=account.key,
                )
                return _PublicProbeRead(
                    changed=True,
                    remote_last_id=remote_last_id,
                    local_last_id=local_last_id,
                )
            return _PublicProbeRead(
                changed=False,
                remote_last_id=remote_last_id,
                local_last_id=local_last_id,
            )
        finally:
            if client is not None:
                with suppress(Exception):
                    _disconnect_worker_client(client)
            _cleanup_isolated_worker_session(account.cfg, worker_id)

    def _public_probe_base_cooldown_seconds(self, row: dict[str, Any]) -> int:
        scope = str(row.get("probe_scope") or "").strip()
        if scope == "joined":
            base_seconds = max(
                1800,
                int(
                    getattr(
                        self._cfg,
                        "db_listener_joined_probe_chat_cooldown_seconds",
                        10800,
                    )
                    or 10800
                ),
            )
        else:
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
        if bool(row.get("probe_inactive")):
            return max(
                base_seconds,
                int(
                    getattr(
                        self._cfg,
                        "db_listener_inactive_probe_chat_cooldown_seconds",
                        43200,
                    )
                    or 43200
                ),
            )
        return base_seconds

    def _public_probe_success_cooldown_seconds(
        self,
        row: dict[str, Any],
        *,
        changed: bool,
    ) -> int:
        base_seconds = self._public_probe_base_cooldown_seconds(row)
        if changed:
            scope = str(row.get("probe_scope") or "").strip()
            short_cooldown_seconds = (
                _DEFAULT_JOINED_PROBE_CHANGED_COOLDOWN_SECONDS
                if scope == "joined"
                else _DEFAULT_PUBLIC_PROBE_CHANGED_COOLDOWN_SECONDS
            )
            return max(300, min(base_seconds, int(short_cooldown_seconds)))
        return base_seconds

    def _public_probe_failure_cooldown_seconds(self, *, flood_wait_seconds: int = 0) -> int:
        if flood_wait_seconds > 0:
            return max(120, min(int(flood_wait_seconds), 900))
        return 180

    def _record_persistent_probe_result(
        self,
        *,
        row: dict[str, Any],
        outcome: _PublicProbeOutcome,
    ) -> None:
        conn = None
        try:
            conn = self._get_conn_fn()
            sync_scheduler.record_probe_result(
                conn,
                chat_id=int(row.get("chat_id") or 0),
                chat_title=str(row.get("chat_title") or ""),
                chat_username=clean_username(row.get("chat_username")),
                status=outcome.status,
                source_account=outcome.source_account,
                remote_last_id=int(outcome.remote_last_id or 0),
                local_last_id=int(outcome.local_last_id or 0),
                cooldown_seconds=int(outcome.cooldown_seconds or 0),
                reason=str(row.get("probe_reason") or "probe"),
            )
        except Exception:
            logging.exception(
                "写入同步调度 probe 结果失败: chat_id=%s",
                row.get("chat_id"),
            )
        finally:
            if conn is not None:
                with suppress(Exception):
                    conn.close()

    def _probe_public_row(self, row: dict[str, Any]) -> _PublicProbeOutcome:
        self._record_probe_attempt()
        accounts = self._probe_account_priority_for_row(row)
        if not accounts:
            outcome = _PublicProbeOutcome(status="no_account", cooldown_seconds=300)
            self._record_probe_result(
                status=outcome.status,
                chat_id=int(row.get("chat_id") or 0),
            )
            self._record_persistent_probe_result(row=row, outcome=outcome)
            return outcome
        last_exc: Exception | None = None
        cache_miss_only = False
        for account in accounts:
            if _account_cooldown_remaining(account) > 0:
                continue
            if not _ensure_base_session_valid(account.cfg, self._job_id, _listener_log):
                continue
            try:
                raw_read_result = self._probe_public_row_with_account(row=row, account=account)
                read_result = (
                    raw_read_result
                    if isinstance(raw_read_result, _PublicProbeRead)
                    else _PublicProbeRead(
                        changed=bool(raw_read_result),
                        remote_last_id=0,
                        local_last_id=0,
                    )
                )
                outcome = _PublicProbeOutcome(
                    status="changed" if read_result.changed else "unchanged",
                    cooldown_seconds=self._public_probe_success_cooldown_seconds(
                        row,
                        changed=read_result.changed
                    ),
                    source_account=account.key,
                    remote_last_id=int(read_result.remote_last_id or 0),
                    local_last_id=int(read_result.local_last_id or 0),
                )
                self._record_probe_result(
                    status=outcome.status,
                    chat_id=int(row.get("chat_id") or 0),
                )
                self._record_persistent_probe_result(row=row, outcome=outcome)
                return outcome
            except Exception as exc:
                last_exc = exc
                if isinstance(exc, AccountFloodWaitError):
                    _remember_account_cooldown(account, exc)
                    self._account_runtime.mark_cooldown(account, int(exc.seconds))
                    continue
                message = admin_error_message(exc)
                if "本地实体缓存未命中" in message:
                    cache_miss_only = True
                    continue
                cache_miss_only = False
                if "不存在" in message or "解散" in message:
                    outcome = _PublicProbeOutcome(
                        status="missing",
                        cooldown_seconds=int(_DEFAULT_MISSED_CHAT_COOLDOWN_SECONDS),
                        source_account=account.key,
                    )
                    self._record_probe_result(
                        status=outcome.status,
                        chat_id=int(row.get("chat_id") or 0),
                    )
                    self._record_persistent_probe_result(row=row, outcome=outcome)
                    return outcome
                outcome = _PublicProbeOutcome(
                    status="failed",
                    cooldown_seconds=self._public_probe_failure_cooldown_seconds(),
                    source_account=account.key,
                )
                self._record_probe_result(
                    status=outcome.status,
                    chat_id=int(row.get("chat_id") or 0),
                )
                self._record_persistent_probe_result(row=row, outcome=outcome)
                return outcome
        if isinstance(last_exc, AccountFloodWaitError):
            outcome = _PublicProbeOutcome(
                status="flood_wait",
                cooldown_seconds=self._public_probe_failure_cooldown_seconds(
                    flood_wait_seconds=int(last_exc.seconds)
                ),
                source_account=str(last_exc.account_label or ""),
            )
            self._record_probe_result(
                status=outcome.status,
                chat_id=int(row.get("chat_id") or 0),
            )
            self._record_persistent_probe_result(row=row, outcome=outcome)
            return outcome
        if last_exc is not None and cache_miss_only:
            outcome = _PublicProbeOutcome(
                status="cache_miss",
                cooldown_seconds=max(
                    600,
                    self._public_probe_base_cooldown_seconds(row),
                ),
            )
            self._record_probe_result(
                status=outcome.status,
                chat_id=int(row.get("chat_id") or 0),
            )
            self._record_persistent_probe_result(row=row, outcome=outcome)
            return outcome
        if last_exc is not None:
            outcome = _PublicProbeOutcome(
                status="failed",
                cooldown_seconds=self._public_probe_failure_cooldown_seconds(),
                source_account="",
            )
            self._record_probe_result(
                status=outcome.status,
                chat_id=int(row.get("chat_id") or 0),
            )
            self._record_persistent_probe_result(row=row, outcome=outcome)
            return outcome
        outcome = _PublicProbeOutcome(status="no_account", cooldown_seconds=300)
        self._record_probe_result(
            status=outcome.status,
            chat_id=int(row.get("chat_id") or 0),
        )
        self._record_persistent_probe_result(row=row, outcome=outcome)
        return outcome

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
                    logging.exception("数据库群组低频轮巡探测失败: chat_id=%s", chat_id)
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


def get_database_chat_listener_runtime() -> DatabaseChatListenerRuntime | None:
    with _LISTENER_SINGLETON_LOCK:
        return _LISTENER_SINGLETON
