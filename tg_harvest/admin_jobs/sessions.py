import asyncio
import logging
import os
import shutil
import sqlite3
import threading
from collections.abc import Callable
from contextlib import contextmanager, suppress
from pathlib import Path
from typing import Any

from telethon.sync import TelegramClient

from tg_harvest.ingest.flood_wait import flood_sleep_threshold_kwargs

JOB_HEARTBEAT_INTERVAL_SEC = 30.0
_SESSION_FILE_EXTENSIONS = (".session", ".session-journal", ".session-wal", ".session-shm")
_SESSION_FILE_LOCKS: dict[str, threading.Lock] = {}
_SESSION_FILE_LOCKS_GUARD = threading.Lock()


def _current_event_loop_or_none() -> asyncio.AbstractEventLoop | None:
    try:
        return asyncio.get_event_loop()
    except RuntimeError:
        return None


def _is_open_asyncio_loop(loop: Any) -> bool:
    return isinstance(loop, asyncio.AbstractEventLoop) and not loop.is_closed()


def _restore_event_loop(loop: Any | None) -> None:
    if _is_open_asyncio_loop(loop):
        asyncio.set_event_loop(loop)
        return
    asyncio.set_event_loop(None)


def _client_event_loop(client: Any) -> asyncio.AbstractEventLoop | None:
    loop = getattr(client, "_tg_harvest_loop", None)
    if _is_open_asyncio_loop(loop):
        return loop
    return None


def _session_sqlite_path(session_name: Any) -> Path:
    path = Path(str(session_name or ""))
    if path.suffix != ".session":
        path = Path(str(path) + ".session")
    return path


def _session_file_lock(session_name: Any) -> threading.Lock:
    key = str(_session_sqlite_path(session_name).resolve())
    with _SESSION_FILE_LOCKS_GUARD:
        lock = _SESSION_FILE_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _SESSION_FILE_LOCKS[key] = lock
        return lock


def _copy_session_storage(base_session_name: Any, worker_session_name: Any) -> None:
    base_lock = _session_file_lock(base_session_name)
    with base_lock:
        for ext in _SESSION_FILE_EXTENSIONS:
            src = f"{base_session_name}{ext}"
            dst = f"{worker_session_name}{ext}"
            if os.path.exists(src):
                with suppress(Exception):
                    shutil.copy2(src, dst)


def _merge_worker_session_entities_into_base_session(
    base_session_name: Any,
    worker_session_name: Any,
) -> int:
    base_path = _session_sqlite_path(base_session_name)
    worker_path = _session_sqlite_path(worker_session_name)
    if not base_path.exists() or not worker_path.exists():
        return 0

    lock = _session_file_lock(base_session_name)
    with lock:
        conn = None
        try:
            conn = sqlite3.connect(str(base_path), timeout=30)
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("ATTACH DATABASE ? AS worker_db", (str(worker_path),))
            before_changes = conn.total_changes
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT OR REPLACE INTO entities(id, hash, username, phone, name, date)
                SELECT
                    w.id,
                    w.hash,
                    w.username,
                    w.phone,
                    w.name,
                    w.date
                FROM worker_db.entities w
                LEFT JOIN entities b
                  ON b.id = w.id
                WHERE b.id IS NULL
                   OR COALESCE(w.date, 0) > COALESCE(b.date, 0)
                   OR (
                        COALESCE(b.username, '') = ''
                        AND COALESCE(w.username, '') <> ''
                    )
                   OR (
                        COALESCE(b.phone, 0) = 0
                        AND COALESCE(w.phone, 0) <> 0
                    )
                   OR (
                        COALESCE(b.name, '') = ''
                        AND COALESCE(w.name, '') <> ''
                    )
                   OR (
                        COALESCE(w.date, 0) = COALESCE(b.date, 0)
                        AND (
                            COALESCE(w.username, '') <> COALESCE(b.username, '')
                            OR COALESCE(w.phone, 0) <> COALESCE(b.phone, 0)
                            OR COALESCE(w.name, '') <> COALESCE(b.name, '')
                        )
                    )
                """
            )
            conn.commit()
            return max(0, int(conn.total_changes - before_changes))
        except Exception:
            if conn is not None:
                with suppress(Exception):
                    conn.rollback()
            logging.exception(
                "合并 Telegram worker session 实体缓存失败: base=%s worker=%s",
                base_path,
                worker_path,
            )
            return 0
        finally:
            if conn is not None:
                with suppress(Exception):
                    conn.execute("DETACH DATABASE worker_db")
                with suppress(Exception):
                    conn.close()


@contextmanager
def bind_client_event_loop(client: Any):
    loop = _client_event_loop(client)
    if loop is None:
        yield
        return

    previous_loop = _current_event_loop_or_none()
    asyncio.set_event_loop(loop)
    try:
        yield
    finally:
        _restore_event_loop(previous_loop)


def _ensure_base_session_valid(cfg: Any, job_id: str, append_log_fn: Callable) -> bool:
    previous_loop = _current_event_loop_or_none()
    loop = None
    client = None
    try:
        Path(str(cfg.session_name)).parent.mkdir(parents=True, exist_ok=True)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        client = TelegramClient(
            str(cfg.session_name), cfg.api_id, cfg.api_hash, loop=loop,
            receive_updates=False,
            **flood_sleep_threshold_kwargs(cfg),
        )
        client.connect()
        if not client.is_user_authorized():
            append_log_fn(
                job_id,
                "Telegram 未登录！请在终端运行 python3 scripts/get_telegram_code.py 完成登录。",
            )
            return False
        return True
    except sqlite3.OperationalError as e:
        if "database is locked" in str(e).lower():
            append_log_fn(job_id, "验证 Session 失败：主文件已被占用。")
        else:
            append_log_fn(job_id, f"Session 数据库异常: {e}")
        return False
    except Exception as e:
        append_log_fn(job_id, f"初始化 Session 失败: {e}")
        return False
    finally:
        if client is not None:
            with suppress(Exception):
                client.disconnect()
        if loop is not None:
            with suppress(Exception):
                loop.close()
        _restore_event_loop(previous_loop)


def _create_isolated_worker_client(cfg: Any, worker_id: str) -> TelegramClient:
    base_session_name = str(cfg.session_name)
    worker_session_name = f"{base_session_name}_worker_{worker_id}"
    Path(worker_session_name).parent.mkdir(parents=True, exist_ok=True)

    _copy_session_storage(base_session_name, worker_session_name)

    previous_loop = _current_event_loop_or_none()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    client = None
    try:
        client = TelegramClient(
            worker_session_name,
            cfg.api_id,
            cfg.api_hash,
            loop=loop,
            request_retries=5,
            connection_retries=5,
            timeout=15,
            receive_updates=False,
            **flood_sleep_threshold_kwargs(cfg),
        )
        client._tg_harvest_loop = loop
        client._tg_harvest_previous_loop = previous_loop
        client.connect()
        return client
    except Exception:
        if client is not None:
            with suppress(Exception):
                client.disconnect()
        with suppress(Exception):
            loop.close()
        _restore_event_loop(previous_loop)
        raise


def _disconnect_worker_client(client: Any) -> None:
    loop = getattr(client, "_tg_harvest_loop", None)
    missing_previous_loop = object()
    previous_loop = getattr(client, "_tg_harvest_previous_loop", missing_previous_loop)
    try:
        with bind_client_event_loop(client):
            client.disconnect()
    finally:
        if loop is not None:
            close_loop = getattr(loop, "close", None)
            with suppress(Exception):
                if callable(close_loop):
                    close_loop()
        if previous_loop is not missing_previous_loop:
            _restore_event_loop(previous_loop)
        elif _current_event_loop_or_none() is loop:
            _restore_event_loop(None)


def _cleanup_isolated_worker_session(cfg: Any, worker_id: str):
    worker_session_name = f"{cfg.session_name}_worker_{worker_id}"
    _merge_worker_session_entities_into_base_session(
        cfg.session_name,
        worker_session_name,
    )
    for ext in _SESSION_FILE_EXTENSIONS:
        try:
            path = f"{worker_session_name}{ext}"
            if os.path.exists(path):
                os.remove(path)
        except Exception:
            pass


def _start_job_heartbeat(job_id: str, heartbeat_fn: Callable[[str], bool]) -> tuple[threading.Event, threading.Thread]:
    stop_event = threading.Event()

    def _heartbeat_loop() -> None:
        while not stop_event.wait(JOB_HEARTBEAT_INTERVAL_SEC):
            try:
                heartbeat_fn(job_id)
            except Exception:
                import logging

                logging.exception("后台任务心跳更新失败: job_id=%s", job_id)

    thread = threading.Thread(
        target=_heartbeat_loop,
        name=f"job-heartbeat-{job_id[:8]}",
        daemon=True,
    )
    thread.start()
    return stop_event, thread
