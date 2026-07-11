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

from telethon.errors import RPCError
from telethon.sync import TelegramClient

from tg_harvest.ingest.flood_wait import flood_sleep_threshold_kwargs
from tg_harvest.runtime.paths import secure_session_artifacts

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
        secure_session_artifacts(base_session_name)
        for ext in _SESSION_FILE_EXTENSIONS:
            dst = f"{worker_session_name}{ext}"
            try:
                os.remove(dst)
            except FileNotFoundError:
                # A concurrent cleanup can win this race; there is no stale file left.
                pass
            except OSError:
                logging.exception(
                    "删除旧 worker Session 文件失败，拒绝复用可能过期的副本: %s",
                    dst,
                )
                raise
        for ext in _SESSION_FILE_EXTENSIONS:
            src = f"{base_session_name}{ext}"
            dst = f"{worker_session_name}{ext}"
            if os.path.exists(src):
                try:
                    shutil.copy2(src, dst)
                except OSError:
                    logging.exception(
                        "复制 worker Session 文件失败: source=%s destination=%s",
                        src,
                        dst,
                    )
                    raise
        secure_session_artifacts(worker_session_name)


def _configure_telethon_session_sqlite(client: Any) -> None:
    try:
        session = getattr(client, "session", None)
        conn = getattr(session, "_conn", None)
        if conn is None:
            return
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
    except (sqlite3.Error, AttributeError):
        logging.debug("Telethon session SQLite 并发保护配置失败", exc_info=True)


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
        except sqlite3.Error:
            if conn is not None:
                try:
                    conn.rollback()
                except sqlite3.Error:
                    logging.exception(
                        "回滚 Telegram worker session 实体缓存合并失败: base=%s worker=%s",
                        base_path,
                        worker_path,
                    )
            logging.exception(
                "合并 Telegram worker session 实体缓存失败: base=%s worker=%s",
                base_path,
                worker_path,
            )
            return 0
        finally:
            if conn is not None:
                try:
                    conn.execute("DETACH DATABASE worker_db")
                except sqlite3.Error:
                    # ATTACH can fail before a worker DB is attached; closing is sufficient.
                    logging.debug(
                        "分离 Telegram worker session 数据库失败: worker=%s",
                        worker_path,
                        exc_info=True,
                    )
                try:
                    conn.close()
                except sqlite3.Error:
                    logging.exception(
                        "关闭 Telegram worker session 数据库失败: base=%s",
                        base_path,
                    )


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
        _configure_telethon_session_sqlite(client)
        client.connect()
        secure_session_artifacts(cfg.session_name)
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
    except (sqlite3.Error, OSError) as e:
        logging.exception("验证 Telegram Session 失败: session=%s", cfg.session_name)
        append_log_fn(job_id, f"Session 初始化失败: {e}")
        return False
    except (RPCError, ConnectionError, TimeoutError) as e:
        logging.exception("连接 Telegram 验证 Session 失败: session=%s", cfg.session_name)
        append_log_fn(job_id, f"连接 Telegram 失败: {e}")
        return False
    except Exception as e:
        logging.exception("初始化 Telegram Session 时发生未知错误: session=%s", cfg.session_name)
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
    return _create_isolated_worker_client_with_options(
        cfg,
        worker_id,
        receive_updates=False,
    )


def _create_isolated_worker_client_with_options(
    cfg: Any,
    worker_id: str,
    *,
    receive_updates: bool,
) -> TelegramClient:
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
            receive_updates=bool(receive_updates),
            **flood_sleep_threshold_kwargs(cfg),
        )
        client._tg_harvest_loop = loop
        client._tg_harvest_previous_loop = previous_loop
        _configure_telethon_session_sqlite(client)
        client.connect()
        secure_session_artifacts(worker_session_name)
        return client
    except Exception:
        # This is the worker-client crash boundary. Cleanup must not hide it.
        logging.exception("创建隔离 Telegram worker 客户端失败: worker_id=%s", worker_id)
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
        path = f"{worker_session_name}{ext}"
        try:
            os.remove(path)
        except FileNotFoundError:
            # The worker may not have created every WAL/SHM artifact.
            continue
        except OSError:
            # Cleanup is non-authoritative after the worker exits, but stale
            # credentials must remain diagnosable for the next launch.
            logging.warning(
                "清理 worker Session 文件失败: worker_id=%s path=%s",
                worker_id,
                path,
                exc_info=True,
            )


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
