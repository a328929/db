import asyncio
import os
import shutil
import sqlite3
import threading
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import Any

from telethon.sync import TelegramClient

JOB_HEARTBEAT_INTERVAL_SEC = 30.0


def _ensure_base_session_valid(cfg: Any, job_id: str, append_log_fn: Callable) -> bool:
    loop = None
    client = None
    try:
        Path(str(cfg.session_name)).parent.mkdir(parents=True, exist_ok=True)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        client = TelegramClient(
            str(cfg.session_name), cfg.api_id, cfg.api_hash, loop=loop,
            receive_updates=False
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


def _create_isolated_worker_client(cfg: Any, worker_id: str) -> TelegramClient:
    base_session_name = str(cfg.session_name)
    worker_session_name = f"{base_session_name}_worker_{worker_id}"
    Path(worker_session_name).parent.mkdir(parents=True, exist_ok=True)

    for ext in [".session", ".session-journal", ".session-wal", ".session-shm"]:
        src = f"{base_session_name}{ext}"
        dst = f"{worker_session_name}{ext}"
        if os.path.exists(src):
            with suppress(Exception):
                shutil.copy2(src, dst)

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
            receive_updates=False
        )
        client._tg_harvest_loop = loop
        client.connect()
        return client
    except Exception:
        if client is not None:
            with suppress(Exception):
                client.disconnect()
        with suppress(Exception):
            loop.close()
        raise


def _disconnect_worker_client(client: Any) -> None:
    loop = getattr(client, "_tg_harvest_loop", None)
    try:
        client.disconnect()
    finally:
        if loop is not None:
            with suppress(Exception):
                loop.close()


def _cleanup_isolated_worker_session(cfg: Any, worker_id: str):
    worker_session_name = f"{cfg.session_name}_worker_{worker_id}"
    for ext in [".session", ".session-journal", ".session-wal", ".session-shm"]:
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
