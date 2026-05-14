# -*- coding: utf-8 -*-
import logging
import sqlite3
import threading
import time
from contextlib import closing
from typing import Callable

from tg_harvest.storage.search_terms import backfill_message_search_terms_upgrade_batch
from tg_harvest.storage.search_terms import drain_message_search_terms_rebuild_queue


_SEARCH_TERM_MAINTENANCE_LOCK = threading.Lock()
_SEARCH_TERM_MAINTENANCE_EVENT = threading.Event()
_SEARCH_TERM_MAINTENANCE_THREAD: threading.Thread | None = None
_SEARCH_TERM_MAINTENANCE_GET_CONN_FN: Callable[[], sqlite3.Connection] | None = None

_SEARCH_TERM_MAINTENANCE_BATCH_SIZE = 500
_SEARCH_TERM_UPGRADE_BATCH_SIZE = 5000
_SEARCH_TERM_MAINTENANCE_IDLE_SEC = 2.0
_SEARCH_TERM_MAINTENANCE_INTER_BATCH_SEC = 0.05


def configure_message_search_maintenance(
    get_conn_fn: Callable[[], sqlite3.Connection],
) -> None:
    global _SEARCH_TERM_MAINTENANCE_GET_CONN_FN, _SEARCH_TERM_MAINTENANCE_THREAD

    with _SEARCH_TERM_MAINTENANCE_LOCK:
        _SEARCH_TERM_MAINTENANCE_GET_CONN_FN = get_conn_fn
        thread = _SEARCH_TERM_MAINTENANCE_THREAD
        if thread is not None and thread.is_alive():
            _SEARCH_TERM_MAINTENANCE_EVENT.set()
            return

        thread = threading.Thread(
            target=_message_search_maintenance_worker,
            name="message-search-maintenance",
            daemon=True,
        )
        _SEARCH_TERM_MAINTENANCE_THREAD = thread
        thread.start()
        _SEARCH_TERM_MAINTENANCE_EVENT.set()


def schedule_message_search_maintenance() -> None:
    _SEARCH_TERM_MAINTENANCE_EVENT.set()


def _message_search_maintenance_worker() -> None:
    while True:
        _SEARCH_TERM_MAINTENANCE_EVENT.wait(timeout=_SEARCH_TERM_MAINTENANCE_IDLE_SEC)
        _SEARCH_TERM_MAINTENANCE_EVENT.clear()

        with _SEARCH_TERM_MAINTENANCE_LOCK:
            get_conn_fn = _SEARCH_TERM_MAINTENANCE_GET_CONN_FN

        if get_conn_fn is None:
            continue

        while True:
            try:
                from tg_harvest.admin_jobs.core import _admin_has_any_active_job

                if _admin_has_any_active_job():
                    break
                with closing(get_conn_fn()) as conn:
                    drained = drain_message_search_terms_rebuild_queue(
                        conn,
                        batch_size=_SEARCH_TERM_MAINTENANCE_BATCH_SIZE,
                    )
                    upgraded = 0
                    if drained <= 0:
                        upgraded = backfill_message_search_terms_upgrade_batch(
                            conn,
                            batch_size=_SEARCH_TERM_UPGRADE_BATCH_SIZE,
                        )
            except Exception:
                logging.exception("后台维护中文短词搜索索引失败")
                break

            if drained <= 0 and upgraded <= 0:
                break
            time.sleep(_SEARCH_TERM_MAINTENANCE_INTER_BATCH_SEC)
