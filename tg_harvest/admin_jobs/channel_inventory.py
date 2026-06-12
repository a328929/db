import logging
from collections.abc import Callable
from contextlib import suppress
from typing import Any

from tg_harvest.admin_jobs.common import (
    admin_error_message,
    finish_job_heartbeat,
    start_admin_job_thread,
)
from tg_harvest.admin_jobs.core import (
    _admin_job_heartbeat,
    _admin_job_update_progress,
    job_context,
)
from tg_harvest.admin_jobs.runtime import _admin_now_iso
from tg_harvest.admin_jobs.sessions import (
    _cleanup_isolated_worker_session,
    _create_isolated_worker_client,
    _disconnect_worker_client,
    _ensure_base_session_valid,
    _start_job_heartbeat,
)
from tg_harvest.domain.chat_inventory import (
    find_database_chats_not_joined,
    find_missing_joined_chats,
    find_restricted_joined_chats,
    load_joined_chat_inventory,
    load_known_chat_ids,
)
from tg_harvest.storage.channel_management import (
    list_database_channels,
    replace_absent_chat_scan_results,
    replace_missing_chat_scan_results,
    replace_restricted_chat_scan_results,
)


def _admin_missing_chats_scan_job_runner(
    job_id: str,
    *,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    job_context.set(str(job_id))
    heartbeat_stop, heartbeat_thread = _start_job_heartbeat(job_id, _admin_job_heartbeat)
    local_client = None
    worker_id = f"{job_id}_missing_chats"
    try:
        admin_job_set_status_fn(job_id, "running")
        _admin_job_update_progress(
            job_id,
            0,
            total=None,
            stage="running",
            log_step=0,
            auto_log=False,
        )
        admin_job_append_log_fn(job_id, "正在验证 Telegram 会话...")
        if not _ensure_base_session_valid(cfg, job_id, admin_job_append_log_fn):
            admin_job_set_status_fn(job_id, "error")
            return

        admin_job_append_log_fn(job_id, "正在读取数据库已有群组清单...")
        conn = get_conn_fn()
        try:
            known_chat_ids = load_known_chat_ids(conn)
        finally:
            conn.close()
        admin_job_append_log_fn(job_id, f"数据库中已有 {len(known_chat_ids)} 个群组/频道")

        admin_job_append_log_fn(job_id, "正在连接 Telegram 并扫描当前账号已加入会话...")
        local_client = _create_isolated_worker_client(cfg, worker_id)
        rows = find_missing_joined_chats(local_client.iter_dialogs(), known_chat_ids)
        scanned_at = _admin_now_iso()

        admin_job_append_log_fn(job_id, "正在保存扫描结果...")
        write_conn = get_conn_fn()
        try:
            saved_count = replace_missing_chat_scan_results(
                write_conn,
                rows,
                scan_job_id=job_id,
                scanned_at=scanned_at,
            )
        finally:
            write_conn.close()

        _admin_job_update_progress(
            job_id,
            saved_count,
            total=saved_count,
            stage="done",
            log_step=0,
            auto_log=False,
        )
        admin_job_append_log_fn(
            job_id,
            f"扫描完成：发现 {saved_count} 个已加入但未入库的群组/频道",
        )
        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        logging.exception("扫描未入库群组失败: job_id=%s", job_id)
        admin_job_append_log_fn(job_id, f"扫描失败：{admin_error_message(exc)}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        if local_client:
            with suppress(Exception):
                _disconnect_worker_client(local_client)
        _cleanup_isolated_worker_session(cfg, worker_id)


def _admin_start_missing_chats_scan_job_thread(job_id: str, **kwargs):
    return start_admin_job_thread(
        _admin_missing_chats_scan_job_runner,
        job_id,
        **kwargs,
    )


def _admin_absent_chats_scan_job_runner(
    job_id: str,
    *,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    job_context.set(str(job_id))
    heartbeat_stop, heartbeat_thread = _start_job_heartbeat(job_id, _admin_job_heartbeat)
    local_client = None
    worker_id = f"{job_id}_absent_chats"
    try:
        admin_job_set_status_fn(job_id, "running")
        _admin_job_update_progress(
            job_id,
            0,
            total=None,
            stage="running",
            log_step=0,
            auto_log=False,
        )
        admin_job_append_log_fn(job_id, "正在验证 Telegram 会话...")
        if not _ensure_base_session_valid(cfg, job_id, admin_job_append_log_fn):
            admin_job_set_status_fn(job_id, "error")
            return

        admin_job_append_log_fn(job_id, "正在读取数据库已有群组清单...")
        conn = get_conn_fn()
        try:
            database_rows = list_database_channels(conn, sort="message_count_desc")
        finally:
            conn.close()
        admin_job_append_log_fn(job_id, f"数据库中已有 {len(database_rows)} 个群组/频道")

        admin_job_append_log_fn(job_id, "正在连接 Telegram 并扫描当前账号已加入会话...")
        local_client = _create_isolated_worker_client(cfg, worker_id)
        joined_rows = load_joined_chat_inventory(local_client.iter_dialogs())
        unavailable_chat_reasons = {
            int(row.chat_id): str(row.unavailable_reason).strip()
            for row in joined_rows
            if str(row.unavailable_reason or "").strip()
        }
        joined_chat_ids = {
            int(row.chat_id) for row in joined_rows if not row.unavailable_reason
        }
        admin_job_append_log_fn(
            job_id,
            f"当前账号可访问 {len(joined_chat_ids)} 个群组/频道，"
            f"发现 {len(unavailable_chat_reasons)} 个已加入但不可用的群组/频道",
        )

        rows = find_database_chats_not_joined(
            database_rows,
            joined_chat_ids,
            unavailable_chat_reasons=unavailable_chat_reasons,
        )
        scanned_at = _admin_now_iso()

        admin_job_append_log_fn(job_id, "正在保存扫描结果...")
        write_conn = get_conn_fn()
        try:
            saved_count = replace_absent_chat_scan_results(
                write_conn,
                rows,
                scan_job_id=job_id,
                scanned_at=scanned_at,
            )
        finally:
            write_conn.close()

        _admin_job_update_progress(
            job_id,
            saved_count,
            total=saved_count,
            stage="done",
            log_step=0,
            auto_log=False,
        )
        admin_job_append_log_fn(
            job_id,
            f"扫描完成：发现 {saved_count} 个数据库中存在但账号未加入或不可用的群组/频道",
        )
        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        logging.exception("扫描账号未加入数据库群组失败: job_id=%s", job_id)
        admin_job_append_log_fn(job_id, f"扫描失败：{admin_error_message(exc)}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        if local_client:
            with suppress(Exception):
                _disconnect_worker_client(local_client)
        _cleanup_isolated_worker_session(cfg, worker_id)


def _admin_start_absent_chats_scan_job_thread(job_id: str, **kwargs):
    return start_admin_job_thread(
        _admin_absent_chats_scan_job_runner,
        job_id,
        **kwargs,
    )


def _admin_restricted_chats_scan_job_runner(
    job_id: str,
    *,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    job_context.set(str(job_id))
    heartbeat_stop, heartbeat_thread = _start_job_heartbeat(job_id, _admin_job_heartbeat)
    local_client = None
    worker_id = f"{job_id}_restricted_chats"
    try:
        admin_job_set_status_fn(job_id, "running")
        _admin_job_update_progress(
            job_id,
            0,
            total=None,
            stage="running",
            log_step=0,
            auto_log=False,
        )
        admin_job_append_log_fn(job_id, "正在验证 Telegram 会话...")
        if not _ensure_base_session_valid(cfg, job_id, admin_job_append_log_fn):
            admin_job_set_status_fn(job_id, "error")
            return

        admin_job_append_log_fn(
            job_id,
            "正在连接 Telegram 并扫描内容限制/风险标记...",
        )
        local_client = _create_isolated_worker_client(cfg, worker_id)
        rows = find_restricted_joined_chats(local_client.iter_dialogs())
        scanned_at = _admin_now_iso()

        admin_job_append_log_fn(job_id, "正在保存扫描结果...")
        write_conn = get_conn_fn()
        try:
            saved_count = replace_restricted_chat_scan_results(
                write_conn,
                rows,
                scan_job_id=job_id,
                scanned_at=scanned_at,
            )
        finally:
            write_conn.close()

        _admin_job_update_progress(
            job_id,
            saved_count,
            total=saved_count,
            stage="done",
            log_step=0,
            auto_log=False,
        )
        admin_job_append_log_fn(
            job_id,
            f"扫描完成：发现 {saved_count} 个带 Telegram 内容限制/风险标记的群组/频道",
        )
        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        logging.exception("扫描内容限制群组失败: job_id=%s", job_id)
        admin_job_append_log_fn(job_id, f"扫描失败：{admin_error_message(exc)}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        if local_client:
            with suppress(Exception):
                _disconnect_worker_client(local_client)
        _cleanup_isolated_worker_session(cfg, worker_id)


def _admin_start_restricted_chats_scan_job_thread(job_id: str, **kwargs):
    return start_admin_job_thread(
        _admin_restricted_chats_scan_job_runner,
        job_id,
        **kwargs,
    )
