import logging
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

from tg_harvest.admin_jobs.common import (
    admin_error_message,
    call_with_conn,
    finish_job_heartbeat,
    mark_admin_job_running,
    start_admin_job_heartbeat,
    start_admin_job_thread,
    update_admin_job_progress,
)
from tg_harvest.admin_jobs.runtime import _admin_now_iso
from tg_harvest.admin_jobs.sessions import (
    _cleanup_isolated_worker_session,
    _create_isolated_worker_client,
    _disconnect_worker_client,
    _ensure_base_session_valid,
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


@dataclass(frozen=True)
class _ChannelInventoryScanSpec:
    worker_suffix: str
    logger_message: str
    scan_rows_fn: Any
    replace_results_fn: Any
    build_success_message_fn: Callable[[int], str]


def _scan_missing_chat_rows(
    ensure_client: Callable[[], Any],
    *,
    get_conn_fn: Callable[[], Any],
    admin_job_append_log_fn: Callable[[str, str], Any],
    job_id: str,
) -> list[Any]:
    admin_job_append_log_fn(job_id, "正在读取数据库已有群组清单...")
    known_chat_ids = call_with_conn(get_conn_fn, load_known_chat_ids)
    admin_job_append_log_fn(job_id, f"数据库中已有 {len(known_chat_ids)} 个群组/频道")
    admin_job_append_log_fn(job_id, "正在连接 Telegram 并扫描当前账号已加入会话...")
    return find_missing_joined_chats(
        ensure_client().iter_dialogs(),
        known_chat_ids,
    )


def _scan_absent_chat_rows(
    ensure_client: Callable[[], Any],
    *,
    get_conn_fn: Callable[[], Any],
    admin_job_append_log_fn: Callable[[str, str], Any],
    job_id: str,
) -> list[dict]:
    admin_job_append_log_fn(job_id, "正在读取数据库已有群组清单...")
    database_rows = call_with_conn(
        get_conn_fn,
        list_database_channels,
        sort="message_count_desc",
    )
    admin_job_append_log_fn(job_id, f"数据库中已有 {len(database_rows)} 个群组/频道")
    admin_job_append_log_fn(job_id, "正在连接 Telegram 并扫描当前账号已加入会话...")
    joined_rows = load_joined_chat_inventory(ensure_client().iter_dialogs())
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
    return find_database_chats_not_joined(
        database_rows,
        joined_chat_ids,
        unavailable_chat_reasons=unavailable_chat_reasons,
    )


def _scan_restricted_chat_rows(
    ensure_client: Callable[[], Any],
    *,
    get_conn_fn: Callable[[], Any],
    admin_job_append_log_fn: Callable[[str, str], Any],
    job_id: str,
) -> list[Any]:
    del get_conn_fn
    admin_job_append_log_fn(
        job_id,
        "正在连接 Telegram 并扫描内容限制/风险标记...",
    )
    return find_restricted_joined_chats(ensure_client().iter_dialogs())


def _run_channel_inventory_scan_job(
    job_id: str,
    *,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
    spec: _ChannelInventoryScanSpec,
) -> None:
    heartbeat_stop, heartbeat_thread = start_admin_job_heartbeat(job_id)
    local_client = None
    worker_id = f"{job_id}_{spec.worker_suffix}"
    try:
        mark_admin_job_running(
            job_id,
            admin_job_set_status_fn=admin_job_set_status_fn,
        )
        update_admin_job_progress(
            job_id,
            0,
            total=None,
            stage="running",
        )
        admin_job_append_log_fn(job_id, "正在验证 Telegram 会话...")
        if not _ensure_base_session_valid(cfg, job_id, admin_job_append_log_fn):
            admin_job_set_status_fn(job_id, "error")
            return

        def ensure_client() -> Any:
            nonlocal local_client
            if local_client is None:
                local_client = _create_isolated_worker_client(cfg, worker_id)
            return local_client

        rows = spec.scan_rows_fn(
            ensure_client,
            get_conn_fn=get_conn_fn,
            admin_job_append_log_fn=admin_job_append_log_fn,
            job_id=job_id,
        )
        scanned_at = _admin_now_iso()

        admin_job_append_log_fn(job_id, "正在保存扫描结果...")
        saved_count = call_with_conn(
            get_conn_fn,
            spec.replace_results_fn,
            rows,
            scan_job_id=job_id,
            scanned_at=scanned_at,
        )

        update_admin_job_progress(
            job_id,
            saved_count,
            total=saved_count,
            stage="done",
        )
        admin_job_append_log_fn(job_id, spec.build_success_message_fn(saved_count))
        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        logging.exception(spec.logger_message, job_id)
        admin_job_append_log_fn(job_id, f"扫描失败：{admin_error_message(exc)}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        if local_client:
            with suppress(Exception):
                _disconnect_worker_client(local_client)
        _cleanup_isolated_worker_session(cfg, worker_id)


_MISSING_CHATS_SCAN_SPEC = _ChannelInventoryScanSpec(
    worker_suffix="missing_chats",
    logger_message="扫描未入库群组失败: job_id=%s",
    scan_rows_fn=_scan_missing_chat_rows,
    replace_results_fn=replace_missing_chat_scan_results,
    build_success_message_fn=(
        lambda saved_count: f"扫描完成：发现 {saved_count} 个已加入但未入库的群组/频道"
    ),
)


_ABSENT_CHATS_SCAN_SPEC = _ChannelInventoryScanSpec(
    worker_suffix="absent_chats",
    logger_message="扫描账号未加入数据库群组失败: job_id=%s",
    scan_rows_fn=_scan_absent_chat_rows,
    replace_results_fn=replace_absent_chat_scan_results,
    build_success_message_fn=(
        lambda saved_count: (
            "扫描完成：发现 "
            f"{saved_count} 个数据库中存在但账号未加入或不可用的群组/频道"
        )
    ),
)


_RESTRICTED_CHATS_SCAN_SPEC = _ChannelInventoryScanSpec(
    worker_suffix="restricted_chats",
    logger_message="扫描内容限制群组失败: job_id=%s",
    scan_rows_fn=_scan_restricted_chat_rows,
    replace_results_fn=replace_restricted_chat_scan_results,
    build_success_message_fn=(
        lambda saved_count: (
            "扫描完成：发现 "
            f"{saved_count} 个带 Telegram 内容限制/风险标记的群组/频道"
        )
    ),
)


def _admin_missing_chats_scan_job_runner(
    job_id: str,
    *,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    _run_channel_inventory_scan_job(
        job_id,
        cfg=cfg,
        get_conn_fn=get_conn_fn,
        admin_job_set_status_fn=admin_job_set_status_fn,
        admin_job_append_log_fn=admin_job_append_log_fn,
        spec=_MISSING_CHATS_SCAN_SPEC,
    )


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
    _run_channel_inventory_scan_job(
        job_id,
        cfg=cfg,
        get_conn_fn=get_conn_fn,
        admin_job_set_status_fn=admin_job_set_status_fn,
        admin_job_append_log_fn=admin_job_append_log_fn,
        spec=_ABSENT_CHATS_SCAN_SPEC,
    )


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
    _run_channel_inventory_scan_job(
        job_id,
        cfg=cfg,
        get_conn_fn=get_conn_fn,
        admin_job_set_status_fn=admin_job_set_status_fn,
        admin_job_append_log_fn=admin_job_append_log_fn,
        spec=_RESTRICTED_CHATS_SCAN_SPEC,
    )


def _admin_start_restricted_chats_scan_job_thread(job_id: str, **kwargs):
    return start_admin_job_thread(
        _admin_restricted_chats_scan_job_runner,
        job_id,
        **kwargs,
    )
