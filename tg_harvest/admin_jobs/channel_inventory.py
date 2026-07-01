import logging
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, replace
from types import SimpleNamespace
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
    ChatInventoryRow,
    RestrictedChatInventoryRow,
    chat_identity_key,
    find_database_chats_not_joined,
    find_missing_joined_chats,
    find_restricted_joined_chats,
    load_joined_chat_inventory,
    load_known_chat_identities,
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


@dataclass(frozen=True)
class _ScanAccount:
    key: str
    label: str
    cfg: Any


def _cfg_with_session_name(cfg: Any, session_name: str) -> Any:
    values = dict(getattr(cfg, "__dict__", {}) or {})
    if not values:
        values = {
            "api_id": getattr(cfg, "api_id", 0),
            "api_hash": getattr(cfg, "api_hash", ""),
        }
    values["session_name"] = session_name
    return SimpleNamespace(**values)


def _scan_accounts(cfg: Any) -> list[_ScanAccount]:
    accounts = [_ScanAccount(key="primary", label="主账号", cfg=cfg)]
    primary_session_name = str(getattr(cfg, "session_name", "") or "").strip()
    secondary_session_name = str(
        getattr(cfg, "secondary_session_name", "") or ""
    ).strip()
    if secondary_session_name and secondary_session_name != primary_session_name:
        accounts.append(
            _ScanAccount(
                key="secondary",
                label="第二账号",
                cfg=_cfg_with_session_name(cfg, secondary_session_name),
            )
        )
    return accounts


def _dedupe_texts(values: list[str]) -> str:
    items: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        items.append(text)
    return "；".join(items)


def _chat_row_priority(row: ChatInventoryRow) -> tuple[int, int, int, str, int]:
    return (
        1 if str(row.unavailable_reason or "").strip() else 0,
        0 if row.chat_username else 1,
        -(row.last_message_ts or 0),
        row.chat_title.casefold(),
        row.chat_id,
    )


def _merge_chat_inventory_row(
    current: ChatInventoryRow | None,
    incoming: ChatInventoryRow,
) -> ChatInventoryRow:
    if current is None:
        return incoming
    preferred = current if _chat_row_priority(current) <= _chat_row_priority(incoming) else incoming
    other = incoming if preferred is current else current
    merged_reason = _dedupe_texts(
        [preferred.unavailable_reason, other.unavailable_reason]
    )
    if merged_reason == str(preferred.unavailable_reason or ""):
        return preferred
    return replace(preferred, unavailable_reason=merged_reason)


def _restricted_row_priority(
    row: RestrictedChatInventoryRow,
) -> tuple[int, int, str, int]:
    return (
        0 if row.chat_username else 1,
        -(row.last_message_ts or 0),
        row.chat_title.casefold(),
        row.chat_id,
    )


def _merge_restricted_chat_row(
    current: RestrictedChatInventoryRow | None,
    incoming: RestrictedChatInventoryRow,
) -> RestrictedChatInventoryRow:
    if current is None:
        return incoming
    return current if _restricted_row_priority(current) <= _restricted_row_priority(incoming) else incoming


def _scan_account_rows(
    account: _ScanAccount,
    *,
    job_id: str,
    worker_suffix: str,
    admin_job_append_log_fn: Callable[[str, str], Any],
    scan_fn: Callable[[Any], list[Any]],
) -> list[Any] | None:
    admin_job_append_log_fn(job_id, f"正在验证{account.label} Telegram 会话...")
    if not _ensure_base_session_valid(account.cfg, job_id, admin_job_append_log_fn):
        admin_job_append_log_fn(job_id, f"{account.label}会话不可用，本轮扫描已跳过")
        return None

    worker_id = f"{job_id}_{worker_suffix}_{account.key}"
    client = None
    try:
        client = _create_isolated_worker_client(account.cfg, worker_id)
        return scan_fn(client)
    finally:
        if client is not None:
            with suppress(Exception):
                _disconnect_worker_client(client)
        _cleanup_isolated_worker_session(account.cfg, worker_id)


def _scan_missing_chat_rows(
    *,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_append_log_fn: Callable[[str, str], Any],
    job_id: str,
) -> list[Any]:
    admin_job_append_log_fn(job_id, "正在读取数据库已有群组清单...")
    known_chat_ids = call_with_conn(get_conn_fn, load_known_chat_identities)
    admin_job_append_log_fn(job_id, f"数据库中已有 {len(known_chat_ids)} 个群组/频道身份")

    merged_rows: dict[tuple[str, int], ChatInventoryRow] = {}
    scanned_account_count = 0
    for account in _scan_accounts(cfg):
        account_rows = _scan_account_rows(
            account,
            job_id=job_id,
            worker_suffix="missing_chats",
            admin_job_append_log_fn=admin_job_append_log_fn,
            scan_fn=lambda client: find_missing_joined_chats(
                client.iter_dialogs(),
                known_chat_ids,
                include_unavailable=True,
            ),
        )
        if account_rows is None:
            continue
        scanned_account_count += 1
        unavailable_count = sum(
            1 for row in account_rows if str(row.unavailable_reason or "").strip()
        )
        admin_job_append_log_fn(
            job_id,
            f"{account.label}扫描到 {len(account_rows)} 个未入库候选，"
            f"其中 {unavailable_count} 个当前不可访问",
        )
        for row in account_rows:
            key = chat_identity_key(row.chat_id, row.chat_type)
            merged_rows[key] = _merge_chat_inventory_row(merged_rows.get(key), row)

    if scanned_account_count <= 0:
        raise RuntimeError("没有可用的 Telegram 会话可执行扫描")

    rows = list(merged_rows.values())
    rows.sort(
        key=lambda item: (
            1 if str(item.unavailable_reason or "").strip() else 0,
            item.chat_title.casefold(),
            item.chat_id,
        )
    )
    return rows


def _scan_absent_chat_rows(
    *,
    cfg: Any,
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
    accessible_chat_keys: set[tuple[str, int]] = set()
    unavailable_chat_reasons: dict[tuple[str, int], list[str]] = {}
    scanned_account_count = 0

    for account in _scan_accounts(cfg):
        joined_rows = _scan_account_rows(
            account,
            job_id=job_id,
            worker_suffix="absent_chats",
            admin_job_append_log_fn=admin_job_append_log_fn,
            scan_fn=lambda client: load_joined_chat_inventory(client.iter_dialogs()),
        )
        if joined_rows is None:
            continue
        scanned_account_count += 1
        accessible_count = 0
        unavailable_count = 0
        for row in joined_rows:
            key = chat_identity_key(row.chat_id, row.chat_type)
            if str(row.unavailable_reason or "").strip():
                unavailable_count += 1
                unavailable_chat_reasons.setdefault(key, []).append(
                    f"{account.label}：{row.unavailable_reason}"
                )
                continue
            accessible_count += 1
            accessible_chat_keys.add(key)
        admin_job_append_log_fn(
            job_id,
            f"{account.label}当前可访问 {accessible_count} 个群组/频道，"
            f"发现 {unavailable_count} 个已加入但不可用的群组/频道",
        )

    if scanned_account_count <= 0:
        raise RuntimeError("没有可用的 Telegram 会话可执行扫描")

    normalized_unavailable_reasons = {
        key: _dedupe_texts(reasons)
        for key, reasons in unavailable_chat_reasons.items()
        if key not in accessible_chat_keys
    }
    return find_database_chats_not_joined(
        database_rows,
        accessible_chat_keys,
        unavailable_chat_reasons=normalized_unavailable_reasons,
    )


def _scan_restricted_chat_rows(
    *,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_append_log_fn: Callable[[str, str], Any],
    job_id: str,
) -> list[Any]:
    del get_conn_fn
    merged_rows: dict[tuple[str, int], RestrictedChatInventoryRow] = {}
    scanned_account_count = 0

    for account in _scan_accounts(cfg):
        admin_job_append_log_fn(
            job_id,
            f"正在连接{account.label}并扫描内容限制/风险标记...",
        )
        account_rows = _scan_account_rows(
            account,
            job_id=job_id,
            worker_suffix="restricted_chats",
            admin_job_append_log_fn=admin_job_append_log_fn,
            scan_fn=lambda client: find_restricted_joined_chats(client.iter_dialogs()),
        )
        if account_rows is None:
            continue
        scanned_account_count += 1
        admin_job_append_log_fn(
            job_id,
            f"{account.label}扫描到 {len(account_rows)} 个内容限制/风险标记候选",
        )
        for row in account_rows:
            key = chat_identity_key(row.chat_id, row.chat_type)
            merged_rows[key] = _merge_restricted_chat_row(
                merged_rows.get(key),
                row,
            )

    if scanned_account_count <= 0:
        raise RuntimeError("没有可用的 Telegram 会话可执行扫描")

    rows = list(merged_rows.values())
    rows.sort(key=lambda item: (item.chat_title.casefold(), item.chat_id))
    return rows


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
        rows = spec.scan_rows_fn(
            cfg=cfg,
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
