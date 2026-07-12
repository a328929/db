import logging
import re
import time
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
    bind_client_event_loop,
)
from tg_harvest.domain.chat_inventory import (
    ChatInventoryRow,
    RestrictedChatInventoryRow,
    chat_identity_candidates,
    chat_identity_key,
    find_missing_joined_chats,
    find_restricted_joined_chats,
    load_joined_chat_inventory,
    load_known_chat_identities,
    restricted_chat_row_from_entity,
)
from tg_harvest.ingest.flood_wait import call_with_bounded_retry, is_flood_wait_error
from tg_harvest.storage.channel_management import (
    list_database_channels,
    list_restricted_chat_scan_results,
    replace_missing_chat_scan_results,
    replace_restricted_chat_scan_results,
)

_TOKEN_SEPARATOR_RE = re.compile(r"[、,，;；|/]+")
_PUBLIC_ENTITY_BATCH_SIZE = 50


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


def _merge_tokens(values: list[str]) -> str:
    tokens: list[str] = []
    seen: set[str] = set()
    for value in values:
        for raw_token in _TOKEN_SEPARATOR_RE.split(str(value or "")):
            token = raw_token.strip()
            key = token.casefold()
            if not token or key in seen:
                continue
            seen.add(key)
            tokens.append(token)
    return "、".join(tokens)


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
    preferred = (
        current
        if _restricted_row_priority(current) <= _restricted_row_priority(incoming)
        else incoming
    )
    return replace(
        preferred,
        restriction_platforms=_merge_tokens(
            [current.restriction_platforms, incoming.restriction_platforms]
        ),
        restriction_reasons=_merge_tokens(
            [current.restriction_reasons, incoming.restriction_reasons]
        ),
        restriction_text=_dedupe_texts(
            [current.restriction_text, incoming.restriction_text]
        ),
        risk_flags=_merge_tokens([current.risk_flags, incoming.risk_flags]),
        membership_scope=(
            "joined"
            if "joined" in {current.membership_scope, incoming.membership_scope}
            else "public_unjoined"
        ),
        scanned_at=max(current.scanned_at, incoming.scanned_at),
    )


def _restricted_row_from_stored(item: dict[str, Any]) -> RestrictedChatInventoryRow:
    return RestrictedChatInventoryRow(
        chat_id=int(item.get("chat_id") or 0),
        chat_title=str(item.get("chat_title") or ""),
        chat_username=str(item.get("chat_username") or ""),
        chat_type=str(item.get("chat_type") or ""),
        is_public=int(item.get("is_public") or 0),
        restriction_platforms=str(item.get("restriction_platforms") or ""),
        restriction_reasons=str(item.get("restriction_reasons") or ""),
        restriction_text=str(item.get("restriction_text") or ""),
        risk_flags=str(item.get("risk_flags") or ""),
        membership_scope="public_unjoined",
        last_message_at=str(item.get("last_message_at") or ""),
        last_message_ts=item.get("last_message_ts"),
        scan_job_id=str(item.get("scan_job_id") or ""),
        scanned_at=str(item.get("scanned_at") or ""),
    )


def _resolved_entity_matches_database_row(entity: Any, row: dict[str, Any]) -> bool:
    entity_id = getattr(entity, "id", None)
    entity_type = entity.__class__.__name__
    if entity_id is None or not entity_type.lower().lstrip("_").startswith(
        ("channel", "chat")
    ):
        return False
    return not chat_identity_candidates(
        row.get("chat_id"), row.get("chat_type")
    ).isdisjoint(chat_identity_candidates(entity_id, entity_type))


def _merge_resolved_public_entity(
    merged_rows: dict[tuple[str, int], RestrictedChatInventoryRow],
    *,
    row: dict[str, Any],
    entity: Any,
) -> None:
    if not _resolved_entity_matches_database_row(entity, row):
        return
    restricted_row = restricted_chat_row_from_entity(
        entity,
        chat_id=int(row.get("chat_id") or 0),
        chat_title=str(row.get("chat_title") or ""),
        chat_username=str(row.get("chat_username") or ""),
        last_message_at=str(row.get("last_message_at") or ""),
        last_message_ts=row.get("last_message_ts"),
        membership_scope="public_unjoined",
    )
    if restricted_row is None:
        return
    key = chat_identity_key(restricted_row.chat_id, restricted_row.chat_type)
    merged_rows[key] = _merge_restricted_chat_row(merged_rows.get(key), restricted_row)


def _fetch_cached_entities(
    client: Any,
    pairs: list[tuple[dict[str, Any], Any]],
) -> list[tuple[dict[str, Any], Any]]:
    if not pairs:
        return []
    try:
        with bind_client_event_loop(client):
            entities = call_with_bounded_retry(
                client.get_entity,
                [input_peer for _row, input_peer in pairs],
                scope="restricted-public-cache-batch",
            )
        return list(zip((row for row, _input_peer in pairs), entities, strict=True))
    except Exception as exc:
        if is_flood_wait_error(exc):
            raise
        if len(pairs) <= 1:
            return []
        midpoint = len(pairs) // 2
        return [
            *_fetch_cached_entities(client, pairs[:midpoint]),
            *_fetch_cached_entities(client, pairs[midpoint:]),
        ]


def _scan_account_rows(
    account: _ScanAccount,
    *,
    job_id: str,
    worker_suffix: str,
    admin_job_append_log_fn: Callable[[str, str], Any],
    scan_fn: Callable[[Any], list[Any]],
) -> list[Any] | None:
    worker_id = f"{job_id}_{worker_suffix}_{account.key}"
    client = None
    try:
        client = _create_isolated_worker_client(account.cfg, worker_id)
        if not client.is_user_authorized():
            admin_job_append_log_fn(
                job_id,
                f"{account.label} Telegram 会话未登录，本轮扫描已跳过",
            )
            return None
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


def _scan_restricted_chat_rows(
    *,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_append_log_fn: Callable[[str, str], Any],
    job_id: str,
) -> list[Any]:
    database_rows = call_with_conn(
        get_conn_fn,
        list_database_channels,
        sort="message_count_desc",
    )
    previous_rows = call_with_conn(get_conn_fn, list_restricted_chat_scan_results)
    merged_rows: dict[tuple[str, int], RestrictedChatInventoryRow] = {}
    joined_identities: set[tuple[str, int]] = set()
    active_clients: list[tuple[_ScanAccount, Any, str]] = []
    probed_public_chat_ids: set[int] = set()

    try:
        for account in _scan_accounts(cfg):
            admin_job_append_log_fn(
                job_id,
                f"正在连接{account.label}并扫描已加入会话风险标记...",
            )
            worker_id = f"{job_id}_restricted_chats_{account.key}"
            client = _create_isolated_worker_client(account.cfg, worker_id)
            if not client.is_user_authorized():
                admin_job_append_log_fn(
                    job_id,
                    f"{account.label} Telegram 会话未登录，本轮扫描已跳过",
                )
                try:
                    _disconnect_worker_client(client)
                finally:
                    _cleanup_isolated_worker_session(account.cfg, worker_id)
                continue
            active_clients.append((account, client, worker_id))
            with bind_client_event_loop(client):
                dialogs = list(
                    client.iter_dialogs(
                        limit=None,
                        archived=None,
                        ignore_migrated=True,
                    )
                )
            for joined_row in load_joined_chat_inventory(dialogs):
                joined_identities.update(
                    chat_identity_candidates(joined_row.chat_id, joined_row.chat_type)
                )
            account_rows = find_restricted_joined_chats(dialogs)
            admin_job_append_log_fn(
                job_id,
                f"{account.label}已加入会话发现 {len(account_rows)} 个风险候选",
            )
            for row in account_rows:
                key = chat_identity_key(row.chat_id, row.chat_type)
                merged_rows[key] = _merge_restricted_chat_row(
                    merged_rows.get(key), row
                )

        if not active_clients:
            raise RuntimeError("没有可用的 Telegram 会话可执行扫描")

        public_rows = [
            row
            for row in database_rows
            if str(row.get("chat_username") or "").strip()
            and joined_identities.isdisjoint(
                chat_identity_candidates(row.get("chat_id"), row.get("chat_type"))
            )
        ]
        admin_job_append_log_fn(
            job_id,
            f"正在补探测 {len(public_rows)} 个账号未加入的数据库公开群组/频道...",
        )

        uncached_by_id = {int(row["chat_id"]): row for row in public_rows}
        for account, client, _worker_id in active_clients:
            cached_pairs: list[tuple[dict[str, Any], Any]] = []
            for row in public_rows:
                try:
                    input_peer = client.session.get_input_entity(row["chat_username"])
                except ValueError:
                    continue
                cached_pairs.append((row, input_peer))

            for offset in range(0, len(cached_pairs), _PUBLIC_ENTITY_BATCH_SIZE):
                batch = cached_pairs[offset : offset + _PUBLIC_ENTITY_BATCH_SIZE]
                try:
                    resolved_pairs = _fetch_cached_entities(client, batch)
                except Exception as exc:
                    if is_flood_wait_error(exc):
                        admin_job_append_log_fn(
                            job_id,
                            f"{account.label}批量刷新公开频道缓存触发频控，已切换保守模式",
                        )
                        break
                    raise
                for row, entity in resolved_pairs:
                    chat_id = int(row["chat_id"])
                    probed_public_chat_ids.add(chat_id)
                    uncached_by_id.pop(chat_id, None)
                    _merge_resolved_public_entity(
                        merged_rows,
                        row=row,
                        entity=entity,
                    )

        resolve_limit = max(
            0,
            int(getattr(cfg, "admin_restricted_public_resolve_limit", 40) or 0),
        )
        resolve_gap = max(
            0.0,
            float(
                getattr(cfg, "admin_restricted_public_resolve_gap_seconds", 1.0)
                or 0.0
            ),
        )
        account_next_resolve_at: dict[str, float] = {}
        unresolved_rows = list(uncached_by_id.values())[:resolve_limit]
        for index, row in enumerate(unresolved_rows):
            for account_offset in range(len(active_clients)):
                account, client, _worker_id = active_clients[
                    (index + account_offset) % len(active_clients)
                ]
                next_allowed = account_next_resolve_at.get(account.key, 0.0)
                wait_seconds = max(0.0, next_allowed - time.monotonic())
                if wait_seconds > 0:
                    time.sleep(wait_seconds)
                account_next_resolve_at[account.key] = time.monotonic() + resolve_gap
                try:
                    with bind_client_event_loop(client):
                        entity = call_with_bounded_retry(
                            client.get_entity,
                            row["chat_username"],
                            max_retries=2,
                            flood_wait_threshold_seconds=int(
                                getattr(account.cfg, "flood_wait_switch_threshold", 30)
                                or 30
                            ),
                            account_label=account.label,
                            scope="restricted-public-username",
                        )
                except Exception as exc:
                    if is_flood_wait_error(exc):
                        continue
                    logging.info(
                        "公开频道风险补探测失败: chat_id=%s account=%s error=%s",
                        row.get("chat_id"),
                        account.key,
                        admin_error_message(exc),
                    )
                    continue
                chat_id = int(row["chat_id"])
                probed_public_chat_ids.add(chat_id)
                _merge_resolved_public_entity(
                    merged_rows,
                    row=row,
                    entity=entity,
                )
                break

        candidate_ids = {int(row["chat_id"]) for row in public_rows}
        for previous in previous_rows:
            chat_id = int(previous.get("chat_id") or 0)
            if chat_id not in candidate_ids or chat_id in probed_public_chat_ids:
                continue
            row = _restricted_row_from_stored(previous)
            key = chat_identity_key(row.chat_id, row.chat_type)
            merged_rows[key] = _merge_restricted_chat_row(
                merged_rows.get(key), row
            )

        admin_job_append_log_fn(
            job_id,
            "公开群组补探测完成："
            f"成功刷新 {len(probed_public_chat_ids)} 个，"
            f"本轮未主动解析 {max(0, len(uncached_by_id) - len(unresolved_rows))} 个",
        )
    finally:
        for account, client, worker_id in reversed(active_clients):
            try:
                with suppress(Exception):
                    _disconnect_worker_client(client)
            finally:
                _cleanup_isolated_worker_session(account.cfg, worker_id)

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
