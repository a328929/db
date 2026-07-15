import logging
import time
from collections.abc import Callable
from contextlib import suppress
from typing import Any

from tg_harvest.admin_jobs.clone_execution import (
    clone_cfg_for_account,
    normalize_clone_nonnegative_int,
    send_clone_text_chunk,
    split_clone_text_chunks,
)
from tg_harvest.admin_jobs.clone_job_state import _clean_text
from tg_harvest.admin_jobs.clone_media_copy import (
    CloneMediaDeliverySafetyError,
    CloneMediaTransferContext,
    CloneTargetDeliveryUnconfirmedError,
    cleanup_pending_clone_relay_messages,
    confirm_clone_target_messages,
    copy_clone_media_direct_without_source,
    copy_clone_media_via_relay_without_source,
    record_clone_media_mapping,
    resolve_clone_relay_chat,
    validate_clone_relay_execution,
)
from tg_harvest.admin_jobs.clone_media_resolver import clone_api_resolve_media_message
from tg_harvest.admin_jobs.clone_timeline_media_groups import handle_media_group_item
from tg_harvest.admin_jobs.clone_timeline_state import (
    build_execution_state,
    first_required_target_message_id,
    load_required_state,
    resolve_final_status,
    summary_log_message,
    timeline_execution_label,
    timeline_preview,
    try_mark_migration_failed,
    update_migration_required,
    validate_plan_for_timeline,
)
from tg_harvest.admin_jobs.clone_timeline_store import (
    CloneMappingPersistenceError,
    media_mapping_done,
    next_timeline_batch,
    prepare_text_mapping_delivery,
    record_text_mapping,
    source_message_from_timeline_item,
    text_mapping_done,
)
from tg_harvest.admin_jobs.common import (
    admin_error_message,
    finish_job_heartbeat,
    is_entity_lookup_miss_error,
    resolve_chat_entity,
    start_admin_job_thread,
)
from tg_harvest.admin_jobs.core import (
    _admin_job_heartbeat,
    _admin_job_stop_requested,
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
    bind_client_event_loop,
)
from tg_harvest.domain.clone_plan import (
    CLONE_TEXT_MIGRATION_MAX_MESSAGE_LIMIT,
    CLONE_TEXT_MIGRATION_MAX_SEND_DELAY_MS,
    clone_plan_media_relay_chat_id,
    clone_plan_source_snapshot_message_id,
)
from tg_harvest.domain.clone_target_permissions import (
    clone_target_send_permission,
)
from tg_harvest.ingest.flood_wait import call_with_bounded_retry


def _execution_entity_lookup_error_message(*, role: str, account: str) -> str:
    role_label = str(role or "").strip() or "会话"
    account_label = str(account or "").strip() or "当前账号"
    return (
        f"{role_label}实体缓存未命中，{account_label} 不能在执行阶段回退公开 username 解析。"
        "请先重新执行在线深度预检，必要时先用对应账号手动打开一次源群/目标群/中转频道后再重试。"
    )


def _resolve_entity_for_execution(
    client: Any,
    *,
    chat_id: int,
    chat_username: str,
    account: str,
    role: str,
) -> Any:
    try:
        return resolve_chat_entity(
            client,
            int(chat_id),
            chat_username,
            allow_username_fallback=False,
        )
    except Exception as exc:
        if is_entity_lookup_miss_error(exc):
            raise RuntimeError(
                _execution_entity_lookup_error_message(
                    role=role,
                    account=account,
                )
            ) from exc
        raise


def _sleep_after_send(send_delay_ms: int) -> None:
    if send_delay_ms <= 0:
        return
    time.sleep(float(send_delay_ms) / 1000.0)


def _source_latest_message_id(client: Any, source_entity: Any) -> int:
    with bind_client_event_loop(client):
        messages = call_with_bounded_retry(
            client.get_messages,
            source_entity,
            limit=1,
            scope="clone-timeline-source-snapshot-check",
        )
    if isinstance(messages, (list, tuple)):
        message_ids = [
            int(getattr(message, "id", 0) or 0)
            for message in messages
            if getattr(message, "id", None) is not None
        ]
        return max(message_ids, default=0)
    try:
        message = next(iter(messages), None)
    except TypeError:
        message = messages
    try:
        return max(0, int(getattr(message, "id", 0) or 0))
    except (TypeError, ValueError):
        return 0


def _plan_source_read_account(plan: dict[str, Any]) -> str:
    capabilities = plan.get("capabilities")
    account_records = (
        capabilities.get("accounts") if isinstance(capabilities, dict) else None
    )
    if not isinstance(account_records, list):
        return ""
    for preferred in ("primary", "secondary"):
        for account in account_records:
            if not isinstance(account, dict):
                continue
            if _clean_text(account.get("account")).lower() != preferred:
                continue
            if _clean_text(account.get("source_access")).lower() == "ok":
                return preferred
    return ""


def _log_timeline_start(
    *,
    state,
    admin_job_append_log_fn,
) -> None:
    admin_job_append_log_fn(
        state.job_id,
        "开始完整时间线迁移："
        f"源={state.source_title} ({state.source_chat_id})，"
        f"目标={state.target_title or state.target_chat_id}，"
        f"文本={state.text_total}，媒体={state.media_total}，媒体组={state.media_group_total}",
    )
    admin_job_append_log_fn(
        state.job_id,
        "时间线策略：按原群 msg_date_ts/message_id 顺序混合发送文本、媒体和相册",
    )
    text_account = _clean_text(state.accounts.get("text_account"))
    if text_account:
        admin_job_append_log_fn(
            state.job_id,
            f"文本发送：{text_account} -> 克隆群",
        )
    if state.using_relay:
        admin_job_append_log_fn(
            state.job_id,
            "媒体复制：源群 -> 中转群 -> 克隆群",
        )
        admin_job_append_log_fn(
            state.job_id,
            "媒体复制策略：固定中转频道桥接；两跳均 drop_author=True，不显示原群或中转频道跳转",
        )
    else:
        media_account = _clean_text(state.accounts.get("media_source_account"))
        if media_account:
            admin_job_append_log_fn(
                state.job_id,
                f"媒体复制：{media_account} -> 克隆群",
            )
        admin_job_append_log_fn(
            state.job_id,
            "媒体复制策略：drop_author=True，不显示原群来源，不带原群跳转",
        )


def _update_progress(
    *,
    state,
    get_conn_fn,
    phase: str = "replaying_timeline",
) -> None:
    update_migration_required(
        get_conn_fn=get_conn_fn,
        migration_id=state.migration_id,
        **state.counters.as_update_fields(),
    )
    _admin_job_update_progress(
        state.job_id,
        state.counters.processed,
        total=max(state.progress_total, state.counters.processed),
        stage=phase,
        log_step=0,
        auto_log=False,
    )


def _finalize_success(
    *,
    state,
    get_conn_fn,
    admin_job_set_status_fn,
    admin_job_append_log_fn,
) -> None:
    final = resolve_final_status(state)
    counters = state.counters
    update_migration_required(
        get_conn_fn=get_conn_fn,
        migration_id=state.migration_id,
        status=final.status,
        phase=final.phase,
        text_total=state.text_total,
        text_sent=counters.text_sent,
        text_skipped=counters.text_skipped,
        text_failed=counters.text_failed,
        media_total=state.media_total,
        media_sent=counters.media_sent,
        media_skipped=counters.media_skipped,
        media_failed=counters.media_failed,
        media_group_total=state.media_group_total,
        media_group_sent=counters.media_group_sent,
        media_group_skipped=counters.media_group_skipped,
        media_group_failed=counters.media_group_failed,
        error_message=final.error_message,
        completed_at=_admin_now_iso(),
    )
    admin_job_append_log_fn(state.job_id, summary_log_message(state))
    _admin_job_update_progress(
        state.job_id,
        state.counters.processed,
        total=max(state.progress_total, state.counters.processed),
        stage=final.phase,
        log_step=0,
        auto_log=False,
    )
    admin_job_set_status_fn(state.job_id, final.status)


def _cleanup_runner_clients(
    clients: dict[str, Any], worker_ids: dict[str, str], account_cfgs: dict[str, Any]
) -> None:
    disconnected_ids: set[int] = set()
    for cleanup_client in clients.values():
        if cleanup_client is None or id(cleanup_client) in disconnected_ids:
            continue
        disconnected_ids.add(id(cleanup_client))
        with suppress(Exception):
            _disconnect_worker_client(cleanup_client)
    for account, account_cfg in account_cfgs.items():
        worker_id = worker_ids.get(account)
        if not worker_id:
            continue
        with suppress(Exception):
            _cleanup_isolated_worker_session(account_cfg, worker_id)


def _admin_clone_timeline_migration_job_runner(
    job_id: str,
    *,
    run_id: str,
    plan_id: str,
    migration_id: str,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
    message_limit: Any = 0,
    send_delay_ms: Any = 0,
) -> None:
    job_context.set(str(job_id))
    heartbeat_stop, heartbeat_thread = _start_job_heartbeat(
        job_id, _admin_job_heartbeat
    )
    clients: dict[str, Any] = {}
    worker_ids: dict[str, str] = {}
    account_cfgs: dict[str, Any] = {}
    target_entities: dict[str, Any] = {}
    normalized_message_limit = normalize_clone_nonnegative_int(
        message_limit,
        max_value=CLONE_TEXT_MIGRATION_MAX_MESSAGE_LIMIT,
    )
    normalized_send_delay_ms = normalize_clone_nonnegative_int(
        send_delay_ms,
        max_value=CLONE_TEXT_MIGRATION_MAX_SEND_DELAY_MS,
    )

    try:
        admin_job_set_status_fn(job_id, "running")
        update_migration_required(
            get_conn_fn=get_conn_fn,
            migration_id=migration_id,
            status="running",
            phase="preparing_timeline",
            error_message="",
        )

        run, plan, _migration = load_required_state(
            get_conn_fn=get_conn_fn,
            run_id=run_id,
            plan_id=plan_id,
            migration_id=migration_id,
        )
        source_chat_id = int(run["source_chat_id"])
        target_chat_id = int(run.get("target_chat_id") or 0)
        source_snapshot_message_id = clone_plan_source_snapshot_message_id(plan)
        if source_snapshot_message_id <= 0:
            raise RuntimeError("迁移计划缺少已核验的源消息快照，请重新执行在线深度预检")
        if not target_chat_id:
            raise RuntimeError("目标副本尚未创建，不能执行完整时间线迁移")

        admin_job_append_log_fn(
            job_id,
            "正在读取本地消息并核验待迁移时间线；大型源群可能需要一些时间。",
        )
        _admin_job_update_progress(
            job_id,
            0,
            total=None,
            stage="preparing_timeline",
            log_step=0,
            auto_log=False,
        )
        preview = timeline_preview(
            get_conn_fn=get_conn_fn,
            run_id=run_id,
            source_chat_id=source_chat_id,
            max_source_message_id=source_snapshot_message_id,
        )
        admin_job_append_log_fn(
            job_id,
            "本地时间线核验完成，正在准备迁移连接。",
        )
        accounts = validate_plan_for_timeline(plan=plan, preview=preview)
        state = build_execution_state(
            job_id=job_id,
            run_id=run_id,
            plan_id=plan_id,
            migration_id=migration_id,
            run=run,
            plan=plan,
            preview=preview,
            accounts=accounts,
            normalized_message_limit=normalized_message_limit,
            normalized_send_delay_ms=normalized_send_delay_ms,
        )

        update_migration_required(
            get_conn_fn=get_conn_fn,
            migration_id=migration_id,
            phase="connecting",
            target_chat_id=target_chat_id,
            target_title=run.get("target_title"),
            target_write_account=timeline_execution_label(accounts),
            requested_limit=normalized_message_limit,
            send_delay_ms=normalized_send_delay_ms,
            text_total=state.text_total,
            media_total=state.media_total,
            media_group_total=state.media_group_total,
        )
        _log_timeline_start(
            state=state, admin_job_append_log_fn=admin_job_append_log_fn
        )

        def account_client(account: str) -> Any:
            normalized = _clean_text(account).lower()
            if not normalized:
                raise RuntimeError("迁移账号为空")
            if normalized in clients:
                return clients[normalized]
            account_cfg = clone_cfg_for_account(cfg, normalized)
            if not _ensure_base_session_valid(
                account_cfg, job_id, admin_job_append_log_fn
            ):
                raise RuntimeError(f"计划指定的账号会话不可用：{normalized}")
            worker_id = f"{job_id}_clone_timeline_{normalized}"
            account_cfgs[normalized] = account_cfg
            worker_ids[normalized] = worker_id
            clients[normalized] = _create_isolated_worker_client(account_cfg, worker_id)
            return clients[normalized]

        def target_entity_for(account: str) -> Any:
            normalized = _clean_text(account).lower()
            if normalized not in target_entities:
                target_entity = _resolve_entity_for_execution(
                    account_client(normalized),
                    chat_id=target_chat_id,
                    chat_username=_clean_text(run.get("target_username")),
                    account=normalized,
                    role="目标群",
                )
                if clone_target_send_permission(target_entity) != "ok":
                    raise RuntimeError(
                        "计划指定的目标写入账号没有可确认的发消息权限："
                        f"{normalized}。请授予目标频道发布消息权限或目标群发消息权限，"
                        "然后重新执行在线深度预检。"
                    )
                target_entities[normalized] = target_entity
            return target_entities[normalized]

        text_account = accounts.get("text_account") or ""
        media_source_account = accounts.get("media_source_account") or ""
        media_target_account = accounts.get("media_target_account") or ""
        text_client = account_client(text_account) if text_account else None
        text_target_entity = target_entity_for(text_account) if text_account else None

        source_read_account = media_source_account or _plan_source_read_account(plan)
        source_client = (
            account_client(source_read_account) if source_read_account else None
        )
        target_client = (
            account_client(media_target_account) if media_target_account else None
        )
        source_entity = (
            _resolve_entity_for_execution(
                source_client,
                chat_id=source_chat_id,
                chat_username=_clean_text(run.get("source_chat_username")),
                account=source_read_account,
                role="源群",
            )
            if source_client is not None
            else None
        )
        if source_client is not None and source_entity is not None:
            latest_source_message_id = _source_latest_message_id(
                source_client,
                source_entity,
            )
            if latest_source_message_id <= 0:
                raise RuntimeError("无法确认源群最新消息，不能执行无遗漏时间线迁移")
            if latest_source_message_id > state.source_snapshot_message_id:
                raise RuntimeError(
                    "源群在预检后已有新消息，请先完成采集并重新执行在线深度预检，"
                    "以免迁移遗漏最新消息"
                )
        media_target_entity = (
            target_entity_for(media_target_account) if media_target_account else None
        )
        relay_entity_for_source = (
            resolve_clone_relay_chat(source_client, plan)
            if state.using_relay and source_client is not None
            else None
        )
        relay_entity_for_target = (
            resolve_clone_relay_chat(target_client, plan)
            if state.using_relay and target_client is not None
            else None
        )
        if state.using_relay:
            if relay_entity_for_source is None or relay_entity_for_target is None:
                raise RuntimeError("固定中转频道桥接实体尚未初始化")
            validate_clone_relay_execution(
                relay_entity_for_source=relay_entity_for_source,
                relay_entity_for_target=relay_entity_for_target,
                relay_chat_id=clone_plan_media_relay_chat_id(plan),
                source_chat_id=source_chat_id,
                target_chat_id=target_chat_id,
            )
        media_transfer_context = CloneMediaTransferContext(
            get_conn_fn=get_conn_fn,
            migration_id=migration_id,
            run_id=run_id,
            plan_id=plan_id,
            source_chat_id=source_chat_id,
            target_chat_id=target_chat_id,
            source_account=_clean_text(media_source_account).lower(),
            target_account=_clean_text(media_target_account).lower(),
            relay_chat_id=(
                clone_plan_media_relay_chat_id(plan) if state.using_relay else 0
            ),
        )
        if state.using_relay:
            if source_client is None or relay_entity_for_source is None:
                raise RuntimeError("固定中转频道清理连接尚未初始化")
            cleanup_pending_clone_relay_messages(
                source_client=source_client,
                relay_entity_for_source=relay_entity_for_source,
                transfer_context=media_transfer_context,
                log_step=lambda message: admin_job_append_log_fn(job_id, message),
            )

        def copy_media_to_target(message_ids: int | list[int]) -> Any:
            if (
                source_client is None
                or source_entity is None
                or media_target_entity is None
            ):
                raise RuntimeError("媒体迁移账号或实体尚未初始化")
            if state.using_relay:
                if (
                    target_client is None
                    or relay_entity_for_source is None
                    or relay_entity_for_target is None
                ):
                    raise RuntimeError("固定中转频道桥接实体尚未初始化")
                return copy_clone_media_via_relay_without_source(
                    source_client=source_client,
                    target_client=target_client,
                    relay_entity_for_source=relay_entity_for_source,
                    relay_entity_for_target=relay_entity_for_target,
                    target_entity=media_target_entity,
                    message_ids=message_ids,
                    source_entity=source_entity,
                    transfer_context=media_transfer_context,
                    log_step=lambda message: admin_job_append_log_fn(job_id, message),
                )
            return copy_clone_media_direct_without_source(
                client=source_client,
                target_entity=media_target_entity,
                message_ids=message_ids,
                source_entity=source_entity,
                transfer_context=media_transfer_context,
            )

        update_migration_required(
            get_conn_fn=get_conn_fn,
            migration_id=migration_id,
            phase="replaying_timeline",
        )

        while True:
            if _admin_job_stop_requested(job_id):
                state.stopped = True
                break
            batch = next_timeline_batch(
                get_conn_fn=get_conn_fn,
                run_id=run_id,
                source_chat_id=source_chat_id,
                after_ts=state.after_ts,
                after_message_id=state.after_message_id,
                max_source_message_id=state.source_snapshot_message_id,
            )
            if not batch:
                break
            for item in batch:
                source_count = max(1, int(item.get("item_count") or 1))
                if (
                    normalized_message_limit > 0
                    and state.counters.processed >= normalized_message_limit
                ):
                    state.limit_reached = True
                    break
                if (
                    normalized_message_limit > 0
                    and state.counters.processed + source_count
                    > normalized_message_limit
                ):
                    state.limit_reached = True
                    break
                if _admin_job_stop_requested(job_id):
                    state.stopped = True
                    break

                state.after_ts = int(item.get("sort_ts") or 0)
                state.after_message_id = int(item.get("sort_message_id") or 0)
                item_type = _clean_text(item.get("item_type"))

                if item_type == "text":
                    source_message = source_message_from_timeline_item(item)
                    chunks = split_clone_text_chunks(str(item.get("text") or ""))
                    if not chunks:
                        state.counters.processed += 1
                        state.counters.text_skipped += 1
                        continue
                    chunk_count = len(chunks)
                    source_message_id = int(source_message["message_id"])
                    if all(
                        text_mapping_done(
                            get_conn_fn=get_conn_fn,
                            run_id=run_id,
                            source_chat_id=source_chat_id,
                            source_message_id=source_message_id,
                            chunk_index=chunk_index,
                        )
                        for chunk_index in range(chunk_count)
                    ):
                        state.counters.processed += 1
                        state.counters.text_skipped += 1
                        continue
                    message_failed = False
                    for chunk_index, chunk in enumerate(chunks):
                        if text_mapping_done(
                            get_conn_fn=get_conn_fn,
                            run_id=run_id,
                            source_chat_id=source_chat_id,
                            source_message_id=source_message_id,
                            chunk_index=chunk_index,
                        ):
                            continue
                        delivery_random_id: int | None = None
                        target_message_id: int | None = None
                        try:
                            delivery = prepare_text_mapping_delivery(
                                get_conn_fn=get_conn_fn,
                                migration_id=migration_id,
                                run_id=run_id,
                                plan_id=plan_id,
                                source_message=source_message,
                                target_chat_id=target_chat_id,
                                target_account=text_account,
                                chunk_index=chunk_index,
                                chunk_count=chunk_count,
                            )
                            delivery_random_id = int(
                                delivery.get("delivery_random_id") or 0
                            )
                            if delivery_random_id <= 0:
                                raise CloneMappingPersistenceError(
                                    "克隆文本交付意图缺少可恢复随机 ID"
                                )
                            if text_client is None or text_target_entity is None:
                                raise RuntimeError("文本迁移账号或目标实体尚未初始化")
                            target_message_id = int(
                                delivery.get("target_message_id") or 0
                            ) or None
                            if target_message_id is None:
                                target_message_id = send_clone_text_chunk(
                                    text_client,
                                    text_target_entity,
                                    chunk,
                                    random_id=delivery_random_id,
                                )
                                if target_message_id is None or target_message_id <= 0:
                                    raise RuntimeError(
                                        "时间线文本发送后未返回有效目标消息 ID"
                                    )
                                record_text_mapping(
                                    get_conn_fn=get_conn_fn,
                                    migration_id=migration_id,
                                    run_id=run_id,
                                    plan_id=plan_id,
                                    source_message=source_message,
                                    target_chat_id=target_chat_id,
                                    target_message_id=target_message_id,
                                    chunk_index=chunk_index,
                                    chunk_count=chunk_count,
                                    status="pending_confirmation",
                                    delivery_random_id=delivery_random_id,
                                    delivery_account=text_account,
                                )
                            confirm_clone_target_messages(
                                text_client,
                                text_target_entity,
                                [int(target_message_id)],
                                context="第二账号文本发送到克隆群确认",
                            )
                        except CloneMappingPersistenceError:
                            raise
                        except Exception as exc:
                            message_failed = True
                            message = admin_error_message(exc)
                            record_text_mapping(
                                get_conn_fn=get_conn_fn,
                                migration_id=migration_id,
                                run_id=run_id,
                                plan_id=plan_id,
                                source_message=source_message,
                                target_chat_id=target_chat_id,
                                target_message_id=target_message_id,
                                chunk_index=chunk_index,
                                chunk_count=chunk_count,
                                status="error",
                                error_message=message,
                                delivery_random_id=delivery_random_id,
                                delivery_account=text_account,
                            )
                            admin_job_append_log_fn(
                                job_id,
                                f"时间线文本发送失败：source_message_id={source_message_id}，{message}",
                            )
                            if isinstance(exc, CloneTargetDeliveryUnconfirmedError):
                                raise
                            break
                        record_text_mapping(
                            get_conn_fn=get_conn_fn,
                            migration_id=migration_id,
                            run_id=run_id,
                            plan_id=plan_id,
                            source_message=source_message,
                            target_chat_id=target_chat_id,
                            target_message_id=target_message_id,
                            chunk_index=chunk_index,
                            chunk_count=chunk_count,
                            status="done",
                            delivery_random_id=delivery_random_id,
                            delivery_account=text_account,
                        )
                        _sleep_after_send(normalized_send_delay_ms)
                    state.counters.processed += 1
                    if message_failed:
                        state.counters.text_failed += 1
                    else:
                        state.counters.text_sent += 1

                elif item_type == "solo_media":
                    source_message = source_message_from_timeline_item(item)
                    source_message_id = int(source_message["message_id"])
                    if media_mapping_done(
                        get_conn_fn=get_conn_fn,
                        run_id=run_id,
                        source_chat_id=source_chat_id,
                        source_message_id=source_message_id,
                        mode="media_copy",
                    ):
                        state.counters.processed += 1
                        state.counters.media_skipped += 1
                        continue
                    try:
                        if source_client is None or source_entity is None:
                            raise RuntimeError("源侧媒体账号或实体尚未初始化")
                        resolved = clone_api_resolve_media_message(
                            source_client,
                            source_entity,
                            source_message_id,
                        )
                        if not resolved.get("ok"):
                            raise RuntimeError(
                                str(resolved.get("error") or "API 源媒体消息解析失败")
                            )
                        api_message_id = int(
                            resolved.get("message_id") or source_message_id
                        )
                        result = copy_media_to_target(api_message_id)
                        target_message_id = first_required_target_message_id(
                            result,
                            "时间线单条媒体复制",
                        )
                    except CloneMediaDeliverySafetyError:
                        raise
                    except Exception as exc:
                        state.counters.media_failed += 1
                        state.counters.processed += 1
                        message = admin_error_message(exc)
                        record_clone_media_mapping(
                            get_conn_fn=get_conn_fn,
                            migration_id=migration_id,
                            run_id=run_id,
                            plan_id=plan_id,
                            source_message=source_message,
                            target_chat_id=target_chat_id,
                            target_message_id=None,
                            mode="media_copy",
                            status="error",
                            error_message=message,
                        )
                        admin_job_append_log_fn(
                            job_id,
                            f"时间线单条媒体复制失败：source_message_id={source_message_id}，{message}",
                        )
                    else:
                        record_clone_media_mapping(
                            get_conn_fn=get_conn_fn,
                            migration_id=migration_id,
                            run_id=run_id,
                            plan_id=plan_id,
                            source_message=source_message,
                            target_chat_id=target_chat_id,
                            target_message_id=target_message_id,
                            mode="media_copy",
                            status="done",
                        )
                        state.counters.media_sent += 1
                        state.counters.processed += 1
                        _sleep_after_send(normalized_send_delay_ms)

                elif item_type == "media_group":
                    handle_media_group_item(
                        state=state,
                        item=item,
                        get_conn_fn=get_conn_fn,
                        admin_job_append_log_fn=admin_job_append_log_fn,
                        source_client=source_client,
                        source_entity=source_entity,
                        copy_media_to_target=copy_media_to_target,
                        sleep_after_send=_sleep_after_send,
                    )
                else:
                    state.counters.processed += source_count

                _update_progress(
                    state=state,
                    get_conn_fn=get_conn_fn,
                    phase="replaying_timeline",
                )
            if state.stopped or state.limit_reached:
                break

        if (
            not state.stopped
            and not state.limit_reached
            and source_client is not None
            and source_entity is not None
        ):
            latest_source_message_id = _source_latest_message_id(
                source_client,
                source_entity,
            )
            if latest_source_message_id <= 0:
                raise RuntimeError("无法确认源群最新消息，不能确认时间线迁移完成")
            if latest_source_message_id > state.source_snapshot_message_id:
                raise RuntimeError(
                    "源群在完整时间线迁移期间已有新消息，请先完成采集并重新执行在线深度预检，"
                    "以免将过期快照标记为完成"
                )

        _finalize_success(
            state=state,
            get_conn_fn=get_conn_fn,
            admin_job_set_status_fn=admin_job_set_status_fn,
            admin_job_append_log_fn=admin_job_append_log_fn,
        )
    except Exception as exc:
        logging.exception("克隆完整时间线迁移任务失败: job_id=%s", job_id)
        message = admin_error_message(exc)
        try_mark_migration_failed(
            get_conn_fn=get_conn_fn,
            migration_id=migration_id,
            message=message,
        )
        admin_job_append_log_fn(job_id, f"完整时间线迁移失败：{message}")
        _admin_job_update_progress(
            job_id,
            0,
            total=0,
            stage="error",
            log_step=0,
            auto_log=False,
        )
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        _cleanup_runner_clients(clients, worker_ids, account_cfgs)


def _admin_start_clone_timeline_migration_job_thread(job_id: str, **kwargs: Any):
    return start_admin_job_thread(
        _admin_clone_timeline_migration_job_runner,
        job_id,
        **kwargs,
    )
