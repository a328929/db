import logging
import sqlite3
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.types import MessageEmpty

from tg_harvest.admin_jobs.clone_forwarding import (
    CloneForwardOutcomeAmbiguousError,
    clone_delete_copied_relay_messages,
    clone_forward_without_source_attribution,
)
from tg_harvest.admin_jobs.clone_timeline_store import CloneMappingPersistenceError
from tg_harvest.admin_jobs.common import call_with_conn, resolve_chat_entity
from tg_harvest.admin_jobs.runtime import _admin_now_iso
from tg_harvest.admin_jobs.sessions import bind_client_event_loop
from tg_harvest.domain.chat_ids import stored_chat_id_from_entity_id
from tg_harvest.domain.clone_plan import (
    clone_plan_media_relay,
    clone_plan_media_relay_chat_id,
)
from tg_harvest.domain.coerce import clean_text as clean_clone_media_text
from tg_harvest.domain.coerce import optional_int
from tg_harvest.ingest.flood_wait import call_with_bounded_retry
from tg_harvest.storage.clone import (
    CLONE_MEDIA_TRANSFER_DIRECT,
    CLONE_MEDIA_TRANSFER_RELAY,
    ensure_clone_media_transfers,
    list_clone_media_target_checkpoints,
    list_pending_clone_relay_cleanup,
    mark_clone_media_transfer_cleanup_done,
    mark_clone_media_transfer_source_hop_sent,
    mark_clone_media_transfer_target_hop_observed,
    mark_clone_media_transfer_target_hop_sent,
    record_clone_media_transfer_error,
    record_clone_message_mapping,
    rewind_clone_mappings_for_deleted_target_messages,
)

CLONE_TARGET_CONFIRM_ATTEMPTS = 3
CLONE_TARGET_CONFIRM_DELAY_SECONDS = 0.25
CLONE_RELAY_CLEANUP_BATCH_SIZE = 100
CLONE_TARGET_AUDIT_BATCH_SIZE = 100


class CloneMediaDeliverySafetyError(RuntimeError):
    """Stop migration when delivery or cleanup cannot be safely confirmed."""


class CloneTargetDeliveryUnconfirmedError(CloneMediaDeliverySafetyError):
    pass


class CloneRelayCleanupError(CloneMediaDeliverySafetyError):
    pass


@dataclass(frozen=True)
class CloneMediaTransferContext:
    get_conn_fn: Callable[[], Any]
    migration_id: str
    run_id: str
    plan_id: str
    source_chat_id: int
    target_chat_id: int
    source_account: str
    target_account: str
    relay_chat_id: int = 0


def clone_sent_message_ids(result: Any) -> list[int | None]:
    if result is None:
        return []
    items = result if isinstance(result, (list, tuple)) else [result]
    return [optional_int(getattr(item, "id", None)) for item in items]


def _source_message_ids(message_ids: int | list[int]) -> list[int]:
    items = message_ids if isinstance(message_ids, list) else [message_ids]
    normalized = [int(message_id) for message_id in items if optional_int(message_id)]
    if not normalized:
        raise RuntimeError("媒体复制缺少源消息 ID")
    if len(set(normalized)) != len(normalized):
        raise RuntimeError("媒体复制源消息 ID 不允许重复")
    return normalized


def _copy_result(message_ids: list[int | None], *, single: bool) -> Any:
    messages = [
        SimpleNamespace(id=int(message_id)) if message_id is not None else None
        for message_id in message_ids
    ]
    return messages[0] if single else messages


def _prepare_transfers(
    context: CloneMediaTransferContext,
    source_message_ids: list[int],
    *,
    strategy: str,
) -> list[dict]:
    return call_with_conn(
        context.get_conn_fn,
        ensure_clone_media_transfers,
        migration_id=context.migration_id,
        run_id=context.run_id,
        plan_id=context.plan_id,
        source_chat_id=context.source_chat_id,
        source_message_ids=source_message_ids,
        target_chat_id=context.target_chat_id,
        transfer_strategy=strategy,
        relay_chat_id=(
            context.relay_chat_id if strategy == CLONE_MEDIA_TRANSFER_RELAY else None
        ),
        source_account=context.source_account,
        target_account=context.target_account,
    )


def _record_transfer_error(
    context: CloneMediaTransferContext,
    source_message_ids: list[int],
    exc: Exception,
    *,
    ambiguous_hop: str = "",
) -> None:
    with suppress(Exception):
        call_with_conn(
            context.get_conn_fn,
            record_clone_media_transfer_error,
            run_id=context.run_id,
            source_chat_id=context.source_chat_id,
            source_message_ids=source_message_ids,
            message=str(exc),
            ambiguous_hop=ambiguous_hop,
        )


def _store_target_ids(
    context: CloneMediaTransferContext,
    source_message_ids: list[int],
    target_message_ids: list[int | None],
) -> None:
    """Store target message IDs in a database transaction.

    If this fails, the entire media transfer should be considered incomplete
    and may need to be retried with fresh random IDs.
    """
    target_ids_by_source = {
        source_message_id: int(target_message_id)
        for source_message_id, target_message_id in zip(
            source_message_ids,
            target_message_ids,
            strict=True,
        )
        if target_message_id is not None and int(target_message_id) > 0
    }
    if not target_ids_by_source:
        return
    try:
        call_with_conn(
            context.get_conn_fn,
            mark_clone_media_transfer_target_hop_sent,
            run_id=context.run_id,
            source_chat_id=context.source_chat_id,
            target_message_ids_by_source=target_ids_by_source,
        )
    except Exception as exc:
        # Database transaction failed - this is a critical error that prevents
        # safe retry with the same random IDs
        logging.exception(
            "Failed to persist target message IDs after successful Telegram delivery: "
            "run_id=%s source_chat_id=%s",
            context.run_id,
            context.source_chat_id,
        )
        raise CloneMappingPersistenceError(
            "媒体已成功发送到目标，但持久化映射失败；"
            "迁移已中止以避免使用相同随机ID重复发送"
        ) from exc


def _store_observed_target_ids(
    context: CloneMediaTransferContext,
    source_message_ids: list[int],
    target_message_ids: list[int | None],
) -> None:
    target_ids_by_source = {
        source_message_id: int(target_message_id)
        for source_message_id, target_message_id in zip(
            source_message_ids,
            target_message_ids,
            strict=True,
        )
        if target_message_id is not None and int(target_message_id) > 0
    }
    if not target_ids_by_source:
        return
    call_with_conn(
        context.get_conn_fn,
        mark_clone_media_transfer_target_hop_observed,
        run_id=context.run_id,
        source_chat_id=context.source_chat_id,
        target_message_ids_by_source=target_ids_by_source,
    )


def _message_items(result: Any) -> list[Any]:
    if result is None:
        return []
    if isinstance(result, (list, tuple)):
        return list(result)
    try:
        return list(result)
    except TypeError:
        return [result]


def load_clone_relay_participant_count(client: Any, entity: Any) -> int | None:
    """Read relay membership from complete channel data when the entity is sparse."""
    participant_count = optional_int(getattr(entity, "participants_count", None))
    if participant_count is not None:
        return participant_count

    def _load_full_channel() -> Any:
        with bind_client_event_loop(client):
            return client(GetFullChannelRequest(entity))

    result = call_with_bounded_retry(
        _load_full_channel,
        scope="clone-relay-full-channel",
    )
    return optional_int(
        getattr(getattr(result, "full_chat", None), "participants_count", None)
    )


def confirm_clone_target_messages(
    client: Any,
    target_entity: Any,
    message_ids: list[int],
    *,
    context: str,
) -> None:
    """Require the target account to read back every Telegram-returned message."""
    normalized_ids = [int(message_id) for message_id in message_ids if message_id]
    if not normalized_ids:
        raise CloneTargetDeliveryUnconfirmedError(f"{context}没有可确认的目标消息 ID")

    visible_ids, last_error = load_clone_visible_target_message_ids(
        client,
        target_entity,
        normalized_ids,
    )
    if all(message_id in visible_ids for message_id in normalized_ids):
        return

    missing_ids = [
        message_id for message_id in normalized_ids if message_id not in visible_ids
    ]
    detail = f"；读取错误：{last_error}" if last_error is not None else ""
    raise CloneTargetDeliveryUnconfirmedError(
        f"{context}已返回目标消息 ID，但第二账号无法从克隆群回读消息 "
        f"{missing_ids}。任务已停止且不会盲目重发；请确认第二账号仍在正确的"
        f"克隆群，并同时具有发消息、查看消息和查看历史权限{detail}"
    )


def load_clone_visible_target_message_ids(
    client: Any,
    target_entity: Any,
    message_ids: list[int],
) -> tuple[set[int], Exception | None]:
    """Return real target messages, excluding Telegram ``MessageEmpty`` tombstones."""
    normalized_ids = [int(message_id) for message_id in message_ids if message_id]
    if not normalized_ids:
        return set(), None

    get_messages = getattr(client, "get_messages", None)
    if not callable(get_messages):
        # Lightweight test clients and compatibility adapters may only expose
        # forwarding. Real Telethon clients always provide get_messages.
        return set(normalized_ids), None

    last_error: Exception | None = None
    visible_ids: set[int] = set()
    for attempt in range(CLONE_TARGET_CONFIRM_ATTEMPTS):
        try:
            def _load_target_messages() -> Any:
                with bind_client_event_loop(client):
                    return get_messages(target_entity, ids=normalized_ids)

            result = call_with_bounded_retry(
                _load_target_messages,
                scope="clone-target-delivery-confirmation",
            )
            visible_ids = {
                int(message_id)
                for item in _message_items(result)
                if item is not None and not isinstance(item, MessageEmpty)
                for message_id in [optional_int(getattr(item, "id", None))]
                if message_id is not None and int(message_id) > 0
            }
            if all(message_id in visible_ids for message_id in normalized_ids):
                return visible_ids, None
            last_error = None
        except Exception as exc:
            last_error = exc

        if attempt + 1 < CLONE_TARGET_CONFIRM_ATTEMPTS:
            time.sleep(CLONE_TARGET_CONFIRM_DELAY_SECONDS)

    return visible_ids, last_error


def _has_complete_sent_message_ids(
    message_ids: list[int | None],
    *,
    expected_count: int,
) -> bool:
    return len(message_ids) == expected_count and all(
        message_id is not None and int(message_id) > 0 for message_id in message_ids
    )


def _store_returned_target_ids_as_observed(
    context: CloneMediaTransferContext,
    source_message_ids: list[int],
    target_message_ids: list[int | None],
) -> None:
    matched_count = min(len(source_message_ids), len(target_message_ids))
    if matched_count:
        _store_observed_target_ids(
            context,
            source_message_ids[:matched_count],
            target_message_ids[:matched_count],
        )


def _transfer_target_ids(transfers: list[dict]) -> list[int | None]:
    return [
        int(item["target_message_id"])
        if item.get("target_hop_status") == "sent"
        and item.get("target_message_id") is not None
        else None
        for item in transfers
    ]


def _rewind_missing_target_deliveries(
    context: CloneMediaTransferContext,
    target_message_ids: list[int],
) -> None:
    if not target_message_ids:
        return
    call_with_conn(
        context.get_conn_fn,
        rewind_clone_mappings_for_deleted_target_messages,
        run_id=context.run_id,
        target_chat_id=context.target_chat_id,
        target_message_ids=target_message_ids,
    )


def _refresh_missing_sent_transfers(
    *,
    client: Any,
    target_entity: Any,
    source_message_ids: list[int],
    transfers: list[dict],
    context: CloneMediaTransferContext,
) -> list[dict]:
    sent_target_ids = [
        int(transfer["target_message_id"])
        for transfer in transfers
        if transfer.get("target_hop_status") == "sent"
        and optional_int(transfer.get("target_message_id")) is not None
    ]
    if not sent_target_ids:
        return transfers
    visible_ids, last_error = load_clone_visible_target_message_ids(
        client,
        target_entity,
        sent_target_ids,
    )
    if last_error is not None and not visible_ids:
        raise CloneTargetDeliveryUnconfirmedError(
            f"恢复媒体检查点时无法读取克隆目标：{last_error}"
        )
    missing_ids = [
        message_id for message_id in sent_target_ids if message_id not in visible_ids
    ]
    if not missing_ids:
        return transfers
    _rewind_missing_target_deliveries(context, missing_ids)
    return _prepare_transfers(
        context,
        source_message_ids,
        strategy=transfers[0]["transfer_strategy"],
    )


def reconcile_clone_media_target_checkpoints(
    *,
    client: Any,
    target_entity: Any,
    transfer_context: CloneMediaTransferContext,
    log_step: Any | None = None,
) -> dict[str, int]:
    """Rewind database successes whose target media no longer exists."""
    checkpoints = call_with_conn(
        transfer_context.get_conn_fn,
        list_clone_media_target_checkpoints,
        run_id=transfer_context.run_id,
        target_chat_id=transfer_context.target_chat_id,
    )
    target_ids = sorted(
        {
            int(target_id)
            for checkpoint in checkpoints
            for target_id in [optional_int(checkpoint.get("target_message_id"))]
            if target_id is not None and int(target_id) > 0
        }
    )
    missing_ids: list[int] = []
    for start in range(0, len(target_ids), CLONE_TARGET_AUDIT_BATCH_SIZE):
        batch = target_ids[start : start + CLONE_TARGET_AUDIT_BATCH_SIZE]
        visible_ids, last_error = load_clone_visible_target_message_ids(
            client,
            target_entity,
            batch,
        )
        if last_error is not None and not visible_ids:
            raise CloneTargetDeliveryUnconfirmedError(
                f"核验历史媒体检查点时无法读取克隆目标：{last_error}"
            )
        missing_ids.extend(
            message_id for message_id in batch if message_id not in visible_ids
        )

    if missing_ids:
        _rewind_missing_target_deliveries(transfer_context, missing_ids)
        if callable(log_step):
            log_step(
                "已发现并回退 "
                f"{len(missing_ids)} 条远端已不存在的媒体成功检查点。"
            )
    return {
        "checked_count": len(target_ids),
        "missing_count": len(missing_ids),
    }


def _forward_target_pending(
    *,
    client: Any,
    target_entity: Any,
    source_peer: Any,
    source_message_ids: list[int],
    transfers: list[dict],
    context: CloneMediaTransferContext,
    forward_message_ids: list[int] | None = None,
    confirmation_context: str = "媒体复制目标确认",
) -> list[dict]:
    transfers = _refresh_missing_sent_transfers(
        client=client,
        target_entity=target_entity,
        source_message_ids=source_message_ids,
        transfers=transfers,
        context=context,
    )
    observed_positions = [
        index
        for index, transfer in enumerate(transfers)
        if transfer.get("target_hop_status") != "sent"
        and optional_int(transfer.get("target_message_id")) is not None
    ]
    if observed_positions:
        observed_target_ids = [
            int(transfers[index]["target_message_id"])
            for index in observed_positions
        ]
        try:
            confirm_clone_target_messages(
                client,
                target_entity,
                observed_target_ids,
                context=confirmation_context,
            )
            _store_target_ids(
                context,
                [source_message_ids[index] for index in observed_positions],
                observed_target_ids,
            )
            transfers = _prepare_transfers(
                context,
                source_message_ids,
                strategy=transfers[0]["transfer_strategy"],
            )
        except Exception as exc:
            _record_transfer_error(
                context,
                [source_message_ids[index] for index in observed_positions],
                exc,
            )
            raise

    pending_positions = [
        index
        for index, transfer in enumerate(transfers)
        if transfer.get("target_hop_status") != "sent"
        or transfer.get("target_message_id") is None
    ]
    if not pending_positions:
        return transfers

    pending_source_ids = [source_message_ids[index] for index in pending_positions]
    effective_forward_ids = forward_message_ids or source_message_ids
    pending_forward_ids = [effective_forward_ids[index] for index in pending_positions]
    pending_random_ids = [
        int(transfers[index]["target_random_id"]) for index in pending_positions
    ]
    forward_messages: int | list[int] = (
        pending_forward_ids[0] if len(pending_forward_ids) == 1 else pending_forward_ids
    )
    try:
        result = clone_forward_without_source_attribution(
            client,
            target_entity,
            forward_messages,
            from_peer=source_peer,
            random_ids=pending_random_ids,
        )
    except Exception as exc:
        if isinstance(exc, CloneForwardOutcomeAmbiguousError):
            safety_error = CloneMediaDeliverySafetyError(str(exc))
            _record_transfer_error(
                context,
                pending_source_ids,
                safety_error,
                ambiguous_hop="target",
            )
            raise safety_error from exc
        _record_transfer_error(context, pending_source_ids, exc)
        raise

    returned_ids = clone_sent_message_ids(result)
    _store_returned_target_ids_as_observed(
        context,
        pending_source_ids,
        returned_ids,
    )
    if not _has_complete_sent_message_ids(
        returned_ids,
        expected_count=len(pending_source_ids),
    ):
        exc = CloneMediaDeliverySafetyError(
            "目标媒体复制后未完整返回有效消息 ID，未返回 ID 的投递结果无法确认"
        )
        ambiguous_source_ids = [
            source_message_id
            for index, source_message_id in enumerate(pending_source_ids)
            if index >= len(returned_ids)
            or optional_int(returned_ids[index]) is None
        ]
        _record_transfer_error(
            context,
            ambiguous_source_ids,
            exc,
            ambiguous_hop="target",
        )
        raise exc
    try:
        confirmed_ids = [int(message_id) for message_id in returned_ids if message_id]
        confirm_clone_target_messages(
            client,
            target_entity,
            confirmed_ids,
            context=confirmation_context,
        )
        _store_target_ids(context, pending_source_ids, returned_ids)
    except Exception as exc:
        _record_transfer_error(context, pending_source_ids, exc)
        raise
    return _prepare_transfers(
        context,
        source_message_ids,
        strategy=transfers[0]["transfer_strategy"],
    )


def resolve_clone_relay_chat(client: Any, plan: dict[str, Any]) -> Any:
    relay = clone_plan_media_relay(plan)
    relay_chat_id = clone_plan_media_relay_chat_id(plan)
    if not relay_chat_id:
        raise RuntimeError("迁移计划缺少固定中转频道")
    return resolve_chat_entity(
        client,
        relay_chat_id,
        clean_clone_media_text(relay.get("username")),
        allow_username_fallback=False,
    )


def validate_clone_relay_execution(
    *,
    relay_entity_for_source: Any,
    relay_entity_for_target: Any,
    relay_chat_id: int,
    source_chat_id: int,
    target_chat_id: int,
    relay_participant_count_for_source: int | None = None,
    relay_participant_count_for_target: int | None = None,
) -> None:
    """Validate relay channel safety before media transfer.

    SECURITY NOTE: This check has a TOCTOU window between validation and actual
    transfer. The participant count could change after validation but before the
    transfer completes. This is an inherent limitation of the Telegram API.
    Mitigation: Keep the window as small as possible and re-validate if the
    transfer fails with unexpected errors.
    """
    expected_id = stored_chat_id_from_entity_id(relay_chat_id)
    protected_ids = {
        stored_chat_id_from_entity_id(source_chat_id),
        stored_chat_id_from_entity_id(target_chat_id),
    }
    if expected_id in protected_ids:
        raise RuntimeError("固定中转频道不能与源群或克隆目标相同，请重新执行在线深度预检")

    entities_with_counts = (
        (relay_entity_for_source, relay_participant_count_for_source),
        (relay_entity_for_target, relay_participant_count_for_target),
    )
    for entity, full_participant_count in entities_with_counts:
        entity_id = optional_int(getattr(entity, "id", None))
        if entity_id is None or stored_chat_id_from_entity_id(entity_id) != expected_id:
            raise RuntimeError("两个账号解析到的中转频道身份不一致，已停止迁移")
        if clean_clone_media_text(getattr(entity, "username", "")):
            raise RuntimeError("固定中转频道已变为公开频道，已停止迁移")
        if not bool(getattr(entity, "broadcast", False)) or bool(
            getattr(entity, "megagroup", False)
        ):
            raise RuntimeError("固定中转目标不再是私有广播频道，已停止迁移")
        participant_count = optional_int(full_participant_count)
        if participant_count is None:
            participant_count = optional_int(
                getattr(entity, "participants_count", None)
            )
        if participant_count is None:
            raise RuntimeError("无法确认固定中转频道成员数量，请重新执行在线深度预检")
        if participant_count > 2:
            raise RuntimeError("固定中转频道存在额外成员，拒绝暂存源媒体")

    source_is_creator = bool(getattr(relay_entity_for_source, "creator", False))
    source_admin_rights = getattr(relay_entity_for_source, "admin_rights", None)
    if not source_is_creator and not bool(
        getattr(source_admin_rights, "delete_messages", False)
    ):
        raise RuntimeError("源侧账号缺少中转频道删除消息权限，无法保证临时媒体清理")


def cleanup_pending_clone_relay_messages(
    *,
    source_client: Any,
    relay_entity_for_source: Any,
    transfer_context: CloneMediaTransferContext,
    log_step: Any | None = None,
    include_incomplete_target: bool = False,
) -> int:
    transfers = call_with_conn(
        transfer_context.get_conn_fn,
        list_pending_clone_relay_cleanup,
        run_id=transfer_context.run_id,
        source_chat_id=transfer_context.source_chat_id,
        relay_chat_id=transfer_context.relay_chat_id,
        include_incomplete_target=include_incomplete_target,
    )
    cleaned = 0
    for index in range(0, len(transfers), CLONE_RELAY_CLEANUP_BATCH_SIZE):
        batch = transfers[index : index + CLONE_RELAY_CLEANUP_BATCH_SIZE]
        cleanup_ids = [int(transfer["relay_message_id"]) for transfer in batch]
        cleanup_source_ids = [int(transfer["source_message_id"]) for transfer in batch]
        try:
            clone_delete_copied_relay_messages(
                source_client,
                relay_entity_for_source,
                cleanup_ids,
            )
            call_with_conn(
                transfer_context.get_conn_fn,
                mark_clone_media_transfer_cleanup_done,
                run_id=transfer_context.run_id,
                source_chat_id=transfer_context.source_chat_id,
                source_message_ids=cleanup_source_ids,
            )
        except Exception as exc:
            detail = (
                "完整回退时中转临时消息清理失败"
                if include_incomplete_target
                else "克隆消息已送达目标，但中转临时消息清理失败"
            )
            error = CloneRelayCleanupError(
                f"{detail}；任务已停止，请保留当前记录并重新发起以重试清理：{exc}"
            )
            _record_transfer_error(transfer_context, cleanup_source_ids, error)
            raise error from exc
        cleaned += len(batch)

    if cleaned and callable(log_step):
        log_step(f"已确认清理 {cleaned} 条中转临时消息")
    return cleaned


def copy_clone_media_direct_without_source(
    *,
    client: Any,
    target_entity: Any,
    message_ids: int | list[int],
    source_entity: Any,
    transfer_context: CloneMediaTransferContext,
) -> Any:
    source_message_ids = _source_message_ids(message_ids)
    transfers = _prepare_transfers(
        transfer_context,
        source_message_ids,
        strategy=CLONE_MEDIA_TRANSFER_DIRECT,
    )
    transfers = _forward_target_pending(
        client=client,
        target_entity=target_entity,
        source_peer=source_entity,
        source_message_ids=source_message_ids,
        transfers=transfers,
        context=transfer_context,
        confirmation_context="媒体复制到克隆群确认",
    )
    return _copy_result(
        _transfer_target_ids(transfers),
        single=not isinstance(message_ids, list),
    )


def copy_clone_media_via_relay_without_source(
    *,
    source_client: Any,
    target_client: Any,
    relay_entity_for_source: Any,
    relay_entity_for_target: Any,
    target_entity: Any,
    message_ids: int | list[int],
    source_entity: Any,
    transfer_context: CloneMediaTransferContext,
    log_step: Any | None = None,
) -> Any:
    source_message_ids = _source_message_ids(message_ids)
    transfers = _prepare_transfers(
        transfer_context,
        source_message_ids,
        strategy=CLONE_MEDIA_TRANSFER_RELAY,
    )
    transfers = _refresh_missing_sent_transfers(
        client=target_client,
        target_entity=target_entity,
        source_message_ids=source_message_ids,
        transfers=transfers,
        context=transfer_context,
    )

    source_pending_positions = [
        index
        for index, transfer in enumerate(transfers)
        if (
            transfer.get("target_hop_status") != "sent"
            or transfer.get("target_message_id") is None
        )
        and (
            transfer.get("source_hop_status") != "sent"
            or transfer.get("relay_message_id") is None
        )
    ]
    if source_pending_positions:
        pending_source_ids = [
            source_message_ids[index] for index in source_pending_positions
        ]
        pending_random_ids = [
            int(transfers[index]["source_random_id"])
            for index in source_pending_positions
        ]
        if callable(log_step):
            log_step("媒体桥接第一跳：源群 -> 中转频道")
        first_hop_messages: int | list[int] = (
            pending_source_ids[0]
            if len(pending_source_ids) == 1
            else pending_source_ids
        )
        try:
            relay_result = clone_forward_without_source_attribution(
                source_client,
                relay_entity_for_source,
                first_hop_messages,
                from_peer=source_entity,
                random_ids=pending_random_ids,
            )
        except Exception as exc:
            if isinstance(exc, CloneForwardOutcomeAmbiguousError):
                safety_error = CloneMediaDeliverySafetyError(str(exc))
                _record_transfer_error(
                    transfer_context,
                    pending_source_ids,
                    safety_error,
                    ambiguous_hop="source",
                )
                raise safety_error from exc
            _record_transfer_error(transfer_context, pending_source_ids, exc)
            raise

        relay_sent_ids = clone_sent_message_ids(relay_result)
        relay_ids_by_source = {
            source_message_id: int(relay_message_id)
            for source_message_id, relay_message_id in zip(
                pending_source_ids,
                relay_sent_ids,
                strict=False,
            )
            if relay_message_id is not None and int(relay_message_id) > 0
        }
        if relay_ids_by_source:
            call_with_conn(
                transfer_context.get_conn_fn,
                mark_clone_media_transfer_source_hop_sent,
                run_id=transfer_context.run_id,
                source_chat_id=transfer_context.source_chat_id,
                relay_message_ids_by_source=relay_ids_by_source,
            )
        if not _has_complete_sent_message_ids(
            relay_sent_ids,
            expected_count=len(pending_source_ids),
        ):
            exc = CloneMediaDeliverySafetyError(
                "媒体复制到固定中转频道后未完整返回有效消息 ID，"
                "未返回 ID 的投递结果无法确认"
            )
            ambiguous_source_ids = [
                source_message_id
                for index, source_message_id in enumerate(pending_source_ids)
                if index >= len(relay_sent_ids)
                or optional_int(relay_sent_ids[index]) is None
            ]
            _record_transfer_error(
                transfer_context,
                ambiguous_source_ids,
                exc,
                ambiguous_hop="source",
            )
            raise exc
        transfers = _prepare_transfers(
            transfer_context,
            source_message_ids,
            strategy=CLONE_MEDIA_TRANSFER_RELAY,
        )

    target_pending_positions = [
        index
        for index, transfer in enumerate(transfers)
        if transfer.get("target_hop_status") != "sent"
        or transfer.get("target_message_id") is None
    ]
    relay_message_ids = [
        optional_int(transfer.get("relay_message_id")) for transfer in transfers
    ]
    if any(relay_message_ids[index] is None for index in target_pending_positions):
        raise RuntimeError("中转媒体状态不完整，不能执行第二跳")

    if callable(log_step) and any(
        transfer.get("target_hop_status") != "sent"
        or transfer.get("target_message_id") is None
        for transfer in transfers
    ):
        log_step("媒体桥接第二跳：中转频道 -> 克隆群（匿名复制）")

    transfers = _forward_target_pending(
        client=target_client,
        target_entity=target_entity,
        source_peer=relay_entity_for_target,
        source_message_ids=source_message_ids,
        transfers=transfers,
        context=transfer_context,
        forward_message_ids=[int(message_id or 0) for message_id in relay_message_ids],
        confirmation_context="中转第二跳（第二账号 -> 克隆群）确认",
    )

    cleanup_pending_clone_relay_messages(
        source_client=source_client,
        relay_entity_for_source=relay_entity_for_source,
        transfer_context=transfer_context,
        log_step=log_step,
    )

    target_ids = [
        int(target_id)
        for target_id in _transfer_target_ids(transfers)
        if target_id is not None
    ]
    visible_ids, last_error = load_clone_visible_target_message_ids(
        target_client,
        target_entity,
        target_ids,
    )
    missing_target_ids = [
        target_id for target_id in target_ids if target_id not in visible_ids
    ]
    if missing_target_ids:
        _rewind_missing_target_deliveries(
            transfer_context,
            missing_target_ids,
        )
        detail = f"；读取错误：{last_error}" if last_error is not None else ""
        raise CloneMediaDeliverySafetyError(
            "中转消息清理后，克隆目标媒体未能保持存在："
            f"{missing_target_ids}{detail}。已回退对应检查点并停止迁移。"
        )

    return _copy_result(
        _transfer_target_ids(transfers),
        single=not isinstance(message_ids, list),
    )


def clone_source_message_for_api_id(
    *,
    source_chat_id: int,
    source_message_id: int,
    db_messages_by_id: dict[int, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    source_message = (db_messages_by_id or {}).get(int(source_message_id))
    if source_message is not None:
        return source_message
    return {
        "chat_id": int(source_chat_id),
        "message_id": int(source_message_id),
        "msg_date_ts": None,
        "msg_date_text": "",
        "sort_ts": 0,
        "caption": "",
    }


def resolved_clone_group_key(
    resolved_group: dict[str, Any],
    message_ids: list[int],
) -> tuple[Any, ...]:
    api_grouped_id = resolved_group.get("grouped_id")
    if api_grouped_id not in (None, ""):
        try:
            return ("grouped_id", int(api_grouped_id))
        except (TypeError, ValueError):
            return ("grouped_id", str(api_grouped_id))
    return ("message_ids", tuple(int(message_id) for message_id in message_ids))


def record_clone_media_mapping(
    *,
    get_conn_fn: Any,
    migration_id: str,
    run_id: str,
    plan_id: str,
    source_message: dict[str, Any],
    target_chat_id: int,
    target_message_id: int | None,
    mode: str,
    status: str,
    error_message: str = "",
) -> None:
    try:
        call_with_conn(
            get_conn_fn,
            record_clone_message_mapping,
            migration_id=migration_id,
            run_id=run_id,
            plan_id=plan_id,
            source_chat_id=int(source_message["chat_id"]),
            source_message_id=int(source_message["message_id"]),
            source_msg_date_ts=source_message.get("msg_date_ts"),
            source_msg_date_text=source_message.get("msg_date_text"),
            target_chat_id=int(target_chat_id),
            target_message_id=target_message_id,
            chunk_index=0,
            chunk_count=1,
            mode=mode,
            status=status,
            error_message=error_message,
            sent_at=_admin_now_iso() if status == "done" else "",
        )
    except (sqlite3.Error, OSError, RuntimeError, TypeError, ValueError) as exc:
        logging.exception(
            "克隆媒体映射持久化失败: run_id=%s source=%s/%s mode=%s status=%s",
            run_id,
            source_message.get("chat_id"),
            source_message.get("message_id"),
            mode,
            status,
        )
        raise CloneMappingPersistenceError(
            "克隆媒体已发送但映射持久化失败，迁移已中止以避免重复发送"
        ) from exc
