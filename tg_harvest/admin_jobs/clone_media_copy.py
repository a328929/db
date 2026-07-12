import logging
import sqlite3
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

from tg_harvest.admin_jobs.clone_forwarding import (
    clone_delete_copied_relay_messages,
    clone_forward_without_source_attribution,
)
from tg_harvest.admin_jobs.clone_timeline_store import CloneMappingPersistenceError
from tg_harvest.admin_jobs.common import call_with_conn, resolve_chat_entity
from tg_harvest.admin_jobs.runtime import _admin_now_iso
from tg_harvest.domain.clone_plan import (
    clone_plan_media_relay,
    clone_plan_media_relay_chat_id,
)
from tg_harvest.domain.coerce import clean_text as clean_clone_media_text
from tg_harvest.domain.coerce import optional_int
from tg_harvest.storage.clone import (
    CLONE_MEDIA_TRANSFER_DIRECT,
    CLONE_MEDIA_TRANSFER_RELAY,
    ensure_clone_media_transfers,
    mark_clone_media_transfer_cleanup_done,
    mark_clone_media_transfer_source_hop_sent,
    mark_clone_media_transfer_target_hop_sent,
    record_clone_media_transfer_error,
    record_clone_message_mapping,
)


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
) -> None:
    with suppress(Exception):
        call_with_conn(
            context.get_conn_fn,
            record_clone_media_transfer_error,
            run_id=context.run_id,
            source_chat_id=context.source_chat_id,
            source_message_ids=source_message_ids,
            message=str(exc),
        )


def _store_target_ids(
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
        mark_clone_media_transfer_target_hop_sent,
        run_id=context.run_id,
        source_chat_id=context.source_chat_id,
        target_message_ids_by_source=target_ids_by_source,
    )


def _has_complete_sent_message_ids(
    message_ids: list[int | None],
    *,
    expected_count: int,
) -> bool:
    return len(message_ids) == expected_count and all(
        message_id is not None and int(message_id) > 0 for message_id in message_ids
    )


def _store_returned_target_ids(
    context: CloneMediaTransferContext,
    source_message_ids: list[int],
    target_message_ids: list[int | None],
) -> None:
    matched_count = min(len(source_message_ids), len(target_message_ids))
    if matched_count:
        _store_target_ids(
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


def _forward_target_pending(
    *,
    client: Any,
    target_entity: Any,
    source_peer: Any,
    source_message_ids: list[int],
    transfers: list[dict],
    context: CloneMediaTransferContext,
) -> list[dict]:
    pending_positions = [
        index
        for index, transfer in enumerate(transfers)
        if transfer.get("target_hop_status") != "sent"
        or transfer.get("target_message_id") is None
    ]
    if not pending_positions:
        return transfers

    pending_source_ids = [source_message_ids[index] for index in pending_positions]
    pending_random_ids = [
        int(transfers[index]["target_random_id"]) for index in pending_positions
    ]
    forward_messages: int | list[int] = (
        pending_source_ids[0] if len(pending_source_ids) == 1 else pending_source_ids
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
        _record_transfer_error(context, pending_source_ids, exc)
        raise

    returned_ids = clone_sent_message_ids(result)
    _store_returned_target_ids(context, pending_source_ids, returned_ids)
    if not _has_complete_sent_message_ids(
        returned_ids,
        expected_count=len(pending_source_ids),
    ):
        exc = RuntimeError("目标媒体复制后未完整返回有效消息 ID")
        _record_transfer_error(context, pending_source_ids, exc)
        raise exc
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

    source_pending_positions = [
        index
        for index, transfer in enumerate(transfers)
        if transfer.get("source_hop_status") != "sent"
        or transfer.get("relay_message_id") is None
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
            exc = RuntimeError("媒体复制到固定中转频道后未完整返回有效消息 ID")
            _record_transfer_error(transfer_context, pending_source_ids, exc)
            raise exc
        transfers = _prepare_transfers(
            transfer_context,
            source_message_ids,
            strategy=CLONE_MEDIA_TRANSFER_RELAY,
        )

    relay_message_ids = [
        optional_int(transfer.get("relay_message_id")) for transfer in transfers
    ]
    if any(message_id is None for message_id in relay_message_ids):
        raise RuntimeError("中转媒体状态不完整，不能执行第二跳")

    target_pending_positions = [
        index
        for index, transfer in enumerate(transfers)
        if transfer.get("target_hop_status") != "sent"
        or transfer.get("target_message_id") is None
    ]
    if target_pending_positions:
        pending_source_ids = [
            source_message_ids[index] for index in target_pending_positions
        ]
        pending_relay_ids = [
            int(relay_message_ids[index]) for index in target_pending_positions
        ]
        pending_random_ids = [
            int(transfers[index]["target_random_id"])
            for index in target_pending_positions
        ]
        if callable(log_step):
            log_step("媒体桥接第二跳：中转频道 -> 克隆群")
        second_hop_messages: int | list[int] = (
            pending_relay_ids[0] if len(pending_relay_ids) == 1 else pending_relay_ids
        )
        try:
            target_result = clone_forward_without_source_attribution(
                target_client,
                target_entity,
                second_hop_messages,
                from_peer=relay_entity_for_target,
                random_ids=pending_random_ids,
            )
        except Exception as exc:
            _record_transfer_error(transfer_context, pending_source_ids, exc)
            raise

        target_sent_ids = clone_sent_message_ids(target_result)
        _store_returned_target_ids(
            transfer_context,
            pending_source_ids,
            target_sent_ids,
        )
        if not _has_complete_sent_message_ids(
            target_sent_ids,
            expected_count=len(pending_source_ids),
        ):
            exc = RuntimeError("中转媒体复制到目标后未完整返回有效消息 ID")
            _record_transfer_error(transfer_context, pending_source_ids, exc)
            raise exc
        transfers = _prepare_transfers(
            transfer_context,
            source_message_ids,
            strategy=CLONE_MEDIA_TRANSFER_RELAY,
        )

    cleanup_transfers = [
        transfer
        for transfer in transfers
        if transfer.get("target_hop_status") == "sent"
        and transfer.get("cleanup_status") != "done"
        and transfer.get("relay_message_id") is not None
    ]
    if cleanup_transfers:
        cleanup_ids = [
            int(transfer["relay_message_id"]) for transfer in cleanup_transfers
        ]
        cleanup_source_ids = [
            int(transfer["source_message_id"]) for transfer in cleanup_transfers
        ]
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
            if callable(log_step):
                log_step("已清理已确认送达的中转临时消息")
        except Exception as exc:
            _record_transfer_error(transfer_context, cleanup_source_ids, exc)
            logging.warning("清理中转媒体失败，将在后续恢复时重试: %s", exc)

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
