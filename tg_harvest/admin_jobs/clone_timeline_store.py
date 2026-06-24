from collections.abc import Callable
from typing import Any

from tg_harvest.admin_jobs.common import call_with_conn
from tg_harvest.admin_jobs.runtime import _admin_now_iso
from tg_harvest.storage.clone import (
    list_clone_media_group_messages,
    list_clone_timeline_replay_batch,
    load_clone_message_mapping,
    record_clone_message_mapping,
)

CLONE_TIMELINE_BATCH_SIZE = 100


def next_timeline_batch(
    *,
    get_conn_fn: Callable[[], Any],
    run_id: str,
    source_chat_id: int,
    after_ts: int | None,
    after_message_id: int | None,
) -> list[dict[str, Any]]:
    return call_with_conn(
        get_conn_fn,
        list_clone_timeline_replay_batch,
        run_id=run_id,
        chat_id=source_chat_id,
        after_ts=after_ts,
        after_message_id=after_message_id,
        limit=CLONE_TIMELINE_BATCH_SIZE,
    )


def load_group_messages(
    *,
    get_conn_fn: Callable[[], Any],
    source_chat_id: int,
    grouped_id: int,
) -> list[dict[str, Any]]:
    return call_with_conn(
        get_conn_fn,
        list_clone_media_group_messages,
        chat_id=source_chat_id,
        grouped_id=grouped_id,
    )


def text_mapping_done(
    *,
    get_conn_fn: Callable[[], Any],
    run_id: str,
    source_chat_id: int,
    source_message_id: int,
    chunk_index: int,
) -> bool:
    mapping = call_with_conn(
        get_conn_fn,
        load_clone_message_mapping,
        run_id=run_id,
        source_chat_id=source_chat_id,
        source_message_id=source_message_id,
        chunk_index=chunk_index,
        mode="text_replay",
    )
    return mapping is not None and mapping.get("status") == "done"


def media_mapping_done(
    *,
    get_conn_fn: Callable[[], Any],
    run_id: str,
    source_chat_id: int,
    source_message_id: int,
    mode: str,
) -> bool:
    mapping = call_with_conn(
        get_conn_fn,
        load_clone_message_mapping,
        run_id=run_id,
        source_chat_id=source_chat_id,
        source_message_id=source_message_id,
        chunk_index=0,
        mode=mode,
    )
    return mapping is not None and mapping.get("status") == "done"


def record_text_mapping(
    *,
    get_conn_fn: Callable[[], Any],
    migration_id: str,
    run_id: str,
    plan_id: str,
    source_message: dict[str, Any],
    target_chat_id: int,
    target_message_id: int | None,
    chunk_index: int,
    chunk_count: int,
    status: str,
    error_message: str = "",
) -> None:
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
        chunk_index=int(chunk_index),
        chunk_count=int(chunk_count),
        mode="text_replay",
        status=status,
        error_message=error_message,
        sent_at=_admin_now_iso() if status == "done" else "",
    )


def source_message_from_timeline_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "chat_id": int(item["chat_id"]),
        "message_id": int(item["source_message_id"]),
        "msg_date_ts": item.get("msg_date_ts"),
        "msg_date_text": item.get("msg_date_text"),
        "sort_ts": int(item.get("sort_ts") or 0),
        "text": str(item.get("text") or ""),
        "caption": str(item.get("text") or ""),
    }
