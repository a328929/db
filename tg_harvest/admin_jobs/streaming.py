# -*- coding: utf-8 -*-
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

from tg_harvest.admin_jobs.update_writer import ChatUpdateWriteCoordinator
from tg_harvest.ingest.parse import HarvestCounters


@dataclass
class StreamedEntityHarvestResult:
    chat_id: int
    chat_title: str
    chat_username: Optional[str]
    counters: Any
    touched_groups: set[int]
    first_sync: bool
    submitted_message_count: int


def _entity_chat_id(entity: Any, fallback_chat_id: Optional[int]) -> int:
    raw_chat_id = (
        fallback_chat_id if fallback_chat_id is not None else getattr(entity, "id", None)
    )
    chat_id = int(raw_chat_id or 0)
    if not chat_id:
        raise RuntimeError("无法识别群组/频道 ID，采集已中止")
    return chat_id


def _entity_chat_title(
    entity: Any, fallback_chat_title: Optional[str], chat_id: int
) -> str:
    return str(
        getattr(entity, "title", None)
        or getattr(entity, "username", None)
        or fallback_chat_title
        or chat_id
    )


def _collect_touched_groups(msg_rows: list[tuple]) -> set[int]:
    touched_groups: set[int] = set()
    for row in msg_rows:
        grouped_id = row[10] if len(row) > 10 else None
        if grouped_id is not None:
            touched_groups.add(int(grouped_id))
    return touched_groups


def _partial_counters(submitted_message_count: int) -> HarvestCounters:
    return HarvestCounters(seen=0, written=max(int(submitted_message_count), 0))


def stream_entity_harvest_to_writer(
    *,
    write_coordinator: ChatUpdateWriteCoordinator,
    get_conn_fn: Callable[[], Any],
    client: Any,
    entity: Any,
    idx: int,
    total: int,
    fallback_chat_id: Optional[int] = None,
    fallback_chat_title: Optional[str] = None,
    fallback_chat_username: Optional[str] = None,
    skip_postprocess_if_unchanged: bool = False,
    enable_dedupe: bool = True,
) -> StreamedEntityHarvestResult:
    """Harvest one entity while delegating all writes to the shared writer thread."""

    from tg_harvest.ingest.runner import _harvest_messages_for_entity

    chat_id = _entity_chat_id(entity, fallback_chat_id)
    chat_title = _entity_chat_title(entity, fallback_chat_title, chat_id)
    chat_username = getattr(entity, "username", None)
    chat_type = entity.__class__.__name__

    logging.info(f"[{idx}/{total}] 正在处理: {chat_title} (ID={chat_id})")

    registered_chat = False
    finalize_submitted = False
    submitted_message_count = 0
    submitted_touched_groups: set[int] = set()
    total_started_at = time.perf_counter()

    def _submit_harvest_batch(msg_rows: list[tuple], media_rows: list[tuple]) -> None:
        nonlocal submitted_message_count
        write_coordinator.submit_batch(
            chat_id=chat_id,
            msg_rows=msg_rows,
            media_rows=media_rows,
        )
        submitted_message_count += len(msg_rows)
        submitted_touched_groups.update(_collect_touched_groups(msg_rows))

    def _submit_partial_finalize() -> None:
        if not registered_chat or submitted_message_count <= 0 or finalize_submitted:
            return
        write_coordinator.submit_finalize(
            chat_id=chat_id,
            chat_title=chat_title,
            counters=_partial_counters(submitted_message_count),
            touched_groups=submitted_touched_groups,
            first_sync=False,
            total_started_at=total_started_at,
            skip_postprocess_if_unchanged=False,
            enable_dedupe=False,
        )
        write_coordinator.wait_for_chat(chat_id)

    try:
        write_coordinator.register_chat(chat_id)
        registered_chat = True
        write_coordinator.submit_chat_start(
            chat_id=chat_id,
            chat_title=chat_title,
            chat_username=chat_username,
            chat_type=chat_type,
        )

        read_conn = get_conn_fn()
        try:
            counters, touched_groups, first_sync = _harvest_messages_for_entity(
                read_conn,
                client,
                entity,
                chat_id,
                write_batch_fn=_submit_harvest_batch,
            )
        finally:
            read_conn.close()

        finalize_submitted = True
        write_coordinator.submit_finalize(
            chat_id=chat_id,
            chat_title=chat_title,
            counters=counters,
            touched_groups=touched_groups,
            first_sync=first_sync,
            total_started_at=total_started_at,
            skip_postprocess_if_unchanged=skip_postprocess_if_unchanged,
            enable_dedupe=enable_dedupe,
        )
        write_coordinator.wait_for_chat(chat_id)
        return StreamedEntityHarvestResult(
            chat_id=chat_id,
            chat_title=chat_title,
            chat_username=chat_username or fallback_chat_username,
            counters=counters,
            touched_groups=set(touched_groups),
            first_sync=first_sync,
            submitted_message_count=submitted_message_count,
        )
    except Exception:
        try:
            _submit_partial_finalize()
        except Exception:
            logging.exception(
                "采集失败后刷新已写入数据也失败: chat_id=%s",
                chat_id,
            )
        raise
