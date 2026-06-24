from collections.abc import Callable
from typing import Any

from tg_harvest.admin_jobs.clone_job_state import _clean_text
from tg_harvest.admin_jobs.clone_media_copy import (
    clone_sent_message_ids,
    clone_source_message_for_api_id,
    record_clone_media_mapping,
    resolved_clone_group_key,
)
from tg_harvest.admin_jobs.clone_media_resolver import (
    clone_album_message_id_chunks,
    clone_api_resolve_media_group,
)
from tg_harvest.admin_jobs.clone_timeline_state import first_required_target_message_id
from tg_harvest.admin_jobs.clone_timeline_store import (
    load_group_messages,
    media_mapping_done,
)
from tg_harvest.admin_jobs.clone_timeline_types import TimelineExecutionState
from tg_harvest.admin_jobs.common import admin_error_message


def handle_media_group_item(
    *,
    state: TimelineExecutionState,
    item: dict[str, Any],
    get_conn_fn: Callable[[], Any],
    admin_job_append_log_fn: Callable[[str, str], Any],
    source_client: Any,
    source_entity: Any,
    copy_media_to_target,
    sleep_after_send,
) -> None:
    grouped_id = item.get("grouped_id")
    if grouped_id in (None, ""):
        source_count = max(1, int(item.get("item_count") or 1))
        state.counters.processed += source_count
        state.counters.media_skipped += source_count
        state.counters.media_group_skipped += 1
        return

    messages = load_group_messages(
        get_conn_fn=get_conn_fn,
        source_chat_id=state.source_chat_id,
        grouped_id=int(grouped_id),
    )
    if not messages:
        source_count = max(1, int(item.get("item_count") or 1))
        state.counters.processed += source_count
        state.counters.media_skipped += source_count
        state.counters.media_group_skipped += 1
        return
    db_messages_by_id = {int(message["message_id"]): message for message in messages}

    try:
        if source_client is None or source_entity is None:
            raise RuntimeError("源侧媒体账号或实体尚未初始化")
        resolved_group = clone_api_resolve_media_group(
            source_client,
            source_entity,
            [int(message["message_id"]) for message in messages],
        )
        if not resolved_group.get("ok"):
            raise RuntimeError(str(resolved_group.get("error") or "API 媒体组解析失败"))
        resolved_message_ids = [
            int(message_id) for message_id in resolved_group.get("message_ids") or []
        ]
        if not resolved_message_ids:
            raise RuntimeError("API 未解析出可复制媒体组成员")
    except Exception as exc:
        message = admin_error_message(exc)
        state.counters.media_failed += len(messages)
        state.counters.media_group_failed += 1
        state.counters.processed += len(messages)
        for source_message in messages:
            record_clone_media_mapping(
                get_conn_fn=get_conn_fn,
                migration_id=state.migration_id,
                run_id=state.run_id,
                plan_id=state.plan_id,
                source_message=source_message,
                target_chat_id=state.target_chat_id,
                target_message_id=None,
                mode="media_group_copy",
                status="error",
                error_message=message,
            )
        admin_job_append_log_fn(
            state.job_id,
            f"时间线媒体组 API 解析失败：grouped_id={grouped_id}，{message}",
        )
        return

    resolved_group_key = resolved_clone_group_key(resolved_group, resolved_message_ids)
    resolved_message_id_set = set(resolved_message_ids)
    resolved_items = [
        media_item
        for media_item in resolved_group.get("items") or []
        if int(media_item.get("message_id") or 0) in resolved_message_id_set
    ]
    resolved_items_by_id = {
        int(media_item["message_id"]): media_item
        for media_item in resolved_items
        if int(media_item.get("message_id") or 0)
    }
    copy_strategy = _clean_text(resolved_group.get("copy_strategy")).lower()
    copy_as_group = copy_strategy == "album" and len(resolved_message_ids) > 1
    album_chunks = (
        clone_album_message_id_chunks(resolved_message_ids) if copy_as_group else []
    )
    if resolved_group_key in state.copied_api_group_keys:
        state.counters.media_skipped += len(resolved_message_ids)
        state.counters.media_group_skipped += 1
        state.counters.processed += len(resolved_message_ids)
        return

    done_by_message_id = {
        int(message_id): media_mapping_done(
            get_conn_fn=get_conn_fn,
            run_id=state.run_id,
            source_chat_id=state.source_chat_id,
            source_message_id=int(message_id),
            mode="media_group_copy",
        )
        for message_id in resolved_message_ids
    }
    if all(done_by_message_id.values()):
        state.counters.media_skipped += len(resolved_message_ids)
        state.counters.media_group_skipped += 1
        state.copied_api_group_keys.add(resolved_group_key)
        state.counters.processed += len(resolved_message_ids)
        return

    def record_group_item_error(source_message_id: int, message: str) -> None:
        source_message = clone_source_message_for_api_id(
            source_chat_id=state.source_chat_id,
            source_message_id=int(source_message_id),
            db_messages_by_id=db_messages_by_id,
        )
        record_clone_media_mapping(
            get_conn_fn=get_conn_fn,
            migration_id=state.migration_id,
            run_id=state.run_id,
            plan_id=state.plan_id,
            source_message=source_message,
            target_chat_id=state.target_chat_id,
            target_message_id=None,
            mode="media_group_copy",
            status="error",
            error_message=message,
        )

    def record_group_item_success(
        source_message_id: int,
        target_message_id: int | None,
    ) -> None:
        source_message = clone_source_message_for_api_id(
            source_chat_id=state.source_chat_id,
            source_message_id=int(source_message_id),
            db_messages_by_id=db_messages_by_id,
        )
        record_clone_media_mapping(
            get_conn_fn=get_conn_fn,
            migration_id=state.migration_id,
            run_id=state.run_id,
            plan_id=state.plan_id,
            source_message=source_message,
            target_chat_id=state.target_chat_id,
            target_message_id=target_message_id,
            mode="media_group_copy",
            status="done",
        )

    def copy_group_sequentially(message_ids: tuple[int, ...]) -> tuple[int, int, int]:
        sent = 0
        skipped = 0
        failed = 0
        for source_message_id in message_ids:
            source_message_id = int(source_message_id)
            if done_by_message_id.get(source_message_id):
                skipped += 1
                continue
            try:
                result = copy_media_to_target(source_message_id)
                target_message_id = first_required_target_message_id(
                    result,
                    "时间线媒体组逐条复制",
                )
                record_group_item_success(source_message_id, target_message_id)
                sent += 1
                sleep_after_send(state.normalized_send_delay_ms)
            except Exception as exc:
                failed += 1
                message = admin_error_message(exc)
                record_group_item_error(source_message_id, message)
                media_kind = _clean_text(
                    (resolved_items_by_id.get(source_message_id) or {}).get("media_kind")
                )
                media_kind_suffix = f"，media_kind={media_kind}" if media_kind else ""
                admin_job_append_log_fn(
                    state.job_id,
                    "时间线媒体组逐条复制失败："
                    f"grouped_id={grouped_id}，"
                    f"source_message_id={source_message_id}"
                    f"{media_kind_suffix}，{message}",
                )
        return sent, skipped, failed

    def copy_album_chunk(
        chunk_message_ids: list[int],
        *,
        chunk_index: int,
        chunk_count: int,
    ) -> tuple[int, int, int]:
        done_in_chunk = [
            bool(done_by_message_id.get(int(source_message_id)))
            for source_message_id in chunk_message_ids
        ]
        if all(done_in_chunk):
            return 0, len(chunk_message_ids), 0

        if any(done_in_chunk):
            message = "相册分块已有部分完成映射，跳过该分块避免重复相册"
            missing_ids = [
                int(source_message_id)
                for source_message_id, done in zip(
                    chunk_message_ids,
                    done_in_chunk,
                    strict=False,
                )
                if not done
            ]
            for source_message_id in missing_ids:
                record_group_item_error(source_message_id, message)
            admin_job_append_log_fn(
                state.job_id,
                "时间线媒体组相册分块存在部分映射，已跳过："
                f"grouped_id={grouped_id}，"
                f"chunk={chunk_index}/{chunk_count}，"
                f"done={sum(1 for done in done_in_chunk if done)}，"
                f"missing={len(missing_ids)}",
            )
            return 0, len(chunk_message_ids) - len(missing_ids), len(missing_ids)

        if len(chunk_message_ids) <= 1:
            return copy_group_sequentially(tuple(chunk_message_ids))

        try:
            result = copy_media_to_target(chunk_message_ids)
        except Exception as exc:
            admin_job_append_log_fn(
                state.job_id,
                "时间线媒体组相册分块复制失败，已降级为逐条复制："
                f"grouped_id={grouped_id}，"
                f"chunk={chunk_index}/{chunk_count}，"
                f"items={len(chunk_message_ids)}，"
                f"reason={admin_error_message(exc)}",
            )
            return copy_group_sequentially(tuple(chunk_message_ids))

        target_ids = clone_sent_message_ids(result)
        sent = 0
        failed = 0
        for index, source_message_id in enumerate(chunk_message_ids):
            target_message_id = target_ids[index] if index < len(target_ids) else None
            if target_message_id is None:
                failed += 1
                message = "媒体组相册复制后未返回目标消息 ID"
                record_group_item_error(int(source_message_id), message)
                admin_job_append_log_fn(
                    state.job_id,
                    "时间线媒体组相册分块部分消息复制失败："
                    f"grouped_id={grouped_id}，"
                    f"chunk={chunk_index}/{chunk_count}，"
                    f"source_message_id={source_message_id}，"
                    f"{message}",
                )
                continue
            sent += 1
            record_group_item_success(int(source_message_id), int(target_message_id))

        if sent:
            sleep_after_send(state.normalized_send_delay_ms)
        return sent, 0, failed

    try:
        if copy_as_group:
            sent = skipped = failed = 0
            for chunk_index, chunk_message_ids in enumerate(album_chunks, start=1):
                chunk_sent, chunk_skipped, chunk_failed = copy_album_chunk(
                    chunk_message_ids,
                    chunk_index=chunk_index,
                    chunk_count=len(album_chunks),
                )
                sent += chunk_sent
                skipped += chunk_skipped
                failed += chunk_failed
            state.counters.media_sent += sent
            state.counters.media_skipped += skipped
            state.counters.media_failed += failed
            if failed:
                state.counters.media_group_failed += 1
            elif sent:
                state.counters.media_group_sent += 1
                state.copied_api_group_keys.add(resolved_group_key)
            else:
                state.counters.media_group_skipped += 1
            state.counters.processed += len(resolved_message_ids)
            if sent and len(album_chunks) > 1:
                admin_job_append_log_fn(
                    state.job_id,
                    "时间线媒体组超过单次相册上限，已按 Telegram 限制分块复制："
                    f"grouped_id={grouped_id}，"
                    f"items={len(resolved_message_ids)}，"
                    f"chunks={len(album_chunks)}",
                )
            return

        sent, skipped, failed = copy_group_sequentially(tuple(resolved_message_ids))
        state.counters.media_sent += sent
        state.counters.media_skipped += skipped
        state.counters.media_failed += failed
        if failed:
            state.counters.media_group_failed += 1
        elif sent:
            state.counters.media_group_sent += 1
            state.copied_api_group_keys.add(resolved_group_key)
        else:
            state.counters.media_group_skipped += 1
        state.counters.processed += len(resolved_message_ids)
        if sent and copy_strategy == "sequential":
            admin_job_append_log_fn(
                state.job_id,
                "时间线媒体组包含非相册兼容媒体，已按原顺序逐条复制："
                f"grouped_id={grouped_id}，"
                f"items={len(resolved_message_ids)}，"
                f"reason={_clean_text(resolved_group.get('album_reason'))}",
            )
    except Exception as exc:
        message = admin_error_message(exc)
        state.counters.media_failed += len(resolved_message_ids)
        state.counters.media_group_failed += 1
        state.counters.processed += len(resolved_message_ids)
        for source_message_id in resolved_message_ids:
            source_message = clone_source_message_for_api_id(
                source_chat_id=state.source_chat_id,
                source_message_id=int(source_message_id),
                db_messages_by_id=db_messages_by_id,
            )
            record_clone_media_mapping(
                get_conn_fn=get_conn_fn,
                migration_id=state.migration_id,
                run_id=state.run_id,
                plan_id=state.plan_id,
                source_message=source_message,
                target_chat_id=state.target_chat_id,
                target_message_id=None,
                mode="media_group_copy",
                status="error",
                error_message=message,
            )
        admin_job_append_log_fn(
            state.job_id,
            f"时间线媒体组复制失败：grouped_id={grouped_id}，{message}",
        )
