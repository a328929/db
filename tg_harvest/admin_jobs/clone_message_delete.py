import logging
import time
from collections.abc import Callable, Iterable
from contextlib import suppress
from typing import Any

from tg_harvest.admin_jobs.clone_execution import clone_cfg_for_account
from tg_harvest.admin_jobs.clone_target_access import clone_run_target_input_channel
from tg_harvest.admin_jobs.common import (
    admin_error_message,
    call_with_conn,
    finish_job_heartbeat,
    mark_admin_job_running,
    start_admin_job_heartbeat,
    start_admin_job_thread,
    update_admin_job_progress,
)
from tg_harvest.admin_jobs.core import _admin_job_stop_requested
from tg_harvest.admin_jobs.sessions import (
    _cleanup_isolated_worker_session,
    _create_isolated_worker_client,
    _disconnect_worker_client,
    _ensure_base_session_valid,
    bind_client_event_loop,
)
from tg_harvest.domain.clone_message_delete import CloneMessageDeleteSelection
from tg_harvest.ingest.flood_wait import call_with_bounded_retry
from tg_harvest.storage.clone import (
    rewind_clone_mappings_for_deleted_target_messages,
)

CLONE_MESSAGE_DELETE_BATCH_SIZE = 100
CLONE_MESSAGE_DELETE_MAX_DELAY_MS = 10_000
_PROGRESS_LOG_BATCH_INTERVAL = 10


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _delete_delay_ms(value: Any) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        normalized = 0
    return min(max(normalized, 0), CLONE_MESSAGE_DELETE_MAX_DELAY_MS)


def _iter_message_id_batches(
    message_ids: Iterable[int],
) -> Iterable[list[int]]:
    batch: list[int] = []
    for message_id in message_ids:
        batch.append(int(message_id))
        if len(batch) >= CLONE_MESSAGE_DELETE_BATCH_SIZE:
            yield batch
            batch = []
    if batch:
        yield batch


def _read_latest_message_ids(
    client: Any,
    target_channel: Any,
    *,
    count: int,
    cfg: Any,
) -> list[int]:
    def collect() -> list[int]:
        message_ids: list[int] = []
        with bind_client_event_loop(client):
            for message in client.iter_messages(
                target_channel,
                limit=int(count),
                wait_time=0,
            ):
                try:
                    message_id = int(getattr(message, "id", 0) or 0)
                except (TypeError, ValueError):
                    continue
                if message_id > 0:
                    message_ids.append(message_id)
        return message_ids

    return call_with_bounded_retry(
        collect,
        flood_wait_threshold_seconds=getattr(
            cfg,
            "flood_wait_switch_threshold",
            30,
        ),
        account_label="secondary",
        scope="clone-message-delete-select-latest",
    )


def _delete_message_batch(
    client: Any,
    target_channel: Any,
    message_ids: list[int],
    *,
    cfg: Any,
) -> None:
    with bind_client_event_loop(client):
        call_with_bounded_retry(
            client.delete_messages,
            target_channel,
            message_ids,
            revoke=True,
            flood_wait_threshold_seconds=getattr(
                cfg,
                "flood_wait_switch_threshold",
                30,
            ),
            account_label="secondary",
            scope="clone-message-delete-batch",
        )


def _selection_message_ids(
    selection: CloneMessageDeleteSelection,
    *,
    client: Any,
    target_channel: Any,
    cfg: Any,
) -> list[int] | range:
    if selection.mode == "latest":
        return _read_latest_message_ids(
            client,
            target_channel,
            count=selection.requested_count,
            cfg=cfg,
        )
    if (
        selection.first_message_id is None
        or selection.last_message_id is None
    ):
        raise RuntimeError("消息 ID 区间参数不完整")
    return range(selection.first_message_id, selection.last_message_id + 1)


def _admin_clone_message_delete_job_runner(
    job_id: str,
    *,
    clone_run: dict,
    selection: CloneMessageDeleteSelection,
    delete_delay_ms: Any,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    heartbeat_stop, heartbeat_thread = start_admin_job_heartbeat(job_id)
    client = None
    secondary_cfg = None
    worker_id = f"{job_id}_clone_message_delete"
    processed = 0
    total = int(selection.requested_count)
    rewound_mapping_count = 0
    rewound_done_mapping_count = 0
    rewound_media_transfer_count = 0
    unmapped_target_message_count = 0
    first_rewound_source_message_id = 0
    try:
        mark_admin_job_running(
            job_id,
            admin_job_set_status_fn=admin_job_set_status_fn,
        )
        target_chat_id = int(clone_run.get("target_chat_id") or 0)
        if target_chat_id <= 0:
            raise RuntimeError("目标副本尚未创建，不能删除局部消息")

        delay_ms = _delete_delay_ms(delete_delay_ms)
        update_admin_job_progress(
            job_id,
            0,
            total=total,
            stage="preparing_message_delete",
        )
        admin_job_append_log_fn(
            job_id,
            "开始局部删除克隆消息："
            f"目标={_clean_text(clone_run.get('target_title')) or target_chat_id} "
            f"({target_chat_id})，规则={selection.description}",
        )
        admin_job_append_log_fn(
            job_id,
            "执行账号：第二账号；每批最多 100 条消息，"
            f"批次间隔={delay_ms}ms",
        )

        secondary_cfg = clone_cfg_for_account(cfg, "secondary")
        if not _ensure_base_session_valid(
            secondary_cfg,
            job_id,
            admin_job_append_log_fn,
        ):
            raise RuntimeError("第二账号会话不可用，无法删除克隆消息")
        client = _create_isolated_worker_client(secondary_cfg, worker_id)
        target_channel = clone_run_target_input_channel(client, clone_run)
        if target_channel is None:
            raise RuntimeError("无法解析目标副本实体，请先用第二账号打开一次目标群后重试")

        message_ids = _selection_message_ids(
            selection,
            client=client,
            target_channel=target_channel,
            cfg=secondary_cfg,
        )
        if selection.mode == "latest":
            total = len(message_ids)
            admin_job_append_log_fn(
                job_id,
                f"已按最新到最早顺序锁定 {total} 条实际存在的消息 ID；"
                "删除后其余消息 ID 不会变化",
            )
            update_admin_job_progress(
                job_id,
                0,
                total=total,
                stage="deleting_messages",
            )
            if not message_ids:
                update_admin_job_progress(job_id, 0, total=0, stage="done")
                admin_job_append_log_fn(job_id, "目标副本当前没有可删除消息，任务完成")
                admin_job_set_status_fn(job_id, "done")
                return
        else:
            admin_job_append_log_fn(
                job_id,
                "将按消息 ID 从小到大提交删除请求；不存在的 ID 不会导致其他 ID 重新编号",
            )
            update_admin_job_progress(
                job_id,
                0,
                total=total,
                stage="deleting_messages",
            )

        batches = _iter_message_id_batches(message_ids)
        for batch_index, batch in enumerate(batches, start=1):
            if _admin_job_stop_requested(job_id):
                admin_job_append_log_fn(
                    job_id,
                    f"停止请求已生效：已提交删除 {processed}/{total} 条消息",
                )
                update_admin_job_progress(
                    job_id,
                    processed,
                    total=total,
                    stage="stopped",
                )
                admin_job_set_status_fn(job_id, "done")
                return

            _delete_message_batch(
                client,
                target_channel,
                batch,
                cfg=secondary_cfg,
            )
            processed += len(batch)
            try:
                rewind = call_with_conn(
                    get_conn_fn,
                    rewind_clone_mappings_for_deleted_target_messages,
                    run_id=_clean_text(clone_run.get("run_id")),
                    target_chat_id=target_chat_id,
                    target_message_ids=batch,
                )
            except Exception as exc:
                admin_job_append_log_fn(
                    job_id,
                    "当前批次已获 Telegram 删除确认，但本地续克隆状态回退失败；"
                    "任务已停止，暂不能继续完整时间线迁移："
                    f"{admin_error_message(exc)}",
                )
                raise RuntimeError(
                    "Telegram 已删除当前批次，但本地续克隆状态回退失败"
                ) from exc

            rewound_mapping_count += int(rewind["rewound_mapping_count"])
            rewound_done_mapping_count += int(rewind["rewound_done_mapping_count"])
            rewound_media_transfer_count += int(
                rewind["rewound_media_transfer_count"]
            )
            unmapped_target_message_count += int(
                rewind["unmapped_target_message_count"]
            )
            batch_first_source_message_id = int(
                rewind["first_rewound_source_message_id"]
            )
            if batch_first_source_message_id > 0 and (
                first_rewound_source_message_id <= 0
                or batch_first_source_message_id < first_rewound_source_message_id
            ):
                first_rewound_source_message_id = batch_first_source_message_id

            is_final_batch = processed >= total
            if (
                batch_index == 1
                or batch_index % _PROGRESS_LOG_BATCH_INTERVAL == 0
                or is_final_batch
            ):
                update_admin_job_progress(
                    job_id,
                    processed,
                    total=total,
                    stage="deleting_messages",
                )
                admin_job_append_log_fn(
                    job_id,
                    "Telegram 已确认删除 "
                    f"{processed}/{total} 条消息；"
                    f"已回退 {rewound_mapping_count} 条本地映射"
                    f"（其中完成映射 {rewound_done_mapping_count} 条），"
                    f"已重置 {rewound_media_transfer_count} 条媒体传输状态，"
                    f"未匹配映射 {unmapped_target_message_count} 条",
                )
            if delay_ms > 0 and not is_final_batch:
                time.sleep(float(delay_ms) / 1000.0)

        update_admin_job_progress(job_id, processed, total=total, stage="done")
        admin_job_append_log_fn(
            job_id,
            "局部删除完成：Telegram 已确认删除 "
            f"{processed} 条消息；已回退 {rewound_mapping_count} 条本地映射"
            f"（完成映射 {rewound_done_mapping_count} 条），"
            f"已重置 {rewound_media_transfer_count} 条媒体传输状态，"
            f"未匹配映射 {unmapped_target_message_count} 条。",
        )
        if first_rewound_source_message_id > 0:
            admin_job_append_log_fn(
                job_id,
                "下次完整时间线迁移会按源群时间线寻找最早未完成项补齐；"
                "本次回退映射中最早的源消息 ID="
                f"{first_rewound_source_message_id}。",
            )
        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        message = admin_error_message(exc)
        logging.exception("局部删除克隆消息任务失败: job_id=%s", job_id)
        admin_job_append_log_fn(job_id, f"局部删除克隆消息失败：{message}")
        update_admin_job_progress(job_id, processed, total=total, stage="error")
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        if client is not None:
            with suppress(Exception):
                _disconnect_worker_client(client)
        if secondary_cfg is not None:
            with suppress(Exception):
                _cleanup_isolated_worker_session(secondary_cfg, worker_id)


def _admin_start_clone_message_delete_job_thread(job_id: str, **kwargs: Any):
    return start_admin_job_thread(
        _admin_clone_message_delete_job_runner,
        job_id,
        **kwargs,
    )
