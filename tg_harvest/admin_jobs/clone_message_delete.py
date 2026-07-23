import logging
import time
from collections.abc import Callable, Iterable
from contextlib import suppress
from typing import Any

from telethon.tl.types import MessageActionChannelCreate

from tg_harvest.admin_jobs.clone_execution import clone_cfg_for_account
from tg_harvest.admin_jobs.clone_media_copy import (
    CloneMediaTransferContext,
    cleanup_pending_clone_relay_messages,
)
from tg_harvest.admin_jobs.clone_target_access import (
    clone_run_target_conflicts_with_source,
    clone_run_target_input_channel,
)
from tg_harvest.admin_jobs.clone_target_delete import _target_owner_cfg
from tg_harvest.admin_jobs.common import (
    admin_error_message,
    call_with_conn,
    finish_job_heartbeat,
    mark_admin_job_running,
    resolve_chat_entity,
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
    list_pending_clone_relay_cleanup_for_run,
    load_clone_tail_delete_selection,
    mark_clone_run_message_reset_required,
    reset_clone_run_timeline,
    rewind_clone_mappings_for_deleted_target_messages,
)

CLONE_MESSAGE_DELETE_BATCH_SIZE = 100
CLONE_MESSAGE_DELETE_MAX_DELAY_MS = 10_000
_PROGRESS_LOG_BATCH_INTERVAL = 10
_DELETE_PROPAGATION_MAX_READS = 8
_DELETE_PROPAGATION_BASE_DELAY_SECONDS = 0.25


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


def _range_message_ids(selection: CloneMessageDeleteSelection) -> range:
    if selection.first_message_id is None or selection.last_message_id is None:
        raise RuntimeError("消息 ID 区间参数不完整")
    return range(selection.first_message_id, selection.last_message_id + 1)


def _load_target_message_batch(
    client: Any,
    target_channel: Any,
    *,
    cfg: Any,
) -> tuple[list[int], list[int], int]:
    with bind_client_event_loop(client):
        result = call_with_bounded_retry(
            client.get_messages,
            target_channel,
            limit=CLONE_MESSAGE_DELETE_BATCH_SIZE,
            flood_wait_threshold_seconds=getattr(
                cfg,
                "flood_wait_switch_threshold",
                30,
            ),
            account_label="clone-owner",
            scope="clone-message-reset-read",
        )
    message_ids: list[int] = []
    immutable_service_message_ids: list[int] = []
    for item in list(result or []):
        message_id = getattr(item, "id", None)
        if message_id is None or int(message_id) <= 0:
            continue
        normalized_message_id = int(message_id)
        if isinstance(getattr(item, "action", None), MessageActionChannelCreate):
            immutable_service_message_ids.append(normalized_message_id)
        else:
            message_ids.append(normalized_message_id)
    try:
        reported_total = max(
            0,
            int(
                getattr(
                    result,
                    "total",
                    len(message_ids) + len(immutable_service_message_ids),
                )
            ),
        )
    except (TypeError, ValueError):
        reported_total = len(message_ids) + len(immutable_service_message_ids)
    return message_ids, immutable_service_message_ids, reported_total


def _cleanup_reset_relay_messages(
    *,
    job_id: str,
    clone_run: dict,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> int:
    pending = call_with_conn(
        get_conn_fn,
        list_pending_clone_relay_cleanup_for_run,
        run_id=_clean_text(clone_run.get("run_id")),
    )
    if not pending:
        return 0

    grouped: dict[tuple[str, int], list[dict]] = {}
    for transfer in pending:
        account = _clean_text(transfer.get("source_account")).lower()
        relay_chat_id = int(transfer.get("relay_chat_id") or 0)
        if account not in {"primary", "secondary"} or relay_chat_id <= 0:
            raise RuntimeError("中转临时消息缺少可恢复的账号或频道信息")
        grouped.setdefault((account, relay_chat_id), []).append(transfer)

    cleaned = 0
    for group_index, ((account, relay_chat_id), transfers) in enumerate(
        grouped.items(),
        start=1,
    ):
        account_cfg = clone_cfg_for_account(cfg, account)
        worker_id = f"{job_id}_clone_message_reset_relay_{group_index}"
        relay_client = None
        try:
            if not _ensure_base_session_valid(
                account_cfg,
                job_id,
                admin_job_append_log_fn,
            ):
                raise RuntimeError(f"{account} 账号会话不可用，无法清理中转消息")
            relay_client = _create_isolated_worker_client(account_cfg, worker_id)
            relay_entity = resolve_chat_entity(
                relay_client,
                relay_chat_id,
                "",
                allow_username_fallback=False,
                retry_scope="clone-message-reset-relay",
            )
            first = transfers[0]
            cleaned += cleanup_pending_clone_relay_messages(
                source_client=relay_client,
                relay_entity_for_source=relay_entity,
                transfer_context=CloneMediaTransferContext(
                    get_conn_fn=get_conn_fn,
                    migration_id=_clean_text(first.get("migration_id")),
                    run_id=_clean_text(clone_run.get("run_id")),
                    plan_id=_clean_text(first.get("plan_id")),
                    source_chat_id=int(first.get("source_chat_id") or 0),
                    target_chat_id=int(clone_run.get("target_chat_id") or 0),
                    source_account=account,
                    target_account=_clean_text(first.get("target_account")),
                    relay_chat_id=relay_chat_id,
                ),
                log_step=lambda message: admin_job_append_log_fn(job_id, message),
                include_incomplete_target=True,
            )
        finally:
            if relay_client is not None:
                with suppress(Exception):
                    _disconnect_worker_client(relay_client)
            with suppress(Exception):
                _cleanup_isolated_worker_session(account_cfg, worker_id)
    return cleaned


def _clear_all_target_messages(
    *,
    job_id: str,
    client: Any,
    target_channel: Any,
    clone_run: dict,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    delay_ms: int,
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> tuple[int, int, int]:
    processed = 0
    initial_total = 0
    submitted_message_ids: set[int] = set()
    propagation_wait_reads = 0
    immutable_service_message_ids: set[int] = set()
    run_id = _clean_text(clone_run.get("run_id"))
    target_chat_id = int(clone_run.get("target_chat_id") or 0)

    batch_index = 0
    while True:
        batch_index += 1
        message_ids, immutable_ids, reported_total = _load_target_message_batch(
            client,
            target_channel,
            cfg=cfg,
        )
        immutable_service_message_ids.update(immutable_ids)
        if batch_index == 1:
            initial_total = max(
                0,
                reported_total - len(immutable_service_message_ids),
            )
            update_admin_job_progress(
                job_id,
                0,
                total=initial_total,
                stage="clearing_all_messages",
            )
        if not message_ids:
            if immutable_service_message_ids:
                admin_job_append_log_fn(
                    job_id,
                    "Telegram 仍保留 "
                    f"{len(immutable_service_message_ids)} 条不可删除的群组创建系统事件；"
                    "该事件不是可见聊天消息，不影响完整回退。",
                )
            update_admin_job_progress(
                job_id,
                processed,
                total=processed,
                stage="clearing_all_messages",
            )
            return processed, processed, len(immutable_service_message_ids)
        pending_message_ids = [
            message_id
            for message_id in message_ids
            if message_id not in submitted_message_ids
        ]
        if not pending_message_ids:
            propagation_wait_reads += 1
            if propagation_wait_reads > _DELETE_PROPAGATION_MAX_READS:
                raise RuntimeError(
                    "Telegram 在等待删除结果同步后仍返回上一批目标消息，"
                    "已停止本地状态重置"
                )
            if propagation_wait_reads == 1:
                admin_job_append_log_fn(
                    job_id,
                    "Telegram 暂时仍返回已提交删除的消息，正在等待删除结果同步。",
                )
            time.sleep(
                min(
                    2.0,
                    _DELETE_PROPAGATION_BASE_DELAY_SECONDS
                    * propagation_wait_reads,
                )
            )
            continue
        propagation_wait_reads = 0
        if _admin_job_stop_requested(job_id):
            raise RuntimeError("用户请求停止，目标消息可能已部分删除；请重新执行完整清空")

        _delete_message_batch(
            client,
            target_channel,
            pending_message_ids,
            cfg=cfg,
        )
        submitted_message_ids.update(pending_message_ids)
        call_with_conn(
            get_conn_fn,
            rewind_clone_mappings_for_deleted_target_messages,
            run_id=run_id,
            target_chat_id=target_chat_id,
            target_message_ids=pending_message_ids,
        )
        processed += len(pending_message_ids)
        progress_total = max(
            initial_total - len(immutable_service_message_ids),
            processed
            + max(
                0,
                reported_total - len(message_ids) - len(immutable_ids),
            ),
        )
        update_admin_job_progress(
            job_id,
            processed,
            total=progress_total,
            stage="clearing_all_messages",
        )
        if (
            batch_index == 1
            or batch_index % _PROGRESS_LOG_BATCH_INTERVAL == 0
            or processed >= progress_total
        ):
            admin_job_append_log_fn(
                job_id,
                f"已从目标副本删除并回退 {processed}/{progress_total} 条消息",
            )
        if delay_ms > 0:
            time.sleep(float(delay_ms) / 1000.0)


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
    account_cfg = None
    worker_id = f"{job_id}_clone_message_delete"
    processed = 0
    total = int(selection.requested_count)
    rewound_mapping_count = 0
    rewound_done_mapping_count = 0
    rewound_media_transfer_count = 0
    unmapped_target_message_count = 0
    first_rewound_source_message_id = 0
    selected_source_message_count = 0
    reset_all = selection.mode == "all"
    operation_label = "目标副本完整清空" if reset_all else "局部删除克隆消息"
    try:
        mark_admin_job_running(
            job_id,
            admin_job_set_status_fn=admin_job_set_status_fn,
        )
        target_chat_id = int(clone_run.get("target_chat_id") or 0)
        if target_chat_id <= 0:
            raise RuntimeError(f"目标副本尚未创建，不能执行{operation_label}")
        if clone_run_target_conflicts_with_source(clone_run):
            raise RuntimeError("目标副本 ID 与源群 ID 冲突，已拒绝删除消息")

        delay_ms = _delete_delay_ms(delete_delay_ms)
        update_admin_job_progress(
            job_id,
            0,
            total=total,
            stage="preparing_message_delete",
        )
        admin_job_append_log_fn(
            job_id,
            f"开始{operation_label}："
            f"目标={_clean_text(clone_run.get('target_title')) or target_chat_id} "
            f"({target_chat_id})，规则={selection.description}",
        )
        rewind_mappings = selection.mode == "latest"
        admin_job_append_log_fn(
            job_id,
            f"执行账号：{'目标副本创建账号' if reset_all else '第二账号'}；"
            f"每批最多 100 条消息，批次间隔={delay_ms}ms",
        )
        if reset_all:
            message_ids = []
            total = 0
            admin_job_append_log_fn(
                job_id,
                "完整清空会删除目标副本内全部当前消息，并移除全部迁移检查点和迁移任务历史；目标副本与预检计划保留。",
            )
        elif rewind_mappings:
            tail_selection = call_with_conn(
                get_conn_fn,
                load_clone_tail_delete_selection,
                run_id=_clean_text(clone_run.get("run_id")),
                target_chat_id=target_chat_id,
                source_message_limit=selection.requested_count,
            )
            message_ids: list[int] | range = list(tail_selection["target_message_ids"])
            selected_source_message_count = int(
                tail_selection["selected_source_message_count"]
            )
            total = len(message_ids)
            admin_job_append_log_fn(
                job_id,
                "已按源消息 ID 从新到旧锁定 "
                f"{selected_source_message_count}/{selection.requested_count} 条已克隆源消息，"
                f"对应 {total} 条目标消息；目标群公告等未映射消息不会参与计数或删除。",
            )
            if selected_source_message_count > 0:
                admin_job_append_log_fn(
                    job_id,
                    "本次源消息回滚范围："
                    f"{tail_selection['first_source_message_id']}-"
                    f"{tail_selection['last_source_message_id']}；"
                    "目标消息 ID 只用于定位删除，不作为续克隆游标。",
                )
            if not message_ids:
                update_admin_job_progress(job_id, 0, total=0, stage="done")
                admin_job_append_log_fn(
                    job_id,
                    "当前克隆记录没有可回滚的已完成消息，任务完成",
                )
                admin_job_set_status_fn(job_id, "done")
                return
        else:
            message_ids = _range_message_ids(selection)
            total = len(message_ids)
            admin_job_append_log_fn(
                job_id,
                "将按目标消息 ID 从小到大提交清理请求；"
                "区间清理不会修改克隆映射，也不会触发续克隆补回。",
            )

        update_admin_job_progress(
            job_id,
            0,
            total=total,
            stage="deleting_messages",
        )

        account_cfg = (
            _target_owner_cfg(cfg, clone_run.get("target_owner_session"))
            if reset_all
            else clone_cfg_for_account(cfg, "secondary")
        )
        if account_cfg is None:
            raise RuntimeError("未找到目标副本对应的创建账号，无法清空消息")
        if not _ensure_base_session_valid(
            account_cfg,
            job_id,
            admin_job_append_log_fn,
        ):
            raise RuntimeError("目标副本创建账号会话不可用，无法删除克隆消息")
        client = _create_isolated_worker_client(account_cfg, worker_id)
        target_channel = clone_run_target_input_channel(client, clone_run)
        if target_channel is None:
            raise RuntimeError(
                "无法解析目标副本实体，请先用"
                f"{'目标副本创建账号' if reset_all else '第二账号'}"
                "打开一次目标群后重试"
            )

        if reset_all:
            call_with_conn(
                get_conn_fn,
                mark_clone_run_message_reset_required,
                run_id=_clean_text(clone_run.get("run_id")),
                target_chat_id=target_chat_id,
            )
            admin_job_append_log_fn(
                job_id,
                "已写入完整回退安全状态；任务中断后将拒绝迁移，直到完整清空重试成功。",
            )
            cleaned_relay_count = _cleanup_reset_relay_messages(
                job_id=job_id,
                clone_run=clone_run,
                cfg=cfg,
                get_conn_fn=get_conn_fn,
                admin_job_append_log_fn=admin_job_append_log_fn,
            )
            processed, remote_total, immutable_service_count = (
                _clear_all_target_messages(
                    job_id=job_id,
                    client=client,
                    target_channel=target_channel,
                    clone_run=clone_run,
                    cfg=account_cfg,
                    get_conn_fn=get_conn_fn,
                    delay_ms=delay_ms,
                    admin_job_append_log_fn=admin_job_append_log_fn,
                )
            )
            reset_counts = call_with_conn(
                get_conn_fn,
                reset_clone_run_timeline,
                run_id=_clean_text(clone_run.get("run_id")),
                target_chat_id=target_chat_id,
            )
            total = max(remote_total, processed)
            update_admin_job_progress(job_id, total, total=total, stage="done")
            admin_job_append_log_fn(
                job_id,
                "目标副本完整清空完成："
                f"已删除 {processed} 条远端消息，"
                f"已识别 {immutable_service_count} 条不可删除的创建系统事件，"
                f"已清理 {cleaned_relay_count} 条中转临时消息，"
                f"已移除 {reset_counts['mapping_count']} 条剩余映射、"
                f"{reset_counts['media_transfer_count']} 条剩余媒体检查点和 "
                f"{reset_counts['migration_count']} 条迁移记录。",
            )
            admin_job_append_log_fn(
                job_id,
                "目标副本已恢复为尚未同步消息的状态；源群、目标副本和在线预检计划均未修改。",
            )
            admin_job_set_status_fn(job_id, "done")
            return

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
                cfg=account_cfg,
            )
            processed += len(batch)
            if rewind_mappings:
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
                if rewind_mappings:
                    progress_detail = (
                        f"已回退 {rewound_mapping_count} 条本地映射"
                        f"（其中完成映射 {rewound_done_mapping_count} 条），"
                        f"已重置 {rewound_media_transfer_count} 条媒体传输状态，"
                        f"未匹配映射 {unmapped_target_message_count} 条"
                    )
                else:
                    progress_detail = "克隆映射保持不变"
                admin_job_append_log_fn(
                    job_id,
                    "Telegram 已受理删除 "
                    f"{processed}/{total} 个目标消息 ID；{progress_detail}",
                )
            if delay_ms > 0 and not is_final_batch:
                time.sleep(float(delay_ms) / 1000.0)

        update_admin_job_progress(job_id, processed, total=total, stage="done")
        if rewind_mappings:
            admin_job_append_log_fn(
                job_id,
                "克隆尾部回滚完成：Telegram 已受理删除 "
                f"{processed} 个目标消息 ID；已回退 {rewound_mapping_count} 条本地映射"
                f"（对应 {selected_source_message_count} 条已克隆源消息），"
                f"已重置 {rewound_media_transfer_count} 条媒体传输状态。",
            )
            if first_rewound_source_message_id > 0:
                admin_job_append_log_fn(
                    job_id,
                    "下次完整时间线迁移会从最早未完成的源消息继续；"
                    "本次最早回退源消息 ID="
                    f"{first_rewound_source_message_id}。",
                )
        else:
            admin_job_append_log_fn(
                job_id,
                "目标消息 ID 区间清理完成：Telegram 已受理 "
                f"{processed} 个 ID；克隆映射未修改，续克隆不会补回这些消息。",
            )
        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        message = admin_error_message(exc)
        logging.exception("%s 任务失败: job_id=%s", operation_label, job_id)
        admin_job_append_log_fn(job_id, f"{operation_label}失败：{message}")
        update_admin_job_progress(job_id, processed, total=total, stage="error")
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        if client is not None:
            with suppress(Exception):
                _disconnect_worker_client(client)
        if account_cfg is not None:
            with suppress(Exception):
                _cleanup_isolated_worker_session(account_cfg, worker_id)


def _admin_start_clone_message_delete_job_thread(job_id: str, **kwargs: Any):
    return start_admin_job_thread(
        _admin_clone_message_delete_job_runner,
        job_id,
        **kwargs,
    )
