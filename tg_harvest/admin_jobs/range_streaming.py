import logging
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass
from typing import Any

from tg_harvest.admin_jobs.core import job_context
from tg_harvest.admin_jobs.sessions import bind_client_event_loop
from tg_harvest.admin_jobs.streaming import StreamedEntityHarvestResult
from tg_harvest.admin_jobs.update_writer import ChatUpdateWriteCoordinator
from tg_harvest.ingest.flood_wait import AccountFloodWaitError
from tg_harvest.ingest.parse import HarvestCounters
from tg_harvest.ingest.range_harvest import (
    MessageIdRange,
    build_message_id_ranges,
    harvest_message_id_range,
)


@dataclass(frozen=True)
class RangeHarvestAccount:
    label: str
    client: Any
    entity: Any


def _merge_counters(target: HarvestCounters, source: HarvestCounters) -> None:
    target.seen += int(source.seen)
    target.written += int(source.written)
    target.parse_failures += int(source.parse_failures)
    target.parse_failure_samples.extend(source.parse_failure_samples)
    target.parse_failure_samples = target.parse_failure_samples[:5]
    for key, count in source.parse_failures_by_type.items():
        target.parse_failures_by_type[key] = (
            target.parse_failures_by_type.get(key, 0) + count
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


def stream_entity_ranges_to_writer(
    *,
    job_id: str,
    write_coordinator: ChatUpdateWriteCoordinator,
    accounts: list[RangeHarvestAccount],
    idx: int,
    total: int,
    chat_id: int,
    chat_title: str,
    chat_username: str | None,
    chat_type: str,
    latest_message_id: int,
    chunk_size: int,
    skip_postprocess_if_unchanged: bool = False,
    enable_dedupe: bool = True,
) -> StreamedEntityHarvestResult:
    if not accounts:
        raise RuntimeError("没有可用账号执行区间采集")

    ranges = build_message_id_ranges(
        latest_message_id,
        chunk_size=chunk_size,
    )
    if not ranges:
        raise RuntimeError(f"无法构建区间采集范围: latest_message_id={latest_message_id}")

    logging.info(
        "[%s/%s] 双账号区间采集启动: chat_id=%s latest_id=%s chunks=%s chunk_size=%s",
        idx,
        total,
        chat_id,
        latest_message_id,
        len(ranges),
        chunk_size,
    )

    total_started_at = time.perf_counter()
    range_queue = deque(ranges)
    queue_lock = threading.Lock()
    account_lock = threading.Lock()
    stop_event = threading.Event()
    aggregate_lock = threading.Lock()
    aggregate_counters = HarvestCounters()
    aggregate_touched_groups: set[int] = set()
    submitted_message_count = 0
    submitted_touched_groups: set[int] = set()
    secondary_empty_ranges: list[MessageIdRange] = []
    disabled_accounts: dict[str, AccountFloodWaitError] = {}

    write_coordinator.register_chat(chat_id)
    write_coordinator.submit_chat_start(
        chat_id=chat_id,
        chat_title=chat_title,
        chat_username=chat_username,
        chat_type=chat_type,
    )

    def next_range() -> MessageIdRange | None:
        with queue_lock:
            if stop_event.is_set() or not range_queue:
                return None
            return range_queue.popleft()

    def account_disabled(account: RangeHarvestAccount) -> bool:
        with account_lock:
            return account.label in disabled_accounts

    def active_accounts() -> list[RangeHarvestAccount]:
        with account_lock:
            return [
                account
                for account in accounts
                if account.label not in disabled_accounts
            ]

    def requeue_after_flood_wait(
        account: RangeHarvestAccount,
        message_range: MessageIdRange,
        exc: AccountFloodWaitError,
    ) -> None:
        with queue_lock:
            range_queue.appendleft(message_range)
        with account_lock:
            first_seen = account.label not in disabled_accounts
            disabled_accounts[account.label] = exc
            active_left = sum(
                1
                for candidate in accounts
                if candidate.label not in disabled_accounts
            )
        if first_seen:
            logging.warning(
                "区间采集账号进入冷却并暂停使用: account=%s chat_id=%s wait=%ss threshold=%ss",
                account.label,
                chat_id,
                exc.seconds,
                exc.threshold_seconds,
            )
        if active_left <= 0:
            stop_event.set()

    def submit_batch(msg_rows: list[tuple], media_rows: list[tuple]) -> None:
        nonlocal submitted_message_count
        write_coordinator.submit_batch(
            chat_id=chat_id,
            msg_rows=msg_rows,
            media_rows=media_rows,
        )
        with aggregate_lock:
            submitted_message_count += len(msg_rows)
            submitted_touched_groups.update(_collect_touched_groups(msg_rows))

    def worker(account: RangeHarvestAccount) -> tuple[HarvestCounters, set[int]]:
        job_context.set(str(job_id))
        local_counters = HarvestCounters()
        local_touched_groups: set[int] = set()
        processed_chunks = 0
        with bind_client_event_loop(account.client):
            while True:
                if account_disabled(account):
                    break
                message_range = next_range()
                if message_range is None:
                    break
                logging.info(
                    "区间采集领取任务: account=%s chat_id=%s range=%s-%s",
                    account.label,
                    chat_id,
                    message_range.start_id,
                    message_range.end_id,
                )
                try:
                    counters, touched_groups = harvest_message_id_range(
                        client=account.client,
                        entity=account.entity,
                        chat_id=chat_id,
                        message_range=message_range,
                        write_batch_fn=submit_batch,
                        account_label=account.label,
                    )
                except AccountFloodWaitError as exc:
                    requeue_after_flood_wait(account, message_range, exc)
                    break
                if account.label != "primary" and counters.seen <= 0:
                    with aggregate_lock:
                        secondary_empty_ranges.append(message_range)
                _merge_counters(local_counters, counters)
                local_touched_groups.update(touched_groups)
                processed_chunks += 1
                logging.info(
                    "区间采集完成: account=%s chat_id=%s range=%s-%s scanned=%s written=%s",
                    account.label,
                    chat_id,
                    message_range.start_id,
                    message_range.end_id,
                    counters.seen,
                    counters.written,
                )
        logging.info(
            "区间采集账号结束: account=%s chat_id=%s chunks=%s scanned=%s written=%s",
            account.label,
            chat_id,
            processed_chunks,
            local_counters.seen,
            local_counters.written,
        )
        return local_counters, local_touched_groups

    try:
        while True:
            current_accounts = active_accounts()
            if not current_accounts:
                with queue_lock:
                    remaining_ranges = len(range_queue)
                reasons = ", ".join(
                    f"{label}: wait={exc.seconds}s"
                    for label, exc in disabled_accounts.items()
                )
                raise RuntimeError(
                    f"所有账号均触发长时间 FloodWait，仍有 {remaining_ranges} 个区间未完成：{reasons}"
                )

            with ThreadPoolExecutor(max_workers=len(current_accounts)) as executor:
                futures = [
                    executor.submit(worker, account) for account in current_accounts
                ]
                done_futures, _pending_futures = wait(futures)
                for future in done_futures:
                    counters, touched_groups = future.result()
                    _merge_counters(aggregate_counters, counters)
                    aggregate_touched_groups.update(touched_groups)

            with queue_lock:
                remaining_ranges = len(range_queue)
            if remaining_ranges <= 0:
                break
            logging.info(
                "区间采集仍有未完成区间，使用剩余账号继续: chat_id=%s remaining_ranges=%s disabled_accounts=%s",
                chat_id,
                remaining_ranges,
                ",".join(sorted(disabled_accounts)) or "-",
            )
        if secondary_empty_ranges:
            primary_account = next(
                (account for account in accounts if account.label == "primary"),
                accounts[0],
            )
            logging.info(
                "第二账号存在空区间，交由主账号复扫: chat_id=%s empty_ranges=%s",
                chat_id,
                len(secondary_empty_ranges),
            )
            with bind_client_event_loop(primary_account.client):
                for message_range in secondary_empty_ranges:
                    counters, touched_groups = harvest_message_id_range(
                        client=primary_account.client,
                        entity=primary_account.entity,
                        chat_id=chat_id,
                        message_range=message_range,
                        write_batch_fn=submit_batch,
                        account_label=primary_account.label,
                    )
                    _merge_counters(aggregate_counters, counters)
                    aggregate_touched_groups.update(touched_groups)
    except Exception:
        stop_event.set()
        if submitted_message_count > 0:
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
        raise

    write_coordinator.submit_finalize(
        chat_id=chat_id,
        chat_title=chat_title,
        counters=aggregate_counters,
        touched_groups=aggregate_touched_groups,
        first_sync=True,
        total_started_at=total_started_at,
        skip_postprocess_if_unchanged=skip_postprocess_if_unchanged,
        enable_dedupe=enable_dedupe,
    )
    write_coordinator.wait_for_chat(chat_id)

    return StreamedEntityHarvestResult(
        chat_id=chat_id,
        chat_title=chat_title,
        chat_username=chat_username,
        counters=aggregate_counters,
        touched_groups=aggregate_touched_groups,
        first_sync=True,
        submitted_message_count=submitted_message_count,
    )
