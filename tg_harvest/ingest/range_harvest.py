import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from tg_harvest.config import CFG
from tg_harvest.domain.coerce import safe_int
from tg_harvest.ingest.flood_wait import (
    AccountFloodWaitError,
    flood_wait_seconds,
    is_flood_wait_error,
    raise_if_long_flood_wait,
)
from tg_harvest.ingest.parse import HarvestCounters, MessageParseError, MessageParser
from tg_harvest.ingest.runner import _last_message_id_in_rows, _prepare_db_rows


@dataclass(frozen=True)
class HistoryAccessProbeResult:
    can_read_history: bool
    latest_message_id: int
    reason: str = ""


@dataclass(frozen=True)
class MessageIdRange:
    start_id: int
    end_id: int


def _first_message(result: Any) -> Any | None:
    if result is None:
        return None
    if getattr(result, "id", None):
        return result
    try:
        for item in result:
            if item is not None:
                return item
    except TypeError:
        pass
    except Exception:
        return None
    try:
        item = result[0]
    except Exception:
        return None
    return item if item is not None else None


def _message_id(message: Any) -> int:
    return safe_int(getattr(message, "id", None))


def _is_readable_message(message: Any) -> bool:
    return _message_id(message) > 0 and getattr(message, "date", None) is not None


def _iter_messages_kwargs(**kwargs: Any) -> dict[str, Any]:
    result = dict(kwargs)
    if CFG.history_wait_time is not None:
        result["wait_time"] = max(0.0, float(CFG.history_wait_time))
    return result


def read_latest_message_id(client: Any, entity: Any) -> int:
    message = _first_message(client.get_messages(entity, limit=1))
    return _message_id(message)


def probe_history_access(
    client: Any,
    entity: Any,
    *,
    min_history_message_id: int,
    account_label: str = "",
) -> HistoryAccessProbeResult:
    try:
        latest_message = _first_message(client.get_messages(entity, limit=1))
    except Exception as exc:
        raise_if_long_flood_wait(
            exc,
            threshold_seconds=CFG.flood_wait_switch_threshold,
            account_label=account_label,
            scope="history-latest-probe",
        )
        return HistoryAccessProbeResult(False, 0, f"读取最新消息失败: {exc}")

    latest_id = _message_id(latest_message)
    if latest_id <= 0 or not _is_readable_message(latest_message):
        return HistoryAccessProbeResult(False, latest_id, "无法读取最新消息内容")

    if latest_id < int(min_history_message_id):
        return HistoryAccessProbeResult(True, latest_id, "目标未达到双账号阈值")

    history_probe_depth = max(1, int(min_history_message_id) // 2)
    probe_offset_id = max(2, latest_id - history_probe_depth)
    try:
        older_message = None
        for message in client.iter_messages(
            entity,
            **_iter_messages_kwargs(limit=1, offset_id=probe_offset_id),
        ):
            older_message = message
            break
    except Exception as exc:
        raise_if_long_flood_wait(
            exc,
            threshold_seconds=CFG.flood_wait_switch_threshold,
            account_label=account_label,
            scope="history-range-probe",
        )
        return HistoryAccessProbeResult(False, latest_id, f"读取历史区间失败: {exc}")

    if older_message is None or not _is_readable_message(older_message):
        return HistoryAccessProbeResult(False, latest_id, "无法读取历史消息内容")

    return HistoryAccessProbeResult(True, latest_id, "")


def build_message_id_ranges(
    latest_message_id: int,
    *,
    chunk_size: int,
    min_message_id: int = 1,
) -> list[MessageIdRange]:
    safe_latest_id = max(0, int(latest_message_id))
    safe_min_id = max(1, int(min_message_id))
    safe_chunk_size = max(1, int(chunk_size))
    if safe_latest_id < safe_min_id:
        return []

    ranges: list[MessageIdRange] = []
    end_id = safe_latest_id
    while end_id >= safe_min_id:
        start_id = max(safe_min_id, end_id - safe_chunk_size + 1)
        ranges.append(MessageIdRange(start_id=start_id, end_id=end_id))
        end_id = start_id - 1
    return ranges


def _flush_range_batch(
    msg_rows: list[tuple],
    media_rows: list[tuple],
    counters: HarvestCounters,
    *,
    write_batch_fn: Callable[[list[tuple], list[tuple]], None],
) -> int:
    if not msg_rows:
        return 0
    write_batch_fn(list(msg_rows), list(media_rows))
    batch_count = len(msg_rows)
    counters.written += batch_count
    return _last_message_id_in_rows(msg_rows)


def _parse_range_message(entity: Any, chat_id: int, message: Any) -> tuple[tuple, tuple | None]:
    try:
        parsed = MessageParser.parse(message)
    except MessageParseError as exc:
        root_exc = exc.cause if isinstance(exc.cause, Exception) else exc
        logging.error(
            "区间消息解析失败，为避免静默丢数已中止当前采集: chat_id=%s message_id=%s error=%s",
            chat_id,
            getattr(message, "id", "?"),
            exc,
        )
        raise RuntimeError(
            "消息解析失败并已中止当前区间采集 "
            f"(chat_id={chat_id}, message_id={getattr(message, 'id', '?')}): {exc}"
        ) from root_exc
    if parsed is None:
        raise RuntimeError(
            f"消息解析为空，已中止当前区间采集 (chat_id={chat_id}, message_id={getattr(message, 'id', '?')})"
        )
    return _prepare_db_rows(entity, chat_id, parsed)


def harvest_message_id_range(
    *,
    client: Any,
    entity: Any,
    chat_id: int,
    message_range: MessageIdRange,
    write_batch_fn: Callable[[list[tuple], list[tuple]], None],
    account_label: str = "",
    max_retries: int = 3,
) -> tuple[HarvestCounters, set[int]]:
    counters = HarvestCounters()
    touched_groups: set[int] = set()
    start_id = int(message_range.start_id)
    end_id = int(message_range.end_id)
    next_offset_id = end_id + 1
    retry_count = 0

    while retry_count < max(1, int(max_retries)):
        msg_rows: list[tuple] = []
        media_rows: list[tuple] = []
        last_processed_id = next_offset_id
        try:
            iterator = client.iter_messages(
                entity,
                **_iter_messages_kwargs(offset_id=next_offset_id),
            )
            for message in iterator:
                current_id = _message_id(message)
                if current_id <= 0:
                    continue
                if current_id > end_id:
                    continue
                if current_id < start_id:
                    break

                last_processed_id = min(last_processed_id, current_id)
                counters.seen += 1
                msg_row, media_row = _parse_range_message(entity, chat_id, message)
                grouped_id = msg_row[10] if len(msg_row) > 10 else None
                if grouped_id is not None:
                    touched_groups.add(int(grouped_id))
                msg_rows.append(msg_row)
                if media_row:
                    media_rows.append(media_row)

                if len(msg_rows) >= CFG.batch_size:
                    _flush_range_batch(
                        msg_rows,
                        media_rows,
                        counters,
                        write_batch_fn=write_batch_fn,
                    )
                    next_offset_id = last_processed_id
                    msg_rows, media_rows = [], []

            _flush_range_batch(
                msg_rows,
                media_rows,
                counters,
                write_batch_fn=write_batch_fn,
            )
            return counters, touched_groups
        except RuntimeError as exc:
            if isinstance(exc, AccountFloodWaitError):
                raise
            if "消息解析失败" in str(exc) or "消息解析为空" in str(exc):
                raise
            retry_count += 1
            if retry_count >= max(1, int(max_retries)):
                raise RuntimeError(
                    f"区间采集失败且重试耗尽: account={account_label or '-'} "
                    f"chat_id={chat_id} range={start_id}-{end_id}: {exc}"
                ) from exc
            wait_seconds = retry_count * 5
            logging.warning(
                "区间采集失败，准备重试: account=%s chat_id=%s range=%s-%s retry=%s wait=%ss error=%s",
                account_label or "-",
                chat_id,
                start_id,
                end_id,
                retry_count,
                wait_seconds,
                exc,
            )
            time.sleep(wait_seconds)
        except Exception as exc:
            if isinstance(exc, AccountFloodWaitError):
                raise
            retry_count += 1
            if retry_count >= max(1, int(max_retries)):
                raise RuntimeError(
                    f"区间采集失败且重试耗尽: account={account_label or '-'} "
                    f"chat_id={chat_id} range={start_id}-{end_id}: {exc}"
                ) from exc
            if is_flood_wait_error(exc):
                raise_if_long_flood_wait(
                    exc,
                    threshold_seconds=CFG.flood_wait_switch_threshold,
                    account_label=account_label,
                    scope=f"range:{chat_id}:{start_id}-{end_id}",
                )
                wait_seconds = flood_wait_seconds(exc)
            else:
                wait_seconds = retry_count * 5
            logging.warning(
                "区间采集异常，准备重试: account=%s chat_id=%s range=%s-%s retry=%s wait=%ss error=%s",
                account_label or "-",
                chat_id,
                start_id,
                end_id,
                retry_count,
                wait_seconds,
                exc,
            )
            time.sleep(wait_seconds)

    raise RuntimeError(
        f"区间采集未完成即退出: account={account_label or '-'} chat_id={chat_id} range={start_id}-{end_id}"
    )
