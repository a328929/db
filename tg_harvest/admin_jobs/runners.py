import logging
import math
import sqlite3
import time
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from threading import Lock, Semaphore
from types import SimpleNamespace
from typing import Any

from tg_harvest.admin_jobs.cleanup import (
    _build_cleanup_like_patterns,
    _build_cleanup_targets_table,
    _execute_cleanup_deletion_batches,
)
from tg_harvest.admin_jobs.common import (
    admin_error_message,
    finish_job_heartbeat,
    is_entity_lookup_miss_error,
    read_chat_username,
    resolve_chat_entity,
    start_admin_job_thread,
)
from tg_harvest.admin_jobs.core import (
    _admin_job_heartbeat,
    _admin_job_stop_requested,
    _admin_job_update_progress,
    job_context,
    job_log_passthrough_enabled,
)
from tg_harvest.admin_jobs.range_streaming import (
    RangeHarvestAccount,
    stream_entity_ranges_to_writer,
)
from tg_harvest.admin_jobs.sessions import (
    _cleanup_isolated_worker_session,
    _create_isolated_worker_client,
    _disconnect_worker_client,
    _ensure_base_session_valid,
    _start_job_heartbeat,
    bind_client_event_loop,
)
from tg_harvest.admin_jobs.streaming import stream_entity_harvest_to_writer
from tg_harvest.admin_jobs.update_writer import ChatUpdateWriteCoordinator
from tg_harvest.domain.chat_ids import stored_chat_id_from_entity_id
from tg_harvest.ingest.flood_wait import (
    AccountFloodWaitError,
    raise_if_long_flood_wait,
)
from tg_harvest.ingest.range_harvest import probe_history_access
from tg_harvest.ingest.store import (
    backfill_message_search_text_from_filenames,
)
from tg_harvest.ingest.store import (
    get_last_message_id as _get_last_message_id,
)
from tg_harvest.storage import fts as _fts
from tg_harvest.storage import search_terms as _search_terms
from tg_harvest.storage.connection import synchronized_write

DELETE_CHAT_FAST_PATH_THRESHOLD = 50000

_ACCOUNT_FLOOD_COOLDOWNS: dict[str, float] = {}
_ACCOUNT_FLOOD_COOLDOWNS_LOCK = Lock()


def _close_write_coordinator(
    write_coordinator: ChatUpdateWriteCoordinator, *, suppress_errors: bool = False
) -> None:
    try:
        write_coordinator.close()
    except Exception:
        if not suppress_errors:
            raise
        logging.exception("写入队列关闭失败，保留原始采集异常")


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        keys = row.keys()
    except Exception:
        keys = None
    if keys is not None and key not in keys:
        return default
    try:
        value = row[key]
    except Exception:
        return default
    return default if value is None else value


def _chat_title_fallback(chat_id: Any, chat_title: Any) -> str:
    title = str(chat_title or "").strip()
    if title:
        return title
    chat_id_text = str(chat_id or "").strip()
    return f"Chat {chat_id_text}" if chat_id_text else "未知群组"


def _chat_log_label(chat_id: Any, chat_title: Any) -> str:
    title = _chat_title_fallback(chat_id, chat_title)
    chat_id_text = str(chat_id or "").strip()
    if not chat_id_text:
        return title
    return f"{title} (ID={chat_id_text})"


def _chat_failure_item(chat_id: Any, chat_title: Any, reason: Any) -> str:
    return f"{_chat_log_label(chat_id, chat_title)}({str(reason or '').strip()})"


def _account_cooldown_key(account: Any) -> str:
    cfg = getattr(account, "cfg", None)
    session_name = str(getattr(cfg, "session_name", "") or "").strip()
    key = str(getattr(account, "key", "") or "").strip() or "account"
    return f"{key}:{session_name or '-'}"


def _account_cooldown_remaining(account: Any) -> int:
    cooldown_key = _account_cooldown_key(account)
    now = time.time()
    with _ACCOUNT_FLOOD_COOLDOWNS_LOCK:
        expires_at = _ACCOUNT_FLOOD_COOLDOWNS.get(cooldown_key)
        if expires_at is None:
            return 0
        remaining = int(expires_at - now)
        if remaining <= 0:
            _ACCOUNT_FLOOD_COOLDOWNS.pop(cooldown_key, None)
            return 0
        return remaining


def _remember_account_cooldown(account: Any, exc: AccountFloodWaitError) -> None:
    cooldown_key = _account_cooldown_key(account)
    expires_at = time.time() + max(1, int(exc.seconds))
    with _ACCOUNT_FLOOD_COOLDOWNS_LOCK:
        current_expires_at = _ACCOUNT_FLOOD_COOLDOWNS.get(cooldown_key, 0.0)
        _ACCOUNT_FLOOD_COOLDOWNS[cooldown_key] = max(current_expires_at, expires_at)


def _admin_update_account_plan(
    cfg: Any,
    *,
    job_id: str,
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> list[Any]:
    accounts = [
        SimpleNamespace(key="primary", label="主账号", cfg=cfg),
    ]
    secondary_session_name = str(getattr(cfg, "secondary_session_name", "") or "").strip()
    primary_session_name = str(getattr(cfg, "session_name", "") or "").strip()
    if not secondary_session_name:
        return accounts
    if secondary_session_name == primary_session_name:
        admin_job_append_log_fn(
            job_id,
            "第二账号 session 与主账号相同，批量更新仍使用主账号",
        )
        return accounts

    secondary_cfg = _cfg_with_session_name(cfg, secondary_session_name)
    if not _ensure_base_session_valid(secondary_cfg, job_id, admin_job_append_log_fn):
        admin_job_append_log_fn(
            job_id,
            "第二账号会话不可用，批量更新仍使用主账号",
        )
        return accounts

    accounts.append(
        SimpleNamespace(key="secondary", label="第二账号", cfg=secondary_cfg),
    )
    cooldown_remaining = _account_cooldown_remaining(accounts[-1])
    if cooldown_remaining > 0:
        admin_job_append_log_fn(
            job_id,
            f"第二账号仍处于 Telegram 长等待冷却，剩余约 {cooldown_remaining}s，本次批量更新不使用第二账号",
        )
        accounts.pop()
        return accounts

    admin_job_append_log_fn(
        job_id,
        "第二账号已加入批量更新调度；公开可访问群组将尽量分配给两个账号并发拉取",
    )
    return accounts


def _account_plan_item(account_plan: list[Any], idx: int) -> Any:
    if not account_plan:
        raise RuntimeError("没有可用账号执行批量更新")
    return account_plan[(max(int(idx), 1) - 1) % len(account_plan)]


def _row_has_public_username(row: Any) -> bool:
    return bool(str(_row_value(row, "chat_username", "") or "").strip())


def _session_file_for_name(session_name: Any) -> Path:
    path = Path(str(session_name or ""))
    if path.suffix != ".session":
        path = Path(str(path) + ".session")
    return path


def _read_session_cached_chat_ids(session_name: Any) -> set[int]:
    path = _session_file_for_name(session_name)
    if not path.exists() or not path.is_file():
        return set()

    try:
        uri = path.resolve().as_uri() + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
    except Exception:
        logging.exception("读取 Telegram session 缓存失败: %s", path)
        return set()

    try:
        cur = conn.cursor()
        try:
            cur.execute("SELECT id FROM entities WHERE id < 0")
            return {
                stored_chat_id_from_entity_id(int(row[0]))
                for row in cur.fetchall()
                if row[0] is not None
            }
        finally:
            cur.close()
    except Exception:
        logging.exception("读取 Telegram session entities 失败: %s", path)
        return set()
    finally:
        conn.close()


def _row_chat_id(row: Any) -> int:
    try:
        return int(_row_value(row, "chat_id", 0) or 0)
    except (TypeError, ValueError):
        return 0


def _row_chat_identity(row: Any) -> int:
    chat_id = _row_chat_id(row)
    if chat_id == 0:
        return 0
    return stored_chat_id_from_entity_id(chat_id)


def _select_evenly(indexed_rows: list[tuple[int, Any]], target_count: int) -> set[int]:
    target = max(0, min(int(target_count), len(indexed_rows)))
    if target <= 0:
        return set()

    selected: set[int] = set()
    assigned = 0
    total = len(indexed_rows)
    for position, (idx, _row) in enumerate(indexed_rows, start=1):
        target_so_far = (position * target * 2 + total - 1) // (2 * total)
        if assigned >= target_so_far:
            continue
        selected.add(idx)
        assigned += 1
    return selected


def _build_admin_update_account_assignments(
    indexed_rows: list[tuple[int, Any]],
    account_plan: list[Any],
    *,
    secondary_cached_chat_ids: set[int] | None = None,
) -> tuple[dict[int, Any], dict[str, int]]:
    primary_account = account_plan[0]
    assignments: dict[int, Any] = {}
    counts = {
        "primary": 0,
        "secondary": 0,
        "primary_only": 0,
        "secondary_eligible": 0,
        "secondary_cached": 0,
        "secondary_cached_eligible": 0,
        "secondary_public": 0,
        "secondary_public_candidates": 0,
    }
    if len(account_plan) < 2:
        for idx, _row in indexed_rows:
            assignments[idx] = primary_account
            counts["primary"] += 1
        return assignments, counts

    secondary_account = next(
        (account for account in account_plan if account.key == "secondary"),
        account_plan[1],
    )
    total = len(indexed_rows)
    cached_chat_ids = {int(chat_id) for chat_id in (secondary_cached_chat_ids or set())}

    cached_rows: list[tuple[int, Any]] = []
    username_rows: list[tuple[int, Any]] = []
    for idx, row in indexed_rows:
        if _row_chat_identity(row) in cached_chat_ids:
            cached_rows.append((idx, row))
            continue
        if _row_has_public_username(row):
            username_rows.append((idx, row))
            continue
        counts["primary_only"] += 1

    counts["secondary_cached_eligible"] = len(cached_rows)
    counts["secondary_public_candidates"] = len(username_rows)
    counts["secondary_eligible"] = len(cached_rows) + len(username_rows)

    if counts["secondary_eligible"] <= 0:
        for idx, _row in indexed_rows:
            assignments[idx] = primary_account
            counts["primary"] += 1
        return assignments, counts

    primary_target = (total + len(account_plan) - 1) // len(account_plan)
    secondary_target = min(counts["secondary_eligible"], max(0, total - primary_target))
    if len(cached_rows) >= secondary_target:
        selected_cached = _select_evenly(cached_rows, secondary_target)
        selected_username: set[int] = set()
    else:
        selected_cached = {idx for idx, _row in cached_rows}
        selected_username = _select_evenly(
            username_rows, secondary_target - len(selected_cached)
        )
    secondary_indexes = selected_cached | selected_username
    counts["secondary_cached"] = len(selected_cached)
    counts["secondary_public"] = len(selected_username)

    for idx, _row in indexed_rows:
        if idx in secondary_indexes:
            assignments[idx] = secondary_account
            counts["secondary"] += 1
        else:
            assignments[idx] = primary_account
            counts["primary"] += 1

    return assignments, counts


def _admin_update_effective_concurrency(
    cfg: Any,
    *,
    configured_concurrency: int,
    active_account_count: int,
) -> tuple[int, int]:
    _ = cfg
    per_account_limit = max(
        1,
        int(configured_concurrency) // max(1, int(active_account_count)),
    )
    effective_concurrency = min(
        int(configured_concurrency),
        max(1, per_account_limit * max(1, int(active_account_count))),
    )
    return per_account_limit, effective_concurrency


def _account_plan_by_key(account_plan: list[Any], key: str) -> Any | None:
    for account in account_plan:
        if getattr(account, "key", "") == key:
            return account
    return None


def _candidate_accounts_for_switch(
    account_plan: list[Any],
    preferred_account: Any,
) -> list[Any]:
    preferred_key = getattr(preferred_account, "key", "")
    return [
        account
        for account in account_plan
        if getattr(account, "key", "") != preferred_key
    ]


def _admin_update_account_error_message(account: Any, exc: Exception) -> str:
    if getattr(account, "key", "") == "secondary" and is_entity_lookup_miss_error(exc):
        return "第二账号无法解析该群组/频道（本地实体缓存未命中、未加入或 username 解析不可用）"
    return admin_error_message(exc)


def _admin_update_should_defer_chat(exc: Exception) -> bool:
    if isinstance(exc, AccountFloodWaitError):
        return True
    return "没有可用账号执行当前群组" in str(exc)


def _admin_update_all_chats(
    job_id, _ignored_client, get_conn_fn, admin_job_append_log_fn, cfg
):
    if not _ensure_base_session_valid(cfg, job_id, admin_job_append_log_fn):
        return False

    conn = get_conn_fn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT chat_id, chat_title, chat_username FROM chats ORDER BY chat_title COLLATE NOCASE ASC, chat_id ASC"
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        admin_job_append_log_fn(job_id, "当前无可更新群聊，任务结束")
        return True

    admin_job_append_log_fn(
        job_id,
        f"数据库中共有 {len(rows)} 个群组/频道，本次将按数据库清单逐一尝试更新",
    )

    account_plan = _admin_update_account_plan(
        cfg,
        job_id=job_id,
        admin_job_append_log_fn=admin_job_append_log_fn,
    )
    secondary_cached_chat_ids: set[int] = set()
    if len(account_plan) > 1:
        secondary_account = (
            _account_plan_by_key(account_plan, "secondary") or account_plan[1]
        )
        secondary_cached_chat_ids = _read_session_cached_chat_ids(
            getattr(secondary_account.cfg, "session_name", "")
        )

    total = len(rows)
    indexed_rows = list(enumerate(rows, start=1))
    account_assignments, assignment_counts = _build_admin_update_account_assignments(
        indexed_rows,
        account_plan,
        secondary_cached_chat_ids=secondary_cached_chat_ids,
    )
    success_count, failed_count, deferred_count, total_added_messages = 0, 0, 0, 0
    failed_chats = []

    try:
        configured_concurrency = max(1, int(getattr(cfg, "admin_update_concurrency", 5)))
    except (TypeError, ValueError):
        configured_concurrency = 5
    per_account_concurrency, effective_concurrency = _admin_update_effective_concurrency(
        cfg,
        configured_concurrency=configured_concurrency,
        active_account_count=len(account_plan),
    )
    admin_job_append_log_fn(
        job_id,
        f"读取到 {total} 个群组，开始执行并发拉取 + 单线程写入（总并发上限：{effective_concurrency}，"
        f"单账号上限：{per_account_concurrency}，账号数：{len(account_plan)}）",
    )
    if len(account_plan) > 1:
        admin_job_append_log_fn(
            job_id,
            "批量更新账号分配计划："
            f"主账号 {assignment_counts['primary']} 个，"
            f"第二账号 {assignment_counts['secondary']} 个；"
            f"第二账号本地缓存可直接解析 {assignment_counts['secondary_cached_eligible']} 个，"
            f"本次安排缓存命中 {assignment_counts['secondary_cached']} 个、"
            f"公开 username 解析 {assignment_counts['secondary_public']} 个；"
            f"{assignment_counts['primary_only']} 个缺少公开用户名且第二账号无缓存，"
            "优先交给主账号；执行中任一账号失败会自动切换另一账号重试",
        )
    _admin_job_update_progress(
        job_id,
        0,
        total=total,
        stage="updating",
        log_step=0,
    )

    write_coordinator = ChatUpdateWriteCoordinator(
        job_id=str(job_id),
        get_conn_fn=get_conn_fn,
        queue_maxsize=max(effective_concurrency * 4, 16),
    )
    account_semaphores = {
        account.key: Semaphore(per_account_concurrency) for account in account_plan
    }
    if len(account_plan) > 1:
        admin_job_append_log_fn(
            job_id,
            "双账号协同策略：两个账号都会尽量参与公开群组更新；"
            "任一账号解析失败、读取失败或触发长等待时，当前群组会自动切换另一账号重试",
        )
    stats_lock = Lock()
    account_stats = {
        account.key: {
            "direct_success": 0,
            "fallback_out": 0,
            "flood_wait_switches": 0,
            "fallback_success": 0,
        }
        for account in account_plan
    }
    account_cooldowns: dict[str, float] = {}
    account_state_lock = Lock()
    no_available_accounts = False
    try:
        max_cooldown_wait_seconds = max(
            0,
            int(getattr(cfg, "admin_update_max_cooldown_wait_seconds", 45) or 0),
        )
    except (TypeError, ValueError):
        max_cooldown_wait_seconds = 45

    def _run_chat_update_with_account(
        *,
        idx: int,
        account: Any,
        current_chat_id: int,
        current_chat_title: str,
        current_chat_label: str,
        current_chat_username: str | None,
    ) -> int:
        local_client = None
        worker_id = f"{job_id}_{account.key}_{idx}"
        semaphore = account_semaphores.get(account.key)
        if semaphore is not None:
            semaphore.acquire()
        try:
            local_client = _create_isolated_worker_client(account.cfg, worker_id)
            before_count = _admin_get_chat_message_count(get_conn_fn, current_chat_id)

            try:
                entity = resolve_chat_entity(
                    local_client,
                    current_chat_id,
                    current_chat_username,
                    allow_username_fallback=True,
                )
            except Exception as exc:
                _raise_if_account_flood_wait(
                    exc,
                    cfg=account.cfg,
                    account_label=str(account.key),
                    scope="admin-update-resolve-entity",
                )
                raise
            entity_title = (
                getattr(entity, "title", None)
                or getattr(entity, "username", None)
                or str(current_chat_id)
            )
            admin_job_append_log_fn(
                job_id,
                f"[{idx}/{total}] 群组连接成功：账号={account.label}，目标={current_chat_label}，名称={entity_title}",
            )

            stream_entity_harvest_to_writer(
                write_coordinator=write_coordinator,
                get_conn_fn=get_conn_fn,
                client=local_client,
                entity=entity,
                idx=idx,
                total=total,
                fallback_chat_id=current_chat_id,
                fallback_chat_title=current_chat_title,
                fallback_chat_username=current_chat_username,
                skip_postprocess_if_unchanged=True,
                enable_dedupe=False,
            )
            after_count = _admin_get_chat_message_count(get_conn_fn, current_chat_id)
            return max(0, after_count - before_count)
        finally:
            if local_client:
                with suppress(Exception):
                    _disconnect_worker_client(local_client)
            _cleanup_isolated_worker_session(account.cfg, worker_id)
            if semaphore is not None:
                semaphore.release()

    def _mark_account_cooldown(account: Any, exc: AccountFloodWaitError) -> None:
        _remember_account_cooldown(account, exc)
        expires_at = time.time() + max(1, int(exc.seconds))
        with account_state_lock:
            already_disabled = account.key in account_cooldowns
            account_cooldowns[account.key] = max(
                account_cooldowns.get(account.key, 0.0), expires_at
            )
        if not already_disabled:
            admin_job_append_log_fn(
                job_id,
                f"{account.label} 进入长等待冷却：wait={exc.seconds}s threshold={exc.threshold_seconds}s，"
                "后续群组将尽量切换其他账号",
            )

    def _active_account_cooldown_keys() -> set[str]:
        now = time.time()
        with account_state_lock:
            expired_keys = [
                key for key, expires_at in account_cooldowns.items() if expires_at <= now
            ]
            for key in expired_keys:
                account_cooldowns.pop(key, None)
            return set(account_cooldowns)

    def _available_accounts(preferred_account: Any) -> list[Any]:
        cooldown_keys = _active_account_cooldown_keys()
        candidates = _candidate_accounts_for_switch(
            account_plan,
            preferred_account,
        )
        return [
            account
            for account in ([preferred_account] + candidates)
            if account.key not in cooldown_keys
            and _account_cooldown_remaining(account) <= 0
        ]

    def _next_account_cooldown_remaining() -> int:
        now = time.time()
        remaining_values: list[int] = []
        for account in account_plan:
            remaining = _account_cooldown_remaining(account)
            if remaining > 0:
                remaining_values.append(remaining)
        with account_state_lock:
            for expires_at in account_cooldowns.values():
                remaining = math.ceil(expires_at - now)
                if remaining > 0:
                    remaining_values.append(remaining)
        if not remaining_values:
            return 0
        return max(1, min(remaining_values))

    def _wait_for_short_account_cooldown() -> bool:
        if _admin_job_stop_requested(str(job_id)):
            return False
        remaining = _next_account_cooldown_remaining()
        if remaining <= 0:
            _active_account_cooldown_keys()
            return True
        if max_cooldown_wait_seconds <= 0 or remaining > max_cooldown_wait_seconds:
            admin_job_append_log_fn(
                job_id,
                "所有账号都处于 Telegram 长等待冷却，"
                f"最近可用还需约 {remaining}s，超过本次最多短等 {max_cooldown_wait_seconds}s；"
                "停止启动剩余群组，避免批量记为失败",
            )
            return False
        admin_job_append_log_fn(
            job_id,
            "所有账号暂时处于 Telegram 冷却，"
            f"等待约 {remaining}s 后继续启动剩余群组",
        )
        time.sleep(remaining)
        if _admin_job_stop_requested(str(job_id)):
            return False
        _active_account_cooldown_keys()
        return True

    def _run_with_fallback(
        *,
        idx: int,
        current_chat_id: int,
        current_chat_title: str,
        current_chat_label: str,
        current_chat_username: str | None,
        preferred_account: Any,
    ) -> tuple[int, Any]:
        last_exc: Exception | None = None
        candidates = _available_accounts(preferred_account)
        if not candidates:
            raise RuntimeError("没有可用账号执行当前群组")

        for attempt_account in candidates:
            try:
                added_count = _run_chat_update_with_account(
                    idx=idx,
                    account=attempt_account,
                    current_chat_id=current_chat_id,
                    current_chat_title=current_chat_title,
                    current_chat_label=current_chat_label,
                    current_chat_username=current_chat_username,
                )
                with stats_lock:
                    if attempt_account.key != preferred_account.key:
                        account_stats[attempt_account.key]["fallback_success"] += 1
                    else:
                        account_stats[attempt_account.key]["direct_success"] += 1
                return added_count, attempt_account
            except AccountFloodWaitError as exc:
                _mark_account_cooldown(attempt_account, exc)
                with stats_lock:
                    account_stats[attempt_account.key]["flood_wait_switches"] += 1
                    account_stats[attempt_account.key]["fallback_out"] += 1
                last_exc = exc
                if attempt_account.key != preferred_account.key:
                    admin_job_append_log_fn(
                        job_id,
                        f"[{idx}/{total}] 备用账号也进入长等待：{attempt_account.label}，继续尝试其他账号",
                    )
                continue
            except Exception as account_exc:
                last_exc = account_exc
                with stats_lock:
                    account_stats[attempt_account.key]["fallback_out"] += 1
                admin_job_append_log_fn(
                    job_id,
                    f"[{idx}/{total}] {attempt_account.label}更新失败，切换其他账号重试："
                    f"群组={current_chat_label}，"
                    f"错误={_admin_update_account_error_message(attempt_account, account_exc)}",
                )
                continue
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("没有可用账号执行当前群组")

    def _worker(idx, row):
        job_context.set(str(job_id))
        passthrough_token = job_log_passthrough_enabled.set(False)
        raw_chat_id = _row_value(row, "chat_id", "")
        current_chat_title = _chat_title_fallback(
            raw_chat_id, _row_value(row, "chat_title", "")
        )
        current_chat_label = _chat_log_label(raw_chat_id, current_chat_title)
        current_chat_username = _row_value(row, "chat_username", None)

        import random

        time.sleep(random.uniform(0.1, 0.5))

        try:
            current_chat_id = int(raw_chat_id)
            if current_chat_id == 0:
                raise RuntimeError("无法识别群组/频道 ID")

            account = account_assignments.get(idx) or _account_plan_item(
                account_plan, idx
            )
            added_count, used_account = _run_with_fallback(
                idx=idx,
                current_chat_id=current_chat_id,
                current_chat_title=current_chat_title,
                current_chat_label=current_chat_label,
                current_chat_username=current_chat_username,
                preferred_account=account,
            )
            if used_account.key != account.key:
                admin_job_append_log_fn(
                    job_id,
                    f"[{idx}/{total}] 已切换账号完成采集：原账号={account.label}，实际账号={used_account.label}，群组={current_chat_label}",
                )
            return current_chat_title, current_chat_id, True, added_count, None
        except Exception as chat_exc:
            logging.exception(f"Worker 执行群组 {current_chat_label} 失败")
            user_msg = admin_error_message(chat_exc)
            if _admin_update_should_defer_chat(chat_exc):
                return (
                    current_chat_title,
                    raw_chat_id,
                    None,
                    0,
                    user_msg,
                )
            return (
                current_chat_title,
                raw_chat_id,
                False,
                0,
                user_msg,
            )
        finally:
            job_log_passthrough_enabled.reset(passthrough_token)

    stopped_early = False
    stop_logged = False
    row_iter = iter(enumerate(rows, start=1))

    def _log_stop_once() -> None:
        nonlocal stop_logged
        if stop_logged:
            return
        stop_logged = True
        admin_job_append_log_fn(
            job_id,
            "已收到停止请求：不再启动新的群组，等待当前并发中的群组完成后收尾",
        )

    def _should_stop_submitting() -> bool:
        nonlocal stopped_early
        if not _admin_job_stop_requested(str(job_id)):
            return False
        stopped_early = True
        _log_stop_once()
        return True

    def _submit_next(executor: ThreadPoolExecutor, futures: dict) -> bool:
        nonlocal stopped_early, no_available_accounts
        if _should_stop_submitting():
            return False
        if not any(_available_accounts(account) for account in account_plan):
            if not _wait_for_short_account_cooldown():
                stopped_early = True
                no_available_accounts = True
                return False
            if not any(_available_accounts(account) for account in account_plan):
                stopped_early = True
                no_available_accounts = True
                admin_job_append_log_fn(
                    job_id,
                    "账号冷却等待结束后仍无可用账号，停止启动剩余群组",
                )
                return False
        try:
            idx, row = next(row_iter)
        except StopIteration:
            return False
        futures[executor.submit(_worker, idx, row)] = (idx, row)
        return True

    try:
        with ThreadPoolExecutor(max_workers=effective_concurrency) as executor:
            futures = {}
            while len(futures) < effective_concurrency and _submit_next(executor, futures):
                pass

            while futures:
                done_futures, _pending_futures = wait(
                    futures.keys(), return_when=FIRST_COMPLETED
                )
                for future in done_futures:
                    idx, row = futures.pop(future)
                    try:
                        chat_title, chat_id, success, added, err_msg = future.result()
                        chat_label = _chat_log_label(chat_id, chat_title)
                        if success:
                            total_added_messages += added
                            success_count += 1
                            admin_job_append_log_fn(
                                job_id,
                                f"[{idx}/{total}] {chat_label} 新增 {added} 条消息",
                            )
                        elif success is None:
                            deferred_count += 1
                            stopped_early = True
                            no_available_accounts = True
                            admin_job_append_log_fn(
                                job_id,
                                f"[{idx}/{total}] 因账号冷却暂缓采集：群组={chat_label}，原因={err_msg}",
                            )
                        else:
                            failed_count += 1
                            failed_chats.append(
                                _chat_failure_item(chat_id, chat_title, err_msg)
                            )
                            admin_job_append_log_fn(
                                job_id,
                                f"[{idx}/{total}] 增量采集失败：群组={chat_label}，错误={err_msg}",
                            )
                    except Exception as e:
                        failed_count += 1
                        raw_chat_id = _row_value(row, "chat_id", "")
                        chat_title = _chat_title_fallback(
                            raw_chat_id, _row_value(row, "chat_title", "")
                        )
                        chat_label = _chat_log_label(raw_chat_id, chat_title)
                        err_msg = admin_error_message(e)
                        failed_chats.append(
                            _chat_failure_item(raw_chat_id, chat_title, err_msg)
                        )
                        admin_job_append_log_fn(
                            job_id,
                            f"[{idx}/{total}] 线程执行异常：群组={chat_label}，错误={err_msg}",
                        )
                    finally:
                        _admin_job_update_progress(
                            job_id,
                            success_count + failed_count + deferred_count,
                            total=total,
                            stage="updating",
                            log_step=0,
                            auto_log=False,
                        )

                while len(futures) < effective_concurrency and _submit_next(executor, futures):
                    pass
    finally:
        if success_count + failed_count >= total:
            _admin_job_update_progress(
                job_id,
                total,
                total=total,
                stage="finalizing",
                log_step=0,
                auto_log=False,
            )
        write_coordinator.close()

    processed_count = success_count + failed_count + deferred_count
    skipped_count = max(0, total - processed_count)
    if stopped_early:
        if no_available_accounts:
            final_log_msg = (
                f"全部群组增量采集因账号冷却提前收尾：成功 {success_count} 个，失败 {failed_count} 个，"
                f"暂缓 {deferred_count} 个，未启动 {skipped_count} 个，总计 {total} 个，"
                f"共新增 {total_added_messages} 条消息；"
                "未启动群组可在冷却结束后再次批量更新"
            )
        else:
            final_log_msg = (
                f"全部群组增量采集已按请求停止：成功 {success_count} 个，失败 {failed_count} 个，"
                f"暂缓 {deferred_count} 个，未启动 {skipped_count} 个，总计 {total} 个，"
                f"共新增 {total_added_messages} 条消息"
            )
    else:
        final_log_msg = (
            f"全部群组增量采集完成：成功 {success_count} 个，失败 {failed_count} 个，暂缓 {deferred_count} 个，"
            f"总计 {total} 个，共新增 {total_added_messages} 条消息"
        )
    if failed_chats:
        final_log_msg += f"。失败列表：{', '.join(failed_chats)}"
    admin_job_append_log_fn(job_id, final_log_msg)
    if len(account_plan) > 1:
        with stats_lock:
            primary_stats = dict(account_stats.get("primary", {}))
            secondary_stats = dict(account_stats.get("secondary", {}))
        admin_job_append_log_fn(
            job_id,
            "账号执行统计："
            f"主账号直接成功 {primary_stats.get('direct_success', 0)} 个，"
            f"接管第二账号失败 {primary_stats.get('fallback_success', 0)} 个，"
            f"失败切出 {primary_stats.get('fallback_out', 0)} 个，"
            f"长等待切换 {primary_stats.get('flood_wait_switches', 0)} 次；"
            f"第二账号直接成功 {secondary_stats.get('direct_success', 0)} 个，"
            f"接管主账号失败 {secondary_stats.get('fallback_success', 0)} 个，"
            f"失败切出 {secondary_stats.get('fallback_out', 0)} 个，"
            f"长等待切换 {secondary_stats.get('flood_wait_switches', 0)} 次",
        )
    _admin_job_update_progress(
        job_id,
        processed_count if stopped_early else total,
        total=total,
        stage="done" if failed_count == 0 else "error",
        log_step=0,
        auto_log=False,
    )
    return failed_count == 0


def _admin_process_single_chat_update(
    *,
    job_id: str,
    client: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_append_log_fn: Callable[[str, str], Any],
    chat_id: int,
    chat_title: str,
    chat_username: str | None = None,
    idx: int,
    total: int,
) -> None:
    entity = resolve_chat_entity(client, chat_id, chat_username)

    entity_title = (
        getattr(entity, "title", None)
        or getattr(entity, "username", None)
        or str(chat_id)
    )
    admin_job_append_log_fn(
        job_id, f"[{idx}/{total}] 群组连接成功：名称={entity_title}"
    )
    write_coordinator = ChatUpdateWriteCoordinator(
        job_id=str(job_id),
        get_conn_fn=get_conn_fn,
        queue_maxsize=16,
    )
    stream_failed = False
    try:
        _admin_job_update_progress(
            job_id,
            0,
            total=1,
            stage="updating",
            log_step=0,
            auto_log=False,
        )
        admin_job_append_log_fn(job_id, "启用边抓取边写入：抓取与数据库写入并行执行")
        stream_entity_harvest_to_writer(
            write_coordinator=write_coordinator,
            get_conn_fn=get_conn_fn,
            client=client,
            entity=entity,
            idx=idx,
            total=total,
            fallback_chat_id=chat_id,
            fallback_chat_title=chat_title,
            fallback_chat_username=chat_username,
            skip_postprocess_if_unchanged=True,
            enable_dedupe=False,
        )
        _admin_job_update_progress(
            job_id,
            1,
            total=1,
            stage="done",
            log_step=0,
            auto_log=False,
        )
    except Exception:
        stream_failed = True
        raise
    finally:
        _close_write_coordinator(write_coordinator, suppress_errors=stream_failed)


def _run_single_chat_update_with_account_fallback(
    *,
    job_id: str,
    cfg: Any,
    primary_client: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_append_log_fn: Callable[[str, str], Any],
    chat_id: int,
    chat_title: str,
    chat_username: str | None,
) -> None:
    primary_exc: Exception
    try:
        _admin_process_single_chat_update(
            job_id=job_id,
            client=primary_client,
            get_conn_fn=get_conn_fn,
            admin_job_append_log_fn=admin_job_append_log_fn,
            chat_id=chat_id,
            chat_title=chat_title,
            chat_username=chat_username,
            idx=1,
            total=1,
        )
        return
    except Exception as exc:
        primary_exc = exc
        if isinstance(exc, AccountFloodWaitError):
            _remember_account_cooldown(
                SimpleNamespace(key="primary", label="主账号", cfg=cfg),
                exc,
            )
            reason = f"触发长等待 wait={exc.seconds}s"
        else:
            reason = admin_error_message(exc)

    secondary_session_name = str(getattr(cfg, "secondary_session_name", "") or "").strip()
    if (
        not secondary_session_name
        or secondary_session_name == str(getattr(cfg, "session_name", "") or "")
    ):
        raise primary_exc

    secondary_cfg = _cfg_with_session_name(cfg, secondary_session_name)
    if not _ensure_base_session_valid(secondary_cfg, job_id, admin_job_append_log_fn):
        raise primary_exc

    admin_job_append_log_fn(
        job_id,
        f"主账号更新失败，立即切换第二账号重试当前群组：群组={_chat_log_label(chat_id, chat_title)}，错误={reason}",
    )
    secondary_client = None
    secondary_worker_id = f"{job_id}_secondary_single"
    try:
        secondary_client = _create_isolated_worker_client(
            secondary_cfg,
            secondary_worker_id,
        )
        try:
            _admin_process_single_chat_update(
                job_id=job_id,
                client=secondary_client,
                get_conn_fn=get_conn_fn,
                admin_job_append_log_fn=admin_job_append_log_fn,
                chat_id=chat_id,
                chat_title=chat_title,
                chat_username=chat_username,
                idx=1,
                total=1,
            )
        except Exception as secondary_exc:
            if isinstance(secondary_exc, AccountFloodWaitError):
                _remember_account_cooldown(
                    SimpleNamespace(
                        key="secondary",
                        label="第二账号",
                        cfg=secondary_cfg,
                    ),
                    secondary_exc,
                )
            raise RuntimeError(
                f"主账号更新失败：{admin_error_message(primary_exc)}；"
                f"第二账号重试失败：{admin_error_message(secondary_exc)}"
            ) from secondary_exc
    finally:
        if secondary_client:
            with suppress(Exception):
                _disconnect_worker_client(secondary_client)
        _cleanup_isolated_worker_session(secondary_cfg, secondary_worker_id)


def _admin_get_chat_message_count(get_conn_fn: Callable[[], Any], chat_id: int) -> int:
    count_conn = get_conn_fn()
    try:
        cur = count_conn.cursor()
        cur.execute(
            "SELECT COALESCE(message_count, 0) AS cnt FROM chats WHERE chat_id = ?",
            (chat_id,),
        )
        row = cur.fetchone()
        return int(row["cnt"] or 0) if row else 0
    finally:
        count_conn.close()


def _cfg_with_session_name(cfg: Any, session_name: str) -> Any:
    values = dict(getattr(cfg, "__dict__", {}) or {})
    if not values:
        values = {
            "api_id": getattr(cfg, "api_id", 0),
            "api_hash": getattr(cfg, "api_hash", ""),
        }
    values["session_name"] = session_name
    return SimpleNamespace(**values)


def _entity_identity(entity: Any) -> int:
    try:
        entity_id = int(getattr(entity, "id", 0) or 0)
    except (TypeError, ValueError):
        return 0
    return stored_chat_id_from_entity_id(entity_id)


def _find_matching_entity(candidate_entities: list[Any], target_entity: Any) -> Any | None:
    target_identity = _entity_identity(target_entity)
    if not target_identity:
        return None
    for entity in candidate_entities:
        if _entity_identity(entity) == target_identity:
            return entity
    return None


@dataclass
class _HarvestTarget:
    entity: Any
    client: Any
    cfg: Any
    account_key: str
    account_label: str


@dataclass
class _HarvestTargetResolution:
    targets: list[_HarvestTarget]
    cleanup_workers: list[tuple[Any, str, Any]]

    def close(self) -> None:
        for worker_cfg, worker_id, worker_client in reversed(self.cleanup_workers):
            if worker_client:
                with suppress(Exception):
                    _disconnect_worker_client(worker_client)
            _cleanup_isolated_worker_session(worker_cfg, worker_id)


def _primary_harvest_targets(
    entities: list[Any],
    *,
    client: Any,
    cfg: Any,
) -> list[_HarvestTarget]:
    return [
        _HarvestTarget(
            entity=entity,
            client=client,
            cfg=cfg,
            account_key="primary",
            account_label="主账号",
        )
        for entity in entities
    ]


def _secondary_harvest_targets(
    entities: list[Any],
    *,
    client: Any,
    cfg: Any,
) -> list[_HarvestTarget]:
    return [
        _HarvestTarget(
            entity=entity,
            client=client,
            cfg=cfg,
            account_key="secondary",
            account_label="第二账号",
        )
        for entity in entities
    ]


def _cfg_flood_wait_threshold(cfg: Any) -> int:
    try:
        return int(getattr(cfg, "flood_wait_switch_threshold", 30) or 30)
    except (TypeError, ValueError):
        return 30


def _raise_if_account_flood_wait(
    exc: BaseException,
    *,
    cfg: Any,
    account_label: str,
    scope: str,
) -> None:
    raise_if_long_flood_wait(
        exc,
        threshold_seconds=_cfg_flood_wait_threshold(cfg),
        account_label=account_label,
        scope=scope,
    )


def _resolve_target_entities_for_account(
    client: Any,
    target: str,
    *,
    cfg: Any,
    account_label: str,
    scope: str,
) -> list[Any]:
    from tg_harvest.ingest.parse import resolve_target_entities

    try:
        with bind_client_event_loop(client):
            return resolve_target_entities(client, target)
    except Exception as exc:
        _raise_if_account_flood_wait(
            exc,
            cfg=cfg,
            account_label=account_label,
            scope=scope,
        )
        raise


def _resolve_matching_entity_for_account(
    client: Any,
    target: str,
    source_entity: Any,
    *,
    cfg: Any,
    account_label: str,
) -> Any | None:
    target_entities = _resolve_target_entities_for_account(
        client,
        target,
        cfg=cfg,
        account_label=account_label,
        scope="resolve-matching-target",
    )
    matched_entity = _find_matching_entity(target_entities, source_entity)
    if matched_entity is not None:
        return matched_entity

    username = str(getattr(source_entity, "username", "") or "").strip().lstrip("@")
    if username:
        try:
            with bind_client_event_loop(client):
                username_entity = client.get_entity(username)
        except Exception as exc:
            _raise_if_account_flood_wait(
                exc,
                cfg=cfg,
                account_label=account_label,
                scope="resolve-matching-username",
            )
            username_entity = None
        if username_entity is not None and _entity_identity(username_entity) == _entity_identity(
            source_entity
        ):
            return username_entity

    return None


def _resolve_harvest_targets(
    *,
    job_id: str,
    target: str,
    cfg: Any,
    primary_client: Any,
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> _HarvestTargetResolution:
    try:
        primary_entities = _resolve_target_entities_for_account(
            primary_client,
            target,
            cfg=cfg,
            account_label="primary",
            scope="resolve-harvest-target",
        )
    except AccountFloodWaitError as exc:
        _remember_account_cooldown(
            SimpleNamespace(key="primary", label="主账号", cfg=cfg),
            exc,
        )
        admin_job_append_log_fn(
            job_id,
            f"主账号解析目标触发长等待 wait={exc.seconds}s threshold={exc.threshold_seconds}s，尝试第二账号辅助解析",
        )
        primary_entities = []
        primary_flood_wait_exc: AccountFloodWaitError | None = exc
    else:
        primary_flood_wait_exc = None
    if primary_entities:
        return _HarvestTargetResolution(
            _primary_harvest_targets(
                primary_entities,
                client=primary_client,
                cfg=cfg,
            ),
            [],
        )

    secondary_session_name = str(getattr(cfg, "secondary_session_name", "") or "").strip()
    if not secondary_session_name:
        if primary_flood_wait_exc is not None:
            raise primary_flood_wait_exc
        return _HarvestTargetResolution([], [])
    if secondary_session_name == str(getattr(cfg, "session_name", "") or "").strip():
        if primary_flood_wait_exc is not None:
            raise primary_flood_wait_exc
        return _HarvestTargetResolution([], [])

    secondary_cfg = _cfg_with_session_name(cfg, secondary_session_name)
    if not _ensure_base_session_valid(secondary_cfg, job_id, admin_job_append_log_fn):
        admin_job_append_log_fn(
            job_id,
            "主账号未解析到目标，第二账号会话不可用，无法继续按名称辅助解析",
        )
        if primary_flood_wait_exc is not None:
            raise primary_flood_wait_exc
        return _HarvestTargetResolution([], [])

    secondary_client = None
    secondary_worker_id = f"{job_id}_secondary_resolve"
    keep_secondary_client = False
    try:
        secondary_client = _create_isolated_worker_client(
            secondary_cfg,
            secondary_worker_id,
        )
        try:
            secondary_entities = _resolve_target_entities_for_account(
                secondary_client,
                target,
                cfg=cfg,
                account_label="secondary",
                scope="resolve-harvest-target",
            )
        except AccountFloodWaitError as exc:
            _remember_account_cooldown(
                SimpleNamespace(key="secondary", label="第二账号", cfg=secondary_cfg),
                exc,
            )
            admin_job_append_log_fn(
                job_id,
                f"第二账号解析目标触发长等待 wait={exc.seconds}s threshold={exc.threshold_seconds}s，无法继续辅助解析",
            )
            if primary_flood_wait_exc is not None:
                raise primary_flood_wait_exc from exc
            return _HarvestTargetResolution([], [])
        if not secondary_entities:
            if primary_flood_wait_exc is not None:
                raise primary_flood_wait_exc
            return _HarvestTargetResolution([], [])

        if primary_flood_wait_exc is not None:
            keep_secondary_client = True
            admin_job_append_log_fn(
                job_id,
                "主账号处于长等待，第二账号已解析目标，"
                f"后续由第二账号采集 {len(secondary_entities)} 个会话",
            )
            return _HarvestTargetResolution(
                _secondary_harvest_targets(
                    secondary_entities,
                    client=secondary_client,
                    cfg=secondary_cfg,
                ),
                [(secondary_cfg, secondary_worker_id, secondary_client)],
            )

        resolved_for_primary: list[Any] = []
        seen_identities: set[int] = set()
        for secondary_entity in secondary_entities:
            try:
                primary_entity = _resolve_matching_entity_for_account(
                    primary_client,
                    target,
                    secondary_entity,
                    cfg=cfg,
                    account_label="primary",
                )
            except AccountFloodWaitError as exc:
                admin_job_append_log_fn(
                    job_id,
                    f"主账号确认第二账号解析结果时触发长等待 wait={exc.seconds}s threshold={exc.threshold_seconds}s，改用第二账号直接采集",
                )
                _remember_account_cooldown(
                    SimpleNamespace(key="primary", label="主账号", cfg=cfg),
                    exc,
                )
                keep_secondary_client = True
                return _HarvestTargetResolution(
                    _secondary_harvest_targets(
                        secondary_entities,
                        client=secondary_client,
                        cfg=secondary_cfg,
                    ),
                    [(secondary_cfg, secondary_worker_id, secondary_client)],
                )
            if primary_entity is None:
                continue
            identity = _entity_identity(primary_entity)
            if not identity or identity in seen_identities:
                continue
            seen_identities.add(identity)
            resolved_for_primary.append(primary_entity)

        if resolved_for_primary:
            admin_job_append_log_fn(
                job_id,
                f"主账号未按名称匹配到目标，已通过第二账号解析并在主账号确认 {len(resolved_for_primary)} 个会话",
            )
        return _HarvestTargetResolution(
            _primary_harvest_targets(
                resolved_for_primary,
                client=primary_client,
                cfg=cfg,
            ),
            [],
        )
    finally:
        if secondary_client and not keep_secondary_client:
            with suppress(Exception):
                _disconnect_worker_client(secondary_client)
            _cleanup_isolated_worker_session(secondary_cfg, secondary_worker_id)


def _resolve_harvest_target_entities(
    *,
    job_id: str,
    target: str,
    cfg: Any,
    primary_client: Any,
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> list[Any]:
    target_resolution = _resolve_harvest_targets(
        job_id=job_id,
        target=target,
        cfg=cfg,
        primary_client=primary_client,
        admin_job_append_log_fn=admin_job_append_log_fn,
    )
    try:
        return [target.entity for target in target_resolution.targets]
    finally:
        target_resolution.close()


def _read_existing_last_message_id(get_conn_fn: Callable[[], Any], chat_id: int) -> int:
    conn = get_conn_fn()
    try:
        return _get_last_message_id(conn, chat_id)
    finally:
        conn.close()


def _try_stream_new_chat_multi_account_ranges(
    *,
    job_id: str,
    target: str,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_append_log_fn: Callable[[str, str], Any],
    write_coordinator: ChatUpdateWriteCoordinator,
    primary_client: Any,
    entity: Any,
    entity_title: str,
    idx: int,
    total: int,
) -> bool:
    secondary_session_name = str(getattr(cfg, "secondary_session_name", "") or "").strip()
    if not secondary_session_name:
        return False

    primary_session_name = str(getattr(cfg, "session_name", "") or "").strip()
    if secondary_session_name == primary_session_name:
        admin_job_append_log_fn(
            job_id,
            f"[{idx}/{total}] 第二账号 session 与主账号相同，跳过双账号区间拉取",
        )
        return False

    try:
        min_message_id = int(getattr(cfg, "multi_account_min_message_id", 0) or 0)
        chunk_size = int(getattr(cfg, "multi_account_range_chunk_size", 0) or 0)
    except (TypeError, ValueError):
        return False
    if min_message_id <= 0 or chunk_size <= 0:
        return False

    try:
        chat_id = int(getattr(entity, "id", 0) or 0)
    except (TypeError, ValueError):
        return False
    if chat_id == 0:
        return False

    existing_last_id = _read_existing_last_message_id(get_conn_fn, chat_id)
    if existing_last_id > 0:
        return False

    with bind_client_event_loop(primary_client):
        primary_probe = probe_history_access(
            primary_client,
            entity,
            min_history_message_id=min_message_id,
            account_label="primary",
        )
    if not primary_probe.can_read_history:
        admin_job_append_log_fn(
            job_id,
            f"[{idx}/{total}] 主账号历史消息探测失败，回退单账号拉取：{primary_probe.reason}",
        )
        return False
    if primary_probe.latest_message_id < min_message_id:
        admin_job_append_log_fn(
            job_id,
            f"[{idx}/{total}] 最新消息 ID={primary_probe.latest_message_id} 未达到双账号阈值 {min_message_id}，使用单账号拉取",
        )
        return False

    secondary_cfg = _cfg_with_session_name(cfg, secondary_session_name)
    if not _ensure_base_session_valid(secondary_cfg, job_id, admin_job_append_log_fn):
        admin_job_append_log_fn(
            job_id,
            f"[{idx}/{total}] 第二账号会话不可用，回退单账号拉取",
        )
        return False

    secondary_client = None
    secondary_worker_id = f"{job_id}_secondary_{idx}"
    try:
        secondary_client = _create_isolated_worker_client(
            secondary_cfg,
            secondary_worker_id,
        )
        try:
            secondary_entity = _resolve_matching_entity_for_account(
                secondary_client,
                target,
                entity,
                cfg=cfg,
                account_label="secondary",
            )
        except AccountFloodWaitError as exc:
            _remember_account_cooldown(
                SimpleNamespace(key="secondary", label="第二账号", cfg=secondary_cfg),
                exc,
            )
            admin_job_append_log_fn(
                job_id,
                f"[{idx}/{total}] 第二账号解析目标触发长等待 wait={exc.seconds}s threshold={exc.threshold_seconds}s，回退主账号单账号拉取",
            )
            return False
        if secondary_entity is None:
            admin_job_append_log_fn(
                job_id,
                f"[{idx}/{total}] 第二账号无法解析到同一目标，回退单账号拉取",
            )
            return False

        try:
            with bind_client_event_loop(secondary_client):
                secondary_probe = probe_history_access(
                    secondary_client,
                    secondary_entity,
                    min_history_message_id=min_message_id,
                    account_label="secondary",
                )
        except AccountFloodWaitError as exc:
            _remember_account_cooldown(
                SimpleNamespace(key="secondary", label="第二账号", cfg=secondary_cfg),
                exc,
            )
            admin_job_append_log_fn(
                job_id,
                f"[{idx}/{total}] 第二账号历史消息探测触发长等待 wait={exc.seconds}s，回退单账号拉取",
            )
            return False
        if not secondary_probe.can_read_history:
            admin_job_append_log_fn(
                job_id,
                f"[{idx}/{total}] 第二账号历史消息探测失败，回退单账号拉取：{secondary_probe.reason}",
            )
            return False
        if secondary_probe.latest_message_id != primary_probe.latest_message_id:
            admin_job_append_log_fn(
                job_id,
                f"[{idx}/{total}] 两个账号最新消息 ID 不一致（主账号 {primary_probe.latest_message_id}，第二账号 {secondary_probe.latest_message_id}），回退单账号拉取",
            )
            return False

        admin_job_append_log_fn(
            job_id,
            f"[{idx}/{total}] 启用双账号区间拉取：最新消息 ID={primary_probe.latest_message_id}，区间大小={chunk_size}",
        )
        stream_entity_ranges_to_writer(
            job_id=job_id,
            write_coordinator=write_coordinator,
            accounts=[
                RangeHarvestAccount("primary", primary_client, entity),
                RangeHarvestAccount("secondary", secondary_client, secondary_entity),
            ],
            idx=idx,
            total=total,
            chat_id=chat_id,
            chat_title=entity_title,
            chat_username=getattr(entity, "username", None),
            chat_type=entity.__class__.__name__,
            latest_message_id=primary_probe.latest_message_id,
            chunk_size=chunk_size,
            skip_postprocess_if_unchanged=False,
            enable_dedupe=True,
        )
        return True
    finally:
        if secondary_client:
            with suppress(Exception):
                _disconnect_worker_client(secondary_client)
        _cleanup_isolated_worker_session(secondary_cfg, secondary_worker_id)


def _stream_new_chat_target(
    *,
    write_coordinator: ChatUpdateWriteCoordinator,
    get_conn_fn: Callable[[], Any],
    harvest_target: _HarvestTarget,
    entity_title: str,
    idx: int,
    total: int,
) -> None:
    stream_entity_harvest_to_writer(
        write_coordinator=write_coordinator,
        get_conn_fn=get_conn_fn,
        client=harvest_target.client,
        entity=harvest_target.entity,
        idx=idx,
        total=total,
        fallback_chat_title=entity_title,
        skip_postprocess_if_unchanged=False,
        enable_dedupe=True,
    )


def _resolve_new_chat_fallback_target(
    *,
    job_id: str,
    target: str,
    cfg: Any,
    primary_client: Any,
    failed_target: _HarvestTarget,
    idx: int,
    total: int,
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> _HarvestTargetResolution:
    if failed_target.account_key == "primary":
        secondary_session_name = str(
            getattr(cfg, "secondary_session_name", "") or ""
        ).strip()
        primary_session_name = str(getattr(cfg, "session_name", "") or "").strip()
        if not secondary_session_name or secondary_session_name == primary_session_name:
            raise RuntimeError("未配置可用第二账号，无法切换账号继续新增采集")

        secondary_cfg = _cfg_with_session_name(cfg, secondary_session_name)
        secondary_account = SimpleNamespace(
            key="secondary", label="第二账号", cfg=secondary_cfg
        )
        cooldown_remaining = _account_cooldown_remaining(secondary_account)
        if cooldown_remaining > 0:
            raise RuntimeError(
                f"第二账号仍处于 Telegram 长等待冷却，剩余约 {cooldown_remaining}s"
            )
        if not _ensure_base_session_valid(
            secondary_cfg, job_id, admin_job_append_log_fn
        ):
            raise RuntimeError("第二账号会话不可用，无法切换账号继续新增采集")

        secondary_client = None
        secondary_worker_id = f"{job_id}_secondary_fallback_{idx}"
        keep_secondary_client = False
        try:
            secondary_client = _create_isolated_worker_client(
                secondary_cfg,
                secondary_worker_id,
            )
            try:
                secondary_entity = _resolve_matching_entity_for_account(
                    secondary_client,
                    target,
                    failed_target.entity,
                    cfg=secondary_cfg,
                    account_label="secondary",
                )
            except AccountFloodWaitError as exc:
                _remember_account_cooldown(secondary_account, exc)
                raise
            if secondary_entity is None:
                raise RuntimeError("第二账号无法解析到同一目标")
            keep_secondary_client = True
            admin_job_append_log_fn(
                job_id,
                f"[{idx}/{total}] 第二账号已匹配同一新增目标，准备接管采集",
            )
            return _HarvestTargetResolution(
                [
                    _HarvestTarget(
                        entity=secondary_entity,
                        client=secondary_client,
                        cfg=secondary_cfg,
                        account_key="secondary",
                        account_label="第二账号",
                    )
                ],
                [(secondary_cfg, secondary_worker_id, secondary_client)],
            )
        finally:
            if secondary_client and not keep_secondary_client:
                with suppress(Exception):
                    _disconnect_worker_client(secondary_client)
                _cleanup_isolated_worker_session(secondary_cfg, secondary_worker_id)

    primary_account = SimpleNamespace(key="primary", label="主账号", cfg=cfg)
    cooldown_remaining = _account_cooldown_remaining(primary_account)
    if cooldown_remaining > 0:
        raise RuntimeError(
            f"主账号仍处于 Telegram 长等待冷却，剩余约 {cooldown_remaining}s"
        )
    try:
        primary_entity = _resolve_matching_entity_for_account(
            primary_client,
            target,
            failed_target.entity,
            cfg=cfg,
            account_label="primary",
        )
    except AccountFloodWaitError as exc:
        _remember_account_cooldown(primary_account, exc)
        raise
    if primary_entity is None:
        raise RuntimeError("主账号无法解析到同一目标")
    admin_job_append_log_fn(
        job_id,
        f"[{idx}/{total}] 主账号已匹配同一新增目标，准备接管采集",
    )
    return _HarvestTargetResolution(
        [
            _HarvestTarget(
                entity=primary_entity,
                client=primary_client,
                cfg=cfg,
                account_key="primary",
                account_label="主账号",
            )
        ],
        [],
    )


def _remember_new_chat_account_failure(
    account_key: str,
    account_label: str,
    cfg: Any,
    exc: Exception,
) -> None:
    if isinstance(exc, AccountFloodWaitError):
        _remember_account_cooldown(
            SimpleNamespace(key=account_key, label=account_label, cfg=cfg),
            exc,
        )


def _stream_new_chat_with_account_fallback(
    *,
    job_id: str,
    target: str,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_append_log_fn: Callable[[str, str], Any],
    write_coordinator: ChatUpdateWriteCoordinator,
    primary_client: Any,
    harvest_target: _HarvestTarget,
    entity_title: str,
    idx: int,
    total: int,
    skip_preferred: bool = False,
) -> None:
    first_exc: Exception | None = None
    if not skip_preferred:
        try:
            _stream_new_chat_target(
                write_coordinator=write_coordinator,
                get_conn_fn=get_conn_fn,
                harvest_target=harvest_target,
                entity_title=entity_title,
                idx=idx,
                total=total,
            )
            return
        except Exception as exc:
            first_exc = exc
            _remember_new_chat_account_failure(
                harvest_target.account_key,
                harvest_target.account_label,
                harvest_target.cfg,
                exc,
            )
            if isinstance(exc, AccountFloodWaitError):
                reason = f"触发长等待 wait={exc.seconds}s"
            else:
                reason = admin_error_message(exc)
            admin_job_append_log_fn(
                job_id,
                f"[{idx}/{total}] {harvest_target.account_label}新增采集失败，尝试切换另一账号："
                f"目标={entity_title}，错误={reason}",
            )

    fallback_resolution = None
    try:
        fallback_resolution = _resolve_new_chat_fallback_target(
            job_id=job_id,
            target=target,
            cfg=cfg,
            primary_client=primary_client,
            failed_target=harvest_target,
            idx=idx,
            total=total,
            admin_job_append_log_fn=admin_job_append_log_fn,
        )
        fallback_target = fallback_resolution.targets[0]
        admin_job_append_log_fn(
            job_id,
            f"[{idx}/{total}] 已切换账号继续新增采集：原账号={harvest_target.account_label}，"
            f"实际账号={fallback_target.account_label}，目标={entity_title}",
        )
        _stream_new_chat_target(
            write_coordinator=write_coordinator,
            get_conn_fn=get_conn_fn,
            harvest_target=fallback_target,
            entity_title=entity_title,
            idx=idx,
            total=total,
        )
    except Exception as fallback_exc:
        if fallback_resolution and fallback_resolution.targets:
            fallback_target = fallback_resolution.targets[0]
            _remember_new_chat_account_failure(
                fallback_target.account_key,
                fallback_target.account_label,
                fallback_target.cfg,
                fallback_exc,
            )
        if first_exc is None:
            raise
        raise RuntimeError(
            f"{harvest_target.account_label}采集失败：{admin_error_message(first_exc)}；"
            f"备用账号接管失败：{admin_error_message(fallback_exc)}"
        ) from fallback_exc
    finally:
        if fallback_resolution is not None:
            with suppress(Exception):
                fallback_resolution.close()


def _admin_harvest_job_runner(
    job_id: str,
    target: str,
    *,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_make_job_log_handler_fn: Callable[[str], logging.Handler],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    root_logger = logging.getLogger()
    job_log_handler = admin_make_job_log_handler_fn(job_id)
    root_logger.addHandler(job_log_handler)

    heartbeat_stop, heartbeat_thread = _start_job_heartbeat(job_id, _admin_job_heartbeat)
    local_client = None
    target_resolution: _HarvestTargetResolution | None = None
    worker_id = f"{job_id}_main"
    try:
        job_context.set(str(job_id))
        admin_job_set_status_fn(job_id, "running")
        admin_job_append_log_fn(job_id, f"开始新增数据采集：目标={target}")
        admin_job_append_log_fn(job_id, "正在验证 Telegram 会话...")

        if not _ensure_base_session_valid(cfg, job_id, admin_job_append_log_fn):
            admin_job_set_status_fn(job_id, "error")
            return

        admin_job_append_log_fn(job_id, "会话验证通过，正在建立 Telegram 连接...")
        local_client = _create_isolated_worker_client(cfg, worker_id)

        target_resolution = _resolve_harvest_targets(
            job_id=job_id,
            target=target,
            cfg=cfg,
            primary_client=local_client,
            admin_job_append_log_fn=admin_job_append_log_fn,
        )
        harvest_targets = target_resolution.targets

        if not harvest_targets:
            admin_job_append_log_fn(
                job_id,
                f"找不到对应目标：{target}。如果你输入的是标题，它可能不在已配置账号已有的群列表中，或另一账号无法通过公开标识确认同一会话。",
            )
            admin_job_set_status_fn(job_id, "error")
            return

        total = len(harvest_targets)
        admin_job_append_log_fn(job_id, f"目标解析成功：匹配到 {total} 个会话")
        admin_job_append_log_fn(job_id, "启用边抓取边写入：抓取与数据库写入并行执行")
        _admin_job_update_progress(
            job_id,
            0,
            total=total,
            stage="harvesting",
            log_step=0,
            auto_log=False,
        )
        write_coordinator = ChatUpdateWriteCoordinator(
            job_id=str(job_id),
            get_conn_fn=get_conn_fn,
            queue_maxsize=max(16, min(total, 4) * 4),
        )
        stream_failed = False
        try:
            for idx, harvest_target in enumerate(harvest_targets, start=1):
                entity = harvest_target.entity
                entity_title = (
                    getattr(entity, "title", None)
                    or getattr(entity, "username", None)
                    or str(target)
                )
                admin_job_append_log_fn(
                    job_id,
                    f"[{idx}/{total}] 导入目标：账号={harvest_target.account_label}，名称={entity_title}",
                )
                if harvest_target.account_key != "primary":
                    _stream_new_chat_with_account_fallback(
                        job_id=job_id,
                        target=target,
                        cfg=cfg,
                        get_conn_fn=get_conn_fn,
                        admin_job_append_log_fn=admin_job_append_log_fn,
                        write_coordinator=write_coordinator,
                        primary_client=local_client,
                        harvest_target=harvest_target,
                        entity_title=entity_title,
                        idx=idx,
                        total=total,
                    )
                    _admin_job_update_progress(
                        job_id,
                        idx,
                        total=total,
                        stage="harvesting",
                        log_step=0,
                        auto_log=False,
                    )
                    continue
                try:
                    used_multi_account_ranges = _try_stream_new_chat_multi_account_ranges(
                        job_id=job_id,
                        target=target,
                        cfg=harvest_target.cfg,
                        get_conn_fn=get_conn_fn,
                        admin_job_append_log_fn=admin_job_append_log_fn,
                        write_coordinator=write_coordinator,
                        primary_client=harvest_target.client,
                        entity=entity,
                        entity_title=entity_title,
                        idx=idx,
                        total=total,
                    )
                except AccountFloodWaitError as exc:
                    _remember_account_cooldown(
                        SimpleNamespace(key="primary", label="主账号", cfg=cfg),
                        exc,
                    )
                    admin_job_append_log_fn(
                        job_id,
                        f"[{idx}/{total}] 主账号历史探测触发长等待 wait={exc.seconds}s，切换第二账号单账号拉取",
                    )
                    _stream_new_chat_with_account_fallback(
                        job_id=job_id,
                        target=target,
                        cfg=harvest_target.cfg,
                        get_conn_fn=get_conn_fn,
                        admin_job_append_log_fn=admin_job_append_log_fn,
                        write_coordinator=write_coordinator,
                        primary_client=harvest_target.client,
                        harvest_target=harvest_target,
                        entity_title=entity_title,
                        idx=idx,
                        total=total,
                        skip_preferred=True,
                    )
                    used_multi_account_ranges = True
                if not used_multi_account_ranges:
                    _stream_new_chat_with_account_fallback(
                        job_id=job_id,
                        target=target,
                        cfg=harvest_target.cfg,
                        get_conn_fn=get_conn_fn,
                        admin_job_append_log_fn=admin_job_append_log_fn,
                        write_coordinator=write_coordinator,
                        primary_client=harvest_target.client,
                        harvest_target=harvest_target,
                        entity_title=entity_title,
                        idx=idx,
                        total=total,
                    )
                _admin_job_update_progress(
                    job_id,
                    idx,
                    total=total,
                    stage="harvesting",
                    log_step=0,
                    auto_log=False,
                )
        except Exception:
            stream_failed = True
            raise
        finally:
            if not stream_failed:
                _admin_job_update_progress(
                    job_id,
                    total,
                    total=total,
                    stage="finalizing",
                    log_step=0,
                    auto_log=False,
                )
            _close_write_coordinator(write_coordinator, suppress_errors=stream_failed)
        _admin_job_update_progress(
            job_id,
            total,
            total=total,
            stage="done",
            log_step=0,
            auto_log=False,
        )
        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        user_msg = admin_error_message(exc)
        admin_job_append_log_fn(job_id, f"新增数据采集失败：{user_msg}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        if target_resolution is not None:
            with suppress(Exception):
                target_resolution.close()
        if local_client:
            with suppress(Exception):
                _disconnect_worker_client(local_client)
        _cleanup_isolated_worker_session(cfg, worker_id)
        root_logger.removeHandler(job_log_handler)


def _admin_update_job_runner(
    job_id: str,
    chat_id: Any,
    chat_title: str,
    *,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_make_job_log_handler_fn: Callable[[str], logging.Handler],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    root_logger = logging.getLogger()
    job_log_handler = admin_make_job_log_handler_fn(job_id)
    root_logger.addHandler(job_log_handler)

    heartbeat_stop, heartbeat_thread = _start_job_heartbeat(job_id, _admin_job_heartbeat)
    local_client = None
    worker_id = f"{job_id}_main"
    try:
        job_context.set(str(job_id))
        admin_job_set_status_fn(job_id, "running")
        admin_job_append_log_fn(job_id, "正在验证 Telegram 会话...")

        is_all_scope = isinstance(chat_id, str) and chat_id.strip().lower() == "all"
        if not _ensure_base_session_valid(cfg, job_id, admin_job_append_log_fn):
            admin_job_set_status_fn(job_id, "error")
            return

        admin_job_append_log_fn(job_id, "会话验证通过，正在建立 Telegram 连接...")
        if is_all_scope:
            all_ok = _admin_update_all_chats(
                job_id, None, get_conn_fn, admin_job_append_log_fn, cfg
            )
            if not all_ok:
                admin_job_set_status_fn(job_id, "error")
                return
        else:
            chat_username = read_chat_username(get_conn_fn, int(chat_id))
            local_client = _create_isolated_worker_client(cfg, worker_id)
            _run_single_chat_update_with_account_fallback(
                job_id=job_id,
                cfg=cfg,
                primary_client=local_client,
                get_conn_fn=get_conn_fn,
                admin_job_append_log_fn=admin_job_append_log_fn,
                chat_id=int(chat_id),
                chat_title=chat_title,
                chat_username=chat_username,
            )
        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        user_msg = admin_error_message(exc)
        admin_job_append_log_fn(job_id, f"采集失败：{user_msg}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        if local_client:
            with suppress(Exception):
                _disconnect_worker_client(local_client)
        _cleanup_isolated_worker_session(cfg, worker_id)
        root_logger.removeHandler(job_log_handler)


def _admin_delete_job_runner(
    job_id: str,
    chat_id: int,
    chat_title: str,
    *,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    job_context.set(str(job_id))
    heartbeat_stop, heartbeat_thread = _start_job_heartbeat(
        job_id, _admin_job_heartbeat
    )
    conn = None
    try:
        admin_job_set_status_fn(job_id, "running")
        admin_job_append_log_fn(
            job_id, f"开始删除数据：目标={chat_title}，群组ID={chat_id}"
        )
        conn = get_conn_fn()
        related_counts = _count_chat_related_rows(conn, int(chat_id))
        admin_job_append_log_fn(
            job_id,
            "待删除数据："
            f"消息 {related_counts['messages']} 条，"
            f"媒体记录 {related_counts['media_rows']} 条，"
            f"媒体组 {related_counts['media_groups']} 个",
        )
        if related_counts["messages"] >= DELETE_CHAT_FAST_PATH_THRESHOLD:
            admin_job_append_log_fn(
                job_id,
                "大型群组启用快速删除模式：批量同步搜索索引，暂停逐条关联索引触发器",
            )
        admin_job_append_log_fn(job_id, "清理关联数据...")
        deleted_messages = _delete_chat_data(conn, int(chat_id))
        admin_job_append_log_fn(
            job_id, f"删除完成：共清除 {deleted_messages} 条消息"
        )
        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        if conn:
            conn.rollback()
        admin_job_append_log_fn(job_id, f"删除失败：{exc}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        if conn:
            conn.close()


def _admin_delete_empty_chats_job_runner(
    job_id: str,
    *,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    job_context.set(str(job_id))
    heartbeat_stop, heartbeat_thread = _start_job_heartbeat(
        job_id, _admin_job_heartbeat
    )
    conn = None
    try:
        admin_job_set_status_fn(job_id, "running")
        admin_job_append_log_fn(job_id, "开始删除零消息群组")
        conn = get_conn_fn()
        stats = _delete_empty_chats_data(conn)
        deleted_chats = int(stats.get("deleted_chats", 0) or 0)
        if deleted_chats <= 0:
            admin_job_append_log_fn(job_id, "未发现消息数量为 0 的可删除群组")
        else:
            admin_job_append_log_fn(
                job_id,
                "零消息群组删除完成："
                f"删除群组 {deleted_chats} 个，"
                f"清理残留消息 {int(stats.get('deleted_messages', 0) or 0)} 条，"
                f"媒体记录 {int(stats.get('deleted_media_rows', 0) or 0)} 条，"
                f"媒体组 {int(stats.get('deleted_media_groups', 0) or 0)} 个",
            )
        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        if conn:
            conn.rollback()
        admin_job_append_log_fn(job_id, f"删除零消息群组失败：{exc}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        if conn:
            conn.close()


def _admin_cleanup_job_runner(
    job_id: str,
    keyword: str,
    scope: str,
    chat_id: int | None,
    target_label: str,
    *,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
    cleanup_mode: str = "keyword",
) -> None:
    job_context.set(str(job_id))
    heartbeat_stop, heartbeat_thread = _start_job_heartbeat(job_id, _admin_job_heartbeat)
    conn = None
    try:
        admin_job_set_status_fn(job_id, "running")
        conn = get_conn_fn()
        cur = conn.cursor()

        # 修正：根据 scope 和 chat_id 构建过滤条件
        scope_filter_sql = ""
        scope_filter_params = []
        if scope == "chat" and chat_id is not None:
            scope_filter_sql = " AND m.chat_id = ?"
            scope_filter_params = [chat_id]

        like_pattern = _build_cleanup_like_patterns(keyword)

        # 安全检查：如果是关键词模式且关键词为空，则直接结束，防止 LIKE '%%' 扫描全库导致崩溃
        if cleanup_mode == "keyword" and not keyword.strip():
            admin_job_append_log_fn(job_id, "关键词不能为空，清理任务取消")
            admin_job_set_status_fn(job_id, "done")
            return

        if cleanup_mode == "empty_media":
            synced = backfill_message_search_text_from_filenames(
                conn,
                chat_id=chat_id if scope == "chat" else None,
                batch_size=5000,
                log_fn=lambda message: admin_job_append_log_fn(job_id, str(message)),
            )
            if synced > 0:
                admin_job_append_log_fn(
                    job_id,
                    f"已先补齐 {synced} 条可搜索文件名文本，用于保留可通过文件名搜索的数据",
                )

        target_count = _build_cleanup_targets_table(
            cur,
            cleanup_mode,
            scope_filter_sql,
            scope_filter_params,
            like_pattern,
        )
        conn.commit()

        if target_count > 0:
            admin_job_append_log_fn(
                job_id, f"检索到待清理数据：{target_count} 条，开始执行物理删除..."
            )
            actual_deleted = _execute_cleanup_deletion_batches(
                conn,
                cur,
                job_id,
                target_count,
                admin_job_append_log_fn,
            )
            admin_job_append_log_fn(job_id, f"清理完成：共删除 {actual_deleted} 条数据")
        else:
            admin_job_append_log_fn(job_id, "未发现符合条件的数据，任务结束")

        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        logging.exception(f"清理任务异常 (job_id: {job_id})")
        admin_job_append_log_fn(job_id, f"清理失败：{exc}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        if conn:
            conn.close()


def _admin_start_harvest_job_thread(job_id, target, **kwargs):
    return start_admin_job_thread(_admin_harvest_job_runner, job_id, target, **kwargs)


def _admin_start_update_job_thread(job_id, chat_id, chat_title, **kwargs):
    return start_admin_job_thread(
        _admin_update_job_runner, job_id, chat_id, chat_title, **kwargs
    )


def _optional_table_exists(cur: Any, table_name: str) -> bool:
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    return cur.fetchone() is not None


def _delete_from_optional_chat_table(cur: Any, table_name: str, chat_id: int) -> None:
    if not _optional_table_exists(cur, table_name):
        return
    cur.execute(f"DELETE FROM {table_name} WHERE chat_id = ?", (chat_id,))


def _delete_from_optional_chat_targets_table(
    cur: Any, table_name: str, target_table: str
) -> int:
    if not _optional_table_exists(cur, table_name):
        return 0
    cur.execute(
        f"""
        DELETE FROM {table_name}
        WHERE chat_id IN (
            SELECT chat_id
            FROM {target_table}
        )
        """
    )
    return int(cur.rowcount or 0)


def _delete_from_optional_message_pk_targets_table(
    cur: Any, table_name: str, target_table: str
) -> int:
    if not _optional_table_exists(cur, table_name):
        return 0
    cur.execute(
        f"""
        DELETE FROM {table_name}
        WHERE pk IN (
            SELECT pk
            FROM {target_table}
        )
        """
    )
    return int(cur.rowcount or 0)


def _prepare_delete_chat_message_targets(cur: Any, chat_id: int) -> int:
    cur.execute("DROP TABLE IF EXISTS temp_delete_chat_messages")
    cur.execute(
        """
        CREATE TEMP TABLE temp_delete_chat_messages (
            pk INTEGER PRIMARY KEY,
            message_id INTEGER NOT NULL
        )
        """
    )
    cur.execute(
        """
        INSERT INTO temp_delete_chat_messages(pk, message_id)
        SELECT pk, message_id
        FROM messages
        WHERE chat_id = ?
        """,
        (int(chat_id),),
    )
    cur.execute("SELECT COUNT(*) FROM temp_delete_chat_messages")
    return int(cur.fetchone()[0] or 0)


def _prepare_empty_chat_targets(cur: Any) -> int:
    cur.execute("DROP TABLE IF EXISTS temp_delete_empty_chats")
    cur.execute(
        """
        CREATE TEMP TABLE temp_delete_empty_chats (
            chat_id INTEGER PRIMARY KEY
        )
        """
    )
    cur.execute(
        """
        INSERT INTO temp_delete_empty_chats(chat_id)
        SELECT c.chat_id
        FROM chats c
        WHERE COALESCE(c.message_count, 0) = 0
          AND NOT EXISTS (
              SELECT 1
              FROM messages m
              WHERE m.chat_id = c.chat_id
          )
        """
    )
    cur.execute("SELECT COUNT(*) FROM temp_delete_empty_chats")
    return int(cur.fetchone()[0] or 0)


def _sqlite_object_exists(cur: Any, object_type: str, name: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = ? AND name = ?
        LIMIT 1
        """,
        (object_type, name),
    )
    return cur.fetchone() is not None


def _chat_delete_has_fts_index(cur: Any) -> bool:
    return _sqlite_object_exists(cur, "table", "messages_fts")


def _drop_message_delete_triggers_for_bulk_delete(
    cur: Any, *, fts_enabled: bool
) -> None:
    if fts_enabled:
        cur.execute("DROP TRIGGER IF EXISTS trg_messages_fts_delete")
    cur.execute("DROP TRIGGER IF EXISTS trg_message_terms_delete")


def _restore_message_delete_triggers_after_bulk_delete(
    cur: Any, *, fts_enabled: bool
) -> None:
    if fts_enabled:
        _fts._create_fts_triggers(cur)
    _search_terms._create_message_search_terms_queue_triggers(cur)


def _delete_fts_entries_for_chat_targets(cur: Any) -> None:
    cur.execute(
        """
        INSERT INTO messages_fts(messages_fts, rowid, content)
        SELECT
            'delete',
            m.pk,
            COALESCE(NULLIF(m.content_norm, ''), m.content, '')
        FROM messages m
        JOIN temp_delete_chat_messages t ON t.pk = m.pk
        """
    )


@synchronized_write
def _delete_chat_data(conn: Any, chat_id: int) -> int:
    cur = conn.cursor()
    fts_enabled = False
    triggers_dropped = False
    try:
        cur.execute("BEGIN IMMEDIATE")
        deleted_messages = _prepare_delete_chat_message_targets(cur, chat_id)
        trigger_optimization_required = (
            int(deleted_messages) >= DELETE_CHAT_FAST_PATH_THRESHOLD
        )
        fts_enabled = trigger_optimization_required and _chat_delete_has_fts_index(cur)
        if trigger_optimization_required:
            _drop_message_delete_triggers_for_bulk_delete(
                cur,
                fts_enabled=fts_enabled,
            )
            triggers_dropped = True
            if fts_enabled:
                _delete_fts_entries_for_chat_targets(cur)
        _delete_from_optional_chat_table(cur, "admin_absent_chats", chat_id)
        _delete_from_optional_chat_table(cur, "admin_missing_chats", chat_id)
        _delete_from_optional_chat_table(cur, "admin_restricted_chats", chat_id)
        _delete_from_optional_message_pk_targets_table(
            cur, "message_search_terms", "temp_delete_chat_messages"
        )
        _delete_from_optional_message_pk_targets_table(
            cur, "message_search_terms_rebuild_queue", "temp_delete_chat_messages"
        )
        cur.execute("DELETE FROM dedupe_actions WHERE chat_id = ?", (chat_id,))
        cur.execute("DELETE FROM dedupe_runs WHERE chat_id = ?", (chat_id,))
        cur.execute("DELETE FROM media_groups WHERE chat_id = ?", (chat_id,))
        cur.execute("DELETE FROM message_media WHERE chat_id = ?", (chat_id,))
        cur.execute("DELETE FROM messages WHERE chat_id = ?", (chat_id,))
        cur.execute("DELETE FROM chats WHERE chat_id = ?", (chat_id,))
        if triggers_dropped:
            _restore_message_delete_triggers_after_bulk_delete(
                cur,
                fts_enabled=fts_enabled,
            )
            triggers_dropped = False
        cur.execute("DROP TABLE IF EXISTS temp_delete_chat_messages")
        conn.commit()
        return deleted_messages
    except Exception:
        with suppress(Exception):
            conn.rollback()
        if triggers_dropped:
            with suppress(Exception):
                _restore_message_delete_triggers_after_bulk_delete(
                    cur,
                    fts_enabled=fts_enabled,
                )
                conn.commit()
        raise
    finally:
        with suppress(Exception):
            cur.execute("DROP TABLE IF EXISTS temp_delete_chat_messages")
        cur.close()


@synchronized_write
def _delete_empty_chats_data(conn: Any) -> dict[str, int]:
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        target_count = _prepare_empty_chat_targets(cur)
        if target_count <= 0:
            cur.execute("DROP TABLE IF EXISTS temp_delete_empty_chats")
            conn.commit()
            return {
                "deleted_chats": 0,
                "deleted_messages": 0,
                "deleted_media_rows": 0,
                "deleted_media_groups": 0,
            }

        _delete_from_optional_chat_targets_table(
            cur, "admin_absent_chats", "temp_delete_empty_chats"
        )
        _delete_from_optional_chat_targets_table(
            cur, "admin_missing_chats", "temp_delete_empty_chats"
        )
        _delete_from_optional_chat_targets_table(
            cur, "admin_restricted_chats", "temp_delete_empty_chats"
        )
        cur.execute(
            """
            DELETE FROM dedupe_actions
            WHERE chat_id IN (SELECT chat_id FROM temp_delete_empty_chats)
            """
        )
        cur.execute(
            """
            DELETE FROM dedupe_runs
            WHERE chat_id IN (SELECT chat_id FROM temp_delete_empty_chats)
            """
        )
        cur.execute(
            """
            DELETE FROM media_groups
            WHERE chat_id IN (SELECT chat_id FROM temp_delete_empty_chats)
            """
        )
        deleted_media_groups = int(cur.rowcount or 0)
        cur.execute(
            """
            DELETE FROM message_media
            WHERE chat_id IN (SELECT chat_id FROM temp_delete_empty_chats)
            """
        )
        deleted_media_rows = int(cur.rowcount or 0)
        cur.execute(
            """
            DELETE FROM messages
            WHERE chat_id IN (SELECT chat_id FROM temp_delete_empty_chats)
            """
        )
        deleted_messages = int(cur.rowcount or 0)
        cur.execute(
            """
            DELETE FROM chats
            WHERE chat_id IN (SELECT chat_id FROM temp_delete_empty_chats)
            """
        )
        deleted_chats = int(cur.rowcount or 0)
        cur.execute("DROP TABLE IF EXISTS temp_delete_empty_chats")
        conn.commit()
        return {
            "deleted_chats": deleted_chats,
            "deleted_messages": deleted_messages,
            "deleted_media_rows": deleted_media_rows,
            "deleted_media_groups": deleted_media_groups,
        }
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        with suppress(Exception):
            cur.execute("DROP TABLE IF EXISTS temp_delete_empty_chats")
        cur.close()


def _count_chat_related_rows(conn: Any, chat_id: int) -> dict[str, int]:
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM messages WHERE chat_id = ?", (int(chat_id),))
        messages = int(cur.fetchone()[0] or 0)
        cur.execute(
            "SELECT COUNT(*) FROM message_media WHERE chat_id = ?", (int(chat_id),)
        )
        media_rows = int(cur.fetchone()[0] or 0)
        cur.execute(
            "SELECT COUNT(*) FROM media_groups WHERE chat_id = ?", (int(chat_id),)
        )
        media_groups = int(cur.fetchone()[0] or 0)
        return {
            "messages": messages,
            "media_rows": media_rows,
            "media_groups": media_groups,
        }
    finally:
        cur.close()


def _admin_start_delete_job_thread(job_id, chat_id, chat_title, **kwargs):
    return start_admin_job_thread(
        _admin_delete_job_runner, job_id, chat_id, chat_title, **kwargs
    )


def _admin_start_delete_empty_chats_job_thread(job_id, **kwargs):
    return start_admin_job_thread(_admin_delete_empty_chats_job_runner, job_id, **kwargs)


def _admin_start_cleanup_job_thread(
    job_id, keyword, scope, chat_id, target_label, **kwargs
):
    return start_admin_job_thread(
        _admin_cleanup_job_runner,
        job_id,
        keyword,
        scope,
        chat_id,
        target_label,
        **kwargs,
    )


def _admin_start_cleanup_empty_job_thread(
    job_id, scope, chat_id, target_label, **kwargs
):
    # 修正：必须明确指定 cleanup_mode="empty_media"
    return start_admin_job_thread(
        _admin_cleanup_job_runner,
        job_id,
        "",
        scope,
        chat_id,
        target_label,
        **{**kwargs, "cleanup_mode": "empty_media"},
    )
