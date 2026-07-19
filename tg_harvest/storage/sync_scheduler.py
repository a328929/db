from __future__ import annotations

import json
import logging
import os
import socket
import sqlite3
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from tg_harvest.domain.coerce import clean_username, enabled_int, optional_int, safe_int
from tg_harvest.runtime.paths import runtime_dir
from tg_harvest.storage.connection import synchronized_write

UTC_TEXT_FORMAT = "%Y-%m-%d %H:%M:%S"
MODEL_KEY = "temporal_batch_predictor"
DEFAULT_MAX_NORMAL_DELAY_SECONDS = 30 * 60


class MembershipScope:
    UNKNOWN = "unknown"
    NONE_JOINED = "none_joined"
    BOTH_JOINED = "both_joined"
    SINGLE_JOINED_PRIMARY = "single_joined_primary"
    SINGLE_JOINED_SECONDARY = "single_joined_secondary"
    UNOBSERVABLE = "unobservable"


@dataclass(frozen=True)
class SyncObservation:
    chat_id: int
    chat_title: str = ""
    chat_username: str | None = None
    reason: str = "event"
    source_account: str = "primary"
    observed_at: str = ""


@dataclass(frozen=True)
class SyncDecision:
    due_at: str
    quiet_delay_seconds: int
    priority_score: float
    preferred_account: str
    source: str
    prediction: dict[str, Any]


@dataclass(frozen=True)
class SyncPendingTask:
    chat_id: int
    chat_title: str
    chat_username: str | None
    reason: str
    preferred_source_account: str
    source_accounts: str
    event_count: int
    generation: int
    in_flight_generation: int
    quiet_delay_seconds: int
    priority_score: float
    first_event_at: str
    last_event_at: str
    due_at: str
    in_flight_owner_instance_id: str = ""
    in_flight_owner_pid: int = 0
    in_flight_owner_host: str = ""


@dataclass(frozen=True)
class SyncUpdateResult:
    chat_id: int
    chat_title: str = ""
    chat_username: str | None = None
    source_account: str = ""
    added_message_count: int = 0
    scanned_message_count: int = 0
    local_last_id: int = 0
    remote_last_id: int = 0
    duration_seconds: float = 0.0
    api_cost: float = 0.0
    failure_type: str = ""
    failure_message: str = ""
    retry_after_seconds: int = 0


def utc_now_text() -> str:
    return datetime.now(UTC).replace(microsecond=0).strftime(UTC_TEXT_FORMAT)


def utc_text_from_seconds(seconds: float) -> str:
    return (
        datetime.fromtimestamp(float(seconds), tz=UTC)
        .replace(microsecond=0)
        .strftime(UTC_TEXT_FORMAT)
    )


def parse_utc_text(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, UTC_TEXT_FORMAT).replace(tzinfo=UTC)
    except ValueError:
        return None


def add_seconds_to_utc_text(value: str, seconds: int) -> str:
    base = parse_utc_text(value) or datetime.now(UTC)
    return (base + timedelta(seconds=max(0, int(seconds)))).strftime(UTC_TEXT_FORMAT)


def default_model_artifact_path() -> str:
    return str(runtime_dir() / "models" / "sync_predictor.pt")


def scheduler_enabled(cfg: Any) -> bool:
    if not hasattr(cfg, "sync_scheduler_enabled"):
        return False
    return enabled_int(getattr(cfg, "sync_scheduler_enabled", 1)) == 1


def ai_enabled(cfg: Any) -> bool:
    if not hasattr(cfg, "sync_ai_enabled"):
        return False
    return enabled_int(getattr(cfg, "sync_ai_enabled", 0)) == 1


def ai_shadow_enabled(cfg: Any) -> bool:
    if not hasattr(cfg, "sync_ai_shadow"):
        return True
    return enabled_int(getattr(cfg, "sync_ai_shadow", 1)) == 1


def ai_auto_promote_enabled(cfg: Any) -> bool:
    if not hasattr(cfg, "sync_ai_auto_promote_enabled"):
        return False
    return enabled_int(getattr(cfg, "sync_ai_auto_promote_enabled", 0)) == 1


def cfg_int(cfg: Any, name: str, default: int, *, minimum: int = 0) -> int:
    return max(int(minimum), int(getattr(cfg, name, default) or default))


def cfg_float(cfg: Any, name: str, default: float, *, minimum: float = 0.0) -> float:
    try:
        value = float(getattr(cfg, name, default) or default)
    except (TypeError, ValueError):
        value = default
    return max(float(minimum), value)


def _json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        return "{}"


def _split_csv(value: Any) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in str(value or "").replace("|", ",").split(","):
        key = item.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(key)
    return result


def _join_csv(values: Any) -> str:
    result: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        key = str(value or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(key)
    return ",".join(result)


def _row_int(row: Any, key: str, default: int = 0) -> int:
    if row is None:
        return default
    try:
        value = row[key]
    except Exception:
        value = default
    return int(optional_int(value) or default)


def _row_text(row: Any, key: str, default: str = "") -> str:
    if row is None:
        return default
    try:
        value = row[key]
    except Exception:
        value = default
    return str(value or default).strip()


def _row_to_dict(row: Any | None) -> dict[str, Any]:
    if row is None:
        return {}
    try:
        keys = row.keys()
    except Exception:
        keys = []
    return {str(key): row[key] for key in keys}


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'table' AND name = ?
            LIMIT 1
            """,
            (str(table_name or "").strip(),),
        )
        return cur.fetchone() is not None
    finally:
        cur.close()


def _rollback_write_transaction(conn: sqlite3.Connection, *, operation: str) -> None:
    """Preserve the original write failure while making rollback observable."""
    try:
        conn.rollback()
    except sqlite3.Error:
        logging.exception("同步调度事务回滚失败: operation=%s", operation)


def classify_membership_scope(
    *,
    chat_id: int,
    account_keys: list[str],
    joined_account_keys: list[str],
    cached_account_keys: list[str],
    chat_username: str | None,
) -> str:
    safe_chat_id = int(chat_id or 0)
    if safe_chat_id <= 0:
        return MembershipScope.UNKNOWN

    accounts = [str(key or "").strip() for key in account_keys if str(key or "").strip()]
    joined = {
        str(key or "").strip()
        for key in joined_account_keys
        if str(key or "").strip()
    }
    cached = {
        str(key or "").strip()
        for key in cached_account_keys
        if str(key or "").strip()
    }

    if joined:
        if len(accounts) >= 2 and all(key in joined for key in accounts[:2]):
            return MembershipScope.BOTH_JOINED
        if "primary" in joined:
            return MembershipScope.SINGLE_JOINED_PRIMARY
        if "secondary" in joined:
            return MembershipScope.SINGLE_JOINED_SECONDARY
        return MembershipScope.SINGLE_JOINED_PRIMARY

    if clean_username(chat_username) or cached:
        return MembershipScope.NONE_JOINED
    return MembershipScope.UNOBSERVABLE if accounts else MembershipScope.UNKNOWN


def _scope_base_priority(scope: str) -> float:
    return {
        MembershipScope.BOTH_JOINED: 140.0,
        MembershipScope.SINGLE_JOINED_PRIMARY: 122.0,
        MembershipScope.SINGLE_JOINED_SECONDARY: 118.0,
        MembershipScope.NONE_JOINED: 72.0,
        MembershipScope.UNOBSERVABLE: 0.0,
    }.get(str(scope or ""), 45.0)


def _scope_idle_status(scope: str) -> str:
    if str(scope or "") == MembershipScope.UNOBSERVABLE:
        return "unobservable"
    return "idle"


def _learning_event_dict(
    *,
    chat_id: int,
    event_type: str,
    reason: str = "",
    source_account: str = "",
    membership_scope: str = MembershipScope.UNKNOWN,
    status: str = "",
    features: dict[str, Any] | None = None,
    prediction: dict[str, Any] | None = None,
    outcome: dict[str, Any] | None = None,
    quiet_delay_seconds: int = 0,
    priority_score: float = 0.0,
    added_message_count: int = 0,
    wait_seconds: int = 0,
    api_cost: float = 0.0,
    failure_type: str = "",
    created_at: str | None = None,
) -> dict[str, Any]:
    return {
        "chat_id": int(chat_id),
        "event_type": str(event_type or "").strip(),
        "reason": str(reason or "").strip(),
        "source_account": str(source_account or "").strip(),
        "membership_scope": str(membership_scope or MembershipScope.UNKNOWN).strip(),
        "status": str(status or "").strip(),
        "features_json": _json_dumps(features or {}),
        "prediction_json": _json_dumps(prediction or {}),
        "outcome_json": _json_dumps(outcome or {}),
        "quiet_delay_seconds": max(0, int(quiet_delay_seconds or 0)),
        "priority_score": float(priority_score or 0.0),
        "added_message_count": max(0, int(added_message_count or 0)),
        "wait_seconds": max(0, int(wait_seconds or 0)),
        "api_cost": float(api_cost or 0.0),
        "failure_type": str(failure_type or "").strip(),
        "created_at": str(created_at or utc_now_text()),
    }


def _insert_learning_event_cur(cur: sqlite3.Cursor, item: dict[str, Any]) -> None:
    cur.execute(
        """
        INSERT INTO sync_learning_events(
            chat_id,
            event_type,
            reason,
            source_account,
            membership_scope,
            status,
            features_json,
            prediction_json,
            outcome_json,
            quiet_delay_seconds,
            priority_score,
            added_message_count,
            wait_seconds,
            api_cost,
            failure_type,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            item["chat_id"],
            item["event_type"],
            item["reason"],
            item["source_account"],
            item["membership_scope"],
            item["status"],
            item["features_json"],
            item["prediction_json"],
            item["outcome_json"],
            item["quiet_delay_seconds"],
            item["priority_score"],
            item["added_message_count"],
            item["wait_seconds"],
            item["api_cost"],
            item["failure_type"],
            item["created_at"],
        ),
    )


def build_heuristic_decision(
    conn: sqlite3.Connection,
    cfg: Any,
    observation: SyncObservation,
    *,
    now_text: str | None = None,
) -> SyncDecision:
    now = str(now_text or observation.observed_at or utc_now_text())
    min_delay = cfg_int(cfg, "sync_min_delay_seconds", 15, minimum=1)
    max_active_delay = cfg_int(
        cfg, "sync_max_active_delay_seconds", 10 * 60, minimum=min_delay
    )
    max_cold_delay = cfg_int(
        cfg, "sync_max_cold_delay_seconds", 2 * 60 * 60, minimum=max_active_delay
    )
    max_normal_delay = min(DEFAULT_MAX_NORMAL_DELAY_SECONDS, max_cold_delay)

    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                s.membership_scope,
                s.remote_last_id,
                s.local_last_id,
                s.failure_count,
                s.last_event_at,
                s.source_accounts,
                s.last_source_account,
                p.event_count
            FROM sync_chat_state s
            LEFT JOIN sync_pending_updates p ON p.chat_id = s.chat_id
            WHERE s.chat_id = ?
            LIMIT 1
            """,
            (int(observation.chat_id),),
        )
        row = cur.fetchone()
    finally:
        cur.close()

    membership_scope = _row_text(row, "membership_scope", MembershipScope.UNKNOWN)
    existing_event_count = _row_int(row, "event_count")
    event_count = existing_event_count + 1
    failure_count = _row_int(row, "failure_count")
    remote_last_id = _row_int(row, "remote_last_id")
    local_last_id = _row_int(row, "local_last_id")
    local_gap = max(0, remote_last_id - local_last_id)
    reason = str(observation.reason or "event").strip()
    is_probe = "probe" in reason
    last_event_dt = parse_utc_text(_row_text(row, "last_event_at"))
    now_dt = parse_utc_text(now) or datetime.now(UTC)
    last_event_age = (
        int((now_dt - last_event_dt).total_seconds()) if last_event_dt else None
    )
    hot_chat = event_count >= 5 or (last_event_age is not None and last_event_age <= 600)

    if membership_scope == MembershipScope.UNOBSERVABLE:
        quiet_delay = max_normal_delay
    elif is_probe:
        quiet_delay = min(max_active_delay, max(min_delay, 30))
    elif event_count >= 20:
        quiet_delay = min(max_active_delay, 120)
    elif event_count >= 5:
        quiet_delay = min(max_active_delay, 60)
    elif membership_scope == MembershipScope.NONE_JOINED:
        quiet_delay = min(max_normal_delay, 180)
    else:
        quiet_delay = min(max_active_delay if hot_chat else max_normal_delay, 30)

    quiet_delay = max(min_delay, min(int(quiet_delay), max_cold_delay))
    heuristic_delay = quiet_delay
    priority = _scope_base_priority(membership_scope)
    priority += min(event_count, 50) * 4.0
    priority += min(local_gap, 5000) / 100.0
    if is_probe:
        priority += 4.0
    priority -= min(failure_count, 10) * 8.0
    priority = max(0.0, round(priority, 3))
    preferred_account = str(observation.source_account or "").strip()
    if not preferred_account:
        preferred_account = _row_text(row, "last_source_account")
    if not preferred_account:
        source_accounts = _split_csv(_row_text(row, "source_accounts"))
        preferred_account = source_accounts[0] if source_accounts else "primary"

    due_at = add_seconds_to_utc_text(now, quiet_delay)
    prediction = {
        "kind": "heuristic",
        "membership_scope": membership_scope,
        "event_count": event_count,
        "local_gap": local_gap,
        "failure_count": failure_count,
        "hot_chat": hot_chat,
        "ai_enabled": ai_enabled(cfg),
        "ai_shadow": ai_shadow_enabled(cfg),
    }
    source = "heuristic"
    if ai_enabled(cfg):
        prediction["heuristic"] = {
            "quiet_delay_seconds": quiet_delay,
            "priority_score": priority,
            "due_at": due_at,
        }
        try:
            from tg_harvest.ml.sync_predictor import predict_sync_decision

            suggestion = predict_sync_decision(
                conn,
                cfg,
                chat_id=int(observation.chat_id),
                now_text=now,
                observation_reason=reason,
                source_account=preferred_account,
                heuristic_delay_seconds=quiet_delay,
                heuristic_priority_score=priority,
                heuristic_context=prediction,
            )
            model_prediction = suggestion.to_prediction_dict()
            prediction["model"] = model_prediction
            if suggestion.available:
                source = "heuristic_with_model_shadow"
                can_take_over = (
                    suggestion.active
                    and not ai_shadow_enabled(cfg)
                    and enabled_int(getattr(cfg, "sync_ai_auto_promote_enabled", 0)) == 1
                )
                prediction["model_can_take_over"] = bool(can_take_over)
                if can_take_over:
                    max_factor = cfg_float(
                        cfg,
                        "sync_model_max_active_delay_factor",
                        2.0,
                        minimum=1.0,
                    )
                    lower_bound = max(
                        min_delay,
                        int(max(1, round(float(heuristic_delay) * 0.5))),
                    )
                    upper_bound = min(
                        max_cold_delay,
                        int(max(heuristic_delay, round(float(heuristic_delay) * max_factor))),
                    )
                    model_delay = int(suggestion.quiet_delay_seconds)
                    if float(model_prediction.get("risk_score") or 0.0) >= 0.55:
                        lower_bound = max(lower_bound, heuristic_delay)
                    quiet_delay = max(lower_bound, min(model_delay, upper_bound))
                    priority = max(0.0, round(float(suggestion.priority_score), 3))
                    due_at = add_seconds_to_utc_text(now, quiet_delay)
                    source = "torch_model_active"
                    prediction["kind"] = "torch_model"
                    prediction["active_model"] = {
                        "raw_quiet_delay_seconds": model_delay,
                        "quiet_delay_seconds": quiet_delay,
                        "priority_score": priority,
                        "due_at": due_at,
                        "lower_bound_seconds": lower_bound,
                        "upper_bound_seconds": upper_bound,
                        "heuristic_delay_seconds": heuristic_delay,
                    }
                elif suggestion.active:
                    prediction["model_takeover_blocked_reason"] = (
                        "shadow_enabled" if ai_shadow_enabled(cfg) else "auto_promote_disabled"
                    )
            else:
                source = (
                    "heuristic_with_model_shadow"
                    if ai_shadow_enabled(cfg)
                    else "heuristic_ai_fallback"
                )
                prediction["ai_reason"] = suggestion.reason
        except Exception as exc:
            logging_message = f"{type(exc).__name__}: {exc}"
            source = (
                "heuristic_with_model_shadow"
                if ai_shadow_enabled(cfg)
                else "heuristic_ai_fallback"
            )
            prediction["ai_reason"] = "model_prediction_failed"
            prediction["model_error"] = logging_message

    return SyncDecision(
        due_at=due_at,
        quiet_delay_seconds=quiet_delay,
        priority_score=priority,
        preferred_account=preferred_account,
        source=source,
        prediction=prediction,
    )


@synchronized_write
def refresh_chat_states(
    conn: sqlite3.Connection,
    *,
    chat_rows: list[dict[str, Any]],
    joined_by_account: dict[str, set[int]],
    cached_by_account: dict[str, set[int]],
    account_keys: list[str],
    now_text: str | None = None,
) -> dict[str, Any]:
    now = str(now_text or utc_now_text())
    cur = conn.cursor()
    active_chat_ids = sorted(
        {
            int(row["chat_id"])
            for row in chat_rows
            if int(row["chat_id"]) > 0
        }
    )
    try:
        cur.execute("BEGIN IMMEDIATE")
        for row in chat_rows:
            chat_id = int(row["chat_id"])
            if chat_id <= 0:
                continue
            chat_username = clean_username(row.get("chat_username"))
            joined_account_keys = [
                account_key
                for account_key, joined_chat_ids in joined_by_account.items()
                if chat_id in joined_chat_ids
            ]
            cached_account_keys = [
                account_key
                for account_key, cached_chat_ids in cached_by_account.items()
                if chat_id in cached_chat_ids
            ]
            scope = classify_membership_scope(
                chat_id=chat_id,
                account_keys=account_keys,
                joined_account_keys=joined_account_keys,
                cached_account_keys=cached_account_keys,
                chat_username=chat_username,
            )
            source_accounts = _join_csv(joined_account_keys or cached_account_keys)
            priority_score = _scope_base_priority(scope)
            initial_next_probe_at = now if scope in {
                MembershipScope.NONE_JOINED,
                MembershipScope.UNOBSERVABLE,
            } else ""
            cur.execute(
                """
                INSERT INTO sync_chat_state(
                    chat_id,
                    chat_title,
                    chat_username,
                    membership_scope,
                    status,
                    local_last_id,
                    priority_score,
                    source_accounts,
                    next_probe_at,
                    is_active,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    chat_title = excluded.chat_title,
                    chat_username = excluded.chat_username,
                    membership_scope = excluded.membership_scope,
                    status = CASE
                        WHEN COALESCE(sync_chat_state.status, '') IN ('pending', 'updating', 'backoff')
                            THEN sync_chat_state.status
                        ELSE excluded.status
                    END,
                    local_last_id = excluded.local_last_id,
                    priority_score = CASE
                        WHEN COALESCE(sync_chat_state.status, '') IN ('pending', 'updating')
                            THEN sync_chat_state.priority_score
                        ELSE excluded.priority_score
                    END,
                    source_accounts = excluded.source_accounts,
                    next_probe_at = CASE
                        WHEN COALESCE(sync_chat_state.next_probe_at, '') = ''
                             AND excluded.next_probe_at <> ''
                            THEN excluded.next_probe_at
                        ELSE sync_chat_state.next_probe_at
                    END,
                    is_active = 1,
                    updated_at = excluded.updated_at
                WHERE sync_chat_state.chat_title IS NOT excluded.chat_title
                   OR sync_chat_state.chat_username IS NOT excluded.chat_username
                   OR sync_chat_state.membership_scope IS NOT excluded.membership_scope
                   OR (
                        COALESCE(sync_chat_state.status, '') NOT IN ('pending', 'updating', 'backoff')
                        AND sync_chat_state.status IS NOT excluded.status
                   )
                   OR sync_chat_state.local_last_id IS NOT excluded.local_last_id
                   OR (
                        COALESCE(sync_chat_state.status, '') NOT IN ('pending', 'updating')
                        AND sync_chat_state.priority_score IS NOT excluded.priority_score
                   )
                   OR sync_chat_state.source_accounts IS NOT excluded.source_accounts
                   OR (
                        COALESCE(sync_chat_state.next_probe_at, '') = ''
                        AND excluded.next_probe_at <> ''
                   )
                   OR sync_chat_state.is_active IS NOT 1
                """,
                (
                    chat_id,
                    str(row.get("chat_title") or "").strip() or f"Chat {chat_id}",
                    chat_username,
                    scope,
                    _scope_idle_status(scope),
                    safe_int(row.get("last_message_id"), 0),
                    priority_score,
                    source_accounts,
                    initial_next_probe_at,
                    now,
                    now,
                ),
            )

        if active_chat_ids:
            cur.execute("DROP TABLE IF EXISTS temp_sync_active_chat_ids")
            cur.execute(
                "CREATE TEMP TABLE temp_sync_active_chat_ids "
                "(chat_id INTEGER PRIMARY KEY)"
            )
            cur.executemany(
                "INSERT INTO temp_sync_active_chat_ids(chat_id) VALUES (?)",
                [(chat_id,) for chat_id in active_chat_ids],
            )
            cur.execute(
                """
                UPDATE sync_chat_state
                SET
                    status = 'deleted',
                    is_active = 0,
                    priority_score = 0,
                    next_probe_at = '',
                    next_update_at = '',
                    updated_at = ?
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM temp_sync_active_chat_ids active
                    WHERE active.chat_id = sync_chat_state.chat_id
                )
                  AND (
                        status IS NOT 'deleted'
                     OR is_active IS NOT 0
                     OR priority_score IS NOT 0
                     OR next_probe_at IS NOT ''
                     OR next_update_at IS NOT ''
                  )
                """,
                (now,),
            )
            cur.execute(
                """
                DELETE FROM sync_pending_updates
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM temp_sync_active_chat_ids active
                    WHERE active.chat_id = sync_pending_updates.chat_id
                )
                """,
            )
            cur.execute("DROP TABLE IF EXISTS temp_sync_active_chat_ids")
        else:
            cur.execute(
                """
                UPDATE sync_chat_state
                SET
                    status = 'deleted',
                    is_active = 0,
                    priority_score = 0,
                    next_probe_at = '',
                    next_update_at = '',
                    updated_at = ?
                WHERE status IS NOT 'deleted'
                   OR is_active IS NOT 0
                   OR priority_score IS NOT 0
                   OR next_probe_at IS NOT ''
                   OR next_update_at IS NOT ''
                """,
                (now,),
            )
            cur.execute("DELETE FROM sync_pending_updates")
        conn.commit()
        return {"active_chat_count": len(active_chat_ids), "updated_at": now}
    except sqlite3.Error:
        _rollback_write_transaction(conn, operation="refresh_chat_states")
        logging.exception("刷新同步调度群组状态失败，事务已回滚")
        raise
    except Exception:
        _rollback_write_transaction(conn, operation="refresh_chat_states")
        logging.exception("刷新同步调度群组状态发生未知错误，事务已回滚")
        raise
    finally:
        with suppress(Exception):
            cur.execute("DROP TABLE IF EXISTS temp_sync_active_chat_ids")
        cur.close()


@synchronized_write
def enqueue_observation(
    conn: sqlite3.Connection,
    *,
    cfg: Any,
    observation: SyncObservation,
) -> SyncDecision:
    observed_at = str(observation.observed_at or utc_now_text())
    safe_observation = SyncObservation(
        chat_id=int(observation.chat_id),
        chat_title=str(observation.chat_title or "").strip()
        or f"Chat {int(observation.chat_id)}",
        chat_username=clean_username(observation.chat_username),
        reason=str(observation.reason or "event").strip() or "event",
        source_account=str(observation.source_account or "").strip() or "primary",
        observed_at=observed_at,
    )
    decision = build_heuristic_decision(conn, cfg, safe_observation, now_text=observed_at)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            """
            SELECT *
            FROM sync_pending_updates
            WHERE chat_id = ?
            LIMIT 1
            """,
            (safe_observation.chat_id,),
        )
        existing = cur.fetchone()
        generation = _row_int(existing, "generation") + 1
        event_count = _row_int(existing, "event_count") + 1
        first_event_at = _row_text(existing, "first_event_at") or observed_at
        source_accounts = _join_csv(
            [
                *_split_csv(_row_text(existing, "source_accounts")),
                safe_observation.source_account,
            ]
        )
        reasons = _join_csv(
            [*_split_csv(_row_text(existing, "reasons")), safe_observation.reason]
        )
        in_flight = _row_int(existing, "in_flight")
        in_flight_generation = _row_int(existing, "in_flight_generation")
        dirty_generation = generation if in_flight else _row_int(existing, "dirty_generation")
        cur.execute(
            """
            INSERT INTO sync_pending_updates(
                chat_id,
                chat_title,
                chat_username,
                first_event_at,
                last_event_at,
                event_count,
                source_accounts,
                reasons,
                preferred_source_account,
                due_at,
                priority_score,
                quiet_delay_seconds,
                generation,
                in_flight,
                in_flight_generation,
                dirty_generation,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                chat_title = excluded.chat_title,
                chat_username = excluded.chat_username,
                last_event_at = excluded.last_event_at,
                event_count = excluded.event_count,
                source_accounts = excluded.source_accounts,
                reasons = excluded.reasons,
                preferred_source_account = excluded.preferred_source_account,
                due_at = excluded.due_at,
                priority_score = excluded.priority_score,
                quiet_delay_seconds = excluded.quiet_delay_seconds,
                generation = excluded.generation,
                dirty_generation = excluded.dirty_generation,
                updated_at = excluded.updated_at
            """,
            (
                safe_observation.chat_id,
                safe_observation.chat_title,
                safe_observation.chat_username,
                first_event_at,
                observed_at,
                event_count,
                source_accounts,
                reasons,
                decision.preferred_account,
                decision.due_at,
                decision.priority_score,
                decision.quiet_delay_seconds,
                generation,
                in_flight,
                in_flight_generation,
                dirty_generation,
                observed_at,
                observed_at,
            ),
        )
        cur.execute(
            """
            UPDATE sync_chat_state
            SET
                chat_title = ?,
                chat_username = ?,
                status = CASE WHEN ? = 1 THEN 'updating' ELSE 'pending' END,
                last_event_at = ?,
                last_event_reason = ?,
                next_update_at = ?,
                model_delay_seconds = ?,
                priority_score = ?,
                source_accounts = ?,
                last_source_account = ?,
                is_active = 1,
                updated_at = ?
            WHERE chat_id = ?
            """,
            (
                safe_observation.chat_title,
                safe_observation.chat_username,
                in_flight,
                observed_at,
                safe_observation.reason,
                decision.due_at,
                decision.quiet_delay_seconds,
                decision.priority_score,
                source_accounts,
                safe_observation.source_account,
                observed_at,
                safe_observation.chat_id,
            ),
        )
        if cur.rowcount <= 0:
            cur.execute(
                """
                INSERT INTO sync_chat_state(
                    chat_id,
                    chat_title,
                    chat_username,
                    membership_scope,
                    status,
                    last_event_at,
                    last_event_reason,
                    next_update_at,
                    model_delay_seconds,
                    priority_score,
                    source_accounts,
                    last_source_account,
                    is_active,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, 'unknown', ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (
                    safe_observation.chat_id,
                    safe_observation.chat_title,
                    safe_observation.chat_username,
                    "updating" if in_flight else "pending",
                    observed_at,
                    safe_observation.reason,
                    decision.due_at,
                    decision.quiet_delay_seconds,
                    decision.priority_score,
                    source_accounts,
                    safe_observation.source_account,
                    observed_at,
                    observed_at,
                ),
            )
        _insert_learning_event_cur(
            cur,
            _learning_event_dict(
                chat_id=safe_observation.chat_id,
                event_type="observation",
                reason=safe_observation.reason,
                source_account=safe_observation.source_account,
                membership_scope=str(decision.prediction.get("membership_scope") or ""),
                status="pending",
                features={
                    "event_count": event_count,
                    "generation": generation,
                    "in_flight": bool(in_flight),
                },
                prediction=decision.prediction
                | {
                    "due_at": decision.due_at,
                    "source": decision.source,
                },
                quiet_delay_seconds=decision.quiet_delay_seconds,
                priority_score=decision.priority_score,
                created_at=observed_at,
            ),
        )
        conn.commit()
        return decision
    except sqlite3.Error:
        _rollback_write_transaction(conn, operation="enqueue_observation")
        logging.exception(
            "同步调度 observation 写入失败，事件未入队: chat_id=%s",
            safe_observation.chat_id,
        )
        raise
    except Exception:
        _rollback_write_transaction(conn, operation="enqueue_observation")
        logging.exception(
            "同步调度 observation 写入发生未知错误，事件未入队: chat_id=%s",
            safe_observation.chat_id,
        )
        raise
    finally:
        cur.close()


@synchronized_write
def claim_due_pending_updates(
    conn: sqlite3.Connection,
    *,
    now_text: str | None = None,
    limit: int = 1,
    exclude_preferred_accounts: set[str] | list[str] | tuple[str, ...] | None = None,
    owner_instance_id: str = "",
    owner_pid: int = 0,
    owner_host: str = "",
) -> list[SyncPendingTask]:
    now = str(now_text or utc_now_text())
    effective_limit = max(1, int(limit or 1))
    normalized_owner_instance_id = str(owner_instance_id or "").strip()
    normalized_owner_host = str(owner_host or "").strip()
    try:
        normalized_owner_pid = max(0, int(owner_pid or 0))
    except (TypeError, ValueError):
        normalized_owner_pid = 0
    excluded = [
        str(item or "").strip()
        for item in (exclude_preferred_accounts or [])
        if str(item or "").strip()
    ]
    exclusion_sql = ""
    params: list[Any] = [now]
    if excluded:
        placeholders = ",".join(["?"] * len(excluded))
        exclusion_sql = (
            "AND COALESCE(NULLIF(p.preferred_source_account, ''), 'primary') "
            f"NOT IN ({placeholders})"
        )
        params.extend(excluded)
    params.append(effective_limit)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            f"""
            SELECT p.*
            FROM sync_pending_updates p
            LEFT JOIN sync_chat_state s ON s.chat_id = p.chat_id
            WHERE p.in_flight = 0
              AND p.due_at <> ''
              AND p.due_at <= ?
              AND COALESCE(s.is_active, 1) = 1
              {exclusion_sql}
            ORDER BY p.due_at ASC, p.priority_score DESC, p.chat_id ASC
            LIMIT ?
            """,
            params,
        )
        rows = cur.fetchall()
        tasks: list[SyncPendingTask] = []
        for row in rows:
            chat_id = int(row["chat_id"])
            generation = _row_int(row, "generation")
            cur.execute(
                """
                UPDATE sync_pending_updates
                SET
                    in_flight = 1,
                    in_flight_generation = ?,
                    in_flight_owner_instance_id = ?,
                    in_flight_owner_pid = ?,
                    in_flight_owner_host = ?,
                    updated_at = ?
                WHERE chat_id = ?
                  AND in_flight = 0
                  AND generation = ?
                """,
                (
                    generation,
                    normalized_owner_instance_id,
                    normalized_owner_pid,
                    normalized_owner_host,
                    now,
                    chat_id,
                    generation,
                ),
            )
            if cur.rowcount <= 0:
                continue
            cur.execute(
                """
                UPDATE sync_chat_state
                SET status = 'updating', updated_at = ?
                WHERE chat_id = ?
                """,
                (now, chat_id),
            )
            tasks.append(
                SyncPendingTask(
                    chat_id=chat_id,
                    chat_title=str(row["chat_title"] or "") or f"Chat {chat_id}",
                    chat_username=clean_username(row["chat_username"]),
                    reason=str(row["reasons"] or "event"),
                    preferred_source_account=str(row["preferred_source_account"] or ""),
                    source_accounts=str(row["source_accounts"] or ""),
                    event_count=_row_int(row, "event_count"),
                    generation=generation,
                    in_flight_generation=generation,
                    quiet_delay_seconds=_row_int(row, "quiet_delay_seconds"),
                    priority_score=float(row["priority_score"] or 0.0),
                    first_event_at=str(row["first_event_at"] or ""),
                    last_event_at=str(row["last_event_at"] or ""),
                    due_at=str(row["due_at"] or ""),
                    in_flight_owner_instance_id=normalized_owner_instance_id,
                    in_flight_owner_pid=normalized_owner_pid,
                    in_flight_owner_host=normalized_owner_host,
                )
            )
        conn.commit()
        return tasks
    except sqlite3.Error:
        _rollback_write_transaction(conn, operation="claim_due_pending_updates")
        logging.exception("领取同步调度任务失败，未确认任何新租约")
        raise
    except Exception:
        _rollback_write_transaction(conn, operation="claim_due_pending_updates")
        logging.exception("领取同步调度任务发生未知错误，未确认任何新租约")
        raise
    finally:
        cur.close()


def _recovered_pending_due_at(due_at: str, now: str) -> str:
    due_dt = parse_utc_text(due_at)
    now_dt = parse_utc_text(now)
    if due_dt is not None and now_dt is not None and due_dt > now_dt:
        return due_at
    return now


def _process_is_alive(pid: int) -> bool:
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _in_flight_owner_can_be_recovered(row: Any, *, local_host: str) -> bool:
    """Only reclaim legacy work or a task whose local owner is known dead."""
    owner_instance_id = _row_text(row, "in_flight_owner_instance_id")
    owner_pid = _row_int(row, "in_flight_owner_pid")
    owner_host = _row_text(row, "in_flight_owner_host")
    if not owner_instance_id and owner_pid <= 0 and not owner_host:
        return True
    if owner_host and owner_host != local_host:
        return False
    if owner_pid <= 0:
        return False
    return not _process_is_alive(owner_pid)


@synchronized_write
def recover_in_flight_pending_updates(
    conn: sqlite3.Connection,
    *,
    now_text: str | None = None,
    local_host: str | None = None,
) -> int:
    """Release only work whose owner is known to have exited."""
    has_pending_updates = _table_exists(conn, "sync_pending_updates")
    has_account_runtime_state = _table_exists(conn, "account_runtime_state")
    if not has_pending_updates and not has_account_runtime_state:
        return 0

    now = str(now_text or utc_now_text())
    effective_local_host = str(local_host or socket.gethostname()).strip()
    has_chat_state = _table_exists(conn, "sync_chat_state")
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        recovered = 0
        has_unrecovered_in_flight = False
        if has_pending_updates:
            cur.execute(
                """
                SELECT
                    chat_id,
                    due_at,
                    in_flight_owner_instance_id,
                    in_flight_owner_pid,
                    in_flight_owner_host
                FROM sync_pending_updates
                WHERE in_flight <> 0
                ORDER BY chat_id ASC
                """,
            )
            rows = cur.fetchall()
            for row in rows:
                if not _in_flight_owner_can_be_recovered(
                    row, local_host=effective_local_host
                ):
                    has_unrecovered_in_flight = True
                    continue
                chat_id = int(row["chat_id"])
                due_at = _recovered_pending_due_at(_row_text(row, "due_at"), now)
                cur.execute(
                    """
                    UPDATE sync_pending_updates
                    SET
                        in_flight = 0,
                        in_flight_generation = 0,
                        in_flight_owner_instance_id = '',
                        in_flight_owner_pid = 0,
                        in_flight_owner_host = '',
                        due_at = ?,
                        updated_at = ?
                    WHERE chat_id = ?
                      AND in_flight <> 0
                    """,
                    (due_at, now, chat_id),
                )
                if cur.rowcount <= 0:
                    continue
                recovered += 1
                if has_chat_state:
                    cur.execute(
                        """
                        UPDATE sync_chat_state
                        SET
                            status = 'pending',
                            next_update_at = ?,
                            updated_at = ?
                        WHERE chat_id = ?
                          AND is_active = 1
                        """,
                        (due_at, now, chat_id),
                    )
        if has_account_runtime_state and recovered > 0 and not has_unrecovered_in_flight:
            cur.execute(
                """
                UPDATE account_runtime_state
                SET in_flight_count = 0, updated_at = ?
                WHERE in_flight_count <> 0
                """,
                (now,),
            )
        conn.commit()
        return recovered
    except sqlite3.Error:
        _rollback_write_transaction(conn, operation="recover_in_flight_pending_updates")
        logging.exception("恢复中断的同步调度任务失败，未释放任何部分租约")
        raise
    except Exception:
        _rollback_write_transaction(conn, operation="recover_in_flight_pending_updates")
        logging.exception("恢复中断的同步调度任务发生未知错误，未释放任何部分租约")
        raise
    finally:
        cur.close()


def _wait_seconds(task: SyncPendingTask, now_text: str) -> int:
    first_dt = parse_utc_text(task.first_event_at)
    now_dt = parse_utc_text(now_text)
    if first_dt is None or now_dt is None:
        return 0
    return max(0, int((now_dt - first_dt).total_seconds()))


def _pending_has_new_generation(row: Any, task: SyncPendingTask) -> bool:
    if row is None:
        return False
    return max(
        _row_int(row, "generation"),
        _row_int(row, "dirty_generation"),
    ) > int(task.in_flight_generation)


def _task_owns_in_flight_pending_update(
    row: Any, task: SyncPendingTask
) -> bool:
    if row is None or _row_int(row, "in_flight") == 0:
        return False
    if _row_int(row, "in_flight_generation") != int(task.in_flight_generation):
        return False
    expected_instance_id = str(task.in_flight_owner_instance_id or "").strip()
    if not expected_instance_id:
        return True
    return (
        _row_text(row, "in_flight_owner_instance_id") == expected_instance_id
        and _row_int(row, "in_flight_owner_pid")
        == int(task.in_flight_owner_pid or 0)
        and _row_text(row, "in_flight_owner_host")
        == str(task.in_flight_owner_host or "").strip()
    )


@synchronized_write
def complete_pending_update(
    conn: sqlite3.Connection,
    *,
    task: SyncPendingTask,
    result: SyncUpdateResult,
    now_text: str | None = None,
) -> None:
    now = str(now_text or utc_now_text())
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            "SELECT * FROM sync_pending_updates WHERE chat_id = ? LIMIT 1",
            (int(task.chat_id),),
        )
        pending_row = cur.fetchone()
        if not _task_owns_in_flight_pending_update(pending_row, task):
            conn.commit()
            return
        cur.execute(
            "SELECT * FROM sync_chat_state WHERE chat_id = ? LIMIT 1",
            (int(task.chat_id),),
        )
        state_row = cur.fetchone()
        has_new_generation = _pending_has_new_generation(pending_row, task)
        status = "pending" if has_new_generation else "idle"
        effective_local_last_id = max(0, int(result.local_last_id or 0))
        effective_remote_last_id = max(
            0,
            int(result.remote_last_id or 0),
            effective_local_last_id,
        )
        if has_new_generation:
            due_at = _row_text(pending_row, "due_at")
            if not due_at or due_at <= now:
                due_at = add_seconds_to_utc_text(
                    now,
                    max(1, _row_int(pending_row, "quiet_delay_seconds", task.quiet_delay_seconds)),
                )
            cur.execute(
                """
                UPDATE sync_pending_updates
                SET
                    in_flight = 0,
                    in_flight_generation = 0,
                    in_flight_owner_instance_id = '',
                    in_flight_owner_pid = 0,
                    in_flight_owner_host = '',
                    dirty_generation = 0,
                    due_at = ?,
                    updated_at = ?
                WHERE chat_id = ?
                """,
                (due_at, now, int(task.chat_id)),
            )
        else:
            cur.execute(
                "DELETE FROM sync_pending_updates WHERE chat_id = ?",
                (int(task.chat_id),),
            )
        cur.execute(
            """
            UPDATE sync_chat_state
            SET
                status = ?,
                last_update_at = ?,
                last_success_at = ?,
                last_failure_message = '',
                failure_count = 0,
                local_last_id = MAX(local_last_id, ?),
                remote_last_id = MAX(remote_last_id, ?),
                next_update_at = CASE WHEN ? = 'pending' THEN next_update_at ELSE '' END,
                last_source_account = ?,
                updated_at = ?
            WHERE chat_id = ?
            """,
            (
                status,
                now,
                now,
                effective_local_last_id,
                effective_remote_last_id,
                status,
                str(result.source_account or ""),
                now,
                int(task.chat_id),
            ),
        )
        _insert_learning_event_cur(
            cur,
            _learning_event_dict(
                chat_id=task.chat_id,
                event_type="update_outcome",
                reason=task.reason,
                source_account=result.source_account,
                membership_scope=_row_text(state_row, "membership_scope", MembershipScope.UNKNOWN),
                status="success",
                features={
                    "pending_snapshot": _row_to_dict(pending_row),
                    "state_snapshot": _row_to_dict(state_row),
                    "task_snapshot": {
                        "chat_id": int(task.chat_id),
                        "event_count": int(task.event_count or 0),
                        "generation": int(task.generation or 0),
                        "in_flight_generation": int(task.in_flight_generation or 0),
                        "quiet_delay_seconds": int(task.quiet_delay_seconds or 0),
                        "priority_score": float(task.priority_score or 0.0),
                        "first_event_at": task.first_event_at,
                        "last_event_at": task.last_event_at,
                        "due_at": task.due_at,
                    },
                    "result_snapshot": {
                        "local_last_id": effective_local_last_id,
                        "remote_last_id": effective_remote_last_id,
                        "scanned_message_count": int(result.scanned_message_count or 0),
                        "duration_seconds": float(result.duration_seconds or 0.0),
                        "api_cost": float(result.api_cost or 0.0),
                    },
                },
                outcome={
                    "scanned_message_count": int(result.scanned_message_count or 0),
                    "duration_seconds": float(result.duration_seconds or 0.0),
                    "event_count": int(task.event_count or 0),
                    "has_new_generation": has_new_generation,
                    "local_last_id": effective_local_last_id,
                    "remote_last_id": effective_remote_last_id,
                },
                quiet_delay_seconds=task.quiet_delay_seconds,
                priority_score=task.priority_score,
                added_message_count=int(result.added_message_count or 0),
                wait_seconds=_wait_seconds(task, now),
                api_cost=float(result.api_cost or 0.0),
                created_at=now,
            ),
        )
        conn.commit()
    except sqlite3.Error:
        _rollback_write_transaction(conn, operation="complete_pending_update")
        logging.exception(
            "同步调度完成确认失败，任务保持 in-flight 等待恢复: chat_id=%s generation=%s",
            task.chat_id,
            task.in_flight_generation,
        )
        raise
    except Exception:
        _rollback_write_transaction(conn, operation="complete_pending_update")
        logging.exception(
            "同步调度完成确认发生未知错误，任务保持 in-flight 等待恢复: chat_id=%s generation=%s",
            task.chat_id,
            task.in_flight_generation,
        )
        raise
    finally:
        cur.close()


@synchronized_write
def fail_pending_update(
    conn: sqlite3.Connection,
    *,
    cfg: Any,
    task: SyncPendingTask,
    result: SyncUpdateResult,
    now_text: str | None = None,
) -> None:
    now = str(now_text or utc_now_text())
    max_cold_delay = cfg_int(cfg, "sync_max_cold_delay_seconds", 2 * 60 * 60, minimum=60)
    if result.failure_type == "flood_wait" and result.retry_after_seconds > 0:
        retry_delay = max(60, min(int(result.retry_after_seconds), max_cold_delay))
    elif result.failure_type == "no_account":
        retry_delay = 120
    else:
        retry_delay = min(15 * 60, max(180, int(task.quiet_delay_seconds or 0)))
    due_at = add_seconds_to_utc_text(now, retry_delay)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            "SELECT * FROM sync_pending_updates WHERE chat_id = ? LIMIT 1",
            (int(task.chat_id),),
        )
        pending_row = cur.fetchone()
        if not _task_owns_in_flight_pending_update(pending_row, task):
            conn.commit()
            return
        cur.execute(
            "SELECT * FROM sync_chat_state WHERE chat_id = ? LIMIT 1",
            (int(task.chat_id),),
        )
        state_row = cur.fetchone()
        cur.execute(
            """
            UPDATE sync_pending_updates
            SET
                in_flight = 0,
                in_flight_generation = 0,
                in_flight_owner_instance_id = '',
                in_flight_owner_pid = 0,
                in_flight_owner_host = '',
                dirty_generation = 0,
                due_at = ?,
                updated_at = ?
            WHERE chat_id = ?
            """,
            (due_at, now, int(task.chat_id)),
        )
        cur.execute(
            """
            UPDATE sync_chat_state
            SET
                status = 'backoff',
                last_update_at = ?,
                last_failure_at = ?,
                last_failure_message = ?,
                failure_count = failure_count + 1,
                next_update_at = ?,
                last_source_account = ?,
                updated_at = ?
            WHERE chat_id = ?
            """,
            (
                now,
                now,
                str(result.failure_message or result.failure_type or "failed")[:500],
                due_at,
                str(result.source_account or ""),
                now,
                int(task.chat_id),
            ),
        )
        _insert_learning_event_cur(
            cur,
            _learning_event_dict(
                chat_id=task.chat_id,
                event_type="update_outcome",
                reason=task.reason,
                source_account=result.source_account,
                membership_scope=_row_text(state_row, "membership_scope", MembershipScope.UNKNOWN),
                status="failed",
                features={
                    "pending_snapshot": _row_to_dict(pending_row),
                    "state_snapshot": _row_to_dict(state_row),
                    "task_snapshot": {
                        "chat_id": int(task.chat_id),
                        "event_count": int(task.event_count or 0),
                        "generation": int(task.generation or 0),
                        "in_flight_generation": int(task.in_flight_generation or 0),
                        "quiet_delay_seconds": int(task.quiet_delay_seconds or 0),
                        "priority_score": float(task.priority_score or 0.0),
                        "first_event_at": task.first_event_at,
                        "last_event_at": task.last_event_at,
                        "due_at": task.due_at,
                    },
                    "result_snapshot": {
                        "failure_type": str(result.failure_type or ""),
                        "failure_message": str(result.failure_message or ""),
                        "retry_after_seconds": int(result.retry_after_seconds or 0),
                        "retry_delay_seconds": int(retry_delay or 0),
                        "api_cost": float(result.api_cost or 0.0),
                    },
                },
                outcome={
                    "retry_delay_seconds": retry_delay,
                    "event_count": int(task.event_count or 0),
                    "message": str(result.failure_message or ""),
                },
                quiet_delay_seconds=task.quiet_delay_seconds,
                priority_score=task.priority_score,
                wait_seconds=_wait_seconds(task, now),
                api_cost=float(result.api_cost or 0.0),
                failure_type=result.failure_type or "failed",
                created_at=now,
            ),
        )
        conn.commit()
    except sqlite3.Error:
        _rollback_write_transaction(conn, operation="fail_pending_update")
        logging.exception(
            "同步调度失败状态写入失败，任务保持 in-flight 等待恢复: chat_id=%s generation=%s",
            task.chat_id,
            task.in_flight_generation,
        )
        raise
    except Exception:
        _rollback_write_transaction(conn, operation="fail_pending_update")
        logging.exception(
            "同步调度失败状态写入发生未知错误，任务保持 in-flight 等待恢复: chat_id=%s generation=%s",
            task.chat_id,
            task.in_flight_generation,
        )
        raise
    finally:
        cur.close()


@synchronized_write
def record_probe_result(
    conn: sqlite3.Connection,
    *,
    chat_id: int,
    chat_title: str = "",
    chat_username: str | None = None,
    status: str,
    source_account: str = "",
    remote_last_id: int = 0,
    local_last_id: int = 0,
    cooldown_seconds: int = 0,
    reason: str = "probe",
    now_text: str | None = None,
) -> None:
    now = str(now_text or utc_now_text())
    next_probe_at = add_seconds_to_utc_text(now, max(0, int(cooldown_seconds or 0)))
    normalized_status = str(status or "").strip() or "unknown"
    if normalized_status == "changed":
        state_status = "pending"
        quarantine_reason = ""
    elif normalized_status in {"missing", "cache_miss"}:
        state_status = "quarantined"
        quarantine_reason = normalized_status
    elif normalized_status in {"failed", "flood_wait", "no_account"}:
        state_status = "backoff"
        quarantine_reason = ""
    else:
        state_status = "idle"
        quarantine_reason = ""

    safe_chat_id = int(chat_id)
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            """
            INSERT INTO sync_chat_state(
                chat_id,
                chat_title,
                chat_username,
                membership_scope,
                status,
                last_probe_at,
                last_probe_status,
                remote_last_id,
                local_last_id,
                quarantine_reason,
                next_probe_at,
                last_source_account,
                is_active,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, 'unknown', ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                chat_title = CASE WHEN excluded.chat_title <> '' THEN excluded.chat_title ELSE sync_chat_state.chat_title END,
                chat_username = COALESCE(excluded.chat_username, sync_chat_state.chat_username),
                status = CASE
                    WHEN sync_chat_state.status = 'pending' AND ? = 'changed'
                        THEN sync_chat_state.status
                    ELSE excluded.status
                END,
                last_probe_at = excluded.last_probe_at,
                last_probe_status = excluded.last_probe_status,
                remote_last_id = MAX(sync_chat_state.remote_last_id, excluded.remote_last_id),
                local_last_id = MAX(sync_chat_state.local_last_id, excluded.local_last_id),
                quarantine_reason = excluded.quarantine_reason,
                next_probe_at = excluded.next_probe_at,
                last_source_account = excluded.last_source_account,
                is_active = 1,
                updated_at = excluded.updated_at
            """,
            (
                safe_chat_id,
                str(chat_title or "").strip() or f"Chat {safe_chat_id}",
                clean_username(chat_username),
                state_status,
                now,
                normalized_status,
                max(0, int(remote_last_id or 0)),
                max(0, int(local_last_id or 0)),
                quarantine_reason,
                next_probe_at,
                str(source_account or ""),
                now,
                now,
                normalized_status,
            ),
        )
        _insert_learning_event_cur(
            cur,
            _learning_event_dict(
                chat_id=safe_chat_id,
                event_type="probe",
                reason=reason,
                source_account=source_account,
                status=normalized_status,
                outcome={
                    "remote_last_id": int(remote_last_id or 0),
                    "local_last_id": int(local_last_id or 0),
                    "cooldown_seconds": int(cooldown_seconds or 0),
                },
                failure_type=normalized_status
                if normalized_status in {"failed", "flood_wait", "no_account", "missing", "cache_miss"}
                else "",
                created_at=now,
            ),
        )
        conn.commit()
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        cur.close()


@synchronized_write
def deactivate_chat(
    conn: sqlite3.Connection,
    chat_id: int,
    *,
    now_text: str | None = None,
    task: SyncPendingTask | None = None,
) -> None:
    now = str(now_text or utc_now_text())
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        if task is not None:
            cur.execute(
                "SELECT * FROM sync_pending_updates WHERE chat_id = ? LIMIT 1",
                (int(chat_id),),
            )
            if not _task_owns_in_flight_pending_update(cur.fetchone(), task):
                conn.commit()
                return
        cur.execute("DELETE FROM sync_pending_updates WHERE chat_id = ?", (int(chat_id),))
        cur.execute(
            """
            UPDATE sync_chat_state
            SET
                status = 'deleted',
                is_active = 0,
                priority_score = 0,
                next_probe_at = '',
                next_update_at = '',
                updated_at = ?
            WHERE chat_id = ?
            """,
            (now, int(chat_id)),
        )
        conn.commit()
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        cur.close()


@synchronized_write
def upsert_account_runtime_state(
    conn: sqlite3.Connection,
    *,
    account_key: str,
    session_name: str = "",
    label: str = "",
    cooldown_until: str = "",
    public_resolve_used: int | None = None,
    success: bool | None = None,
    duration_seconds: float | None = None,
    failure_message: str = "",
    in_flight_delta: int = 0,
    now_text: str | None = None,
) -> None:
    if not _table_exists(conn, "account_runtime_state"):
        return
    key = str(account_key or "").strip()
    if not key:
        return
    now = str(now_text or utc_now_text())
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            """
            INSERT INTO account_runtime_state(
                account_key,
                session_name,
                label,
                cooldown_until,
                public_resolve_used,
                recent_success_count,
                recent_failure_count,
                avg_duration_seconds,
                last_success_at,
                last_failure_at,
                last_failure_message,
                in_flight_count,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_key) DO UPDATE SET
                session_name = CASE WHEN excluded.session_name <> '' THEN excluded.session_name ELSE account_runtime_state.session_name END,
                label = CASE WHEN excluded.label <> '' THEN excluded.label ELSE account_runtime_state.label END,
                cooldown_until = CASE WHEN excluded.cooldown_until <> '' THEN excluded.cooldown_until ELSE account_runtime_state.cooldown_until END,
                public_resolve_used = CASE
                    WHEN ? IS NULL THEN account_runtime_state.public_resolve_used
                    ELSE excluded.public_resolve_used
                END,
                recent_success_count = account_runtime_state.recent_success_count + ?,
                recent_failure_count = account_runtime_state.recent_failure_count + ?,
                avg_duration_seconds = CASE
                    WHEN ? IS NULL THEN account_runtime_state.avg_duration_seconds
                    WHEN account_runtime_state.avg_duration_seconds <= 0 THEN excluded.avg_duration_seconds
                    ELSE (account_runtime_state.avg_duration_seconds * 0.8) + (excluded.avg_duration_seconds * 0.2)
                END,
                last_success_at = CASE WHEN ? = 1 THEN excluded.last_success_at ELSE account_runtime_state.last_success_at END,
                last_failure_at = CASE WHEN ? = 1 THEN excluded.last_failure_at ELSE account_runtime_state.last_failure_at END,
                last_failure_message = CASE WHEN ? = 1 THEN excluded.last_failure_message ELSE account_runtime_state.last_failure_message END,
                in_flight_count = MAX(0, account_runtime_state.in_flight_count + ?),
                updated_at = excluded.updated_at
            """,
            (
                key,
                str(session_name or "").strip(),
                str(label or "").strip(),
                str(cooldown_until or "").strip(),
                max(0, int(public_resolve_used or 0)),
                1 if success is True else 0,
                1 if success is False else 0,
                max(0.0, float(duration_seconds or 0.0)),
                now if success is True else "",
                now if success is False else "",
                str(failure_message or "")[:500] if success is False else "",
                max(0, int(in_flight_delta or 0)),
                now,
                None if public_resolve_used is None else int(public_resolve_used),
                1 if success is True else 0,
                1 if success is False else 0,
                None if duration_seconds is None else float(duration_seconds),
                1 if success is True else 0,
                1 if success is False else 0,
                1 if success is False else 0,
                int(in_flight_delta or 0),
            ),
        )
        conn.commit()
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        cur.close()


def list_account_runtime_states(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    if not _table_exists(conn, "account_runtime_state"):
        return []
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT *
            FROM account_runtime_state
            ORDER BY account_key ASC
            """
        )
        return [_row_to_dict(row) for row in cur.fetchall()]
    finally:
        cur.close()


@synchronized_write
def prune_learning_events(
    conn: sqlite3.Connection,
    cfg: Any,
    *,
    now_text: str | None = None,
) -> dict[str, Any]:
    if not _table_exists(conn, "sync_learning_events"):
        return {"ok": True, "deleted": 0, "remaining": 0}
    now = str(now_text or utc_now_text())
    retention_days = cfg_int(cfg, "sync_learning_retention_days", 90, minimum=1)
    max_rows = cfg_int(cfg, "sync_learning_max_rows", 200000, minimum=1000)
    deleted = 0
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            """
            DELETE FROM sync_learning_events
            WHERE created_at < datetime(?, ?)
              AND event_type <> 'update_outcome'
              AND failure_type = ''
            """,
            (now, f"-{retention_days} days"),
        )
        deleted += max(0, int(cur.rowcount or 0))
        cur.execute("SELECT COUNT(*) AS c FROM sync_learning_events")
        remaining = _row_int(cur.fetchone(), "c")
        excess = max(0, remaining - max_rows)
        if excess > 0:
            cur.execute(
                """
                DELETE FROM sync_learning_events
                WHERE id IN (
                    SELECT id
                    FROM sync_learning_events
                    ORDER BY
                        CASE
                            WHEN event_type = 'update_outcome' OR failure_type <> ''
                                THEN 1
                            ELSE 0
                        END ASC,
                        created_at ASC,
                        id ASC
                    LIMIT ?
                )
                """,
                (excess,),
            )
            deleted += max(0, int(cur.rowcount or 0))
            remaining = max(0, remaining - excess)
        conn.commit()
        return {
            "ok": True,
            "deleted": deleted,
            "remaining": remaining,
            "retention_days": retention_days,
            "max_rows": max_rows,
        }
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        cur.close()


def _empty_scheduler_summary(health_snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
    snapshot = dict(health_snapshot or {})
    ai_shadow = bool(snapshot.get("ai_shadow"))
    ai_enabled_value = bool(snapshot.get("ai_enabled"))
    auto_promote = bool(snapshot.get("ai_auto_promote_enabled"))
    model_can_take_over = ai_enabled_value and not ai_shadow and auto_promote
    effective_model_mode = (
        "已接管" if model_can_take_over else "仅观察" if ai_enabled_value else "已关闭"
    )
    return {
        "enabled": bool(snapshot.get("scheduler_enabled")),
        "ai_enabled": ai_enabled_value,
        "ai_shadow": ai_shadow,
        "ai_auto_promote_enabled": auto_promote,
        "effective_model_mode": effective_model_mode,
        "model_can_take_over": model_can_take_over,
        "pending_count": 0,
        "due_count": 0,
        "in_flight_count": 0,
        "learning_event_count": 0,
        "outcome_sample_count": 0,
        "avg_quiet_delay_seconds": 0,
        "next_due_at": "",
        "coalesced_event_count": 0,
        "membership_counts": [],
        "status_counts": [],
        "accounts": list(snapshot.get("accounts") or []),
        "account_capacity": {
            "configured": int(snapshot.get("configured_listener_count") or 0),
            "connected": int(snapshot.get("active_listener_count") or 0),
            "available": 0,
            "cooldown": 0,
            "concurrency": int(snapshot.get("scheduler_concurrency") or 1),
        },
        "backpressure": dict(snapshot.get("backpressure") or {}),
        "model": {
            "model_key": MODEL_KEY,
            "model_version": "",
            "backend": "disabled",
            "artifact_path": default_model_artifact_path(),
            "trained_at": "",
            "sample_count": 0,
            "metrics": {},
        },
        "recent": {
            "update_count": 0,
            "added_message_count": 0,
            "avg_wait_seconds": 0,
            "failure_count": 0,
        },
        "recent_failures": [],
    }


def _decode_json_dict(value: Any) -> dict[str, Any]:
    try:
        parsed = json.loads(str(value or "{}"))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def build_scheduler_summary(
    conn: sqlite3.Connection,
    *,
    health_snapshot: dict[str, Any] | None = None,
    now_text: str | None = None,
) -> dict[str, Any]:
    if not _table_exists(conn, "sync_chat_state") or not _table_exists(
        conn, "sync_pending_updates"
    ):
        return _empty_scheduler_summary(health_snapshot)

    now = str(now_text or utc_now_text())
    payload = _empty_scheduler_summary(health_snapshot)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                COUNT(*) AS pending_count,
                SUM(CASE WHEN in_flight = 0 AND due_at <> '' AND due_at <= ? THEN 1 ELSE 0 END) AS due_count,
                SUM(CASE WHEN in_flight <> 0 THEN 1 ELSE 0 END) AS in_flight_count,
                COALESCE(AVG(NULLIF(quiet_delay_seconds, 0)), 0) AS avg_quiet_delay_seconds,
                COALESCE(MIN(CASE WHEN in_flight = 0 THEN due_at END), '') AS next_due_at,
                COALESCE(SUM(CASE WHEN event_count > 1 THEN event_count - 1 ELSE 0 END), 0) AS coalesced_event_count
            FROM sync_pending_updates
            """,
            (now,),
        )
        row = cur.fetchone()
        payload.update(
            {
                "pending_count": _row_int(row, "pending_count"),
                "due_count": _row_int(row, "due_count"),
                "in_flight_count": _row_int(row, "in_flight_count"),
                "avg_quiet_delay_seconds": _row_int(row, "avg_quiet_delay_seconds"),
                "next_due_at": _row_text(row, "next_due_at"),
                "coalesced_event_count": _row_int(row, "coalesced_event_count"),
            }
        )
        accounts = list(payload.get("accounts") or [])
        available_accounts = [
            item for item in accounts if int(item.get("cooldown_seconds") or 0) <= 0
        ]
        cooldown_accounts = [
            item for item in accounts if int(item.get("cooldown_seconds") or 0) > 0
        ]
        payload["account_capacity"] = {
            "configured": len(accounts)
            or int((health_snapshot or {}).get("configured_listener_count") or 0),
            "connected": int((health_snapshot or {}).get("active_listener_count") or 0),
            "available": len(available_accounts),
            "cooldown": len(cooldown_accounts),
            "concurrency": int((health_snapshot or {}).get("scheduler_concurrency") or max(1, len(accounts) or 1)),
        }
        payload["backpressure"] = dict((health_snapshot or {}).get("backpressure") or {})

        cur.execute(
            """
            SELECT membership_scope, COUNT(*) AS c
            FROM sync_chat_state
            WHERE is_active = 1
            GROUP BY membership_scope
            ORDER BY c DESC, membership_scope ASC
            """
        )
        payload["membership_counts"] = [
            {"scope": str(row["membership_scope"] or ""), "count": int(row["c"] or 0)}
            for row in cur.fetchall()
        ]

        cur.execute(
            """
            SELECT status, COUNT(*) AS c
            FROM sync_chat_state
            WHERE is_active = 1
            GROUP BY status
            ORDER BY c DESC, status ASC
            """
        )
        payload["status_counts"] = [
            {"status": str(row["status"] or ""), "count": int(row["c"] or 0)}
            for row in cur.fetchall()
        ]

        cur.execute(
            """
            SELECT
                COUNT(*) AS update_count,
                COALESCE(SUM(added_message_count), 0) AS added_message_count,
                COALESCE(AVG(NULLIF(wait_seconds, 0)), 0) AS avg_wait_seconds,
                SUM(CASE WHEN failure_type <> '' THEN 1 ELSE 0 END) AS failure_count
            FROM sync_learning_events
            WHERE event_type = 'update_outcome'
              AND created_at >= datetime(?, '-24 hours')
            """,
            (now,),
        )
        recent = cur.fetchone()
        payload["recent"] = {
            "update_count": _row_int(recent, "update_count"),
            "added_message_count": _row_int(recent, "added_message_count"),
            "avg_wait_seconds": _row_int(recent, "avg_wait_seconds"),
            "failure_count": _row_int(recent, "failure_count"),
        }

        cur.execute(
            """
            SELECT chat_id, event_type, reason, source_account, failure_type, created_at, outcome_json
            FROM sync_learning_events INDEXED BY idx_sync_learning_failure_created
            WHERE failure_type <> ''
            ORDER BY created_at DESC, id DESC
            LIMIT 8
            """
        )
        payload["recent_failures"] = [
            {
                "chat_id": int(row["chat_id"] or 0),
                "event_type": str(row["event_type"] or ""),
                "reason": str(row["reason"] or ""),
                "source_account": str(row["source_account"] or ""),
                "failure_type": str(row["failure_type"] or ""),
                "created_at": str(row["created_at"] or ""),
                "outcome": _decode_json_dict(row["outcome_json"]),
            }
            for row in cur.fetchall()
        ]

        cur.execute(
            """
            SELECT
                COUNT(*) AS learning_event_count,
                SUM(CASE WHEN event_type = 'update_outcome' THEN 1 ELSE 0 END) AS outcome_sample_count
            FROM sync_learning_events
            """
        )
        event_count_row = cur.fetchone()
        learning_event_count = _row_int(event_count_row, "learning_event_count")
        outcome_sample_count = _row_int(event_count_row, "outcome_sample_count")
        payload["learning_event_count"] = learning_event_count
        payload["outcome_sample_count"] = outcome_sample_count
        cur.execute(
            """
            SELECT model_key, model_version, backend, metrics_json, trained_at, artifact_path, state_json, updated_at
            FROM sync_model_state
            WHERE model_key = ?
            LIMIT 1
            """,
            (MODEL_KEY,),
        )
        model_row = cur.fetchone()
        if model_row is None:
            backend = "torch_shadow_missing" if payload["ai_enabled"] else "disabled"
            if payload["ai_enabled"] and not payload["ai_shadow"]:
                backend = "heuristic_fallback"
            payload["model"] = {
                "model_key": MODEL_KEY,
                "model_version": "",
                "backend": backend,
                "artifact_path": default_model_artifact_path(),
                "trained_at": "",
                "sample_count": outcome_sample_count,
                "learning_event_count": learning_event_count,
                "outcome_sample_count": outcome_sample_count,
                "metrics": {},
            }
        else:
            model_state = _decode_json_dict(model_row["state_json"])
            state_mode = str(model_state.get("mode") or "shadow")
            if payload["ai_enabled"]:
                if payload["ai_shadow"]:
                    payload["effective_model_mode"] = "仅观察"
                    payload["model_can_take_over"] = False
                elif state_mode == "active" and payload.get("ai_auto_promote_enabled"):
                    payload["effective_model_mode"] = "已接管"
                    payload["model_can_take_over"] = True
                else:
                    payload["effective_model_mode"] = "启发式接管"
                    payload["model_can_take_over"] = False
            payload["model"] = {
                "model_key": str(model_row["model_key"] or MODEL_KEY),
                "model_version": str(model_row["model_version"] or ""),
                "backend": str(model_row["backend"] or ""),
                "artifact_path": str(model_row["artifact_path"] or default_model_artifact_path()),
                "trained_at": str(model_row["trained_at"] or ""),
                "sample_count": outcome_sample_count,
                "learning_event_count": learning_event_count,
                "outcome_sample_count": outcome_sample_count,
                "metrics": _decode_json_dict(model_row["metrics_json"]),
                "state": model_state,
                "updated_at": str(model_row["updated_at"] or ""),
            }
        return payload
    finally:
        cur.close()


def list_scheduler_chats(
    conn: sqlite3.Connection,
    *,
    membership: str = "",
    status: str = "",
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    if not _table_exists(conn, "sync_chat_state"):
        return {"ok": True, "items": [], "count": 0, "limit": limit, "offset": offset}
    effective_limit = max(1, min(500, int(limit or 100)))
    effective_offset = max(0, int(offset or 0))
    filters = ["s.is_active = 1"]
    params: list[Any] = []
    if str(membership or "").strip():
        filters.append("s.membership_scope = ?")
        params.append(str(membership).strip())
    if str(status or "").strip():
        filters.append("s.status = ?")
        params.append(str(status).strip())
    where_sql = " AND ".join(filters)
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) AS c FROM sync_chat_state s WHERE {where_sql}", params)
        count = _row_int(cur.fetchone(), "c")
        cur.execute(
            f"""
            SELECT
                s.chat_id,
                s.chat_title,
                s.chat_username,
                s.membership_scope,
                s.status,
                s.last_event_at,
                s.last_probe_at,
                s.last_probe_status,
                s.last_update_at,
                s.last_success_at,
                s.last_failure_at,
                s.last_failure_message,
                s.remote_last_id,
                s.local_last_id,
                s.failure_count,
                s.quarantine_reason,
                s.next_probe_at,
                s.next_update_at,
                s.model_delay_seconds,
                s.priority_score,
                s.source_accounts,
                s.last_source_account,
                p.event_count,
                p.due_at,
                p.in_flight,
                p.generation,
                p.dirty_generation
            FROM sync_chat_state s
            LEFT JOIN sync_pending_updates p ON p.chat_id = s.chat_id
            WHERE {where_sql}
            ORDER BY
                CASE WHEN p.due_at IS NULL OR p.due_at = '' THEN 1 ELSE 0 END ASC,
                p.due_at ASC,
                s.priority_score DESC,
                s.chat_id ASC
            LIMIT ? OFFSET ?
            """,
            [*params, effective_limit, effective_offset],
        )
        items = []
        for row in cur.fetchall():
            items.append(
                {
                    "chat_id": int(row["chat_id"] or 0),
                    "chat_title": str(row["chat_title"] or ""),
                    "chat_username": str(row["chat_username"] or ""),
                    "membership_scope": str(row["membership_scope"] or ""),
                    "status": str(row["status"] or ""),
                    "last_event_at": str(row["last_event_at"] or ""),
                    "last_probe_at": str(row["last_probe_at"] or ""),
                    "last_probe_status": str(row["last_probe_status"] or ""),
                    "last_update_at": str(row["last_update_at"] or ""),
                    "last_success_at": str(row["last_success_at"] or ""),
                    "last_failure_at": str(row["last_failure_at"] or ""),
                    "last_failure_message": str(row["last_failure_message"] or ""),
                    "remote_last_id": int(row["remote_last_id"] or 0),
                    "local_last_id": int(row["local_last_id"] or 0),
                    "failure_count": int(row["failure_count"] or 0),
                    "quarantine_reason": str(row["quarantine_reason"] or ""),
                    "next_probe_at": str(row["next_probe_at"] or ""),
                    "next_update_at": str(row["next_update_at"] or ""),
                    "model_delay_seconds": int(row["model_delay_seconds"] or 0),
                    "priority_score": float(row["priority_score"] or 0.0),
                    "source_accounts": str(row["source_accounts"] or ""),
                    "last_source_account": str(row["last_source_account"] or ""),
                    "event_count": int(row["event_count"] or 0),
                    "due_at": str(row["due_at"] or ""),
                    "in_flight": int(row["in_flight"] or 0),
                    "generation": int(row["generation"] or 0),
                    "dirty_generation": int(row["dirty_generation"] or 0),
                }
            )
        return {
            "ok": True,
            "items": items,
            "count": count,
            "limit": effective_limit,
            "offset": effective_offset,
        }
    finally:
        cur.close()


def build_update_preflight(
    conn: sqlite3.Connection,
    cfg: Any,
    *,
    chat_id: str | int = "all",
    health_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw_chat_id = str(chat_id or "all").strip()
    all_scope = raw_chat_id.lower() == "all"
    cur = conn.cursor()
    try:
        if all_scope:
            cur.execute(
                """
                SELECT
                    COUNT(*) AS target_count,
                    SUM(CASE WHEN COALESCE(chat_username, '') <> '' THEN 1 ELSE 0 END) AS public_count
                FROM chats
                """
            )
            row = cur.fetchone()
            target_count = _row_int(row, "target_count")
            public_count = _row_int(row, "public_count")
            target_title = "全部群聊"
            safe_chat_id: int | None = None
        else:
            safe_chat_id = int(raw_chat_id)
            cur.execute(
                """
                SELECT chat_id, chat_title, chat_username
                FROM chats
                WHERE chat_id = ?
                LIMIT 1
                """,
                (safe_chat_id,),
            )
            row = cur.fetchone()
            if row is None:
                return {"ok": False, "error": "chat_id 不存在"}
            target_count = 1
            public_count = 1 if _row_text(row, "chat_username") else 0
            target_title = _row_text(row, "chat_title", f"Chat {safe_chat_id}")
    finally:
        cur.close()

    snapshot = dict(health_snapshot or {})
    account_rows = list(snapshot.get("accounts") or [])
    if not account_rows:
        account_rows = [
            {
                "key": "primary",
                "label": "主账号",
                "cooldown_seconds": 0,
                "connected": False,
            }
        ]
        secondary_session = str(getattr(cfg, "secondary_session_name", "") or "").strip()
        primary_session = str(getattr(cfg, "session_name", "") or "").strip()
        if secondary_session and secondary_session != primary_session:
            account_rows.append(
                {
                    "key": "secondary",
                    "label": "第二账号",
                    "cooldown_seconds": 0,
                    "connected": False,
                }
            )

    configured_concurrency = cfg_int(cfg, "admin_update_concurrency", 4, minimum=1)
    active_account_count = max(1, len(account_rows))
    per_account_concurrency = max(1, configured_concurrency // active_account_count)
    effective_concurrency = max(
        1,
        min(configured_concurrency, per_account_concurrency * active_account_count),
    )
    secondary_limit = getattr(cfg, "admin_update_secondary_public_resolve_limit", None)
    if secondary_limit is None:
        secondary_public_budget = max(0, min(public_count, max(2, target_count // 20)))
    else:
        secondary_public_budget = max(0, int(secondary_limit or 0))
    cooldown_accounts = [
        item
        for item in account_rows
        if int(item.get("cooldown_seconds") or 0) > 0
    ]
    available_accounts = [
        item
        for item in account_rows
        if int(item.get("cooldown_seconds") or 0) <= 0
    ]
    risks: list[dict[str, str]] = []
    if not available_accounts:
        risks.append(
            {
                "level": "critical",
                "message": "所有账号都处于冷却中，启动后大概率会立即等待或失败。",
            }
        )
    elif cooldown_accounts:
        risks.append(
            {
                "level": "warning",
                "message": "部分账号处于冷却中，任务会降低并发并优先使用可用账号。",
            }
        )
    if all_scope and target_count >= 500 and secondary_public_budget <= 0 and public_count > 0:
        risks.append(
            {
                "level": "warning",
                "message": "公开 username 主动解析预算为 0，第二账号只能处理已缓存或已加入群组。",
            }
        )
    if effective_concurrency > len(available_accounts) and len(available_accounts) <= 1:
        risks.append(
            {
                "level": "info",
                "message": "当前实际可用账号较少，任务会按单账号节流执行。",
            }
        )
    risk_level = "low"
    if any(item["level"] == "critical" for item in risks):
        risk_level = "critical"
    elif any(item["level"] == "warning" for item in risks):
        risk_level = "warning"

    return {
        "ok": True,
        "target": {
            "scope": "all" if all_scope else "chat",
            "chat_id": None if all_scope else safe_chat_id,
            "label": target_title,
            "target_count": target_count,
            "public_username_count": public_count,
        },
        "accounts": [
            {
                "key": str(item.get("key") or ""),
                "label": str(item.get("label") or item.get("key") or ""),
                "connected": bool(item.get("connected")),
                "cooldown_seconds": int(item.get("cooldown_seconds") or 0),
                "status_label": "冷却中"
                if int(item.get("cooldown_seconds") or 0) > 0
                else "可用",
            }
            for item in account_rows
        ],
        "account_capacity": {
            "configured": len(account_rows),
            "available": len(available_accounts),
            "cooldown": len(cooldown_accounts),
        },
        "strategy": {
            "configured_concurrency": configured_concurrency,
            "effective_concurrency": effective_concurrency,
            "per_account_concurrency": per_account_concurrency,
            "scheduler_concurrency": cfg_int(cfg, "sync_scheduler_concurrency", 2, minimum=1),
            "secondary_public_resolve_budget": secondary_public_budget,
            "max_cooldown_wait_seconds": cfg_int(
                cfg,
                "admin_update_max_cooldown_wait_seconds",
                45,
                minimum=0,
            ),
        },
        "risk_level": risk_level,
        "risks": risks,
        "confirm_summary": (
            f"将对{target_title}执行手动全量增量更新；"
            f"目标 {target_count} 个，可用账号 {len(available_accounts)}/{len(account_rows)}，"
            f"预计总并发 {effective_concurrency}，公开解析预算 {secondary_public_budget}。"
        ),
    }


@synchronized_write
def reset_model_state(
    conn: sqlite3.Connection,
    *,
    artifact_path: str | None = None,
    now_text: str | None = None,
) -> dict[str, Any]:
    path = Path(artifact_path or default_model_artifact_path())
    removed_artifact = False
    with suppress(FileNotFoundError):
        path.unlink()
        removed_artifact = True
    with suppress(Exception):
        from tg_harvest.ml.sync_predictor import invalidate_model_cache

        invalidate_model_cache(str(path))
    now = str(now_text or utc_now_text())
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("DELETE FROM sync_model_state WHERE model_key = ?", (MODEL_KEY,))
        conn.commit()
        return {
            "ok": True,
            "model_key": MODEL_KEY,
            "artifact_path": str(path),
            "removed_artifact": removed_artifact,
            "reset_at": now,
        }
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        cur.close()
