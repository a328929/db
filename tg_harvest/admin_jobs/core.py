import contextvars
import logging
import os
import sqlite3
import threading
import uuid
from contextlib import closing
from datetime import UTC, datetime, timedelta
from typing import Any

import tg_harvest.admin_jobs.runtime as _job_runtime
from tg_harvest.admin_jobs.store import (
    _admin_active_job_summary_from_row,
    _admin_connect,
    _admin_fetch_job_snapshot_row,
    _admin_fetch_last_seq,
    _admin_insert_job_row,
    _admin_persist_job_create,
    _admin_persist_log_locked,
    _admin_snapshot_from_row,
)
from tg_harvest.config import CFG
from tg_harvest.ops_bot.notify import (
    maybe_notify_admin_job_log,
    notify_admin_job_created,
    notify_admin_job_status,
)
from tg_harvest.storage.row_access import row_int as _row_int

# 核心：使用 contextvars 确保日志与任务绑定，无视线程池复用和异步切换。
job_context: contextvars.ContextVar[str] = contextvars.ContextVar("job_id", default="")
job_log_passthrough_enabled: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "job_log_passthrough_enabled", default=True
)

# 任务真相源已迁移到 SQLite；内存仅保留轻量锁与递增日志序号缓存。
ADMIN_JOBS: dict[str, dict[str, Any]] = {}
ADMIN_JOBS_LOCK = threading.RLock()


def _admin_cache_entry_locked(job_id: str) -> dict[str, Any]:
    entry = ADMIN_JOBS.get(job_id)
    if not isinstance(entry, dict):
        entry = {
            "job_id": str(job_id),
            "next_log_seq": None,
            "_lock": threading.Lock(),
        }
        ADMIN_JOBS[job_id] = entry
        return entry
    if "_lock" not in entry:
        entry["_lock"] = threading.Lock()
    return entry


def _admin_job_trim_locked() -> None:
    now = datetime.now(UTC)
    with closing(_admin_connect()) as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT job_id, status, updated_at, heartbeat_at
                FROM admin_jobs
                ORDER BY updated_at ASC, created_at ASC
                """
            )
            rows = cur.fetchall()
            if not rows:
                return

            to_delete: list[str] = []
            terminal_statuses = {"done", "error"}
            remaining = len(rows)

            for row in rows:
                job_id = str(row["job_id"] or "")
                status = str(row["status"] or "").lower()
                updated_at_str = str(row["updated_at"] or "")
                heartbeat_at_str = str(row["heartbeat_at"] or "")
                updated_at = _job_runtime._admin_parse_timestamp(updated_at_str, now)
                heartbeat_at = _job_runtime._admin_parse_timestamp(heartbeat_at_str, updated_at)

                if status in terminal_statuses and remaining > CFG.admin_job_max_count:
                    to_delete.append(job_id)
                    remaining -= 1
                    continue

                if now - heartbeat_at > timedelta(hours=2):
                    to_delete.append(job_id)
                    remaining -= 1

            if not to_delete:
                return

            placeholders = ",".join(["?"] * len(to_delete))
            cur.execute(
                f"DELETE FROM admin_job_logs WHERE job_id IN ({placeholders})",
                to_delete,
            )
            cur.execute(
                f"DELETE FROM admin_jobs WHERE job_id IN ({placeholders})",
                to_delete,
            )
            conn.commit()

            for job_id in to_delete:
                ADMIN_JOBS.pop(job_id, None)
        finally:
            cur.close()


class _AdminJobThreadLogHandler(logging.Handler):
    def __init__(self, job_id: str) -> None:
        super().__init__()
        self._target_job_id = str(job_id)

    def emit(self, record: logging.LogRecord) -> None:
        if job_context.get() != self._target_job_id:
            return
        if not bool(job_log_passthrough_enabled.get()):
            return
        _admin_job_append_log(self._target_job_id, record.getMessage())


def _admin_make_job_log_handler(job_id: str, **kwargs) -> logging.Handler:
    return _AdminJobThreadLogHandler(job_id=job_id)


def _admin_finalize_created_job_locked(job_id: str) -> dict[str, Any]:
    cache_entry = _admin_cache_entry_locked(job_id)
    cache_entry["next_log_seq"] = 1
    _admin_job_trim_locked()
    _admin_job_append_log(job_id, "任务已创建（占位）")
    snapshot = _admin_job_get_snapshot(job_id)
    if snapshot is None:
        raise RuntimeError("任务创建后无法读取快照")
    try:
        notify_admin_job_created(job_id, snapshot)
    except Exception:
        logging.exception("运维机器人任务创建通知失败")
    return snapshot


def _admin_job_create(
    job_type: str,
    target_chat_id: int | None = None,
    target_label: str | None = None,
) -> dict[str, Any]:
    with ADMIN_JOBS_LOCK:
        created_at = _job_runtime._admin_now_iso()
        job_id = uuid.uuid4().hex
        owner_instance_id = _job_runtime._admin_runtime_instance_id()
        owner_pid = os.getpid()
        owner_host = _job_runtime._admin_runtime_host()
        _admin_persist_job_create(
            job_id=job_id,
            job_type=str(job_type or "unknown"),
            target_chat_id=target_chat_id,
            target_label=target_label,
            created_at=created_at,
            owner_instance_id=owner_instance_id,
            owner_pid=owner_pid,
            owner_host=owner_host,
        )
        return _admin_finalize_created_job_locked(job_id)


def _admin_try_create_exclusive_job(
    job_type: str,
    target_chat_id: int | None = None,
    target_label: str | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    with ADMIN_JOBS_LOCK:
        _admin_recover_interrupted_jobs_locked(include_foreign_owner=False)
        created_at = _job_runtime._admin_now_iso()
        job_id = uuid.uuid4().hex
        owner_instance_id = _job_runtime._admin_runtime_instance_id()
        owner_pid = os.getpid()
        owner_host = _job_runtime._admin_runtime_host()

        with closing(_admin_connect()) as conn:
            cur = conn.cursor()
            try:
                cur.execute("BEGIN IMMEDIATE")
                cur.execute(
                    """
                    SELECT
                        job_id,
                        job_type,
                        status,
                        target_chat_id,
                        target_label,
                        stop_requested
                    FROM admin_jobs
                    WHERE status IN ('queued', 'running')
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
                if row is not None:
                    conn.rollback()
                    return None, _admin_active_job_summary_from_row(row)

                _admin_insert_job_row(
                    cur,
                    job_id=job_id,
                    job_type=job_type,
                    target_chat_id=target_chat_id,
                    target_label=target_label,
                    created_at=created_at,
                    owner_instance_id=owner_instance_id,
                    owner_pid=owner_pid,
                    owner_host=owner_host,
                )
                conn.commit()
            finally:
                cur.close()

        return _admin_finalize_created_job_locked(job_id), None


def _admin_create_chat_job_if_absent(
    job_type: str, chat_id: int, target_label: str | None = None
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    with ADMIN_JOBS_LOCK:
        _admin_recover_interrupted_jobs_locked(include_foreign_owner=False)
        created_at = _job_runtime._admin_now_iso()
        job_id = uuid.uuid4().hex
        owner_instance_id = _job_runtime._admin_runtime_instance_id()
        owner_pid = os.getpid()
        owner_host = _job_runtime._admin_runtime_host()

        with closing(_admin_connect()) as conn:
            cur = conn.cursor()
            try:
                cur.execute("BEGIN IMMEDIATE")
                cur.execute(
                    """
                    SELECT
                        job_id,
                        job_type,
                        status,
                        target_chat_id,
                        target_label,
                        stop_requested
                    FROM admin_jobs
                    WHERE target_chat_id = ?
                      AND job_type IN ('update', 'delete')
                      AND status IN ('queued', 'running')
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """,
                    (int(chat_id),),
                )
                row = cur.fetchone()
                if row is not None:
                    conn.rollback()
                    return None, _admin_active_job_summary_from_row(row)

                _admin_insert_job_row(
                    cur,
                    job_id=job_id,
                    job_type=job_type,
                    target_chat_id=chat_id,
                    target_label=target_label,
                    created_at=created_at,
                    owner_instance_id=owner_instance_id,
                    owner_pid=owner_pid,
                    owner_host=owner_host,
                )
                conn.commit()
            finally:
                cur.close()

        return _admin_finalize_created_job_locked(job_id), None


def _admin_has_any_active_job() -> bool:
    with ADMIN_JOBS_LOCK:
        _admin_recover_interrupted_jobs_locked(include_foreign_owner=False)
        with closing(_admin_connect()) as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT 1
                    FROM admin_jobs
                    WHERE status IN ('queued', 'running')
                    LIMIT 1
                    """
                )
                return cur.fetchone() is not None
            finally:
                cur.close()


def _admin_get_active_job() -> dict[str, Any] | None:
    with ADMIN_JOBS_LOCK:
        _admin_recover_interrupted_jobs_locked(include_foreign_owner=False)
        with closing(_admin_connect()) as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT
                        job_id,
                        job_type,
                        status,
                        target_chat_id,
                        target_label,
                        stop_requested
                    FROM admin_jobs
                    WHERE status IN ('queued', 'running')
                    ORDER BY updated_at DESC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
                if row is None:
                    return None
                return _admin_active_job_summary_from_row(row)
            finally:
                cur.close()


def _admin_job_stop_requested(job_id: str) -> bool:
    with ADMIN_JOBS_LOCK, closing(_admin_connect()) as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                    SELECT stop_requested
                    FROM admin_jobs
                    WHERE job_id = ?
                    LIMIT 1
                    """,
                (str(job_id),),
            )
            row = cur.fetchone()
            return _row_int(row, "stop_requested") == 1
        finally:
            cur.close()


def _admin_request_job_stop(job_id: str) -> tuple[bool, str | None]:
    with ADMIN_JOBS_LOCK:
        updated_at = _job_runtime._admin_now_iso()
        with closing(_admin_connect()) as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT status
                    FROM admin_jobs
                    WHERE job_id = ?
                    LIMIT 1
                    """,
                    (str(job_id),),
                )
                row = cur.fetchone()
                if row is None:
                    return False, "任务不存在"

                status = str(row["status"] or "").lower()
                if status not in {"queued", "running"}:
                    return False, "任务已结束，不能停止"

                cur.execute(
                    """
                    UPDATE admin_jobs
                    SET stop_requested = 1,
                        updated_at = ?
                    WHERE job_id = ?
                      AND status IN ('queued', 'running')
                    """,
                    (updated_at, str(job_id)),
                )
                conn.commit()
                return int(cur.rowcount or 0) > 0, None
            finally:
                cur.close()


def _admin_job_append_log(job_id: str, message: str) -> dict[str, Any] | None:
    with ADMIN_JOBS_LOCK:
        snapshot = _admin_job_get_snapshot(job_id)
        if snapshot is None:
            return None

        cache_entry = _admin_cache_entry_locked(job_id)
        with cache_entry["_lock"]:
            next_log_seq = cache_entry.get("next_log_seq")
            if not isinstance(next_log_seq, int) or next_log_seq <= 0:
                next_log_seq = _admin_fetch_last_seq(job_id) + 1

            log_item = {
                "seq": next_log_seq,
                "ts": _job_runtime._admin_now_iso(),
                "message": str(message),
            }
            _admin_persist_log_locked(
                job_id,
                log_item,
            )
            cache_entry["next_log_seq"] = int(next_log_seq) + 1
            try:
                maybe_notify_admin_job_log(job_id, str(message))
            except Exception:
                logging.exception("运维机器人任务日志通知失败")
            return log_item


def _admin_job_set_status(job_id: str, status: str) -> bool:
    with ADMIN_JOBS_LOCK:
        normalized_status = _job_runtime._normalize_status(status)
        updated_at = _job_runtime._admin_now_iso()
        previous_status = ""
        with closing(_admin_connect()) as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    SELECT status
                    FROM admin_jobs
                    WHERE job_id = ?
                    LIMIT 1
                    """,
                    (str(job_id),),
                )
                row = cur.fetchone()
                if row is None:
                    return False
                previous_status = str(row["status"] or "").lower()
                cur.execute(
                    """
                    UPDATE admin_jobs
                    SET status = ?,
                        updated_at = ?,
                        heartbeat_at = ?
                    WHERE job_id = ?
                    """,
                    (normalized_status, updated_at, updated_at, str(job_id)),
                )
                conn.commit()
                updated = int(cur.rowcount or 0) > 0
            finally:
                cur.close()

        if updated and normalized_status in {"done", "error"}:
            _admin_job_trim_locked()
            if previous_status != normalized_status:
                snapshot = _admin_job_get_snapshot(str(job_id))
                try:
                    notify_admin_job_status(str(job_id), normalized_status, snapshot)
                except Exception:
                    logging.exception("运维机器人任务状态通知失败")
        return updated


def _admin_job_update_progress(
    job_id: str,
    current: int,
    total: int | None = None,
    stage: str | None = None,
    log_step: int = 1000,
    force_log: bool = False,
    auto_log: bool = True,
) -> bool:
    safe_current = max(int(current), 0)
    safe_total = _job_runtime._safe_progress_total(total)

    with ADMIN_JOBS_LOCK:
        row = _admin_fetch_job_snapshot_row(job_id)
        if row is None:
            return False

        progress_total = (
            safe_total
            if total is not None
            else _job_runtime._safe_progress_total(row["progress_total"])
        )
        progress_stage = str(stage) if stage is not None else str(
            row["progress_stage"] or "running"
        )
        last_logged_current = _row_int(row, "last_logged_current")
        updated_at = _job_runtime._admin_now_iso()
        owner_instance_id = _job_runtime._admin_runtime_instance_id()
        owner_pid = os.getpid()
        owner_host = _job_runtime._admin_runtime_host()

        with closing(_admin_connect()) as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    UPDATE admin_jobs
                    SET progress_current = ?,
                        progress_total = ?,
                        progress_stage = ?,
                        last_logged_current = ?,
                        updated_at = ?,
                        owner_instance_id = ?,
                        owner_pid = ?,
                        owner_host = ?,
                        heartbeat_at = ?
                    WHERE job_id = ?
                    """,
                    (
                        safe_current,
                        progress_total,
                        progress_stage,
                        last_logged_current,
                        updated_at,
                        owner_instance_id,
                        int(owner_pid),
                        owner_host,
                        updated_at,
                        job_id,
                    ),
                )
                conn.commit()
                updated = int(cur.rowcount or 0) > 0
            finally:
                cur.close()

        if not updated:
            return False

        if isinstance(log_step, int) and log_step > 0:
            on_step = safe_current > 0 and safe_current % log_step == 0
        else:
            on_step = False
        is_final = isinstance(progress_total, int) and safe_current >= progress_total
        should_log = force_log or on_step or is_final

        if not auto_log or not should_log or safe_current == last_logged_current:
            return True

        if isinstance(progress_total, int):
            log_message = f"正在抓取消息（占位）：第 {safe_current}/{progress_total} 条"
        else:
            log_message = f"正在抓取消息（占位）：第 {safe_current} 条"
        stage_display = _job_runtime.ADMIN_JOB_STATUS_DISPLAY_MAP.get(
            progress_stage, progress_stage
        )
        stage_prefix = f"[{stage_display}] " if stage_display else ""
        _admin_job_append_log(job_id, f"{stage_prefix}{log_message}")

        with closing(_admin_connect()) as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    UPDATE admin_jobs
                    SET last_logged_current = ?,
                        updated_at = ?,
                        owner_instance_id = ?,
                        owner_pid = ?,
                        owner_host = ?,
                        heartbeat_at = ?
                    WHERE job_id = ?
                    """,
                    (
                        safe_current,
                        _job_runtime._admin_now_iso(),
                        owner_instance_id,
                        int(owner_pid),
                        owner_host,
                        _job_runtime._admin_now_iso(),
                        job_id,
                    ),
                )
                conn.commit()
            finally:
                cur.close()
        return True


def _admin_job_heartbeat(job_id: str) -> bool:
    with ADMIN_JOBS_LOCK:
        heartbeat_at = _job_runtime._admin_now_iso()
        owner_instance_id = _job_runtime._admin_runtime_instance_id()
        owner_pid = os.getpid()
        owner_host = _job_runtime._admin_runtime_host()
        with closing(_admin_connect()) as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    """
                    UPDATE admin_jobs
                    SET owner_instance_id = ?,
                        owner_pid = ?,
                        owner_host = ?,
                        heartbeat_at = ?,
                        updated_at = CASE
                            WHEN status IN ('queued', 'running') THEN updated_at
                            ELSE ?
                        END
                    WHERE job_id = ? AND status IN ('queued', 'running')
                    """,
                    (
                        owner_instance_id,
                        int(owner_pid),
                        owner_host,
                        heartbeat_at,
                        heartbeat_at,
                        job_id,
                    ),
                )
                conn.commit()
                return int(cur.rowcount or 0) > 0
            finally:
                cur.close()


def _admin_job_get_snapshot(job_id: str) -> dict[str, Any] | None:
    row = _admin_fetch_job_snapshot_row(job_id)
    if row is None:
        return None

    with ADMIN_JOBS_LOCK:
        cache_entry = _admin_cache_entry_locked(job_id)
        cache_entry["next_log_seq"] = _row_int(row, "last_seq") + 1
    return _admin_snapshot_from_row(row)


def _admin_job_get_logs(
    job_id: str, after_seq: int = 0
) -> list[dict[str, Any]] | None:
    snapshot = _admin_job_get_snapshot(job_id)
    if snapshot is None:
        return None

    with closing(_admin_connect()) as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT seq, ts, message
                FROM admin_job_logs
                WHERE job_id = ? AND seq > ?
                ORDER BY seq ASC
                """,
                (job_id, max(int(after_seq), 0)),
            )
            rows = cur.fetchall()
            return [
                {
                    "seq": _row_int(row, "seq"),
                    "ts": str(row["ts"] or ""),
                    "message": str(row["message"] or ""),
                }
                for row in rows
            ]
        finally:
            cur.close()


def _admin_recover_interrupted_jobs_locked(*, include_foreign_owner: bool) -> int:
    now = datetime.now(UTC)
    current_instance_id = _job_runtime._admin_runtime_instance_id()
    with closing(_admin_connect()) as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT
                    job_id,
                    owner_instance_id,
                    owner_pid,
                    owner_host,
                    heartbeat_at,
                    updated_at,
                    created_at
                FROM admin_jobs
                WHERE status IN ('queued', 'running')
                ORDER BY created_at ASC
                """
            )
            rows = list(cur.fetchall())
        finally:
            cur.close()

    recovered = 0
    for row in rows:
        job_id = str(row["job_id"] or "")
        if not job_id:
            continue
        created_at = _job_runtime._admin_parse_timestamp(row["created_at"], now)
        updated_at = _job_runtime._admin_parse_timestamp(row["updated_at"], created_at)
        heartbeat_at = _job_runtime._admin_parse_timestamp(
            row["heartbeat_at"], updated_at
        )
        is_stale = now - heartbeat_at > timedelta(
            seconds=_job_runtime.ADMIN_JOB_STALE_AFTER_SECONDS
        )
        owner_instance_id = str(row["owner_instance_id"] or "").strip()
        owner_is_confirmed_dead = (
            include_foreign_owner
            and owner_instance_id != current_instance_id
            and _job_runtime._admin_owner_is_alive(
                row["owner_pid"], row["owner_host"]
            )
            is False
        )
        if not is_stale and not owner_is_confirmed_dead:
            continue
        _admin_job_set_status(job_id, "error")
        _admin_mark_interrupted_clone_records(job_id, now=now)
        _admin_job_append_log(
            job_id,
            "任务因服务进程重启或退出而中断；已标记为失败，请按需要重新发起。",
        )
        recovered += 1
    _admin_reconcile_terminal_clone_records(now=now)
    return recovered


def _admin_table_has_column(
    cur: sqlite3.Cursor,
    table_name: str,
    column_name: str,
) -> bool:
    """Allow job recovery to run against a database still on an older schema."""
    try:
        cur.execute(f"PRAGMA table_info({table_name})")
        return any(str(row[1] or "") == column_name for row in cur.fetchall())
    except sqlite3.Error:
        return False


def _admin_mark_interrupted_clone_records(job_id: str, *, now: datetime) -> None:
    """Keep clone-specific records consistent with a recovered admin job."""
    message = "任务因服务进程重启或退出而中断，请重新发起以恢复未完成步骤"
    now_text = now.isoformat()
    with closing(_admin_connect()) as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE admin_clone_migrations
                SET status = 'error', phase = 'interrupted', error_message = ?,
                    updated_at = ?, completed_at = ?
                WHERE job_id = ? AND status IN ('queued', 'running')
                """,
                (message, now_text, now_text, str(job_id)),
            )
            cur.execute(
                """
                UPDATE admin_clone_plans
                SET status = 'error', error_message = ?, updated_at = ?, completed_at = ?
                WHERE job_id = ? AND status IN ('queued', 'running')
                """,
                (message, now_text, now_text, str(job_id)),
            )
            cur.execute(
                """
                UPDATE admin_clone_runs
                SET status = 'error', phase = 'interrupted', error_message = ?,
                    updated_at = ?, completed_at = ?
                WHERE job_id = ? AND status IN ('queued', 'running')
                """,
                (message, now_text, now_text, str(job_id)),
            )
            if _admin_table_has_column(
                cur,
                "admin_clone_runs",
                "deletion_job_id",
            ):
                cur.execute(
                    """
                    UPDATE admin_clone_runs
                    SET status = 'error', phase = 'interrupted', error_message = ?,
                        updated_at = ?
                    WHERE deletion_job_id = ? AND status = 'deleting'
                    """,
                    (message, now_text, str(job_id)),
                )
            conn.commit()
        except sqlite3.OperationalError as exc:
            conn.rollback()
            if "no such table" not in str(exc).lower():
                raise
        finally:
            cur.close()


def _admin_reconcile_terminal_clone_records(*, now: datetime) -> None:
    """Heal clone rows left active after their owning job already failed."""
    message = "所属后台任务已失败或中断，请重新发起以恢复未完成步骤"
    now_text = now.isoformat()
    updates = [
        """
        UPDATE admin_clone_migrations
        SET status = 'error', phase = 'interrupted', error_message = ?,
            updated_at = ?, completed_at = ?
        WHERE status IN ('queued', 'running')
          AND EXISTS (
              SELECT 1 FROM admin_jobs j
              WHERE j.job_id = admin_clone_migrations.job_id
                AND j.status = 'error'
          )
        """,
        """
        UPDATE admin_clone_plans
        SET status = 'error', error_message = ?, updated_at = ?, completed_at = ?
        WHERE status IN ('queued', 'running')
          AND EXISTS (
              SELECT 1 FROM admin_jobs j
              WHERE j.job_id = admin_clone_plans.job_id
                AND j.status = 'error'
          )
        """,
        """
        UPDATE admin_clone_runs
        SET status = 'error', phase = 'interrupted', error_message = ?,
            updated_at = ?, completed_at = ?
        WHERE status IN ('queued', 'running')
          AND EXISTS (
              SELECT 1 FROM admin_jobs j
              WHERE j.job_id = admin_clone_runs.job_id
                AND j.status = 'error'
          )
        """,
    ]
    with closing(_admin_connect()) as conn:
        cur = conn.cursor()
        try:
            for sql in updates:
                cur.execute(sql, (message, now_text, now_text))
            if _admin_table_has_column(
                cur,
                "admin_clone_runs",
                "deletion_job_id",
            ):
                cur.execute(
                    """
                    UPDATE admin_clone_runs
                    SET status = 'error', phase = 'interrupted', error_message = ?,
                        updated_at = ?
                    WHERE status = 'deleting'
                      AND EXISTS (
                          SELECT 1 FROM admin_jobs j
                          WHERE j.job_id = admin_clone_runs.deletion_job_id
                            AND j.status = 'error'
                      )
                    """,
                    (message, now_text),
                )
            conn.commit()
        except sqlite3.OperationalError as exc:
            conn.rollback()
            if "no such table" not in str(exc).lower():
                raise
        finally:
            cur.close()


def _admin_recover_interrupted_jobs() -> int:
    with ADMIN_JOBS_LOCK:
        return _admin_recover_interrupted_jobs_locked(include_foreign_owner=True)
