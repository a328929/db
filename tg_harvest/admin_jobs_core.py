# -*- coding: utf-8 -*-
import importlib
import logging
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

ADMIN_JOBS: Dict[str, Dict[str, Any]] = {}
ADMIN_JOBS_LOCK = threading.Lock()
ADMIN_JOB_LOG_MAX_LINES = 200
ADMIN_JOB_MAX_COUNT = 100
ADMIN_JOB_ALLOWED_STATUSES = {"queued", "running", "done", "error"}
ADMIN_PROGRESS_LOG_STEP_FALLBACK = 1000
ADMIN_JOB_STATUS_DISPLAY_MAP = {
    "queued": "排队中",
    "running": "执行中",
    "done": "完成",
    "error": "失败",
}


class _AdminJobThreadLogHandler(logging.Handler):
    def __init__(self, job_id: str, thread_ident: Optional[int] = None) -> None:
        super().__init__()
        self._job_id = str(job_id)
        self._thread_ident = int(thread_ident) if isinstance(thread_ident, int) else threading.get_ident()

    def emit(self, record: logging.LogRecord) -> None:
        if threading.get_ident() != self._thread_ident:
            return
        message = record.getMessage()
        _admin_job_append_log(self._job_id, message)


def _admin_make_job_log_handler(job_id: str, thread_ident: Optional[int] = None) -> logging.Handler:
    return _AdminJobThreadLogHandler(job_id=job_id, thread_ident=thread_ident)


def _admin_get_progress_log_step() -> int:
    try:
        config_module = importlib.import_module("tg_harvest.config")
        cfg = getattr(config_module, "CFG", None)
        step = int(getattr(cfg, "log_every", ADMIN_PROGRESS_LOG_STEP_FALLBACK))
        if step > 0:
            return step
    except Exception:
        pass
    return ADMIN_PROGRESS_LOG_STEP_FALLBACK


def _admin_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _admin_job_trim_locked() -> None:
    # 仅裁剪终态任务（done/error），避免误删 queued/running 导致轮询中途 404。
    terminal_statuses = {"done", "error"}
    while len(ADMIN_JOBS) > ADMIN_JOB_MAX_COUNT:
        removable_job_id = None
        for job_id, job in ADMIN_JOBS.items():
            status = str((job or {}).get("status") or "").lower()
            if status in terminal_statuses:
                removable_job_id = job_id
                break

        if removable_job_id is None:
            # 当前超上限但无可删终态任务：保护 active 任务，暂不裁剪。
            return
        ADMIN_JOBS.pop(removable_job_id, None)


def _admin_job_append_log_locked(job: Dict[str, Any], message: str) -> Dict[str, Any]:
    next_seq = int(job.get("next_log_seq", 1))
    log_item = {
        "seq": next_seq,
        "ts": _admin_now_iso(),
        "message": str(message),
    }
    logs = job.setdefault("logs", [])
    logs.append(log_item)
    if len(logs) > ADMIN_JOB_LOG_MAX_LINES:
        del logs[: len(logs) - ADMIN_JOB_LOG_MAX_LINES]

    job["next_log_seq"] = next_seq + 1
    job["updated_at"] = log_item["ts"]
    return dict(log_item)


def _admin_job_get_snapshot_locked(job: Dict[str, Any]) -> Dict[str, Any]:
    progress = dict(job.get("progress") or {})
    return {
        "job_id": str(job.get("job_id", "")),
        "job_type": str(job.get("job_type", "unknown")),
        "status": str(job.get("status", "queued")),
        "target_chat_id": job.get("target_chat_id"),
        "target_label": job.get("target_label"),
        "created_at": str(job.get("created_at", "")),
        "updated_at": str(job.get("updated_at", "")),
        "progress": {
            "current": int(progress.get("current") or 0),
            "total": progress.get("total"),
            "stage": str(progress.get("stage") or "queued"),
        },
        "log_count": len(job.get("logs", [])),
        "last_seq": int(job.get("next_log_seq", 1)) - 1,
    }


def _admin_job_create_locked(job_type: str, target_chat_id: Optional[int] = None, target_label: Optional[str] = None) -> Dict[str, Any]:
    created_at = _admin_now_iso()
    job_id = uuid.uuid4().hex
    job: Dict[str, Any] = {
        "job_id": job_id,
        "job_type": str(job_type or "unknown"),
        "status": "queued",
        "target_chat_id": target_chat_id,
        "target_label": (target_label or "").strip() or None,
        "created_at": created_at,
        "updated_at": created_at,
        "logs": [],
        "next_log_seq": 1,
        "progress": {
            "current": 0,
            "total": None,
            "stage": "queued",
            "last_logged_current": 0,
        },
    }
    ADMIN_JOBS[job_id] = job
    _admin_job_trim_locked()
    _admin_job_append_log_locked(job, "任务已创建（占位）")
    return _admin_job_get_snapshot_locked(job)


def _admin_job_create(job_type: str, target_chat_id: Optional[int] = None, target_label: Optional[str] = None) -> Dict[str, Any]:
    with ADMIN_JOBS_LOCK:
        return _admin_job_create_locked(job_type=job_type, target_chat_id=target_chat_id, target_label=target_label)


def _admin_find_active_chat_job_locked(chat_id: int) -> Optional[Dict[str, Any]]:
    active_statuses = {"queued", "running"}
    guarded_job_types = {"update", "delete"}
    for job in ADMIN_JOBS.values():
        if not isinstance(job, dict):
            continue
        if job.get("target_chat_id") != chat_id:
            continue
        job_type = str(job.get("job_type") or "").lower()
        if job_type not in guarded_job_types:
            continue
        status = str(job.get("status") or "").lower()
        if status not in active_statuses:
            continue
        return {
            "job_id": str(job.get("job_id") or ""),
            "job_type": job_type,
            "status": status,
        }
    return None


# 防止同一 chat_id 同时存在并发 update/delete 活跃任务（queued/running）。
def _admin_create_chat_job_if_absent(job_type: str, chat_id: int, target_label: Optional[str] = None) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    with ADMIN_JOBS_LOCK:
        existing_job = _admin_find_active_chat_job_locked(chat_id)
        if existing_job is not None:
            return None, existing_job
        created_job = _admin_job_create_locked(job_type=job_type, target_chat_id=chat_id, target_label=target_label)
        return created_job, None


def _admin_has_any_active_job() -> bool:
    with ADMIN_JOBS_LOCK:
        for job in ADMIN_JOBS.values():
            if isinstance(job, dict) and str(job.get("status") or "").lower() in {"queued", "running"}:
                return True
    return False


def _admin_find_active_chat_job(chat_id: int) -> Optional[Dict[str, Any]]:
    with ADMIN_JOBS_LOCK:
        return _admin_find_active_chat_job_locked(chat_id)


def _admin_job_append_log(job_id: str, message: str) -> Optional[Dict[str, Any]]:
    with ADMIN_JOBS_LOCK:
        job = ADMIN_JOBS.get(job_id)
        if job is None:
            return None
        return _admin_job_append_log_locked(job, message)


def _admin_job_set_status(job_id: str, status: str) -> bool:
    with ADMIN_JOBS_LOCK:
        job = ADMIN_JOBS.get(job_id)
        if job is None:
            return False
        # 后端状态契约收口，避免非法状态破坏前端轮询白名单逻辑。
        normalized_status = str(status or "queued").strip().lower()
        if normalized_status not in ADMIN_JOB_ALLOWED_STATUSES:
            normalized_status = "error"
        job["status"] = normalized_status
        job["updated_at"] = _admin_now_iso()
        return True


def _admin_job_update_progress(
    job_id: str,
    current: int,
    total: Optional[int] = None,
    stage: Optional[str] = None,
    log_step: int = 1000,
    force_log: bool = False,
) -> bool:
    should_log = False
    log_message: Optional[str] = None
    with ADMIN_JOBS_LOCK:
        job = ADMIN_JOBS.get(job_id)
        if job is None:
            return False

        safe_current = max(int(current), 0)
        safe_total = int(total) if isinstance(total, int) and total >= 0 else None

        progress = job.setdefault(
            "progress",
            {
                "current": 0,
                "total": None,
                "stage": "queued",
                "last_logged_current": 0,
            },
        )
        progress["current"] = safe_current
        if total is not None:
            progress["total"] = safe_total
        if stage is not None:
            progress["stage"] = str(stage)
        job["updated_at"] = _admin_now_iso()

        progress_total = progress.get("total")
        last_logged_current = int(progress.get("last_logged_current") or 0)
        progress_stage = str(progress.get("stage") or "running")
        if isinstance(log_step, int) and log_step > 0:
            on_step = safe_current > 0 and safe_current % log_step == 0
        else:
            on_step = False
        is_final = isinstance(progress_total, int) and safe_current >= progress_total
        should_log = force_log or on_step or is_final

        if should_log and safe_current != last_logged_current:
            if isinstance(progress_total, int):
                log_message = f"正在抓取消息（占位）：第 {safe_current}/{progress_total} 条"
            else:
                log_message = f"正在抓取消息（占位）：第 {safe_current} 条"
            progress["last_logged_current"] = safe_current
        else:
            should_log = False

    if should_log and log_message:
        stage_display = ADMIN_JOB_STATUS_DISPLAY_MAP.get(progress_stage, progress_stage)
        stage_prefix = f"[{stage_display}] " if stage_display else ""
        _admin_job_append_log(job_id, f"{stage_prefix}{log_message}")
    return True


def _admin_job_get_snapshot(job_id: str) -> Optional[Dict[str, Any]]:
    with ADMIN_JOBS_LOCK:
        job = ADMIN_JOBS.get(job_id)
        if job is None:
            return None
        return _admin_job_get_snapshot_locked(job)


def _admin_job_get_logs(job_id: str, after_seq: int = 0) -> Optional[List[Dict[str, Any]]]:
    with ADMIN_JOBS_LOCK:
        job = ADMIN_JOBS.get(job_id)
        if job is None:
            return None
        logs = job.get("logs", [])
        return [dict(item) for item in logs if int(item.get("seq", 0)) > after_seq]
