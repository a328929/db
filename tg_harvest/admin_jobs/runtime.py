# -*- coding: utf-8 -*-
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Optional


ADMIN_JOB_ALLOWED_STATUSES = {"queued", "running", "done", "error"}

ADMIN_JOB_STATUS_DISPLAY_MAP = {
    "queued": "排队中",
    "running": "执行中",
    "done": "完成",
    "error": "失败",
    "updating": "更新中",
    "finalizing": "整理中",
    "fetching": "抓取中",
}

ADMIN_JOB_STALE_AFTER_SECONDS = 15 * 60
ADMIN_JOB_HEARTBEAT_INTERVAL_SECONDS = 30.0

_ADMIN_RUNTIME_INSTANCE_ID = f"pid-{os.getpid()}-{uuid.uuid4().hex[:8]}"


def _admin_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _admin_parse_timestamp(value: Any, default: datetime) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value or ""))
    except Exception:
        return default
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def configure_admin_job_runtime(instance_id: Optional[str] = None) -> str:
    global _ADMIN_RUNTIME_INSTANCE_ID

    normalized = str(instance_id or "").strip()
    if not normalized:
        normalized = f"pid-{os.getpid()}-{uuid.uuid4().hex[:8]}"
    _ADMIN_RUNTIME_INSTANCE_ID = normalized
    return _ADMIN_RUNTIME_INSTANCE_ID


def _admin_runtime_instance_id() -> str:
    return _ADMIN_RUNTIME_INSTANCE_ID


def _normalize_status(status: str) -> str:
    normalized = str(status or "queued").strip().lower()
    if normalized not in ADMIN_JOB_ALLOWED_STATUSES:
        return "error"
    return normalized


def _safe_progress_total(value: Any) -> Optional[int]:
    if isinstance(value, int) and value >= 0:
        return value
    return None
