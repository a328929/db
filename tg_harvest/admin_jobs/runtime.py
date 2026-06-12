import os
import uuid
from datetime import UTC, datetime
from typing import Any

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
    return datetime.now(UTC).isoformat()


def _admin_parse_timestamp(value: Any, default: datetime) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value or ""))
    except Exception:
        return default
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def configure_admin_job_runtime(instance_id: str | None = None) -> str:
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


def _safe_progress_total(value: Any) -> int | None:
    if isinstance(value, int) and value >= 0:
        return value
    return None
