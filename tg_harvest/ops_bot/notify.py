from typing import Any

from tg_harvest.config import CFG
from tg_harvest.ops_bot.client import enqueue_message

_IMPORTANT_LOG_KEYWORDS = (
    "长等待",
    "FloodWait",
    "flood wait",
    "切换第二账号",
    "第二账号已接管",
    "进入长等待冷却",
)

_JOB_TYPE_LABELS = {
    "harvest": "新增群组采集",
    "update": "群组更新",
    "delete": "删除群组",
    "delete_empty_chats": "删除空群组",
    "cleanup": "清理消息",
    "cleanup_empty": "清理空内容",
    "clone_structure": "结构克隆",
    "clone_deep_preflight": "克隆深度预检",
    "clone_timeline_migration": "克隆完整时间线迁移",
    "recovery_scan": "恢复扫描",
    "recovery_restore": "恢复入库",
    "missing_chats_scan": "缺失群组扫描",
    "restricted_chats_scan": "受限群组扫描",
}

_STATUS_LABELS = {
    "queued": "排队",
    "running": "运行中",
    "done": "完成",
    "error": "失败",
}

def _snapshot_value(snapshot: dict[str, Any] | None, key: str) -> Any:
    if not isinstance(snapshot, dict):
        return None
    return snapshot.get(key)


def _job_type_label(job_type: Any) -> str:
    value = str(job_type or "unknown").strip().lower()
    return _JOB_TYPE_LABELS.get(value, value or "unknown")


def _job_target_label(snapshot: dict[str, Any] | None) -> str:
    target_label = str(_snapshot_value(snapshot, "target_label") or "").strip()
    if target_label:
        return target_label
    target_chat_id = _snapshot_value(snapshot, "target_chat_id")
    if target_chat_id is not None:
        return str(target_chat_id)
    return "-"


def _job_progress_label(snapshot: dict[str, Any] | None) -> str:
    progress = _snapshot_value(snapshot, "progress")
    if not isinstance(progress, dict):
        return "-"
    current = int(progress.get("current") or 0)
    total = progress.get("total")
    stage = str(progress.get("stage") or "").strip()
    base = f"{current}/{total}" if isinstance(total, int) else str(current)
    return f"{base} {stage}".strip()


def _job_message(
    *,
    title: str,
    job_id: str,
    snapshot: dict[str, Any] | None,
    extra: str | None = None,
) -> str:
    lines = [
        title,
        f"任务: {_job_type_label(_snapshot_value(snapshot, 'job_type'))}",
        f"ID: {job_id}",
        f"目标: {_job_target_label(snapshot)}",
        f"进度: {_job_progress_label(snapshot)}",
    ]
    if extra:
        lines.append(f"说明: {extra}")
    return "\n".join(lines)


def notify_admin_job_created(
    job_id: str,
    snapshot: dict[str, Any] | None,
    *,
    cfg: Any = CFG,
) -> bool:
    return enqueue_message(
        cfg,
        _job_message(
            title="后台任务已创建",
            job_id=str(job_id),
            snapshot=snapshot,
        ),
    )


def notify_admin_job_status(
    job_id: str,
    status: str,
    snapshot: dict[str, Any] | None,
    *,
    cfg: Any = CFG,
) -> bool:
    normalized = str(status or "").strip().lower()
    label = _STATUS_LABELS.get(normalized, normalized or "未知")
    return enqueue_message(
        cfg,
        _job_message(
            title=f"后台任务{label}",
            job_id=str(job_id),
            snapshot=snapshot,
        ),
    )


def should_notify_log_message(message: str) -> bool:
    text = str(message or "")
    folded = text.lower()
    return any(keyword.lower() in folded for keyword in _IMPORTANT_LOG_KEYWORDS)


def maybe_notify_admin_job_log(
    job_id: str,
    message: str,
    *,
    cfg: Any = CFG,
) -> bool:
    if not should_notify_log_message(message):
        return False
    return enqueue_message(
        cfg,
        f"后台任务重要日志\nID: {job_id}\n内容: {str(message or '').strip()}",
    )
