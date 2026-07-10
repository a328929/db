from __future__ import annotations

import logging
from collections.abc import Callable, Iterable
from typing import Any

from flask import jsonify, request


def json_error(message: Any, status_code: int, **extra: Any):
    payload = {"ok": False, "error": str(message)}
    payload.update(extra)
    return jsonify(payload), int(status_code)


def require_json_dict():
    if not request.is_json:
        return None, json_error("请求必须为 JSON", 400)

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return None, json_error("请求 JSON 格式错误", 400)
    return data, None


def logged_json_error(
    logger: Any,
    log_message: str,
    error_message: str | None = None,
    status_code: int = 500,
):
    logger.exception(log_message)
    return json_error(error_message or log_message, status_code)


def job_conflict_response(existing_job: Any):
    return json_error(
        "当前已有进行中的任务，请等待完成后再试",
        409,
        existing_job=existing_job,
    )


def create_exclusive_job_or_response(
    create_job_fn: Callable[..., tuple[dict[str, Any] | None, dict[str, Any] | None]],
    job_type: str,
    *,
    target_chat_id: int | None = None,
    target_label: str | None = None,
) -> tuple[str | None, Any]:
    job, existing_job = create_job_fn(
        job_type,
        target_chat_id=target_chat_id,
        target_label=target_label,
    )
    if existing_job is not None:
        return None, job_conflict_response(existing_job)

    job_id = str((job or {}).get("job_id") or "")
    if not job_id:
        return None, json_error("任务创建失败", 500)
    return job_id, None


def created_job_snapshot_response(
    job_id: str,
    get_snapshot_fn: Callable[[str], dict[str, Any] | None],
    **extra: Any,
):
    snapshot = get_snapshot_fn(job_id)
    if snapshot is None:
        return json_error("任务创建失败", 500)

    payload = {"ok": True, "job": snapshot}
    payload.update(extra)
    return jsonify(payload)


def create_started_exclusive_job_response(
    create_job_fn: Callable[..., tuple[dict[str, Any] | None, dict[str, Any] | None]],
    get_snapshot_fn: Callable[[str], dict[str, Any] | None],
    *,
    job_type: str,
    target_chat_id: int | None = None,
    target_label: str | None = None,
    append_log_fn: Callable[[str, str], None],
    set_status_fn: Callable[[str, str], Any],
    initial_logs: Iterable[str] = (),
    start_job_fn: Callable[[str], Any],
    response_extra: dict[str, Any] | None = None,
):
    job_id, error_response = create_exclusive_job_or_response(
        create_job_fn,
        job_type,
        target_chat_id=target_chat_id,
        target_label=target_label,
    )
    if error_response is not None:
        return error_response

    try:
        for message in initial_logs:
            append_log_fn(job_id, str(message))
        start_job_fn(job_id)
    except Exception:
        logging.exception("后台任务启动失败: job_id=%s", job_id)
        try:
            append_log_fn(job_id, "后台任务启动失败，已标记为失败")
        except Exception:
            logging.exception("记录后台任务启动失败日志失败: job_id=%s", job_id)
        try:
            set_status_fn(job_id, "error")
        except Exception:
            logging.exception("标记后台任务启动失败状态失败: job_id=%s", job_id)
        return json_error("任务启动失败", 500)

    return created_job_snapshot_response(
        job_id,
        get_snapshot_fn,
        **(response_extra or {}),
    )
