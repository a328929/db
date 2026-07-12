from contextlib import suppress
from typing import Any
from uuid import uuid4

from tg_harvest.admin_jobs.clone_execution import clone_cfg_for_account
from tg_harvest.admin_jobs.clone_target_access import clone_run_target_input_channel
from tg_harvest.admin_jobs.sessions import (
    _cleanup_isolated_worker_session,
    _create_isolated_worker_client,
    _disconnect_worker_client,
    _ensure_base_session_valid,
    bind_client_event_loop,
)
from tg_harvest.ingest.flood_wait import call_with_bounded_retry


def load_clone_target_message_count(clone_run: dict, *, cfg: Any) -> int:
    try:
        target_chat_id = int(clone_run.get("target_chat_id") or 0)
    except (TypeError, ValueError) as exc:
        raise ValueError("目标副本标识无效") from exc
    if target_chat_id <= 0:
        raise ValueError("目标副本尚未创建，不能读取消息数量")

    secondary_cfg = clone_cfg_for_account(cfg, "secondary")
    worker_id = f"clone_target_metrics_{uuid4().hex}"
    client = None
    session_messages: list[str] = []

    def append_session_message(_job_id: str, message: str) -> None:
        session_messages.append(str(message))

    try:
        if not _ensure_base_session_valid(
            secondary_cfg,
            "clone-target-message-count",
            append_session_message,
        ):
            detail = session_messages[-1] if session_messages else "第二账号会话不可用"
            raise RuntimeError(detail)

        client = _create_isolated_worker_client(secondary_cfg, worker_id)
        target_channel = clone_run_target_input_channel(client, clone_run)
        if target_channel is None:
            raise RuntimeError("无法解析目标副本实体，请先用第二账号打开一次目标群后重试")

        with bind_client_event_loop(client):
            messages = call_with_bounded_retry(
                client.get_messages,
                target_channel,
                limit=0,
                flood_wait_threshold_seconds=getattr(
                    secondary_cfg,
                    "flood_wait_switch_threshold",
                    30,
                ),
                account_label="secondary",
                scope="clone-target-message-count",
            )
        try:
            count = int(getattr(messages, "total", -1))
        except (TypeError, ValueError) as exc:
            raise RuntimeError("Telegram 未返回目标副本消息总数") from exc
        if count < 0:
            raise RuntimeError("Telegram 未返回目标副本消息总数")
        return count
    finally:
        if client is not None:
            with suppress(Exception):
                _disconnect_worker_client(client)
        with suppress(Exception):
            _cleanup_isolated_worker_session(secondary_cfg, worker_id)
