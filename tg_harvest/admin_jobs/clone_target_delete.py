import logging
from collections.abc import Callable
from contextlib import closing, suppress
from typing import Any

from telethon.tl.functions.channels import DeleteChannelRequest
from telethon.tl.types import InputChannel, InputPeerChannel

from tg_harvest.admin_jobs.clone import _cfg_with_session_name
from tg_harvest.admin_jobs.common import (
    admin_error_message,
    finish_job_heartbeat,
    mark_admin_job_running,
    start_admin_job_heartbeat,
    start_admin_job_thread,
    update_admin_job_progress,
)
from tg_harvest.admin_jobs.sessions import (
    _cleanup_isolated_worker_session,
    _create_isolated_worker_client,
    _disconnect_worker_client,
    _ensure_base_session_valid,
    bind_client_event_loop,
)
from tg_harvest.storage.clone import delete_clone_run


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _target_owner_cfg(cfg: Any, target_owner_session: Any) -> Any | None:
    primary_session = _clean_text(getattr(cfg, "session_name", ""))
    secondary_session = _clean_text(getattr(cfg, "secondary_session_name", ""))
    owner_session = _clean_text(target_owner_session)

    if not owner_session:
        owner_session = secondary_session
    if owner_session and owner_session == primary_session:
        return _cfg_with_session_name(cfg, primary_session)
    if owner_session and owner_session == secondary_session:
        return _cfg_with_session_name(cfg, secondary_session)
    return None


def _target_input_channel(client: Any, clone_run: dict) -> InputChannel | None:
    try:
        target_chat_id = int(clone_run.get("target_chat_id") or 0)
    except (TypeError, ValueError):
        return None
    if target_chat_id <= 0:
        return None

    try:
        access_hash = int(clone_run.get("target_access_hash") or 0)
    except (TypeError, ValueError):
        access_hash = 0
    if access_hash:
        return InputChannel(target_chat_id, access_hash)

    with bind_client_event_loop(client):
        cached_entity = client.get_input_entity(target_chat_id)
    if isinstance(cached_entity, InputChannel):
        return cached_entity
    if isinstance(cached_entity, InputPeerChannel):
        return InputChannel(cached_entity.channel_id, cached_entity.access_hash)
    return None


def _remote_delete_outcome(exc: Exception) -> tuple[str, str]:
    error_text = f"{exc.__class__.__name__}: {exc}".lower().replace("_", "")
    if any(
        marker in error_text
        for marker in (
            "channelinvalid",
            "peeridinvalid",
            "could not find the input entity",
            "not exist",
            "deleted",
            "deactivated",
        )
    ):
        return "already_absent", "目标副本已解散或不存在"
    if any(
        marker in error_text
        for marker in (
            "channelprivate",
            "chatforbidden",
            "channelforbidden",
            "chatadminrequired",
            "usernotparticipant",
        )
    ):
        return "not_accessible", "创建账号已无法访问目标副本"
    return "unconfirmed", admin_error_message(exc)


def _delete_remote_clone_target(
    *,
    clone_run: dict,
    cfg: Any,
    job_id: str,
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> tuple[str, str]:
    if not clone_run.get("target_chat_id"):
        return "not_created", "这条记录没有已创建的目标副本"

    owner_cfg = _target_owner_cfg(cfg, clone_run.get("target_owner_session"))
    if owner_cfg is None:
        return "owner_unavailable", "未找到目标副本对应的已配置创建账号"
    if not _ensure_base_session_valid(owner_cfg, job_id, admin_job_append_log_fn):
        return "owner_unavailable", "目标副本创建账号当前不可用"

    client = None
    worker_id = f"{job_id}_clone_target_delete"
    try:
        client = _create_isolated_worker_client(owner_cfg, worker_id)
        channel = _target_input_channel(client, clone_run)
        if channel is None:
            return "not_accessible", "目标副本实体已失效，无法向 Telegram 确认删除"
        with bind_client_event_loop(client):
            client(DeleteChannelRequest(channel=channel))
        return "deleted", "已从 Telegram 删除目标副本"
    except Exception as exc:
        logging.info("删除 Telegram 克隆目标未获确认: job_id=%s", job_id, exc_info=True)
        return _remote_delete_outcome(exc)
    finally:
        if client is not None:
            with suppress(Exception):
                _disconnect_worker_client(client)
        with suppress(Exception):
            _cleanup_isolated_worker_session(owner_cfg, worker_id)


def _admin_clone_target_delete_job_runner(
    job_id: str,
    *,
    clone_run: dict,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    heartbeat_stop, heartbeat_thread = start_admin_job_heartbeat(job_id)
    run_id = _clean_text(clone_run.get("run_id"))
    try:
        if not run_id:
            raise RuntimeError("缺少克隆运行记录标识")
        mark_admin_job_running(
            job_id,
            admin_job_set_status_fn=admin_job_set_status_fn,
        )
        update_admin_job_progress(job_id, 0, total=2, stage="deleting_target")
        admin_job_append_log_fn(job_id, "开始删除克隆目标；不会读取或删除源群")

        remote_status, remote_message = _delete_remote_clone_target(
            clone_run=clone_run,
            cfg=cfg,
            job_id=job_id,
            admin_job_append_log_fn=admin_job_append_log_fn,
        )
        admin_job_append_log_fn(job_id, f"目标副本处理结果：{remote_message}")
        update_admin_job_progress(job_id, 1, total=2, stage="purging_local")

        with closing(get_conn_fn()) as conn:
            deleted = delete_clone_run(conn, run_id=run_id)
        if not deleted:
            raise RuntimeError("克隆记录不存在或本地清理未完成")

        admin_job_append_log_fn(
            job_id,
            "已清除克隆记录、迁移计划、消息映射及关联任务历史；源群数据未改动",
        )
        update_admin_job_progress(job_id, 2, total=2, stage="done")
        admin_job_set_status_fn(job_id, "done")
        if remote_status == "unconfirmed":
            admin_job_append_log_fn(
                job_id,
                "无法确认 Telegram 远端删除结果，但本地克隆痕迹已按请求清除",
            )
    except Exception as exc:
        message = admin_error_message(exc)
        logging.exception("删除克隆目标任务失败: job_id=%s", job_id)
        admin_job_append_log_fn(job_id, f"删除克隆目标失败：{message}")
        update_admin_job_progress(job_id, 0, total=2, stage="error")
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)


def _admin_start_clone_target_delete_job_thread(job_id: str, **kwargs: Any):
    return start_admin_job_thread(
        _admin_clone_target_delete_job_runner,
        job_id,
        **kwargs,
    )
