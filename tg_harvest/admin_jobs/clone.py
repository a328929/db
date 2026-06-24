import logging
from collections.abc import Callable
from contextlib import suppress
from types import SimpleNamespace
from typing import Any

from telethon.tl.functions.channels import CreateChannelRequest

from tg_harvest.admin_jobs.common import (
    admin_error_message,
    finish_job_heartbeat,
    mark_admin_job_running,
    start_admin_job_heartbeat,
    update_admin_job_progress,
)
from tg_harvest.admin_jobs.clone_job_state import (
    _clean_text,
    _load_required_record,
    _try_update_record,
    _update_required_record,
)
from tg_harvest.admin_jobs.core import _admin_job_update_progress
from tg_harvest.admin_jobs.runtime import _admin_now_iso
from tg_harvest.admin_jobs.sessions import (
    _cleanup_isolated_worker_session,
    _create_isolated_worker_client,
    _disconnect_worker_client,
    _ensure_base_session_valid,
    _start_job_heartbeat,
)
from tg_harvest.domain.coerce import optional_int
from tg_harvest.storage.clone import load_clone_source_chat, update_clone_run

CLONE_TARGET_TITLE_MAX_LEN = 128
CLONE_TARGET_ABOUT_MAX_LEN = 255


def normalize_clone_target_kind(raw_kind: Any, *, source_chat_type: Any = "") -> str:
    value = str(raw_kind or "").strip().lower()
    if value in {"channel", "broadcast"}:
        return "channel"
    if value in {"megagroup", "supergroup", "group"}:
        return "megagroup"

    source_type = str(source_chat_type or "").strip().lower()
    if "megagroup" in source_type or source_type in {"chat", "group"}:
        return "megagroup"
    return "channel"


def normalize_clone_target_title(raw_title: Any, *, fallback_title: str) -> str:
    title = str(raw_title or "").strip()
    if not title:
        title = str(fallback_title or "").strip()
    if not title:
        title = "克隆副本"
    if len(title) > CLONE_TARGET_TITLE_MAX_LEN:
        title = title[:CLONE_TARGET_TITLE_MAX_LEN].rstrip()
    return title or "克隆副本"


def _cfg_with_session_name(cfg: Any, session_name: str) -> Any:
    values = dict(getattr(cfg, "__dict__", {}) or {})
    if not values:
        values = {
            "api_id": getattr(cfg, "api_id", 0),
            "api_hash": getattr(cfg, "api_hash", ""),
        }
    values["session_name"] = session_name
    return SimpleNamespace(**values)


def _secondary_cfg_or_error(cfg: Any) -> Any:
    primary_session_name = str(getattr(cfg, "session_name", "") or "").strip()
    secondary_session_name = str(getattr(cfg, "secondary_session_name", "") or "").strip()
    if not secondary_session_name:
        raise RuntimeError("未配置 TG_SECONDARY_SESSION_NAME，无法由第二账号创建副本")
    if secondary_session_name == primary_session_name:
        raise RuntimeError("第二账号 session 与主账号相同，无法作为独立克隆目标账号")
    return _cfg_with_session_name(cfg, secondary_session_name)


def _clone_about(source_chat: dict[str, Any]) -> str:
    parts = [
        "由 tg_harvest 克隆系统创建。",
        f"源 chat_id={int(source_chat['chat_id'])}。",
        "第一版仅创建结构副本，未迁移历史消息或媒体。",
    ]
    about = " ".join(parts)
    if len(about) > CLONE_TARGET_ABOUT_MAX_LEN:
        about = about[:CLONE_TARGET_ABOUT_MAX_LEN].rstrip()
    return about


def _create_channel_request(
    *,
    title: str,
    about: str,
    target_kind: str,
) -> CreateChannelRequest:
    return CreateChannelRequest(
        title=title,
        about=about,
        broadcast=target_kind == "channel",
        megagroup=target_kind == "megagroup",
    )


def _extract_created_chat(result: Any) -> Any | None:
    chats = getattr(result, "chats", None)
    if chats is None:
        return None
    try:
        for chat in chats:
            if chat is not None:
                return chat
    except TypeError:
        return None
    return None


def _created_chat_label(created_chat: Any) -> str:
    if created_chat is None:
        return "未知目标"
    title = str(getattr(created_chat, "title", "") or "").strip()
    raw_id = getattr(created_chat, "id", "")
    if title and raw_id not in (None, ""):
        return f"{title} (ID={raw_id})"
    if title:
        return title
    if raw_id not in (None, ""):
        return f"ID={raw_id}"
    return "未知目标"


def _created_chat_id(created_chat: Any) -> int | None:
    return optional_int(getattr(created_chat, "id", None))


def _created_chat_access_hash(created_chat: Any) -> str:
    raw_hash = getattr(created_chat, "access_hash", "")
    if raw_hash in (None, ""):
        return ""
    return str(raw_hash)


def _created_chat_username(created_chat: Any) -> str:
    return _clean_text(getattr(created_chat, "username", ""))


def _created_chat_title(created_chat: Any, fallback_title: str) -> str:
    return _clean_text(getattr(created_chat, "title", "")) or fallback_title


def _try_mark_clone_run_failed(
    *,
    get_conn_fn: Callable[[], Any],
    run_id: str,
    message: str,
) -> None:
    _try_update_record(
        get_conn_fn=get_conn_fn,
        update_fn=update_clone_run,
        run_id=run_id,
        status="error",
        phase="error",
        error_message=message,
        completed_at=_admin_now_iso(),
    )


def _admin_clone_structure_job_runner(
    job_id: str,
    *,
    source_chat_id: int,
    target_title: str,
    target_kind: str,
    clone_run_id: str | None = None,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    heartbeat_stop, heartbeat_thread = start_admin_job_heartbeat(job_id)
    client = None
    worker_id = f"{job_id}_clone_structure"
    run_id = str(clone_run_id or job_id)
    try:
        mark_admin_job_running(
            job_id,
            admin_job_set_status_fn=admin_job_set_status_fn,
        )
        _update_required_record(
            get_conn_fn=get_conn_fn,
            update_fn=update_clone_run,
            missing_message="克隆运行记录不存在，已停止结构克隆",
            run_id=run_id,
            status="running",
            phase="loading_source",
        )
        update_admin_job_progress(
            job_id,
            0,
            total=4,
            stage="running",
        )

        source_chat = _load_required_record(
            get_conn_fn=get_conn_fn,
            load_fn=load_clone_source_chat,
            missing_message="源群组不存在，无法执行结构克隆",
            chat_id=int(source_chat_id),
        )

        clone_kind = normalize_clone_target_kind(
            target_kind,
            source_chat_type=source_chat.get("chat_type"),
        )
        clone_title = normalize_clone_target_title(
            target_title,
            fallback_title=str(source_chat["chat_title"]),
        )
        admin_job_append_log_fn(
            job_id,
            f"开始结构克隆：源={source_chat['chat_title']} ({source_chat['chat_id']})，目标标题={clone_title}，类型={clone_kind}",
        )
        update_admin_job_progress(
            job_id,
            1,
            total=4,
            stage="validating",
        )
        _update_required_record(
            get_conn_fn=get_conn_fn,
            update_fn=update_clone_run,
            missing_message="克隆运行记录不存在，已停止结构克隆",
            run_id=run_id,
            status="running",
            phase="validating",
        )

        secondary_cfg = _secondary_cfg_or_error(cfg)
        if not _ensure_base_session_valid(secondary_cfg, job_id, admin_job_append_log_fn):
            raise RuntimeError("第二账号会话不可用，无法创建克隆副本")

        update_admin_job_progress(
            job_id,
            2,
            total=4,
            stage="creating",
        )
        _update_required_record(
            get_conn_fn=get_conn_fn,
            update_fn=update_clone_run,
            missing_message="克隆运行记录不存在，已停止结构克隆",
            run_id=run_id,
            status="running",
            phase="creating",
            target_owner_session=str(getattr(secondary_cfg, "session_name", "") or ""),
        )
        client = _create_isolated_worker_client(secondary_cfg, worker_id)
        about = _clone_about(source_chat)
        result = client(
            _create_channel_request(
                title=clone_title,
                about=about,
                target_kind=clone_kind,
            )
        )
        created_chat = _extract_created_chat(result)
        target_chat_id = _created_chat_id(created_chat)
        if created_chat is None or target_chat_id is None:
            raise RuntimeError("Telegram 创建响应缺少目标群组实体，无法写入克隆运行记录")
        target_created_at = _admin_now_iso()
        _update_required_record(
            get_conn_fn=get_conn_fn,
            update_fn=update_clone_run,
            missing_message="克隆运行记录不存在，已停止结构克隆",
            run_id=run_id,
            status="done",
            phase="done",
            target_chat_id=target_chat_id,
            target_access_hash=_created_chat_access_hash(created_chat),
            target_title=_created_chat_title(created_chat, clone_title),
            target_kind=clone_kind,
            target_username=_created_chat_username(created_chat),
            target_owner_session=str(getattr(secondary_cfg, "session_name", "") or ""),
            target_created_at=target_created_at,
            completed_at=target_created_at,
            error_message="",
        )
        admin_job_append_log_fn(
            job_id,
            f"目标结构创建完成：{_created_chat_label(created_chat)}",
        )
        admin_job_append_log_fn(
            job_id,
            "第一版已停止在结构克隆阶段：未迁移历史消息、媒体、成员、评论、反应或原始时间。",
        )
        update_admin_job_progress(
            job_id,
            4,
            total=4,
            stage="done",
        )
        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        logging.exception("结构克隆任务失败: job_id=%s", job_id)
        message = admin_error_message(exc)
        _try_mark_clone_run_failed(
            get_conn_fn=get_conn_fn,
            run_id=run_id,
            message=message,
        )
        admin_job_append_log_fn(job_id, f"结构克隆失败：{message}")
        update_admin_job_progress(
            job_id,
            0,
            total=4,
            stage="error",
        )
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        if client:
            with suppress(Exception):
                _disconnect_worker_client(client)
        with suppress(Exception):
            secondary_cfg = _secondary_cfg_or_error(cfg)
            _cleanup_isolated_worker_session(secondary_cfg, worker_id)


def _admin_start_clone_structure_job_thread(job_id: str, **kwargs):
    from tg_harvest.admin_jobs.common import start_admin_job_thread

    return start_admin_job_thread(
        _admin_clone_structure_job_runner,
        job_id,
        **kwargs,
    )
