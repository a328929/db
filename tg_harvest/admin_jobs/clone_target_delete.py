import logging
from collections.abc import Callable
from contextlib import closing, suppress
from typing import Any

from telethon.tl.functions.channels import DeleteChannelRequest

from tg_harvest.admin_jobs.clone import _cfg_with_session_name
from tg_harvest.admin_jobs.clone_target_access import (
    clone_run_target_conflicts_with_source,
    clone_run_target_input_channel,
)
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
from tg_harvest.storage.clone import (
    claim_clone_run_for_deletion,
    delete_clone_run,
)


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

    if clone_run_target_conflicts_with_source(clone_run):
        return (
            "source_identity_conflict",
            "目标副本 ID 与源群 ID 冲突，已拒绝向 Telegram 发出删除请求",
        )

    owner_cfg = _target_owner_cfg(cfg, clone_run.get("target_owner_session"))
    if owner_cfg is None:
        return "owner_unavailable", "未找到目标副本对应的已配置创建账号"
    if not _ensure_base_session_valid(owner_cfg, job_id, admin_job_append_log_fn):
        return "owner_unavailable", "目标副本创建账号当前不可用"

    client = None
    worker_id = f"{job_id}_clone_target_delete"
    try:
        client = _create_isolated_worker_client(owner_cfg, worker_id)
        channel = clone_run_target_input_channel(client, clone_run)
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


def _safe_target_delete_log(
    append_log_fn: Callable[[str, str], Any], job_id: str, message: str
) -> None:
    try:
        append_log_fn(job_id, message)
    except Exception:
        logging.exception("记录克隆目标删除任务日志失败: job_id=%s", job_id)


def _safe_target_delete_progress(
    job_id: str, current: int, *, total: int, stage: str
) -> None:
    try:
        update_admin_job_progress(job_id, current, total=total, stage=stage)
    except Exception:
        logging.exception(
            "更新克隆目标删除任务进度失败: job_id=%s stage=%s", job_id, stage
        )


def _safe_target_delete_status(
    set_status_fn: Callable[[str, str], bool], job_id: str, status: str
) -> None:
    try:
        if not set_status_fn(job_id, status):
            logging.warning(
                "克隆目标删除任务状态回调未确认成功: job_id=%s status=%s",
                job_id,
                status,
            )
    except Exception:
        logging.exception(
            "更新克隆目标删除任务状态失败: job_id=%s status=%s",
            job_id,
            status,
        )


def _try_mark_claimed_clone_delete_failed(
    *,
    get_conn_fn: Callable[[], Any],
    run_id: str,
    message: str,
    job_id: str,
) -> None:
    try:
        with closing(get_conn_fn()) as conn:
            cur = conn.cursor()
            cur.execute("PRAGMA table_info(admin_clone_runs)")
            columns = {str(row[1] or "") for row in cur.fetchall()}
            if "deletion_job_id" in columns:
                cur.execute(
                    """
                    UPDATE admin_clone_runs
                    SET status = 'error', phase = 'delete_error', error_message = ?,
                        updated_at = datetime('now')
                    WHERE run_id = ?
                      AND deletion_job_id = ?
                      AND status = 'deleting'
                    """,
                    (message, run_id, job_id),
                )
            else:
                # A pre-token database cannot prove ownership; preserve the
                # legacy best-effort status repair without failing the job.
                cur.execute(
                    """
                    UPDATE admin_clone_runs
                    SET status = 'error', phase = 'delete_error', error_message = ?,
                        updated_at = datetime('now')
                    WHERE run_id = ? AND status = 'deleting'
                    """,
                    (message, run_id),
                )
            updated = int(cur.rowcount or 0) > 0
            cur.close()
            conn.commit()
        if not updated:
            logging.warning(
                "删除失败状态未写入，克隆记录已被清理或删除任务已被接管: "
                "run_id=%s job_id=%s",
                run_id,
                job_id,
            )
    except Exception:
        logging.exception("记录克隆目标删除失败状态失败: run_id=%s", run_id)


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
    deletion_claimed = False
    local_purge_completed = False
    try:
        if not run_id:
            raise RuntimeError("缺少克隆运行记录标识")
        mark_admin_job_running(
            job_id,
            admin_job_set_status_fn=admin_job_set_status_fn,
        )
        update_admin_job_progress(job_id, 0, total=2, stage="deleting_target")
        admin_job_append_log_fn(job_id, "开始删除克隆目标；不会读取或删除源群")

        with closing(get_conn_fn()) as conn:
            if not claim_clone_run_for_deletion(
                conn,
                run_id=run_id,
                job_id=job_id,
            ):
                raise RuntimeError("克隆记录不存在或已被其他删除任务占用")
            deletion_claimed = True

        remote_status, remote_message = _delete_remote_clone_target(
            clone_run=clone_run,
            cfg=cfg,
            job_id=job_id,
            admin_job_append_log_fn=admin_job_append_log_fn,
        )
        _safe_target_delete_log(
            admin_job_append_log_fn,
            job_id,
            f"目标副本处理结果：{remote_message}",
        )
        _safe_target_delete_progress(job_id, 1, total=2, stage="purging_local")

        with closing(get_conn_fn()) as conn:
            deleted = delete_clone_run(conn, run_id=run_id, job_id=job_id)
        if not deleted:
            raise RuntimeError("克隆记录不存在或本地清理未完成")
        local_purge_completed = True

        admin_job_append_log_fn(
            job_id,
            "已清除克隆记录、迁移计划、消息映射及关联任务历史；源群数据未改动",
        )
        update_admin_job_progress(job_id, 2, total=2, stage="done")
        if not admin_job_set_status_fn(job_id, "done"):
            raise RuntimeError("克隆目标删除任务完成状态回调未确认成功")
        if remote_status == "unconfirmed":
            admin_job_append_log_fn(
                job_id,
                "无法确认 Telegram 远端删除结果，但本地克隆痕迹已按请求清除",
            )
    except Exception as exc:
        message = admin_error_message(exc)
        logging.exception("删除克隆目标任务失败: job_id=%s", job_id)
        if local_purge_completed:
            logging.error(
                "克隆目标已完成本地清理，但后续状态汇报失败: job_id=%s",
                job_id,
            )
            _safe_target_delete_progress(job_id, 2, total=2, stage="done")
            _safe_target_delete_status(admin_job_set_status_fn, job_id, "done")
        else:
            if deletion_claimed:
                _try_mark_claimed_clone_delete_failed(
                    get_conn_fn=get_conn_fn,
                    run_id=run_id,
                    message=message,
                    job_id=job_id,
                )
            _safe_target_delete_log(
                admin_job_append_log_fn,
                job_id,
                f"删除克隆目标失败：{message}",
            )
            _safe_target_delete_progress(job_id, 0, total=2, stage="error")
            _safe_target_delete_status(admin_job_set_status_fn, job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)


def _admin_start_clone_target_delete_job_thread(job_id: str, **kwargs: Any):
    return start_admin_job_thread(
        _admin_clone_target_delete_job_runner,
        job_id,
        **kwargs,
    )
