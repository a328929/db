from collections.abc import Callable
from typing import Any

from tg_harvest.admin_jobs.common import call_with_conn
from tg_harvest.admin_jobs.clone_job_state import (
    _clean_text,
    _load_required_record,
    _try_update_record,
    _update_required_record,
)
from tg_harvest.admin_jobs.clone_media_copy import clone_sent_message_ids
from tg_harvest.admin_jobs.runtime import _admin_now_iso
from tg_harvest.admin_jobs.clone_timeline_types import (
    TimelineExecutionState,
    TimelineFinalStatus,
)
from tg_harvest.domain.clone_plan import (
    CLONE_MEDIA_STRATEGY_RELAY_COPY_WITHOUT_ATTRIBUTION,
    CLONE_MEDIA_STRATEGY_SOURCE_COPY_WITHOUT_ATTRIBUTION,
    clone_plan_blocking_issues,
    clone_plan_media_execution_label,
    clone_plan_media_relay_ready,
    clone_plan_media_source_account,
    clone_plan_media_target_account,
    clone_plan_target_write_account,
    clone_plan_uses_media_relay,
)
from tg_harvest.storage.clone import (
    build_clone_timeline_replay_preview,
    load_clone_migration,
    load_clone_plan,
    load_clone_run,
    update_clone_migration,
)


def first_required_target_message_id(result: Any, context: str) -> int:
    target_message_id = (clone_sent_message_ids(result) or [None])[0]
    if target_message_id is None:
        raise RuntimeError(f"{context} 未返回目标消息 ID")
    return int(target_message_id)


def load_required_state(
    *,
    get_conn_fn: Callable[[], Any],
    run_id: str,
    plan_id: str,
    migration_id: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    run = _load_required_record(
        get_conn_fn=get_conn_fn,
        load_fn=load_clone_run,
        missing_message="克隆运行记录不存在，无法执行完整时间线迁移",
        run_id=run_id,
    )
    plan = _load_required_record(
        get_conn_fn=get_conn_fn,
        load_fn=load_clone_plan,
        missing_message="克隆迁移计划不存在，无法执行完整时间线迁移",
        plan_id=plan_id,
    )
    migration = _load_required_record(
        get_conn_fn=get_conn_fn,
        load_fn=load_clone_migration,
        missing_message="克隆时间线迁移记录不存在，无法执行完整时间线迁移",
        migration_id=migration_id,
    )
    return run, plan, migration


def update_migration_required(
    *,
    get_conn_fn: Callable[[], Any],
    migration_id: str,
    **kwargs: Any,
) -> dict[str, Any]:
    return _update_required_record(
        get_conn_fn=get_conn_fn,
        update_fn=update_clone_migration,
        missing_message="克隆时间线迁移记录不存在，已停止完整时间线迁移",
        migration_id=migration_id,
        **kwargs,
    )


def try_mark_migration_failed(
    *,
    get_conn_fn: Callable[[], Any],
    migration_id: str,
    message: str,
) -> None:
    _try_update_record(
        get_conn_fn=get_conn_fn,
        update_fn=update_clone_migration,
        migration_id=migration_id,
        status="error",
        phase="error",
        error_message=message,
        completed_at=_admin_now_iso(),
    )


def timeline_preview(
    *,
    get_conn_fn: Callable[[], Any],
    run_id: str,
    source_chat_id: int,
) -> dict[str, Any]:
    return call_with_conn(
        get_conn_fn,
        build_clone_timeline_replay_preview,
        run_id=run_id,
        source_chat_id=source_chat_id,
    )


def validate_plan_for_timeline(
    *,
    plan: dict[str, Any],
    preview: dict[str, Any],
) -> dict[str, str]:
    if plan.get("status") != "done":
        raise RuntimeError("最新迁移计划尚未完成，请先执行在线深度预检")
    if clone_plan_blocking_issues(plan):
        raise RuntimeError("最新迁移计划存在阻断项，不能执行完整时间线迁移")
    if plan.get("target_access") != "ok":
        raise RuntimeError("目标副本不可访问，不能执行完整时间线迁移")

    text_remaining = int(preview.get("text_remaining") or 0)
    media_remaining = int(preview.get("media_remaining") or 0)
    if text_remaining <= 0 and media_remaining <= 0:
        raise RuntimeError("没有剩余可迁移时间线消息")

    text_account = ""
    if text_remaining > 0:
        if plan.get("text_strategy") != "database_replay":
            raise RuntimeError("最新迁移计划不允许数据库文本重放")
        text_account = clone_plan_target_write_account(plan)
        if not text_account:
            raise RuntimeError("最新迁移计划缺少可写目标账号")

    media_execution_account = ""
    media_source_account = ""
    media_target_account = ""
    if media_remaining > 0:
        if plan.get("source_access") != "ok":
            raise RuntimeError("源群不可访问，不能执行媒体时间线复制")
        if plan.get("media_strategy") not in {
            CLONE_MEDIA_STRATEGY_SOURCE_COPY_WITHOUT_ATTRIBUTION,
            CLONE_MEDIA_STRATEGY_RELAY_COPY_WITHOUT_ATTRIBUTION,
        }:
            raise RuntimeError("最新迁移计划不允许隐藏来源媒体复制，请重新执行在线深度预检")
        if clone_plan_uses_media_relay(plan) and not clone_plan_media_relay_ready(plan):
            raise RuntimeError("固定中转频道桥接计划未就绪，请重新执行在线深度预检")
        media_execution_account = clone_plan_media_execution_label(plan)
        media_source_account = clone_plan_media_source_account(plan)
        media_target_account = clone_plan_media_target_account(plan)
        if not media_execution_account or not media_source_account or not media_target_account:
            raise RuntimeError("最新迁移计划缺少媒体迁移账号")

    return {
        "text_account": text_account,
        "media_execution_account": media_execution_account,
        "media_source_account": media_source_account,
        "media_target_account": media_target_account,
    }


def timeline_execution_label(accounts: dict[str, str]) -> str:
    labels = []
    if accounts.get("text_account"):
        labels.append(f"text:{accounts['text_account']}")
    if accounts.get("media_execution_account"):
        labels.append(f"media:{accounts['media_execution_account']}")
    return "; ".join(labels)


def build_execution_state(
    *,
    job_id: str,
    run_id: str,
    plan_id: str,
    migration_id: str,
    run: dict[str, Any],
    plan: dict[str, Any],
    preview: dict[str, Any],
    accounts: dict[str, str],
    normalized_message_limit: int,
    normalized_send_delay_ms: int,
) -> TimelineExecutionState:
    source_chat_id = int(run["source_chat_id"])
    target_chat_id = int(run.get("target_chat_id") or 0)
    text_total = int(preview.get("text_total") or 0)
    media_total = int(preview.get("media_total") or 0)
    media_group_total = int(preview.get("media_group_total") or 0)
    progress_total = int(preview.get("timeline_remaining") or 0)
    if normalized_message_limit > 0:
        progress_total = min(progress_total, normalized_message_limit)
    return TimelineExecutionState(
        job_id=job_id,
        run_id=run_id,
        plan_id=plan_id,
        migration_id=migration_id,
        source_chat_id=source_chat_id,
        target_chat_id=target_chat_id,
        target_title=str(run.get("target_title") or target_chat_id),
        source_title=str(run.get("source_title") or source_chat_id),
        preview=preview,
        plan=plan,
        run=run,
        accounts=accounts,
        using_relay=clone_plan_uses_media_relay(plan)
        and bool(accounts.get("media_source_account")),
        normalized_message_limit=normalized_message_limit,
        normalized_send_delay_ms=normalized_send_delay_ms,
        text_total=text_total,
        media_total=media_total,
        media_group_total=media_group_total,
        progress_total=progress_total,
    )


def resolve_final_status(state: TimelineExecutionState) -> TimelineFinalStatus:
    counters = state.counters
    if state.stopped:
        return TimelineFinalStatus(
            status="error",
            phase="stopped",
            error_message="用户请求停止，完整时间线迁移已在安全边界收尾",
        )
    if (
        counters.text_failed > 0
        or counters.media_failed > 0
        or counters.media_group_failed > 0
    ):
        return TimelineFinalStatus(
            status="error",
            phase="error",
            error_message=(
                f"完整时间线迁移完成但有 {counters.text_failed} 条文本、"
                f"{counters.media_failed} 条媒体或 {counters.media_group_failed} 个媒体组失败"
            ),
        )
    if state.limit_reached:
        return TimelineFinalStatus(status="done", phase="limited_done", error_message="")
    return TimelineFinalStatus(status="done", phase="done", error_message="")


def summary_log_message(state: TimelineExecutionState) -> str:
    counters = state.counters
    return (
        "完整时间线迁移收尾："
        f"文本发送={counters.text_sent}，文本跳过={counters.text_skipped}，文本失败={counters.text_failed}，"
        f"媒体复制={counters.media_sent}，媒体跳过={counters.media_skipped}，媒体失败={counters.media_failed}，"
        f"媒体组复制={counters.media_group_sent}，媒体组跳过={counters.media_group_skipped}，媒体组失败={counters.media_group_failed}"
    )
