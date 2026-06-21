import logging
import time
from collections.abc import Callable
from contextlib import suppress
from typing import Any

from tg_harvest.admin_jobs.clone_execution import (
    clone_cfg_for_account,
    normalize_clone_nonnegative_int,
    send_clone_text_chunk,
    split_clone_text_chunks,
)
from tg_harvest.admin_jobs.clone_media_copy import (
    clone_sent_message_ids,
    clone_source_message_for_api_id,
    copy_clone_media_direct_without_source,
    copy_clone_media_via_relay_without_source,
    record_clone_media_mapping,
    resolve_clone_relay_chat,
    resolved_clone_group_key,
)
from tg_harvest.admin_jobs.clone_media_resolver import (
    clone_album_message_id_chunks,
    clone_api_resolve_media_group,
    clone_api_resolve_media_message,
)
from tg_harvest.admin_jobs.common import (
    admin_error_message,
    finish_job_heartbeat,
    resolve_chat_entity,
    start_admin_job_thread,
)
from tg_harvest.admin_jobs.core import (
    _admin_job_heartbeat,
    _admin_job_stop_requested,
    _admin_job_update_progress,
    job_context,
)
from tg_harvest.admin_jobs.runtime import _admin_now_iso
from tg_harvest.admin_jobs.sessions import (
    _cleanup_isolated_worker_session,
    _create_isolated_worker_client,
    _disconnect_worker_client,
    _ensure_base_session_valid,
    _start_job_heartbeat,
)
from tg_harvest.domain.clone_plan import (
    CLONE_MEDIA_STRATEGY_RELAY_COPY_WITHOUT_ATTRIBUTION,
    CLONE_MEDIA_STRATEGY_SOURCE_COPY_WITHOUT_ATTRIBUTION,
    CLONE_TEXT_MIGRATION_MAX_MESSAGE_LIMIT,
    CLONE_TEXT_MIGRATION_MAX_SEND_DELAY_MS,
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
    list_clone_media_group_messages,
    list_clone_timeline_replay_batch,
    load_clone_message_mapping,
    load_clone_migration,
    load_clone_plan,
    load_clone_run,
    record_clone_message_mapping,
    update_clone_migration,
)

CLONE_TIMELINE_BATCH_SIZE = 100


def _first_required_target_message_id(result: Any, context: str) -> int:
    target_message_id = (clone_sent_message_ids(result) or [None])[0]
    if target_message_id is None:
        raise RuntimeError(f"{context} 未返回目标消息 ID")
    return int(target_message_id)


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _sleep_after_send(send_delay_ms: int) -> None:
    if send_delay_ms <= 0:
        return
    time.sleep(float(send_delay_ms) / 1000.0)


def _load_required_state(
    *,
    get_conn_fn: Callable[[], Any],
    run_id: str,
    plan_id: str,
    migration_id: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    conn = get_conn_fn()
    try:
        run = load_clone_run(conn, run_id)
        plan = load_clone_plan(conn, plan_id)
        migration = load_clone_migration(conn, migration_id)
    finally:
        conn.close()
    if run is None:
        raise RuntimeError("克隆运行记录不存在，无法执行完整时间线迁移")
    if plan is None:
        raise RuntimeError("克隆迁移计划不存在，无法执行完整时间线迁移")
    if migration is None:
        raise RuntimeError("克隆时间线迁移记录不存在，无法执行完整时间线迁移")
    return run, plan, migration


def _update_migration_required(
    *,
    get_conn_fn: Callable[[], Any],
    migration_id: str,
    **kwargs: Any,
) -> dict[str, Any]:
    conn = get_conn_fn()
    try:
        migration = update_clone_migration(conn, migration_id=migration_id, **kwargs)
    finally:
        conn.close()
    if migration is None:
        raise RuntimeError("克隆时间线迁移记录不存在，已停止完整时间线迁移")
    return migration


def _try_mark_migration_failed(
    *,
    get_conn_fn: Callable[[], Any],
    migration_id: str,
    message: str,
) -> None:
    with suppress(Exception):
        conn = get_conn_fn()
        try:
            update_clone_migration(
                conn,
                migration_id=migration_id,
                status="error",
                phase="error",
                error_message=message,
                completed_at=_admin_now_iso(),
            )
        finally:
            conn.close()


def _timeline_preview(
    *,
    get_conn_fn: Callable[[], Any],
    run_id: str,
    source_chat_id: int,
) -> dict[str, Any]:
    conn = get_conn_fn()
    try:
        return build_clone_timeline_replay_preview(
            conn,
            run_id=run_id,
            source_chat_id=source_chat_id,
        )
    finally:
        conn.close()


def _next_timeline_batch(
    *,
    get_conn_fn: Callable[[], Any],
    run_id: str,
    source_chat_id: int,
    after_ts: int | None,
    after_message_id: int | None,
) -> list[dict[str, Any]]:
    conn = get_conn_fn()
    try:
        return list_clone_timeline_replay_batch(
            conn,
            run_id=run_id,
            chat_id=source_chat_id,
            after_ts=after_ts,
            after_message_id=after_message_id,
            limit=CLONE_TIMELINE_BATCH_SIZE,
        )
    finally:
        conn.close()


def _load_group_messages(
    *,
    get_conn_fn: Callable[[], Any],
    source_chat_id: int,
    grouped_id: int,
) -> list[dict[str, Any]]:
    conn = get_conn_fn()
    try:
        return list_clone_media_group_messages(
            conn,
            chat_id=source_chat_id,
            grouped_id=grouped_id,
        )
    finally:
        conn.close()


def _text_mapping_done(
    *,
    get_conn_fn: Callable[[], Any],
    run_id: str,
    source_chat_id: int,
    source_message_id: int,
    chunk_index: int,
) -> bool:
    conn = get_conn_fn()
    try:
        mapping = load_clone_message_mapping(
            conn,
            run_id=run_id,
            source_chat_id=source_chat_id,
            source_message_id=source_message_id,
            chunk_index=chunk_index,
            mode="text_replay",
        )
    finally:
        conn.close()
    return mapping is not None and mapping.get("status") == "done"


def _media_mapping_done(
    *,
    get_conn_fn: Callable[[], Any],
    run_id: str,
    source_chat_id: int,
    source_message_id: int,
    mode: str,
) -> bool:
    conn = get_conn_fn()
    try:
        mapping = load_clone_message_mapping(
            conn,
            run_id=run_id,
            source_chat_id=source_chat_id,
            source_message_id=source_message_id,
            chunk_index=0,
            mode=mode,
        )
    finally:
        conn.close()
    return mapping is not None and mapping.get("status") == "done"


def _record_text_mapping(
    *,
    get_conn_fn: Callable[[], Any],
    migration_id: str,
    run_id: str,
    plan_id: str,
    source_message: dict[str, Any],
    target_chat_id: int,
    target_message_id: int | None,
    chunk_index: int,
    chunk_count: int,
    status: str,
    error_message: str = "",
) -> None:
    conn = get_conn_fn()
    try:
        record_clone_message_mapping(
            conn,
            migration_id=migration_id,
            run_id=run_id,
            plan_id=plan_id,
            source_chat_id=int(source_message["chat_id"]),
            source_message_id=int(source_message["message_id"]),
            source_msg_date_ts=source_message.get("msg_date_ts"),
            source_msg_date_text=source_message.get("msg_date_text"),
            target_chat_id=int(target_chat_id),
            target_message_id=target_message_id,
            chunk_index=int(chunk_index),
            chunk_count=int(chunk_count),
            mode="text_replay",
            status=status,
            error_message=error_message,
            sent_at=_admin_now_iso() if status == "done" else "",
        )
    finally:
        conn.close()


def _source_message_from_timeline_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "chat_id": int(item["chat_id"]),
        "message_id": int(item["source_message_id"]),
        "msg_date_ts": item.get("msg_date_ts"),
        "msg_date_text": item.get("msg_date_text"),
        "sort_ts": int(item.get("sort_ts") or 0),
        "text": str(item.get("text") or ""),
        "caption": str(item.get("text") or ""),
    }


def _validate_plan_for_timeline(
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


def _timeline_execution_label(accounts: dict[str, str]) -> str:
    labels = []
    if accounts.get("text_account"):
        labels.append(f"text:{accounts['text_account']}")
    if accounts.get("media_execution_account"):
        labels.append(f"media:{accounts['media_execution_account']}")
    return "; ".join(labels)


def _admin_clone_timeline_migration_job_runner(
    job_id: str,
    *,
    run_id: str,
    plan_id: str,
    migration_id: str,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
    message_limit: Any = 0,
    send_delay_ms: Any = 0,
) -> None:
    job_context.set(str(job_id))
    heartbeat_stop, heartbeat_thread = _start_job_heartbeat(job_id, _admin_job_heartbeat)
    clients: dict[str, Any] = {}
    worker_ids: dict[str, str] = {}
    account_cfgs: dict[str, Any] = {}
    target_entities: dict[str, Any] = {}
    normalized_message_limit = normalize_clone_nonnegative_int(
        message_limit,
        max_value=CLONE_TEXT_MIGRATION_MAX_MESSAGE_LIMIT,
    )
    normalized_send_delay_ms = normalize_clone_nonnegative_int(
        send_delay_ms,
        max_value=CLONE_TEXT_MIGRATION_MAX_SEND_DELAY_MS,
    )

    try:
        admin_job_set_status_fn(job_id, "running")
        _update_migration_required(
            get_conn_fn=get_conn_fn,
            migration_id=migration_id,
            status="running",
            phase="validating",
            error_message="",
        )

        run, plan, _migration = _load_required_state(
            get_conn_fn=get_conn_fn,
            run_id=run_id,
            plan_id=plan_id,
            migration_id=migration_id,
        )
        source_chat_id = int(run["source_chat_id"])
        target_chat_id = run.get("target_chat_id")
        if not target_chat_id:
            raise RuntimeError("目标副本尚未创建，不能执行完整时间线迁移")

        preview = _timeline_preview(
            get_conn_fn=get_conn_fn,
            run_id=run_id,
            source_chat_id=source_chat_id,
        )
        accounts = _validate_plan_for_timeline(plan=plan, preview=preview)
        using_relay = clone_plan_uses_media_relay(plan) and bool(
            accounts.get("media_source_account")
        )
        text_total = int(preview.get("text_total") or 0)
        media_total = int(preview.get("media_total") or 0)
        media_group_total = int(preview.get("media_group_total") or 0)
        progress_total = int(preview.get("timeline_remaining") or 0)
        if normalized_message_limit > 0:
            progress_total = min(progress_total, normalized_message_limit)

        _update_migration_required(
            get_conn_fn=get_conn_fn,
            migration_id=migration_id,
            phase="connecting",
            target_chat_id=target_chat_id,
            target_title=run.get("target_title"),
            target_write_account=_timeline_execution_label(accounts),
            requested_limit=normalized_message_limit,
            send_delay_ms=normalized_send_delay_ms,
            text_total=text_total,
            media_total=media_total,
            media_group_total=media_group_total,
        )
        admin_job_append_log_fn(
            job_id,
            "开始完整时间线迁移："
            f"源={run['source_title']} ({source_chat_id})，"
            f"目标={run.get('target_title') or target_chat_id}，"
            f"文本={text_total}，媒体={media_total}，媒体组={media_group_total}",
        )
        admin_job_append_log_fn(
            job_id,
            "时间线策略：按原群 msg_date_ts/message_id 顺序混合发送文本、媒体和相册",
        )
        if using_relay:
            admin_job_append_log_fn(
                job_id,
                "媒体复制策略：固定中转频道桥接；两跳均 drop_author=True，不显示原群或中转频道跳转",
            )
        else:
            admin_job_append_log_fn(
                job_id,
                "媒体复制策略：drop_author=True，不显示原群来源，不带原群跳转",
            )

        def account_client(account: str) -> Any:
            normalized = _clean_text(account).lower()
            if not normalized:
                raise RuntimeError("迁移账号为空")
            if normalized in clients:
                return clients[normalized]
            account_cfg = clone_cfg_for_account(cfg, normalized)
            if not _ensure_base_session_valid(account_cfg, job_id, admin_job_append_log_fn):
                raise RuntimeError(f"计划指定的账号会话不可用：{normalized}")
            worker_id = f"{job_id}_clone_timeline_{normalized}"
            account_cfgs[normalized] = account_cfg
            worker_ids[normalized] = worker_id
            clients[normalized] = _create_isolated_worker_client(account_cfg, worker_id)
            return clients[normalized]

        def target_entity_for(account: str) -> Any:
            normalized = _clean_text(account).lower()
            if normalized not in target_entities:
                target_entities[normalized] = resolve_chat_entity(
                    account_client(normalized),
                    int(target_chat_id),
                    _clean_text(run.get("target_username")),
                    allow_username_fallback=True,
                )
            return target_entities[normalized]

        text_account = accounts.get("text_account") or ""
        media_source_account = accounts.get("media_source_account") or ""
        media_target_account = accounts.get("media_target_account") or ""
        text_client = account_client(text_account) if text_account else None
        text_target_entity = target_entity_for(text_account) if text_account else None

        source_client = account_client(media_source_account) if media_source_account else None
        target_client = account_client(media_target_account) if media_target_account else None
        source_entity = (
            resolve_chat_entity(
                source_client,
                source_chat_id,
                _clean_text(run.get("source_chat_username")),
                allow_username_fallback=True,
            )
            if source_client is not None
            else None
        )
        media_target_entity = (
            target_entity_for(media_target_account) if media_target_account else None
        )
        relay_entity_for_source = (
            resolve_clone_relay_chat(source_client, plan)
            if using_relay and source_client is not None
            else None
        )
        relay_entity_for_target = (
            resolve_clone_relay_chat(target_client, plan)
            if using_relay and target_client is not None
            else None
        )

        def copy_media_to_target(
            message_ids: int | list[int],
            *,
            as_album: bool | None = None,
        ) -> Any:
            if source_client is None or source_entity is None or media_target_entity is None:
                raise RuntimeError("媒体迁移账号或实体尚未初始化")
            if using_relay:
                if (
                    target_client is None
                    or relay_entity_for_source is None
                    or relay_entity_for_target is None
                ):
                    raise RuntimeError("固定中转频道桥接实体尚未初始化")
                return copy_clone_media_via_relay_without_source(
                    source_client=source_client,
                    target_client=target_client,
                    relay_entity_for_source=relay_entity_for_source,
                    relay_entity_for_target=relay_entity_for_target,
                    target_entity=media_target_entity,
                    message_ids=message_ids,
                    source_entity=source_entity,
                    as_album=as_album,
                )
            return copy_clone_media_direct_without_source(
                client=source_client,
                target_entity=media_target_entity,
                message_ids=message_ids,
                source_entity=source_entity,
                as_album=as_album,
            )

        _update_migration_required(
            get_conn_fn=get_conn_fn,
            migration_id=migration_id,
            phase="replaying_timeline",
        )

        text_sent = 0
        text_skipped = 0
        text_failed = 0
        media_sent = 0
        media_skipped = 0
        media_failed = 0
        media_group_sent = 0
        media_group_skipped = 0
        media_group_failed = 0
        processed = 0
        stopped = False
        limit_reached = False
        after_ts: int | None = None
        after_message_id: int | None = None
        copied_api_group_keys: set[tuple[Any, ...]] = set()

        while True:
            if _admin_job_stop_requested(job_id):
                stopped = True
                break
            batch = _next_timeline_batch(
                get_conn_fn=get_conn_fn,
                run_id=run_id,
                source_chat_id=source_chat_id,
                after_ts=after_ts,
                after_message_id=after_message_id,
            )
            if not batch:
                break
            for item in batch:
                source_count = max(1, int(item.get("item_count") or 1))
                if normalized_message_limit > 0 and processed >= normalized_message_limit:
                    limit_reached = True
                    break
                if (
                    normalized_message_limit > 0
                    and processed + source_count > normalized_message_limit
                ):
                    limit_reached = True
                    break
                if _admin_job_stop_requested(job_id):
                    stopped = True
                    break

                after_ts = int(item.get("sort_ts") or 0)
                after_message_id = int(item.get("sort_message_id") or 0)
                item_type = _clean_text(item.get("item_type"))
                if item_type == "text":
                    source_message = _source_message_from_timeline_item(item)
                    chunks = split_clone_text_chunks(str(item.get("text") or ""))
                    if not chunks:
                        processed += 1
                        text_skipped += 1
                        continue
                    chunk_count = len(chunks)
                    source_message_id = int(source_message["message_id"])
                    if all(
                        _text_mapping_done(
                            get_conn_fn=get_conn_fn,
                            run_id=run_id,
                            source_chat_id=source_chat_id,
                            source_message_id=source_message_id,
                            chunk_index=chunk_index,
                        )
                        for chunk_index in range(chunk_count)
                    ):
                        processed += 1
                        text_skipped += 1
                        continue
                    message_failed = False
                    for chunk_index, chunk in enumerate(chunks):
                        if _text_mapping_done(
                            get_conn_fn=get_conn_fn,
                            run_id=run_id,
                            source_chat_id=source_chat_id,
                            source_message_id=source_message_id,
                            chunk_index=chunk_index,
                        ):
                            continue
                        try:
                            if text_client is None or text_target_entity is None:
                                raise RuntimeError("文本迁移账号或目标实体尚未初始化")
                            target_message_id = send_clone_text_chunk(
                                text_client,
                                text_target_entity,
                                chunk,
                            )
                            _record_text_mapping(
                                get_conn_fn=get_conn_fn,
                                migration_id=migration_id,
                                run_id=run_id,
                                plan_id=plan_id,
                                source_message=source_message,
                                target_chat_id=int(target_chat_id),
                                target_message_id=target_message_id,
                                chunk_index=chunk_index,
                                chunk_count=chunk_count,
                                status="done",
                            )
                            _sleep_after_send(normalized_send_delay_ms)
                        except Exception as exc:
                            message_failed = True
                            message = admin_error_message(exc)
                            _record_text_mapping(
                                get_conn_fn=get_conn_fn,
                                migration_id=migration_id,
                                run_id=run_id,
                                plan_id=plan_id,
                                source_message=source_message,
                                target_chat_id=int(target_chat_id),
                                target_message_id=None,
                                chunk_index=chunk_index,
                                chunk_count=chunk_count,
                                status="error",
                                error_message=message,
                            )
                            admin_job_append_log_fn(
                                job_id,
                                f"时间线文本发送失败：source_message_id={source_message_id}，{message}",
                            )
                            break
                    processed += 1
                    if message_failed:
                        text_failed += 1
                    else:
                        text_sent += 1
                elif item_type == "solo_media":
                    source_message = _source_message_from_timeline_item(item)
                    source_message_id = int(source_message["message_id"])
                    if _media_mapping_done(
                        get_conn_fn=get_conn_fn,
                        run_id=run_id,
                        source_chat_id=source_chat_id,
                        source_message_id=source_message_id,
                        mode="media_copy",
                    ):
                        processed += 1
                        media_skipped += 1
                        continue
                    try:
                        if source_client is None or source_entity is None:
                            raise RuntimeError("源侧媒体账号或实体尚未初始化")
                        resolved = clone_api_resolve_media_message(
                            source_client,
                            source_entity,
                            source_message_id,
                        )
                        if not resolved.get("ok"):
                            raise RuntimeError(
                                str(resolved.get("error") or "API 源媒体消息解析失败")
                            )
                        api_message_id = int(
                            resolved.get("message_id") or source_message_id
                        )
                        result = copy_media_to_target(api_message_id)
                        target_message_id = _first_required_target_message_id(
                            result,
                            "时间线单条媒体复制",
                        )
                        record_clone_media_mapping(
                            get_conn_fn=get_conn_fn,
                            migration_id=migration_id,
                            run_id=run_id,
                            plan_id=plan_id,
                            source_message=source_message,
                            target_chat_id=int(target_chat_id),
                            target_message_id=target_message_id,
                            mode="media_copy",
                            status="done",
                        )
                        media_sent += 1
                        processed += 1
                        _sleep_after_send(normalized_send_delay_ms)
                    except Exception as exc:
                        media_failed += 1
                        processed += 1
                        message = admin_error_message(exc)
                        record_clone_media_mapping(
                            get_conn_fn=get_conn_fn,
                            migration_id=migration_id,
                            run_id=run_id,
                            plan_id=plan_id,
                            source_message=source_message,
                            target_chat_id=int(target_chat_id),
                            target_message_id=None,
                            mode="media_copy",
                            status="error",
                            error_message=message,
                        )
                        admin_job_append_log_fn(
                            job_id,
                            f"时间线单条媒体复制失败：source_message_id={source_message_id}，{message}",
                        )
                elif item_type == "media_group":
                    grouped_id = item.get("grouped_id")
                    if grouped_id in (None, ""):
                        processed += source_count
                        media_skipped += source_count
                        media_group_skipped += 1
                        continue
                    messages = _load_group_messages(
                        get_conn_fn=get_conn_fn,
                        source_chat_id=source_chat_id,
                        grouped_id=int(grouped_id),
                    )
                    if not messages:
                        processed += source_count
                        media_skipped += source_count
                        media_group_skipped += 1
                        continue
                    db_messages_by_id = {
                        int(message["message_id"]): message for message in messages
                    }
                    try:
                        if source_client is None or source_entity is None:
                            raise RuntimeError("源侧媒体账号或实体尚未初始化")
                        resolved_group = clone_api_resolve_media_group(
                            source_client,
                            source_entity,
                            [int(message["message_id"]) for message in messages],
                        )
                        if not resolved_group.get("ok"):
                            raise RuntimeError(
                                str(resolved_group.get("error") or "API 媒体组解析失败")
                            )
                        resolved_message_ids = [
                            int(message_id)
                            for message_id in resolved_group.get("message_ids") or []
                        ]
                        if not resolved_message_ids:
                            raise RuntimeError("API 未解析出可复制媒体组成员")
                    except Exception as exc:
                        media_failed += len(messages)
                        media_group_failed += 1
                        processed += len(messages)
                        message = admin_error_message(exc)
                        for source_message in messages:
                            record_clone_media_mapping(
                                get_conn_fn=get_conn_fn,
                                migration_id=migration_id,
                                run_id=run_id,
                                plan_id=plan_id,
                                source_message=source_message,
                                target_chat_id=int(target_chat_id),
                                target_message_id=None,
                                mode="media_group_copy",
                                status="error",
                                error_message=message,
                            )
                        admin_job_append_log_fn(
                            job_id,
                            f"时间线媒体组 API 解析失败：grouped_id={grouped_id}，{message}",
                        )
                        continue

                    resolved_group_key = resolved_clone_group_key(
                        resolved_group,
                        resolved_message_ids,
                    )
                    resolved_message_id_set = set(resolved_message_ids)
                    resolved_items = [
                        item
                        for item in resolved_group.get("items") or []
                        if int(item.get("message_id") or 0) in resolved_message_id_set
                    ]
                    resolved_items_by_id = {
                        int(item["message_id"]): item
                        for item in resolved_items
                        if int(item.get("message_id") or 0)
                    }
                    copy_strategy = _clean_text(
                        resolved_group.get("copy_strategy")
                    ).lower()
                    copy_as_group = (
                        copy_strategy == "album" and len(resolved_message_ids) > 1
                    )
                    album_chunks = (
                        clone_album_message_id_chunks(resolved_message_ids)
                        if copy_as_group
                        else []
                    )
                    if resolved_group_key in copied_api_group_keys:
                        media_skipped += len(resolved_message_ids)
                        media_group_skipped += 1
                        processed += len(resolved_message_ids)
                        continue
                    done_by_message_id = {
                        int(message_id): _media_mapping_done(
                            get_conn_fn=get_conn_fn,
                            run_id=run_id,
                            source_chat_id=source_chat_id,
                            source_message_id=int(message_id),
                            mode="media_group_copy",
                        )
                        for message_id in resolved_message_ids
                    }
                    if all(done_by_message_id.values()):
                        media_skipped += len(resolved_message_ids)
                        media_group_skipped += 1
                        copied_api_group_keys.add(resolved_group_key)
                        processed += len(resolved_message_ids)
                        continue

                    def record_group_item_error(
                        source_message_id: int,
                        message: str,
                        db_messages_by_id: dict[int, dict[str, Any]] = db_messages_by_id,
                    ) -> None:
                        source_message = clone_source_message_for_api_id(
                            source_chat_id=source_chat_id,
                            source_message_id=int(source_message_id),
                            db_messages_by_id=db_messages_by_id,
                        )
                        record_clone_media_mapping(
                            get_conn_fn=get_conn_fn,
                            migration_id=migration_id,
                            run_id=run_id,
                            plan_id=plan_id,
                            source_message=source_message,
                            target_chat_id=int(target_chat_id),
                            target_message_id=None,
                            mode="media_group_copy",
                            status="error",
                            error_message=message,
                        )

                    def record_group_item_success(
                        source_message_id: int,
                        target_message_id: int | None,
                        db_messages_by_id: dict[int, dict[str, Any]] = db_messages_by_id,
                    ) -> None:
                        source_message = clone_source_message_for_api_id(
                            source_chat_id=source_chat_id,
                            source_message_id=int(source_message_id),
                            db_messages_by_id=db_messages_by_id,
                        )
                        record_clone_media_mapping(
                            get_conn_fn=get_conn_fn,
                            migration_id=migration_id,
                            run_id=run_id,
                            plan_id=plan_id,
                            source_message=source_message,
                            target_chat_id=int(target_chat_id),
                            target_message_id=target_message_id,
                            mode="media_group_copy",
                            status="done",
                        )

                    def copy_group_sequentially(
                        message_ids: tuple[int, ...] = tuple(resolved_message_ids),
                        done_by_message_id: dict[int, bool] = done_by_message_id,
                        resolved_items_by_id: dict[int, dict[str, Any]] = (
                            resolved_items_by_id
                        ),
                        grouped_id: Any = grouped_id,
                    ) -> tuple[int, int, int]:
                        sent = 0
                        skipped = 0
                        failed = 0
                        for source_message_id in message_ids:
                            source_message_id = int(source_message_id)
                            if done_by_message_id.get(source_message_id):
                                skipped += 1
                                continue
                            try:
                                result = copy_media_to_target(source_message_id)
                                target_message_id = _first_required_target_message_id(
                                    result,
                                    "时间线媒体组逐条复制",
                                )
                                record_group_item_success(
                                    source_message_id,
                                    target_message_id,
                                )
                                sent += 1
                                _sleep_after_send(normalized_send_delay_ms)
                            except Exception as exc:
                                failed += 1
                                message = admin_error_message(exc)
                                record_group_item_error(source_message_id, message)
                                media_kind = _clean_text(
                                    (
                                        resolved_items_by_id.get(source_message_id)
                                        or {}
                                    ).get("media_kind")
                                )
                                media_kind_suffix = (
                                    f"，media_kind={media_kind}" if media_kind else ""
                                )
                                admin_job_append_log_fn(
                                    job_id,
                                    "时间线媒体组逐条复制失败："
                                    f"grouped_id={grouped_id}，"
                                    f"source_message_id={source_message_id}"
                                    f"{media_kind_suffix}，{message}",
                                )
                        return sent, skipped, failed

                    def copy_album_chunk(
                        chunk_message_ids: list[int],
                        *,
                        chunk_index: int,
                        chunk_count: int,
                        done_by_message_id: dict[int, bool] = done_by_message_id,
                        grouped_id: Any = grouped_id,
                    ) -> tuple[int, int, int]:
                        done_in_chunk = [
                            bool(done_by_message_id.get(int(source_message_id)))
                            for source_message_id in chunk_message_ids
                        ]
                        if all(done_in_chunk):
                            return 0, len(chunk_message_ids), 0

                        if any(done_in_chunk):
                            message = "相册分块已有部分完成映射，跳过该分块避免重复相册"
                            missing_ids = [
                                int(source_message_id)
                                for source_message_id, done in zip(
                                    chunk_message_ids,
                                    done_in_chunk,
                                    strict=False,
                                )
                                if not done
                            ]
                            for source_message_id in missing_ids:
                                record_group_item_error(source_message_id, message)
                            admin_job_append_log_fn(
                                job_id,
                                "时间线媒体组相册分块存在部分映射，已跳过："
                                f"grouped_id={grouped_id}，"
                                f"chunk={chunk_index}/{chunk_count}，"
                                f"done={sum(1 for done in done_in_chunk if done)}，"
                                f"missing={len(missing_ids)}",
                            )
                            return 0, len(chunk_message_ids) - len(missing_ids), len(
                                missing_ids
                            )

                        if len(chunk_message_ids) <= 1:
                            return copy_group_sequentially(tuple(chunk_message_ids))

                        try:
                            result = copy_media_to_target(chunk_message_ids)
                        except Exception as exc:
                            admin_job_append_log_fn(
                                job_id,
                                "时间线媒体组相册分块复制失败，已降级为逐条复制："
                                f"grouped_id={grouped_id}，"
                                f"chunk={chunk_index}/{chunk_count}，"
                                f"items={len(chunk_message_ids)}，"
                                f"reason={admin_error_message(exc)}",
                            )
                            return copy_group_sequentially(tuple(chunk_message_ids))

                        target_ids = clone_sent_message_ids(result)
                        sent = 0
                        failed = 0
                        for index, source_message_id in enumerate(chunk_message_ids):
                            target_message_id = (
                                target_ids[index] if index < len(target_ids) else None
                            )
                            if target_message_id is None:
                                failed += 1
                                message = "媒体组相册复制后未返回目标消息 ID"
                                record_group_item_error(int(source_message_id), message)
                                admin_job_append_log_fn(
                                    job_id,
                                    "时间线媒体组相册分块部分消息复制失败："
                                    f"grouped_id={grouped_id}，"
                                    f"chunk={chunk_index}/{chunk_count}，"
                                    f"source_message_id={source_message_id}，"
                                    f"{message}",
                                )
                                continue
                            sent += 1
                            record_group_item_success(
                                int(source_message_id),
                                int(target_message_id),
                            )

                        if sent:
                            _sleep_after_send(normalized_send_delay_ms)
                        return sent, 0, failed

                    try:
                        if copy_as_group:
                            sent = 0
                            skipped = 0
                            failed = 0
                            for chunk_index, chunk_message_ids in enumerate(
                                album_chunks,
                                start=1,
                            ):
                                chunk_sent, chunk_skipped, chunk_failed = (
                                    copy_album_chunk(
                                        chunk_message_ids,
                                        chunk_index=chunk_index,
                                        chunk_count=len(album_chunks),
                                    )
                                )
                                sent += chunk_sent
                                skipped += chunk_skipped
                                failed += chunk_failed
                            media_sent += sent
                            media_skipped += skipped
                            media_failed += failed
                            if failed:
                                media_group_failed += 1
                            elif sent:
                                media_group_sent += 1
                                copied_api_group_keys.add(resolved_group_key)
                            else:
                                media_group_skipped += 1
                            processed += len(resolved_message_ids)
                            if sent and len(album_chunks) > 1:
                                admin_job_append_log_fn(
                                    job_id,
                                    "时间线媒体组超过单次相册上限，已按 Telegram 限制分块复制："
                                    f"grouped_id={grouped_id}，"
                                    f"items={len(resolved_message_ids)}，"
                                    f"chunks={len(album_chunks)}",
                                )
                            continue

                        sent, skipped, failed = copy_group_sequentially()
                        media_sent += sent
                        media_skipped += skipped
                        media_failed += failed
                        if failed:
                            media_group_failed += 1
                        elif sent:
                            media_group_sent += 1
                            copied_api_group_keys.add(resolved_group_key)
                        else:
                            media_group_skipped += 1
                        processed += len(resolved_message_ids)
                        if sent and copy_strategy == "sequential":
                            admin_job_append_log_fn(
                                job_id,
                                "时间线媒体组包含非相册兼容媒体，已按原顺序逐条复制："
                                f"grouped_id={grouped_id}，"
                                f"items={len(resolved_message_ids)}，"
                                f"reason={_clean_text(resolved_group.get('album_reason'))}",
                            )
                    except Exception as exc:
                        media_failed += len(resolved_message_ids)
                        media_group_failed += 1
                        processed += len(resolved_message_ids)
                        message = admin_error_message(exc)
                        for source_message_id in resolved_message_ids:
                            source_message = clone_source_message_for_api_id(
                                source_chat_id=source_chat_id,
                                source_message_id=int(source_message_id),
                                db_messages_by_id=db_messages_by_id,
                            )
                            record_clone_media_mapping(
                                get_conn_fn=get_conn_fn,
                                migration_id=migration_id,
                                run_id=run_id,
                                plan_id=plan_id,
                                source_message=source_message,
                                target_chat_id=int(target_chat_id),
                                target_message_id=None,
                                mode="media_group_copy",
                                status="error",
                                error_message=message,
                            )
                        admin_job_append_log_fn(
                            job_id,
                            f"时间线媒体组复制失败：grouped_id={grouped_id}，{message}",
                        )
                else:
                    processed += source_count

                _update_migration_required(
                    get_conn_fn=get_conn_fn,
                    migration_id=migration_id,
                    text_sent=text_sent,
                    text_skipped=text_skipped,
                    text_failed=text_failed,
                    media_sent=media_sent,
                    media_skipped=media_skipped,
                    media_failed=media_failed,
                    media_group_sent=media_group_sent,
                    media_group_skipped=media_group_skipped,
                    media_group_failed=media_group_failed,
                )
                _admin_job_update_progress(
                    job_id,
                    processed,
                    total=max(progress_total, processed),
                    stage="replaying_timeline",
                    log_step=0,
                    auto_log=False,
                )
            if stopped or limit_reached:
                break

        if stopped:
            final_status = "error"
            final_phase = "stopped"
            final_error = "用户请求停止，完整时间线迁移已在安全边界收尾"
        elif text_failed > 0 or media_failed > 0 or media_group_failed > 0:
            final_status = "error"
            final_phase = "error"
            final_error = (
                f"完整时间线迁移完成但有 {text_failed} 条文本、"
                f"{media_failed} 条媒体或 {media_group_failed} 个媒体组失败"
            )
        elif limit_reached:
            final_status = "done"
            final_phase = "limited_done"
            final_error = ""
        else:
            final_status = "done"
            final_phase = "done"
            final_error = ""

        _update_migration_required(
            get_conn_fn=get_conn_fn,
            migration_id=migration_id,
            status=final_status,
            phase=final_phase,
            text_total=text_total,
            text_sent=text_sent,
            text_skipped=text_skipped,
            text_failed=text_failed,
            media_total=media_total,
            media_sent=media_sent,
            media_skipped=media_skipped,
            media_failed=media_failed,
            media_group_total=media_group_total,
            media_group_sent=media_group_sent,
            media_group_skipped=media_group_skipped,
            media_group_failed=media_group_failed,
            error_message=final_error,
            completed_at=_admin_now_iso(),
        )
        admin_job_append_log_fn(
            job_id,
            "完整时间线迁移收尾："
            f"文本发送={text_sent}，文本跳过={text_skipped}，文本失败={text_failed}，"
            f"媒体复制={media_sent}，媒体跳过={media_skipped}，媒体失败={media_failed}，"
            f"媒体组复制={media_group_sent}，媒体组跳过={media_group_skipped}，媒体组失败={media_group_failed}",
        )
        _admin_job_update_progress(
            job_id,
            processed,
            total=max(progress_total, processed),
            stage=final_phase,
            log_step=0,
            auto_log=False,
        )
        admin_job_set_status_fn(job_id, final_status)
    except Exception as exc:
        logging.exception("克隆完整时间线迁移任务失败: job_id=%s", job_id)
        message = admin_error_message(exc)
        _try_mark_migration_failed(
            get_conn_fn=get_conn_fn,
            migration_id=migration_id,
            message=message,
        )
        admin_job_append_log_fn(job_id, f"完整时间线迁移失败：{message}")
        _admin_job_update_progress(
            job_id,
            0,
            total=0,
            stage="error",
            log_step=0,
            auto_log=False,
        )
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        disconnected_ids: set[int] = set()
        for cleanup_client in clients.values():
            if cleanup_client is None or id(cleanup_client) in disconnected_ids:
                continue
            disconnected_ids.add(id(cleanup_client))
            with suppress(Exception):
                _disconnect_worker_client(cleanup_client)
        for account, account_cfg in account_cfgs.items():
            worker_id = worker_ids.get(account)
            if not worker_id:
                continue
            with suppress(Exception):
                _cleanup_isolated_worker_session(account_cfg, worker_id)


def _admin_start_clone_timeline_migration_job_thread(job_id: str, **kwargs: Any):
    return start_admin_job_thread(
        _admin_clone_timeline_migration_job_runner,
        job_id,
        **kwargs,
    )
