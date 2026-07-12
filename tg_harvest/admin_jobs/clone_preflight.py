import logging
from collections.abc import Callable
from contextlib import suppress
from typing import Any

from tg_harvest.admin_jobs.clone import _cfg_with_session_name
from tg_harvest.admin_jobs.clone_job_state import (
    _clean_text,
    _load_required_record,
    _try_update_record,
    _update_required_record,
)
from tg_harvest.admin_jobs.common import (
    admin_error_message,
    call_with_conn,
    finish_job_heartbeat,
    is_entity_lookup_miss_error,
    mark_admin_job_running,
    resolve_chat_entity,
    start_admin_job_heartbeat,
    update_admin_job_progress,
)
from tg_harvest.admin_jobs.core import _admin_job_update_progress
from tg_harvest.admin_jobs.runtime import _admin_now_iso
from tg_harvest.admin_jobs.sessions import (
    _cleanup_isolated_worker_session,
    _create_isolated_worker_client,
    _disconnect_worker_client,
    _ensure_base_session_valid,
    _start_job_heartbeat,
    bind_client_event_loop,
)
from tg_harvest.domain.clone_plan import (
    CLONE_FORWARD_PRIVACY_MODE,
    CLONE_MEDIA_STRATEGY_RELAY_COPY_WITHOUT_ATTRIBUTION,
    CLONE_MEDIA_STRATEGY_SOURCE_COPY_WITHOUT_ATTRIBUTION,
)
from tg_harvest.domain.coerce import safe_int
from tg_harvest.ingest.flood_wait import call_with_bounded_retry
from tg_harvest.storage.clone import (
    build_clone_source_snapshot,
    load_clone_run,
    update_clone_plan,
)

CLONE_DEEP_PREFLIGHT_TOTAL_STEPS = 5

_COMPAT_PATCH_EXPORTS = (_admin_job_update_progress, _start_job_heartbeat)


def _access_error_state(exc: Exception) -> str:
    err = f"{type(exc).__name__}: {exc}".lower()
    if is_entity_lookup_miss_error(exc):
        return "missing"
    if "channelprivate" in err or "forbidden" in err or "writeforbidden" in err:
        return "forbidden"
    if "floodwait" in err or "flood wait" in err:
        return "rate_limited"
    return "error"


def _first_message(messages: Any) -> Any | None:
    if messages is None:
        return None
    if isinstance(messages, (list, tuple)):
        return messages[0] if messages else None
    try:
        iterator = iter(messages)
    except TypeError:
        return messages
    return next(iterator, None)


def _message_summary(message: Any) -> dict[str, Any]:
    if message is None:
        return {}
    message_id = getattr(message, "id", None)
    date = getattr(message, "date", "")
    return {
        "message_id": int(message_id) if message_id not in (None, "") else None,
        "date": str(date or ""),
    }


def _target_send_permission(entity: Any) -> str:
    if entity is None:
        return "unknown"
    if bool(getattr(entity, "creator", False)):
        return "ok"

    admin_rights = getattr(entity, "admin_rights", None)
    if admin_rights is not None and bool(getattr(admin_rights, "post_messages", False)):
        return "ok"

    default_banned_rights = getattr(entity, "default_banned_rights", None)
    if default_banned_rights is not None and bool(
        getattr(default_banned_rights, "send_messages", False)
    ):
        return "blocked"
    return "unknown"


def _source_forwarding_permission(entity: Any) -> str:
    if entity is None:
        return "unknown"
    if bool(getattr(entity, "noforwards", False)):
        return "blocked"
    if hasattr(entity, "noforwards"):
        return "ok"
    return "unknown"


def _relay_safety_state(entity: Any) -> str:
    """Only a private broadcast channel is safe for temporary source media."""
    if entity is None:
        return "unknown"
    username = _clean_text(getattr(entity, "username", ""))
    if username:
        return "unsafe_public"
    if bool(getattr(entity, "megagroup", False)):
        return "unsafe_group"
    if getattr(entity, "broadcast", None) is True:
        return "private_channel"
    if getattr(entity, "broadcast", None) is False:
        return "unsafe_group"
    return "unknown"


def _resolve_access(
    client: Any,
    *,
    chat_id: int,
    chat_username: str,
) -> tuple[str, Any | None, str]:
    try:
        entity = resolve_chat_entity(
            client,
            int(chat_id),
            chat_username,
            allow_username_fallback=True,
        )
        return "ok", entity, ""
    except Exception as exc:
        return _access_error_state(exc), None, admin_error_message(exc)


def _read_latest_source_message(client: Any, entity: Any) -> tuple[dict[str, Any], str]:
    try:
        with bind_client_event_loop(client):
            return (
                _message_summary(
                    _first_message(
                        call_with_bounded_retry(
                            client.get_messages,
                            entity,
                            limit=1,
                            scope="clone-preflight-latest-source",
                        )
                    )
                ),
                "",
            )
    except Exception as exc:
        return {}, admin_error_message(exc)


def _base_account_result(
    *,
    account: str,
    session_name: str,
    configured: bool,
) -> dict[str, Any]:
    return {
        "account": account,
        "session": session_name,
        "configured": bool(configured),
        "session_status": "unknown" if configured else "not_configured",
        "authorized": False,
        "source_access": "unknown",
        "source_error": "",
        "source_forwarding_permission": "unknown",
        "target_access": "unknown",
        "target_error": "",
        "target_send_permission": "unknown",
        "relay_access": "not_configured",
        "relay_error": "",
        "relay_send_permission": "unknown",
        "relay_safety": "not_configured",
        "source_latest_message": {},
        "source_latest_error": "",
    }


def _check_account_access(
    *,
    account: str,
    cfg: Any,
    job_id: str,
    run: dict[str, Any],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> dict[str, Any]:
    session_name = _clean_text(getattr(cfg, "session_name", ""))
    result = _base_account_result(
        account=account,
        session_name=session_name,
        configured=bool(session_name),
    )
    if not session_name:
        return result

    worker_id = f"{job_id}_clone_deep_{account}"
    client = None
    try:
        if not _ensure_base_session_valid(cfg, job_id, admin_job_append_log_fn):
            result["session_status"] = "invalid"
            return result

        result["session_status"] = "ok"
        result["authorized"] = True
        client = _create_isolated_worker_client(cfg, worker_id)

        source_access, source_entity, source_error = _resolve_access(
            client,
            chat_id=int(run["source_chat_id"]),
            chat_username=_clean_text(run.get("source_chat_username")),
        )
        result["source_access"] = source_access
        result["source_error"] = source_error
        if source_entity is not None:
            result["source_forwarding_permission"] = _source_forwarding_permission(
                source_entity
            )
            latest_message, latest_error = _read_latest_source_message(
                client,
                source_entity,
            )
            result["source_latest_message"] = latest_message
            result["source_latest_error"] = latest_error

        target_chat_id = run.get("target_chat_id")
        if target_chat_id:
            target_access, target_entity, target_error = _resolve_access(
                client,
                chat_id=int(target_chat_id),
                chat_username=_clean_text(run.get("target_username")),
            )
            result["target_access"] = target_access
            result["target_error"] = target_error
            result["target_send_permission"] = _target_send_permission(target_entity)
        else:
            result["target_access"] = "missing"
            result["target_error"] = "目标副本尚未创建"

        relay_chat_id = safe_int(getattr(cfg, "clone_relay_chat_id", None))
        if relay_chat_id:
            relay_access, relay_entity, relay_error = _resolve_access(
                client,
                chat_id=relay_chat_id,
                chat_username=_clean_text(
                    getattr(cfg, "clone_relay_chat_username", "")
                ),
            )
            result["relay_access"] = relay_access
            result["relay_error"] = relay_error
            result["relay_send_permission"] = _target_send_permission(relay_entity)
            result["relay_safety"] = _relay_safety_state(relay_entity)
        return result
    except Exception as exc:
        logging.exception(
            "克隆深度预检账号检查失败: job_id=%s account=%s", job_id, account
        )
        result["session_status"] = "error"
        result["source_error"] = result["source_error"] or admin_error_message(exc)
        result["target_error"] = result["target_error"] or admin_error_message(exc)
        return result
    finally:
        if client is not None:
            with suppress(Exception):
                _disconnect_worker_client(client)
        with suppress(Exception):
            _cleanup_isolated_worker_session(cfg, worker_id)


def _aggregate_access(accounts: list[dict[str, Any]], key: str) -> str:
    values = [_clean_text(account.get(key)) for account in accounts if account]
    if "ok" in values:
        return "ok"
    for candidate in ("forbidden", "missing", "rate_limited", "error", "unknown"):
        if candidate in values:
            return candidate
    return "unknown"


def _session_status(accounts: list[dict[str, Any]], account_name: str) -> str:
    for account in accounts:
        if account.get("account") == account_name:
            return _clean_text(account.get("session_status")) or "unknown"
    return "not_configured"


def _first_account(
    accounts: list[dict[str, Any]],
    predicate: Callable[[dict[str, Any]], bool],
) -> str:
    for preferred in ("primary", "secondary"):
        for account in accounts:
            if account.get("account") == preferred and predicate(account):
                return preferred
    return ""


def _target_usable(account: dict[str, Any]) -> bool:
    return (
        account.get("target_access") == "ok"
        and account.get("target_send_permission") != "blocked"
    )


def _account_can_migrate_media(account: dict[str, Any]) -> bool:
    return (
        account.get("source_access") == "ok"
        and account.get("source_forwarding_permission") != "blocked"
        and _target_usable(account)
    )


def _relay_usable(account: dict[str, Any]) -> bool:
    return (
        account.get("relay_access") == "ok"
        and account.get("relay_send_permission") != "blocked"
        and account.get("relay_safety") == "private_channel"
    )


def _account_can_relay_from_source(account: dict[str, Any]) -> bool:
    return (
        account.get("source_access") == "ok"
        and account.get("source_forwarding_permission") != "blocked"
        and _relay_usable(account)
    )


def _account_can_relay_to_target(account: dict[str, Any]) -> bool:
    return account.get("target_access") == "ok" and _relay_usable(account)


def _configured_relay(cfg: Any | None = None) -> dict[str, Any]:
    if cfg is None:
        return {}
    chat_id = safe_int(getattr(cfg, "clone_relay_chat_id", None))
    if not chat_id:
        return {}
    return {
        "chat_id": chat_id,
        "username": _clean_text(getattr(cfg, "clone_relay_chat_username", "")),
    }


def _relay_account_diagnostics(accounts: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    for account_name in ("primary", "secondary"):
        account = next(
            (item for item in accounts if item.get("account") == account_name),
            None,
        )
        if not account:
            parts.append(f"{account_name}=not_checked")
            continue
        relay_access = _clean_text(account.get("relay_access")) or "unknown"
        send_permission = _clean_text(account.get("relay_send_permission")) or "unknown"
        relay_safety = _clean_text(account.get("relay_safety")) or "unknown"
        relay_error = _clean_text(account.get("relay_error"))
        detail = (
            f"{account_name}: access={relay_access}, send={send_permission}, "
            f"safety={relay_safety}"
        )
        if relay_error:
            detail += f", error={relay_error}"
        parts.append(detail)
    return "；".join(parts)


def _first_latest_message(accounts: list[dict[str, Any]]) -> dict[str, Any]:
    for account in accounts:
        latest = account.get("source_latest_message")
        if isinstance(latest, dict) and latest:
            return latest
    return {}


def _build_deep_preflight_outcome(
    *,
    run: dict[str, Any],
    accounts: list[dict[str, Any]],
    network_access_checked: bool,
    source_snapshot: dict[str, Any] | None = None,
    cfg: Any | None = None,
) -> dict[str, Any]:
    source_access = _aggregate_access(accounts, "source_access")
    target_access = _aggregate_access(accounts, "target_access")
    primary_session_status = _session_status(accounts, "primary")
    secondary_session_status = _session_status(accounts, "secondary")
    target_write_account = _first_account(accounts, _target_usable)
    migration_account = _first_account(accounts, _account_can_migrate_media)
    relay = _configured_relay(cfg)
    relay_source_account = (
        _first_account(accounts, _account_can_relay_from_source) if relay else ""
    )
    relay_target_account = (
        _first_account(accounts, _account_can_relay_to_target) if relay else ""
    )
    relay_ready = bool(relay and relay_source_account and relay_target_account)
    local_snapshot = source_snapshot if isinstance(source_snapshot, dict) else {}
    local_latest_message_id = safe_int(local_snapshot.get("latest_message_id"))
    remote_latest_message = _first_latest_message(accounts)
    remote_latest_message_id = safe_int(remote_latest_message.get("message_id"))
    source_snapshot_payload = {
        "message_id": remote_latest_message_id,
        "local_message_id": local_latest_message_id,
        "local_message_count": safe_int(local_snapshot.get("message_count")),
        "local_message_ts": safe_int(local_snapshot.get("latest_message_ts")),
    }

    blocking_issues: list[str] = []
    warnings: list[str] = []

    if not run.get("target_chat_id"):
        blocking_issues.append("目标副本尚未创建，不能进行在线深度预检。")
        target_access = "missing"

    if not any(account.get("session_status") == "ok" for account in accounts):
        blocking_issues.append("没有可用的 Telegram 登录会话，无法执行在线迁移。")

    if target_access != "ok" or not target_write_account:
        blocking_issues.append("没有账号能访问目标副本，后续文本和媒体迁移均不可执行。")

    if any(
        account.get("target_access") == "ok"
        and account.get("target_send_permission") == "blocked"
        for account in accounts
    ):
        blocking_issues.append("检测到目标副本可能禁止当前账号发消息。")

    if remote_latest_message_id <= 0:
        blocking_issues.append("未能确认源群最新消息，不能建立无遗漏迁移快照。")
    elif local_latest_message_id < remote_latest_message_id:
        blocking_issues.append(
            "本地数据库尚未采集到源群最新消息，不能开始迁移以免遗漏；"
            "请先完成源群同步后重新执行在线深度预检。"
        )

    if source_access != "ok":
        warnings.append(
            "源群无法被当前可用账号稳定访问；媒体只能依赖已有数据库文本，不能从源群转发。"
        )

    if any(
        account.get("source_forwarding_permission") == "blocked"
        for account in accounts
        if account.get("source_access") == "ok"
    ):
        warnings.append(
            "源群已限制转发或保存内容；媒体迁移会被 Telegram 阻断，不会尝试绕过限制。"
        )

    if not migration_account and relay_ready:
        warnings.append(
            "没有单个账号同时访问源群与目标副本；已启用固定中转频道桥接媒体和相册迁移。"
        )
    elif not migration_account:
        if relay:
            warnings.append(
                "没有账号同时访问源群与目标副本，且固定中转频道访问不完整；"
                f"媒体、头像和相册迁移会被阻断或跳过。中转检测：{_relay_account_diagnostics(accounts)}"
            )
        else:
            warnings.append(
                "没有账号同时访问源群与目标副本；已加入中转频道还不够，"
                "必须配置 TG_CLONE_RELAY_CHAT_ID 后重新执行在线深度预检，"
                "才能让两个账号桥接媒体和相册迁移。"
            )

    if relay and any(
        _clean_text(account.get("relay_safety")).startswith("unsafe_")
        for account in accounts
    ):
        blocking_issues.append(
            "固定中转目标不是私有广播频道，拒绝暂存源媒体以避免向第三方暴露内容。"
        )
    elif relay and any(
        account.get("relay_access") == "ok" and account.get("relay_safety") == "unknown"
        for account in accounts
    ):
        blocking_issues.append(
            "无法验证固定中转频道的私有广播属性，拒绝执行中转媒体迁移。"
        )

    if secondary_session_status == "not_configured":
        warnings.append("未配置独立第二账号，无法验证第二账号迁移链路。")

    if target_access == "ok" and any(
        account.get("target_send_permission") == "unknown"
        for account in accounts
        if account.get("target_access") == "ok"
    ):
        warnings.append(
            "目标副本写入权限未执行试发验证，真正迁移前仍建议先小批量试跑。"
        )

    if any(account.get("source_latest_error") for account in accounts):
        warnings.append("源群最新消息读取失败，源群访问只确认到实体解析层。")

    text_strategy = "database_replay" if target_write_account else "blocked"
    if migration_account:
        media_strategy = CLONE_MEDIA_STRATEGY_SOURCE_COPY_WITHOUT_ATTRIBUTION
        media_group_strategy = "strict_skip_incomplete"
        avatar_strategy = "skip_not_implemented"
    elif relay_ready:
        media_strategy = CLONE_MEDIA_STRATEGY_RELAY_COPY_WITHOUT_ATTRIBUTION
        media_group_strategy = "relay_api_rebuild"
        avatar_strategy = "skip_not_implemented"
    elif target_write_account:
        media_strategy = "impossible_without_local_vault"
        media_group_strategy = "blocked_by_source_access"
        avatar_strategy = "skip"
    else:
        media_strategy = "blocked"
        media_group_strategy = "blocked"
        avatar_strategy = "skip"

    capabilities = {
        "network_access_checked": bool(network_access_checked),
        "target_write_account": target_write_account,
        "forward_privacy": CLONE_FORWARD_PRIVACY_MODE,
        "forward_requires_drop_author": True,
        "forward_keeps_source_link": False,
        "source_latest_message": _first_latest_message(accounts),
        "source_snapshot": source_snapshot_payload,
        "accounts": accounts,
        "media_relay": {
            **relay,
            "enabled": bool(relay_ready),
            "source_account": relay_source_account,
            "target_account": relay_target_account,
            "privacy": CLONE_FORWARD_PRIVACY_MODE,
            "requires_drop_author_each_hop": True,
            "keeps_source_link": False,
            "keeps_relay_link": False,
            "requires_private_broadcast_channel": True,
        }
        if relay
        else {},
    }
    plan = {
        "version": 2,
        "generated_at": _admin_now_iso(),
        "run_id": run.get("run_id"),
        "source": {
            "chat_id": run.get("source_chat_id"),
            "title": run.get("source_title"),
            "username": run.get("source_chat_username"),
            "message_count": run.get("source_message_count"),
            "last_message_at": run.get("source_last_message_at"),
            "last_message_ts": run.get("source_last_message_ts"),
            "snapshot": source_snapshot_payload,
        },
        "target": {
            "chat_id": run.get("target_chat_id"),
            "title": run.get("target_title"),
            "kind": run.get("target_kind"),
            "username": run.get("target_username"),
            "owner_session": run.get("target_owner_session"),
        },
        "network_access_checked": bool(network_access_checked),
        "source_access": source_access,
        "target_access": target_access,
        "primary_session_status": primary_session_status,
        "secondary_session_status": secondary_session_status,
        "migration_account": migration_account or "unavailable",
        "target_write_account": target_write_account or "unavailable",
        "media_relay": capabilities["media_relay"],
        "forward_privacy": CLONE_FORWARD_PRIVACY_MODE,
        "forward_requires_drop_author": True,
        "forward_keeps_source_link": False,
        "source_snapshot": source_snapshot_payload,
        "text_strategy": text_strategy,
        "media_strategy": media_strategy,
        "media_group_strategy": media_group_strategy,
        "avatar_strategy": avatar_strategy,
        "blocking_issues": blocking_issues,
        "warnings": warnings,
        "capabilities": capabilities,
    }
    return {
        "status": "done",
        "source_access": source_access,
        "target_access": target_access,
        "primary_session_status": primary_session_status,
        "secondary_session_status": secondary_session_status,
        "migration_account": migration_account or "unavailable",
        "text_strategy": text_strategy,
        "media_strategy": media_strategy,
        "media_group_strategy": media_group_strategy,
        "avatar_strategy": avatar_strategy,
        "blocking_issues": blocking_issues,
        "warnings": warnings,
        "capabilities": capabilities,
        "plan": plan,
        "error_message": "",
    }


def _try_mark_clone_plan_failed(
    *,
    get_conn_fn: Callable[[], Any],
    plan_id: str,
    message: str,
) -> None:
    _try_update_record(
        get_conn_fn=get_conn_fn,
        update_fn=update_clone_plan,
        plan_id=plan_id,
        status="error",
        error_message=message,
        completed_at=_admin_now_iso(),
    )


def _load_clone_run_required(
    *,
    get_conn_fn: Callable[[], Any],
    run_id: str,
) -> dict[str, Any]:
    return _load_required_record(
        get_conn_fn=get_conn_fn,
        load_fn=load_clone_run,
        missing_message="克隆运行记录不存在，无法执行深度预检",
        run_id=run_id,
    )


def _primary_cfg(cfg: Any) -> Any:
    return _cfg_with_session_name(cfg, _clean_text(getattr(cfg, "session_name", "")))


def _secondary_cfg(cfg: Any) -> Any | None:
    primary_session_name = _clean_text(getattr(cfg, "session_name", ""))
    secondary_session_name = _clean_text(getattr(cfg, "secondary_session_name", ""))
    if not secondary_session_name or secondary_session_name == primary_session_name:
        return None
    return _cfg_with_session_name(cfg, secondary_session_name)


def _admin_clone_deep_preflight_job_runner(
    job_id: str,
    *,
    run_id: str,
    plan_id: str,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    heartbeat_stop, heartbeat_thread = start_admin_job_heartbeat(job_id)
    try:
        mark_admin_job_running(
            job_id,
            admin_job_set_status_fn=admin_job_set_status_fn,
        )
        _update_required_record(
            get_conn_fn=get_conn_fn,
            update_fn=update_clone_plan,
            missing_message="克隆迁移计划不存在，已停止深度预检",
            plan_id=plan_id,
            status="running",
            error_message="",
        )
        update_admin_job_progress(
            job_id,
            0,
            total=CLONE_DEEP_PREFLIGHT_TOTAL_STEPS,
            stage="running",
        )

        run = _load_clone_run_required(get_conn_fn=get_conn_fn, run_id=run_id)
        source_snapshot = call_with_conn(
            get_conn_fn,
            build_clone_source_snapshot,
            source_chat_id=int(run["source_chat_id"]),
        )
        admin_job_append_log_fn(
            job_id,
            f"开始在线深度预检：run={run_id}，源={run['source_title']}，目标={run.get('target_title') or '未创建'}",
        )
        update_admin_job_progress(
            job_id,
            1,
            total=CLONE_DEEP_PREFLIGHT_TOTAL_STEPS,
            stage="loaded_run",
        )

        accounts: list[dict[str, Any]] = []
        if not run.get("target_chat_id"):
            admin_job_append_log_fn(job_id, "目标副本尚未创建，生成阻断型迁移计划")
            outcome = _build_deep_preflight_outcome(
                run=run,
                accounts=accounts,
                network_access_checked=False,
                source_snapshot=source_snapshot,
                cfg=cfg,
            )
        else:
            update_admin_job_progress(
                job_id,
                2,
                total=CLONE_DEEP_PREFLIGHT_TOTAL_STEPS,
                stage="checking_accounts",
            )
            primary_result = _check_account_access(
                account="primary",
                cfg=_primary_cfg(cfg),
                job_id=job_id,
                run=run,
                admin_job_append_log_fn=admin_job_append_log_fn,
            )
            accounts.append(primary_result)

            secondary_cfg = _secondary_cfg(cfg)
            if secondary_cfg is None:
                accounts.append(
                    _base_account_result(
                        account="secondary",
                        session_name=_clean_text(
                            getattr(cfg, "secondary_session_name", "")
                        ),
                        configured=False,
                    )
                )
            else:
                accounts.append(
                    _check_account_access(
                        account="secondary",
                        cfg=secondary_cfg,
                        job_id=job_id,
                        run=run,
                        admin_job_append_log_fn=admin_job_append_log_fn,
                    )
                )

            update_admin_job_progress(
                job_id,
                4,
                total=CLONE_DEEP_PREFLIGHT_TOTAL_STEPS,
                stage="planning",
            )
            outcome = _build_deep_preflight_outcome(
                run=run,
                accounts=accounts,
                network_access_checked=True,
                source_snapshot=source_snapshot,
                cfg=cfg,
            )

        completed_at = _admin_now_iso()
        plan = _update_required_record(
            get_conn_fn=get_conn_fn,
            update_fn=update_clone_plan,
            missing_message="克隆迁移计划不存在，已停止深度预检",
            plan_id=plan_id,
            completed_at=completed_at,
            **outcome,
        )
        if plan.get("blocking_issues"):
            admin_job_append_log_fn(
                job_id,
                "深度预检完成，但存在阻断项："
                + "；".join(str(item) for item in plan["blocking_issues"]),
            )
        else:
            admin_job_append_log_fn(job_id, "深度预检完成：迁移计划可进入下一阶段")
        update_admin_job_progress(
            job_id,
            CLONE_DEEP_PREFLIGHT_TOTAL_STEPS,
            total=CLONE_DEEP_PREFLIGHT_TOTAL_STEPS,
            stage="done",
        )
        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        logging.exception("克隆深度预检任务失败: job_id=%s", job_id)
        message = admin_error_message(exc)
        _try_mark_clone_plan_failed(
            get_conn_fn=get_conn_fn,
            plan_id=plan_id,
            message=message,
        )
        admin_job_append_log_fn(job_id, f"深度预检失败：{message}")
        update_admin_job_progress(
            job_id,
            0,
            total=CLONE_DEEP_PREFLIGHT_TOTAL_STEPS,
            stage="error",
        )
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)


def _admin_start_clone_deep_preflight_job_thread(job_id: str, **kwargs: Any):
    from tg_harvest.admin_jobs.common import start_admin_job_thread

    return start_admin_job_thread(
        _admin_clone_deep_preflight_job_runner,
        job_id,
        **kwargs,
    )
