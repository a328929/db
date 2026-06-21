import logging
from collections.abc import Callable, Iterable
from contextlib import suppress
from typing import Any

from telethon.tl.functions.channels import GetChannelsRequest
from telethon.tl.types import InputChannel, InputPeerChannel, InputPeerChat

from tg_harvest.admin_jobs.common import (
    admin_error_message,
    finish_job_heartbeat,
    resolve_chat_entity,
    start_admin_job_thread,
)
from tg_harvest.admin_jobs.core import (
    _admin_job_heartbeat,
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
    bind_client_event_loop,
)
from tg_harvest.domain.chat_inventory import (
    discover_session_files,
    entity_has_all_platform_terms_restriction,
    scan_session_chat_recovery_rows,
)
from tg_harvest.storage.recovery import (
    recover_chats_from_candidates,
    replace_recovery_chat_scan_results,
)

RECOVERY_VALIDATION_BATCH_SIZE = 100


def _row_chat_label(row: Any) -> str:
    chat_id = int(getattr(row, "chat_id", 0) or 0)
    title = str(getattr(row, "chat_title", "") or "").strip() or f"Chat {chat_id}"
    return f"{title} (ID={chat_id})"


def _recovery_row_input_peer(row: Any) -> Any | None:
    source_entity_id = getattr(row, "source_entity_id", None)
    if source_entity_id is None:
        return None
    try:
        raw_entity_id = int(source_entity_id)
    except (TypeError, ValueError):
        return None

    raw_text = str(abs(raw_entity_id))
    if raw_text.startswith("100") and len(raw_text) > 3:
        access_hash = getattr(row, "source_access_hash", None)
        if access_hash in (None, ""):
            return None
        try:
            return InputPeerChannel(int(raw_text[3:]), int(access_hash))
        except (TypeError, ValueError):
            return None

    return InputPeerChat(abs(raw_entity_id))


def _recovery_row_input_channel(row: Any) -> InputChannel | None:
    input_peer = _recovery_row_input_peer(row)
    if not isinstance(input_peer, InputPeerChannel):
        return None
    try:
        return InputChannel(
            int(input_peer.channel_id),
            int(input_peer.access_hash),
        )
    except (TypeError, ValueError):
        return None


def _exception_text(exc: Exception) -> str:
    return f"{exc.__class__.__name__}: {exc}".lower().replace("_", "")


def _dissolved_exception_reason(exc: Exception) -> str:
    text = _exception_text(exc)
    dissolved_markers = (
        "could not find the input entity",
        "not exist",
        "no user has",
        "deleted",
        "deactivated",
        "chatidinvalid",
        "channelinvalid",
    )
    if any(marker in text for marker in dissolved_markers):
        return "该群组/频道已解散或不存在"
    return ""


def _unavailable_exception_reason(exc: Exception) -> str:
    text = _exception_text(exc)
    unavailable_markers = (
        "channelprivate",
        "channel private",
        "chatforbidden",
        "channelforbidden",
    )
    if any(marker in text for marker in unavailable_markers):
        return "Telegram 返回该会话不可访问"
    return ""


def _entity_exclusion_reason(entity: Any) -> str:
    entity_type = entity.__class__.__name__.lower().lstrip("_")
    if entity_type in {"channelforbidden", "chatforbidden"}:
        return "Telegram 返回该会话不可访问"
    if bool(getattr(entity, "deactivated", False)) or bool(
        getattr(entity, "deleted", False)
    ):
        return "该群组/频道已解散或不存在"
    if entity_has_all_platform_terms_restriction(entity):
        return "Telegram 返回全部平台/违反条款，该会话不可访问"
    return ""


def _resolve_recovery_candidate_entity(client: Any, row: Any) -> Any:
    exceptions: list[Exception] = []
    input_peer = _recovery_row_input_peer(row)
    if input_peer is not None:
        try:
            with bind_client_event_loop(client):
                return client.get_entity(input_peer)
        except Exception as exc:
            exceptions.append(exc)

    chat_username = str(getattr(row, "chat_username", "") or "").strip() or None
    try:
        return resolve_chat_entity(client, int(row.chat_id), chat_username)
    except Exception as exc:
        exceptions.append(exc)

    if chat_username:
        try:
            with bind_client_event_loop(client):
                return client.get_entity(chat_username)
        except Exception as exc:
            exceptions.append(exc)

    for exc in reversed(exceptions):
        if _dissolved_exception_reason(exc) or _unavailable_exception_reason(exc):
            raise exc
    if exceptions:
        raise exceptions[-1]
    raise RuntimeError("无法识别群组/频道 ID")


def _keep_recovery_row_for_entity(entity: Any, stats: dict[str, Any]) -> bool:
    exclusion_reason = _entity_exclusion_reason(entity)
    if exclusion_reason == "该群组/频道已解散或不存在":
        stats["dissolved_count"] += 1
        return False
    if exclusion_reason:
        stats["unavailable_count"] += 1
        return False
    return True


def _keep_recovery_row_for_exception(
    row: Any,
    exc: Exception,
    stats: dict[str, Any],
) -> bool:
    dissolved_reason = _dissolved_exception_reason(exc)
    unavailable_reason = _unavailable_exception_reason(exc)
    if dissolved_reason:
        stats["dissolved_count"] += 1
        return False
    if unavailable_reason:
        stats["unavailable_count"] += 1
        return False
    stats["warning_count"] += 1
    stats["warnings"].append(f"{_row_chat_label(row)}：{admin_error_message(exc)}")
    return True


def _validate_recovery_row(client: Any, row: Any, stats: dict[str, Any]) -> bool:
    try:
        entity = _resolve_recovery_candidate_entity(client, row)
    except Exception as exc:
        return _keep_recovery_row_for_exception(row, exc, stats)
    return _keep_recovery_row_for_entity(entity, stats)


def _batch_resolve_recovery_channel_entities(
    client: Any,
    batch: list[tuple[int, Any, InputChannel]],
) -> dict[int, Any] | None:
    if not batch or not callable(client):
        return None

    try:
        with bind_client_event_loop(client):
            payload = client(GetChannelsRequest([item[2] for item in batch]))
    except Exception:
        return None

    entities = getattr(payload, "chats", None)
    if entities is None:
        entities = payload if isinstance(payload, list) else []

    rows_by_chat_id = {
        int(getattr(row, "chat_id", 0) or 0): row_index
        for row_index, row, _input_channel in batch
    }
    resolved: dict[int, Any] = {}
    for entity in entities:
        try:
            chat_id = int(getattr(entity, "id", 0) or 0)
        except (TypeError, ValueError):
            continue
        row_index = rows_by_chat_id.get(chat_id)
        if row_index is not None:
            resolved[row_index] = entity
    return resolved


def _filter_recovery_chat_scan_rows(
    client: Any,
    rows: Iterable[Any],
    progress_callback: Callable[[int, int], None] | None = None,
) -> tuple[list[Any], dict[str, Any]]:
    row_list = list(rows)
    kept_rows: list[Any] = []
    stats: dict[str, Any] = {
        "dissolved_count": 0,
        "unavailable_count": 0,
        "warning_count": 0,
        "warnings": [],
    }
    total = len(row_list)
    row_keep_by_index: dict[int, bool] = {}
    processed_count = 0

    def mark_processed(row_index: int, keep: bool) -> None:
        nonlocal processed_count
        row_keep_by_index[row_index] = keep
        processed_count += 1
        if progress_callback is not None:
            progress_callback(processed_count, total)

    batchable_rows: list[tuple[int, Any, InputChannel]] = []
    individual_rows: list[tuple[int, Any]] = []
    for row_index, row in enumerate(row_list):
        input_channel = _recovery_row_input_channel(row)
        if input_channel is None or not callable(client):
            individual_rows.append((row_index, row))
            continue
        batchable_rows.append((row_index, row, input_channel))

    for start in range(0, len(batchable_rows), RECOVERY_VALIDATION_BATCH_SIZE):
        batch = batchable_rows[start : start + RECOVERY_VALIDATION_BATCH_SIZE]
        resolved_entities = _batch_resolve_recovery_channel_entities(client, batch)
        if resolved_entities is None:
            for row_index, row, _input_channel in batch:
                mark_processed(row_index, _validate_recovery_row(client, row, stats))
            continue

        for row_index, row, _input_channel in batch:
            entity = resolved_entities.get(row_index)
            if entity is None:
                keep = _validate_recovery_row(client, row, stats)
            else:
                keep = _keep_recovery_row_for_entity(entity, stats)
            mark_processed(row_index, keep)

    for row_index, row in individual_rows:
        mark_processed(row_index, _validate_recovery_row(client, row, stats))

    kept_rows = [
        row
        for row_index, row in enumerate(row_list)
        if bool(row_keep_by_index.get(row_index))
    ]

    return kept_rows, stats


def _admin_recovery_scan_job_runner(
    job_id: str,
    *,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    job_context.set(str(job_id))
    heartbeat_stop, heartbeat_thread = _start_job_heartbeat(job_id, _admin_job_heartbeat)
    local_client = None
    worker_id = f"{job_id}_recovery_scan"
    try:
        admin_job_set_status_fn(job_id, "running")
        _admin_job_update_progress(
            job_id,
            0,
            total=None,
            stage="running",
            log_step=0,
            auto_log=False,
        )

        admin_job_append_log_fn(job_id, "正在查找本地 Telegram Session 文件...")
        session_files = discover_session_files(getattr(cfg, "session_name", ""))
        if not session_files:
            admin_job_append_log_fn(job_id, "未找到可读取的 .session 文件")
            _admin_job_update_progress(
                job_id,
                0,
                total=0,
                stage="done",
                log_step=0,
                auto_log=False,
            )
            admin_job_set_status_fn(job_id, "done")
            return

        admin_job_append_log_fn(
            job_id,
            "发现 Session 文件：" + "、".join(path.name for path in session_files),
        )
        rows, errors = scan_session_chat_recovery_rows(session_files)
        for error in errors[:20]:
            admin_job_append_log_fn(job_id, f"Session 读取警告：{error}")
        if len(errors) > 20:
            admin_job_append_log_fn(
                job_id,
                f"还有 {len(errors) - 20} 个 Session 读取警告已省略",
            )

        scanned_at = _admin_now_iso()
        admin_job_append_log_fn(job_id, f"扫描到 {len(rows)} 个群组/频道候选")
        if rows:
            admin_job_append_log_fn(job_id, "正在验证 Telegram 会话...")
            if not _ensure_base_session_valid(cfg, job_id, admin_job_append_log_fn):
                admin_job_set_status_fn(job_id, "error")
                return
            admin_job_append_log_fn(
                job_id,
                "正在连接 Telegram 验证候选是否已解散或不可访问...",
            )
            _admin_job_update_progress(
                job_id,
                0,
                total=len(rows),
                stage="validating",
                log_step=0,
                auto_log=False,
            )
            local_client = _create_isolated_worker_client(cfg, worker_id)
            validation_progress_step = 25
            last_validation_progress = 0

            def _update_validation_progress(current: int, total: int) -> None:
                nonlocal last_validation_progress
                if (
                    current < total
                    and current != 1
                    and current - last_validation_progress < validation_progress_step
                ):
                    return
                last_validation_progress = current
                _admin_job_update_progress(
                    job_id,
                    current,
                    total=total,
                    stage="validating",
                    log_step=0,
                    auto_log=False,
                )

            rows, filter_stats = _filter_recovery_chat_scan_rows(
                local_client,
                rows,
                progress_callback=_update_validation_progress,
            )
            warning_messages = list(filter_stats.get("warnings") or [])
            for warning in warning_messages[:20]:
                admin_job_append_log_fn(job_id, f"候选验证警告：{warning}")
            if len(warning_messages) > 20:
                admin_job_append_log_fn(
                    job_id,
                    f"还有 {len(warning_messages) - 20} 个候选验证警告已省略",
                )
            admin_job_append_log_fn(
                job_id,
                "候选验证完成："
                f"保留 {len(rows)} 个，"
                f"过滤已解散 {int(filter_stats.get('dissolved_count') or 0)} 个，"
                f"过滤不可访问 {int(filter_stats.get('unavailable_count') or 0)} 个，"
                f"验证异常保留 {int(filter_stats.get('warning_count') or 0)} 个",
            )
            _admin_job_update_progress(
                job_id,
                len(rows),
                total=len(rows),
                stage="saving",
                log_step=0,
                auto_log=False,
            )
        admin_job_append_log_fn(job_id, "正在保存恢复候选...")
        conn = get_conn_fn()
        try:
            saved_count = replace_recovery_chat_scan_results(
                conn,
                rows,
                scan_job_id=job_id,
                scanned_at=scanned_at,
            )
        finally:
            conn.close()

        _admin_job_update_progress(
            job_id,
            saved_count,
            total=saved_count,
            stage="done",
            log_step=0,
            auto_log=False,
        )
        admin_job_append_log_fn(job_id, f"恢复候选保存完成：{saved_count} 个")
        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        logging.exception("扫描 Session 恢复候选失败: job_id=%s", job_id)
        admin_job_append_log_fn(job_id, f"扫描失败：{admin_error_message(exc)}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)
        if local_client:
            with suppress(Exception):
                _disconnect_worker_client(local_client)
        _cleanup_isolated_worker_session(cfg, worker_id)


def _admin_start_recovery_scan_job_thread(job_id: str, **kwargs):
    return start_admin_job_thread(
        _admin_recovery_scan_job_runner,
        job_id,
        **kwargs,
    )


def _admin_recovery_restore_job_runner(
    job_id: str,
    *,
    chat_ids: Iterable[int] | None,
    target_label: str,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    job_context.set(str(job_id))
    heartbeat_stop, heartbeat_thread = _start_job_heartbeat(job_id, _admin_job_heartbeat)
    try:
        normalized_chat_ids = (
            None if chat_ids is None else sorted({int(chat_id) for chat_id in chat_ids})
        )
        admin_job_set_status_fn(job_id, "running")
        _admin_job_update_progress(
            job_id,
            0,
            total=None if normalized_chat_ids is None else len(normalized_chat_ids),
            stage="running",
            log_step=0,
            auto_log=False,
        )
        admin_job_append_log_fn(job_id, f"恢复目标：{target_label}")
        admin_job_append_log_fn(job_id, "正在写入 chats 摘要...")

        conn = get_conn_fn()
        try:
            result = recover_chats_from_candidates(
                conn,
                chat_ids=normalized_chat_ids,
                job_id=job_id,
                recovered_at=_admin_now_iso(),
            )
        finally:
            conn.close()

        candidate_count = int(result["candidate_count"])
        recovered_count = int(result["recovered_count"])
        skipped_count = int(result["skipped_count"])
        _admin_job_update_progress(
            job_id,
            candidate_count,
            total=candidate_count,
            stage="done",
            log_step=0,
            auto_log=False,
        )
        admin_job_append_log_fn(
            job_id,
            f"恢复完成：新增 {recovered_count} 个，已存在跳过 {skipped_count} 个",
        )
        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        logging.exception("恢复 Session 群组候选失败: job_id=%s", job_id)
        admin_job_append_log_fn(job_id, f"恢复失败：{admin_error_message(exc)}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)


def _admin_start_recovery_restore_job_thread(job_id: str, **kwargs):
    return start_admin_job_thread(
        _admin_recovery_restore_job_runner,
        job_id,
        **kwargs,
    )
