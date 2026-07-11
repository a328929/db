import logging
import sqlite3
from contextlib import suppress
from typing import Any

from tg_harvest.admin_jobs.clone_forwarding import (
    clone_delete_copied_relay_messages,
    clone_forward_without_source_attribution,
)
from tg_harvest.admin_jobs.clone_timeline_store import CloneMappingPersistenceError
from tg_harvest.admin_jobs.common import call_with_conn, resolve_chat_entity
from tg_harvest.admin_jobs.runtime import _admin_now_iso
from tg_harvest.domain.clone_plan import (
    clone_plan_media_relay,
    clone_plan_media_relay_chat_id,
)
from tg_harvest.domain.coerce import clean_text as clean_clone_media_text
from tg_harvest.domain.coerce import optional_int
from tg_harvest.ingest.flood_wait import AccountFloodWaitError, raise_if_long_flood_wait
from tg_harvest.storage.clone import record_clone_message_mapping


def clone_sent_message_ids(result: Any) -> list[int | None]:
    if result is None:
        return []
    items = result if isinstance(result, (list, tuple)) else [result]
    return [optional_int(getattr(item, "id", None)) for item in items]


def resolve_clone_relay_chat(client: Any, plan: dict[str, Any]) -> Any:
    relay = clone_plan_media_relay(plan)
    relay_chat_id = clone_plan_media_relay_chat_id(plan)
    if not relay_chat_id:
        raise RuntimeError("迁移计划缺少固定中转频道")
    return resolve_chat_entity(
        client,
        relay_chat_id,
        clean_clone_media_text(relay.get("username")),
        allow_username_fallback=False,
    )


def copy_clone_media_direct_without_source(
    *,
    client: Any,
    target_entity: Any,
    message_ids: int | list[int],
    source_entity: Any,
    as_album: bool | None = None,
) -> Any:
    return clone_forward_without_source_attribution(
        client,
        target_entity,
        message_ids,
        from_peer=source_entity,
        as_album=as_album,
    )


def copy_clone_media_via_relay_without_source(
    *,
    source_client: Any,
    target_client: Any,
    relay_entity_for_source: Any,
    relay_entity_for_target: Any,
    target_entity: Any,
    message_ids: int | list[int],
    source_entity: Any,
    as_album: bool | None = None,
    log_step: Any | None = None,
    target_attempts: list[dict[str, Any]] | None = None,
    flood_wait_threshold_seconds: int = 30,
) -> Any:
    if callable(log_step):
        log_step(
            "媒体桥接第一跳：源群 -> 中转群"
        )
    relay_result = clone_forward_without_source_attribution(
        source_client,
        relay_entity_for_source,
        message_ids,
        from_peer=source_entity,
        as_album=as_album,
    )
    relay_sent_ids = clone_sent_message_ids(relay_result)
    relay_cleanup_ids = [
        int(message_id) for message_id in relay_sent_ids if message_id is not None
    ]
    if isinstance(message_ids, list):
        if len(relay_sent_ids) != len(message_ids) or any(
            message_id is None for message_id in relay_sent_ids
        ):
            if relay_cleanup_ids:
                with suppress(Exception):
                    clone_delete_copied_relay_messages(
                        source_client,
                        relay_entity_for_source,
                        relay_cleanup_ids,
                    )
            raise RuntimeError("媒体复制到固定中转频道后未完整返回消息 ID")
        relay_message_ids = [int(message_id) for message_id in relay_sent_ids]
    else:
        relay_message_id = (relay_sent_ids or [None])[0]
        if relay_message_id is None:
            raise RuntimeError("媒体复制到固定中转频道后未返回消息 ID")
        relay_message_ids = [int(relay_message_id)]
    if not relay_message_ids:
        raise RuntimeError("媒体复制到固定中转频道后未返回消息 ID")

    relay_copy_messages: int | list[int]
    relay_copy_as_album: bool | None = None
    if isinstance(message_ids, list):
        relay_copy_messages = relay_message_ids
        relay_copy_as_album = bool(as_album) if as_album is not None else True
    else:
        relay_copy_messages = int(relay_message_ids[0])
        relay_copy_as_album = None

    attempts = target_attempts or [
        {
            "client": target_client,
            "relay_entity": relay_entity_for_target,
            "target_entity": target_entity,
            "account": "",
        }
    ]

    try:
        for index, attempt in enumerate(attempts):
            attempt_client = attempt.get("client")
            attempt_relay_entity = attempt.get("relay_entity")
            attempt_target_entity = attempt.get("target_entity")
            attempt_account = clean_clone_media_text(attempt.get("account"))
            if callable(log_step):
                if index == 0:
                    log_step("媒体桥接第二跳：中转群 -> 克隆群")
                elif attempt_account:
                    log_step(f"第二跳切换账号：改由 {attempt_account} 接管中转群 -> 克隆群")
                else:
                    log_step("第二跳切换账号：改由备用账号接管中转群 -> 克隆群")
            try:
                return clone_forward_without_source_attribution(
                    attempt_client,
                    attempt_target_entity,
                    relay_copy_messages,
                    from_peer=attempt_relay_entity,
                    as_album=relay_copy_as_album,
                )
            except Exception as exc:
                try:
                    raise_if_long_flood_wait(
                        exc,
                        threshold_seconds=int(flood_wait_threshold_seconds or 30),
                        account_label=attempt_account,
                        scope="clone-relay-target-hop",
                    )
                except AccountFloodWaitError as flood_exc:
                    if callable(log_step):
                        wait_text = f"{flood_exc.seconds}s"
                        if index + 1 < len(attempts):
                            log_step(
                                f"第二跳触发频控：{attempt_account or '当前账号'} 需等待 {wait_text}，准备切换下一账号"
                            )
                        else:
                            log_step(
                                f"第二跳触发频控：{attempt_account or '当前账号'} 需等待 {wait_text}"
                            )
                    if index + 1 < len(attempts):
                        continue
                    raise
                raise
        raise RuntimeError("中转群到克隆群的桥接发送未成功完成")
    finally:
        with suppress(Exception):
            clone_delete_copied_relay_messages(
                source_client,
                relay_entity_for_source,
                relay_message_ids,
            )
            if callable(log_step):
                log_step("已清理本次中转临时消息")


def clone_source_message_for_api_id(
    *,
    source_chat_id: int,
    source_message_id: int,
    db_messages_by_id: dict[int, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    source_message = (db_messages_by_id or {}).get(int(source_message_id))
    if source_message is not None:
        return source_message
    return {
        "chat_id": int(source_chat_id),
        "message_id": int(source_message_id),
        "msg_date_ts": None,
        "msg_date_text": "",
        "sort_ts": 0,
        "caption": "",
    }


def resolved_clone_group_key(
    resolved_group: dict[str, Any],
    message_ids: list[int],
) -> tuple[Any, ...]:
    api_grouped_id = resolved_group.get("grouped_id")
    if api_grouped_id not in (None, ""):
        try:
            return ("grouped_id", int(api_grouped_id))
        except (TypeError, ValueError):
            return ("grouped_id", str(api_grouped_id))
    return ("message_ids", tuple(int(message_id) for message_id in message_ids))


def record_clone_media_mapping(
    *,
    get_conn_fn: Any,
    migration_id: str,
    run_id: str,
    plan_id: str,
    source_message: dict[str, Any],
    target_chat_id: int,
    target_message_id: int | None,
    mode: str,
    status: str,
    error_message: str = "",
) -> None:
    try:
        call_with_conn(
            get_conn_fn,
            record_clone_message_mapping,
            migration_id=migration_id,
            run_id=run_id,
            plan_id=plan_id,
            source_chat_id=int(source_message["chat_id"]),
            source_message_id=int(source_message["message_id"]),
            source_msg_date_ts=source_message.get("msg_date_ts"),
            source_msg_date_text=source_message.get("msg_date_text"),
            target_chat_id=int(target_chat_id),
            target_message_id=target_message_id,
            chunk_index=0,
            chunk_count=1,
            mode=mode,
            status=status,
            error_message=error_message,
            sent_at=_admin_now_iso() if status == "done" else "",
        )
    except (sqlite3.Error, OSError, RuntimeError, TypeError, ValueError) as exc:
        logging.exception(
            "克隆媒体映射持久化失败: run_id=%s source=%s/%s mode=%s status=%s",
            run_id,
            source_message.get("chat_id"),
            source_message.get("message_id"),
            mode,
            status,
        )
        raise CloneMappingPersistenceError(
            "克隆媒体已发送但映射持久化失败，迁移已中止以避免重复发送"
        ) from exc
