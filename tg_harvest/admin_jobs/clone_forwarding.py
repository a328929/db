from typing import Any

from tg_harvest.admin_jobs.sessions import bind_client_event_loop


class CloneForwardOutcomeAmbiguousError(RuntimeError):
    """Telegram may have accepted the forward but no recoverable response remains."""


def _message_id_list(messages: Any) -> tuple[list[int], bool]:
    if isinstance(messages, (list, tuple)):
        return [int(message_id) for message_id in messages], False
    return [int(messages)], True


def _forward_with_random_ids(
    client: Any,
    target_entity: Any,
    messages: Any,
    *,
    from_peer: Any,
    random_ids: list[int],
    silent: bool,
) -> Any:
    """Use MTProto random IDs so a resumed delivery cannot create duplicates."""
    from telethon.tl.functions.messages import ForwardMessagesRequest

    message_ids, single = _message_id_list(messages)
    if len(message_ids) != len(random_ids):
        raise ValueError("媒体转发随机 ID 数量与消息数量不一致")

    with bind_client_event_loop(client):
        target_peer = client.get_input_entity(target_entity)
        source_peer = client.get_input_entity(from_peer)
        request = ForwardMessagesRequest(
            from_peer=source_peer,
            id=message_ids,
            to_peer=target_peer,
            silent=bool(silent),
            drop_author=True,
            random_id=[int(random_id) for random_id in random_ids],
        )
        previous_raise_last_error = getattr(client, "_raise_last_call_error", None)
        if previous_raise_last_error is not None:
            client._raise_last_call_error = True
        try:
            result = client(request)
        except Exception as exc:
            error_text = f"{type(exc).__name__}: {exc}".lower()
            if "randomidduplicate" in error_text or (
                "random id" in error_text
                and ("duplicate" in error_text or "already used" in error_text)
            ):
                raise CloneForwardOutcomeAmbiguousError(
                    "Telegram 报告该随机 ID 已被使用，说明此前转发可能已经成功，"
                    "但本地没有保存对应消息 ID；任务已停止以避免重复消息"
                ) from exc
            raise
        finally:
            if previous_raise_last_error is not None:
                client._raise_last_call_error = previous_raise_last_error
        resolve_messages = getattr(client, "_get_response_message", None)
        if not callable(resolve_messages):
            raise RuntimeError("当前 Telegram 客户端不支持可恢复媒体转发")
        sent = resolve_messages(request, result, target_peer)
    return sent[0] if single else sent


def clone_forward_without_source_attribution(
    client: Any,
    target_entity: Any,
    messages: Any,
    *,
    from_peer: Any,
    random_ids: list[int] | None = None,
    silent: bool = True,
) -> Any:
    """Forward as a copied message, hiding Telegram's source attribution."""
    if (
        random_ids is not None
        and callable(getattr(client, "get_input_entity", None))
        and callable(client)
    ):
        return _forward_with_random_ids(
            client,
            target_entity,
            messages,
            from_peer=from_peer,
            random_ids=random_ids,
            silent=silent,
        )

    kwargs: dict[str, Any] = {
        "from_peer": from_peer,
        "drop_author": True,
        "silent": bool(silent),
    }

    with bind_client_event_loop(client):
        return client.forward_messages(target_entity, messages, **kwargs)


def clone_delete_copied_relay_messages(
    client: Any,
    relay_entity: Any,
    message_ids: list[int],
) -> Any:
    """Best-effort cleanup for temporary relay copies after a target copy attempt."""
    with bind_client_event_loop(client):
        return client.delete_messages(relay_entity, message_ids, revoke=True)
