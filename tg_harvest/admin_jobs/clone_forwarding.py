from typing import Any

from tg_harvest.admin_jobs.sessions import bind_client_event_loop


def clone_forward_without_source_attribution(
    client: Any,
    target_entity: Any,
    messages: Any,
    *,
    from_peer: Any,
    as_album: bool | None = None,
) -> Any:
    """Forward as a copied message, hiding Telegram's source attribution."""
    kwargs: dict[str, Any] = {
        "from_peer": from_peer,
        "drop_author": True,
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
