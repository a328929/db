from typing import Any

from telethon.tl.types import InputChannel, InputPeerChannel

from tg_harvest.admin_jobs.sessions import bind_client_event_loop


def clone_run_target_input_channel(
    client: Any,
    clone_run: dict,
) -> InputChannel | None:
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
