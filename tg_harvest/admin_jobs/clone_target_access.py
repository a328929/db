from typing import Any

from telethon.tl.types import InputChannel, InputPeerChannel

from tg_harvest.admin_jobs.sessions import bind_client_event_loop


def _clone_chat_identity_candidates(value: Any) -> set[int]:
    try:
        original = int(value or 0)
    except (TypeError, ValueError):
        return set()
    identity = abs(original)
    if not identity:
        return set()
    candidates = {identity}
    raw = str(identity)
    # Telegram's channel marker is the *negative* ``-100...`` form.  A
    # positive entity ID is already canonical even when its digits happen to
    # begin with 100.
    if original < 0 and raw.startswith("100") and len(raw) > 3:
        candidates.add(int(raw[3:]))
    return candidates


def _source_uses_channel_namespace(clone_run: dict) -> bool | None:
    source_type = str(clone_run.get("source_chat_type") or "").strip().lower()
    if "megagroup" in source_type or "supergroup" in source_type:
        return True
    if source_type.startswith("channel"):
        return True
    if source_type.startswith("chat") or source_type in {"group", "basic_group"}:
        return False
    return None


def clone_run_target_conflicts_with_source(clone_run: dict) -> bool:
    # Structure clones are always Telegram channels (broadcast or megagroup).
    # A basic Chat and a Channel may legitimately share the same numeric id.
    if _source_uses_channel_namespace(clone_run) is False:
        return False
    source_identities = _clone_chat_identity_candidates(clone_run.get("source_chat_id"))
    target_identities = _clone_chat_identity_candidates(clone_run.get("target_chat_id"))
    return not source_identities.isdisjoint(target_identities)


def clone_run_target_input_channel(
    client: Any,
    clone_run: dict,
) -> InputChannel | None:
    """Resolve target channel input from clone run metadata.

    NOTE: This has a potential race condition - the access_hash or cached entity
    could become stale between retrieval and use. Callers should handle
    CHANNEL_INVALID or similar errors and potentially retry with fresh entity resolution.
    """
    if clone_run_target_conflicts_with_source(clone_run):
        return None
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

    # Fallback to session cache - this could be stale but is better than nothing
    try:
        with bind_client_event_loop(client):
            cached_entity = client.get_input_entity(target_chat_id)
        if isinstance(cached_entity, InputChannel):
            return cached_entity
        if isinstance(cached_entity, InputPeerChannel):
            return InputChannel(cached_entity.channel_id, cached_entity.access_hash)
    except Exception:
        # Session cache miss or error - return None and let caller handle
        pass
    return None
