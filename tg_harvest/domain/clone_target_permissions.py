from typing import Any


def _send_messages_restricted(rights: Any) -> bool:
    return bool(rights is not None and getattr(rights, "send_messages", False))


def clone_target_send_permission(entity: Any) -> str:
    """Return whether the current account can write to a clone target entity.

    Resolving a public channel only proves that it exists.  Broadcast channels
    additionally require the current account to be the creator or to have the
    ``post_messages`` administrator right.  Megagroups permit ordinary
    members unless their participant or default rights prohibit sending.
    """

    if entity is None:
        return "unknown"
    if bool(getattr(entity, "creator", False)):
        return "ok"
    if bool(getattr(entity, "left", False)) or bool(
        getattr(entity, "kicked", False)
    ):
        return "blocked"

    admin_rights = getattr(entity, "admin_rights", None)
    if bool(getattr(entity, "broadcast", False)):
        return (
            "ok"
            if admin_rights is not None
            and bool(getattr(admin_rights, "post_messages", False))
            else "blocked"
        )

    if bool(getattr(entity, "megagroup", False)):
        if admin_rights is not None:
            return "ok"
        if _send_messages_restricted(getattr(entity, "banned_rights", None)):
            return "blocked"
        if _send_messages_restricted(
            getattr(entity, "default_banned_rights", None)
        ):
            return "blocked"
        return "ok"

    # The clone workflow creates Channel/Megagroup targets.  This fallback
    # keeps ordinary legacy group chats writable when Telegram returns one,
    # while broadcast channels always take the strict branch above.
    return "ok"


def clone_target_write_was_rejected(error: Any) -> bool:
    """Whether Telegram conclusively rejected a target-side write request."""

    text = f"{type(error).__name__}: {error}".lower()
    markers = (
        "chatadminrequired",
        "chatwriteforbidden",
        "chatrestricted",
        "userbanned",
        "not enough rights",
        "缺少目标频道发布消息",
        "目标群管理权限",
    )
    return any(marker in text for marker in markers)
