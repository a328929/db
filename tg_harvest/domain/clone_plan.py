from typing import Any

from tg_harvest.domain.coerce import clean_text as _clean_text, safe_int

CLONE_TEXT_MIGRATION_DEFAULT_SEND_DELAY_MS = 500
CLONE_TEXT_MIGRATION_MAX_MESSAGE_LIMIT = 100_000
CLONE_TEXT_MIGRATION_MAX_SEND_DELAY_MS = 60_000
CLONE_TEXT_REPLAY_CHUNK_MAX_LEN = 3900
CLONE_MEDIA_STRATEGY_SOURCE_COPY_WITHOUT_ATTRIBUTION = (
    "source_copy_without_attribution"
)
CLONE_MEDIA_STRATEGY_RELAY_COPY_WITHOUT_ATTRIBUTION = (
    "relay_copy_without_attribution"
)
CLONE_FORWARD_PRIVACY_MODE = "without_source_attribution"
VALID_CLONE_WRITE_ACCOUNTS = frozenset({"primary", "secondary"})
VALID_CLONE_MEDIA_STRATEGIES = frozenset(
    {
        CLONE_MEDIA_STRATEGY_SOURCE_COPY_WITHOUT_ATTRIBUTION,
        CLONE_MEDIA_STRATEGY_RELAY_COPY_WITHOUT_ATTRIBUTION,
    }
)


def clone_plan_payload(plan: dict[str, Any]) -> dict[str, Any]:
    payload = plan.get("plan")
    return payload if isinstance(payload, dict) else {}


def clone_plan_capabilities(plan: dict[str, Any]) -> dict[str, Any]:
    capabilities = plan.get("capabilities")
    return capabilities if isinstance(capabilities, dict) else {}


def clone_plan_blocking_issues(plan: dict[str, Any]) -> list[Any]:
    issues = plan.get("blocking_issues")
    return issues if isinstance(issues, list) else []


def clone_plan_target_write_account(plan: dict[str, Any]) -> str:
    capabilities = clone_plan_capabilities(plan)
    payload = clone_plan_payload(plan)
    for value in (
        capabilities.get("target_write_account"),
        payload.get("target_write_account"),
        plan.get("migration_account"),
        payload.get("migration_account"),
    ):
        normalized = _clean_text(value).lower()
        if normalized in VALID_CLONE_WRITE_ACCOUNTS:
            return normalized
    return ""


def clone_plan_media_migration_account(plan: dict[str, Any]) -> str:
    payload = clone_plan_payload(plan)
    relay = clone_plan_media_relay(plan)
    if clone_plan_uses_media_relay(plan):
        normalized = _clean_text(relay.get("target_account")).lower()
        if normalized in VALID_CLONE_WRITE_ACCOUNTS:
            return normalized
    for value in (
        plan.get("migration_account"),
        payload.get("migration_account"),
    ):
        normalized = _clean_text(value).lower()
        if normalized in VALID_CLONE_WRITE_ACCOUNTS:
            return normalized
    return ""


def clone_plan_uses_media_relay(plan: dict[str, Any]) -> bool:
    return (
        _clean_text(plan.get("media_strategy"))
        == CLONE_MEDIA_STRATEGY_RELAY_COPY_WITHOUT_ATTRIBUTION
    )


def clone_plan_media_relay(plan: dict[str, Any]) -> dict[str, Any]:
    capabilities = clone_plan_capabilities(plan)
    payload = clone_plan_payload(plan)
    for value in (
        capabilities.get("media_relay"),
        payload.get("media_relay"),
    ):
        if isinstance(value, dict):
            return value
    return {}


def clone_plan_media_relay_chat_id(plan: dict[str, Any]) -> int:
    relay = clone_plan_media_relay(plan)
    return safe_int(relay.get("chat_id"))


def clone_plan_media_source_account(plan: dict[str, Any]) -> str:
    if clone_plan_uses_media_relay(plan):
        relay = clone_plan_media_relay(plan)
        normalized = _clean_text(relay.get("source_account")).lower()
        if normalized in VALID_CLONE_WRITE_ACCOUNTS:
            return normalized
        return ""
    return clone_plan_media_migration_account(plan)


def clone_plan_media_target_account(plan: dict[str, Any]) -> str:
    if clone_plan_uses_media_relay(plan):
        relay = clone_plan_media_relay(plan)
        normalized = _clean_text(relay.get("target_account")).lower()
        if normalized in VALID_CLONE_WRITE_ACCOUNTS:
            return normalized
        return ""
    return clone_plan_media_migration_account(plan)


def clone_plan_media_relay_ready(plan: dict[str, Any]) -> bool:
    if not clone_plan_uses_media_relay(plan):
        return False
    relay = clone_plan_media_relay(plan)
    return bool(
        relay.get("enabled")
        and clone_plan_media_relay_chat_id(plan) != 0
        and clone_plan_media_source_account(plan)
        and clone_plan_media_target_account(plan)
    )


def clone_plan_media_execution_label(plan: dict[str, Any]) -> str:
    if clone_plan_uses_media_relay(plan):
        source_account = clone_plan_media_source_account(plan)
        target_account = clone_plan_media_target_account(plan)
        if source_account and target_account:
            return f"{source_account}->relay->{target_account}"
        return ""
    return clone_plan_media_migration_account(plan)
