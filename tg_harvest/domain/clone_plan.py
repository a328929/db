from typing import Any

from tg_harvest.domain.coerce import clean_text as _clean_text
from tg_harvest.domain.coerce import safe_int

CLONE_TEXT_MIGRATION_DEFAULT_SEND_DELAY_MS = 500
CLONE_TEXT_MIGRATION_MAX_MESSAGE_LIMIT = 100_000
CLONE_TEXT_MIGRATION_MAX_SEND_DELAY_MS = 60_000
CLONE_TEXT_REPLAY_CHUNK_MAX_LEN = 3900
CLONE_MEDIA_STRATEGY_SOURCE_COPY_WITHOUT_ATTRIBUTION = "source_copy_without_attribution"
CLONE_MEDIA_STRATEGY_RELAY_COPY_WITHOUT_ATTRIBUTION = "relay_copy_without_attribution"
CLONE_FORWARD_PRIVACY_MODE = "without_source_attribution"
VALID_CLONE_WRITE_ACCOUNTS = frozenset({"primary", "secondary"})
VALID_CLONE_MEDIA_STRATEGIES = frozenset(
    {
        CLONE_MEDIA_STRATEGY_SOURCE_COPY_WITHOUT_ATTRIBUTION,
        CLONE_MEDIA_STRATEGY_RELAY_COPY_WITHOUT_ATTRIBUTION,
    }
)


def _preview_nonnegative_int(preview: dict[str, Any] | None, key: str) -> int:
    if not isinstance(preview, dict):
        return 0
    try:
        value = int(preview.get(key) or 0)
    except (TypeError, ValueError):
        return 0
    return value if value > 0 else 0


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


def clone_plan_source_snapshot(plan: dict[str, Any]) -> dict[str, Any]:
    capabilities = clone_plan_capabilities(plan)
    payload = clone_plan_payload(plan)
    for value in (
        capabilities.get("source_snapshot"),
        payload.get("source_snapshot"),
        plan.get("source_snapshot"),
    ):
        if isinstance(value, dict):
            return value
    return {}


def clone_plan_source_snapshot_message_id(plan: dict[str, Any]) -> int:
    return safe_int(clone_plan_source_snapshot(plan).get("message_id"))


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


def clone_plan_timeline_readiness(
    plan: dict[str, Any] | None,
    *,
    preview: dict[str, Any] | None = None,
) -> dict[str, Any]:
    has_preview = isinstance(preview, dict)
    text_remaining = _preview_nonnegative_int(preview, "text_remaining")
    media_remaining = _preview_nonnegative_int(preview, "media_remaining")
    timeline_remaining = _preview_nonnegative_int(preview, "timeline_remaining")
    if has_preview and timeline_remaining <= 0:
        timeline_remaining = text_remaining + media_remaining

    if not isinstance(plan, dict):
        return {
            "can_migrate_timeline": False,
            "reason_codes": ["plan_missing"],
            "text_remaining": text_remaining,
            "media_remaining": media_remaining,
            "timeline_remaining": timeline_remaining,
            "target_write_account": "",
            "migration_account": "",
            "media_execution_account": "",
            "media_source_account": "",
            "media_target_account": "",
            "media_relay_ready": False,
            "source_snapshot_message_id": 0,
            "text_account": "",
        }

    target_write_account = clone_plan_target_write_account(plan)
    migration_account = clone_plan_media_migration_account(plan)
    media_execution_account = clone_plan_media_execution_label(plan)
    media_source_account = clone_plan_media_source_account(plan)
    media_target_account = clone_plan_media_target_account(plan)
    media_relay_ready = clone_plan_media_relay_ready(plan)
    source_snapshot_message_id = clone_plan_source_snapshot_message_id(plan)

    reasons: list[str] = []
    if plan.get("status") != "done":
        reasons.append("plan_not_done")
    if clone_plan_blocking_issues(plan):
        reasons.append("plan_blocked")
    if plan.get("target_access") != "ok":
        reasons.append("target_inaccessible")
    if source_snapshot_message_id <= 0:
        reasons.append("source_snapshot_missing")

    if has_preview and text_remaining <= 0 and media_remaining <= 0:
        reasons.append("no_timeline_remaining")

    if text_remaining > 0:
        if plan.get("text_strategy") != "database_replay":
            reasons.append("text_strategy_blocked")
        if not target_write_account:
            reasons.append("missing_target_write_account")

    if media_remaining > 0:
        if plan.get("source_access") != "ok":
            reasons.append("source_inaccessible")
        media_strategy = _clean_text(plan.get("media_strategy"))
        if media_strategy not in VALID_CLONE_MEDIA_STRATEGIES:
            reasons.append("media_strategy_blocked")
        elif clone_plan_uses_media_relay(plan) and not media_relay_ready:
            reasons.append("media_relay_not_ready")
        elif (
            not media_execution_account
            or not media_source_account
            or not media_target_account
        ):
            reasons.append("missing_media_account")

    return {
        "can_migrate_timeline": not reasons,
        "reason_codes": reasons,
        "text_remaining": text_remaining,
        "media_remaining": media_remaining,
        "timeline_remaining": timeline_remaining,
        "target_write_account": target_write_account,
        "migration_account": migration_account,
        "media_execution_account": media_execution_account,
        "media_source_account": media_source_account,
        "media_target_account": media_target_account,
        "media_relay_ready": media_relay_ready,
        "source_snapshot_message_id": source_snapshot_message_id,
        "text_account": target_write_account if text_remaining > 0 else "",
    }
