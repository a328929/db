from typing import Any

from tg_harvest.admin_jobs.clone import _cfg_with_session_name
from tg_harvest.admin_jobs.sessions import bind_client_event_loop
from tg_harvest.domain.clone_plan import CLONE_TEXT_REPLAY_CHUNK_MAX_LEN


def clean_clone_text(value: Any) -> str:
    return str(value or "").strip()


def normalize_clone_nonnegative_int(
    value: Any,
    *,
    default: int = 0,
    max_value: int | None = None,
) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        normalized = int(default)
    normalized = max(0, normalized)
    if max_value is not None:
        normalized = min(normalized, int(max_value))
    return normalized


def clone_cfg_for_account(cfg: Any, account: str) -> Any:
    normalized = clean_clone_text(account).lower()
    primary_session_name = clean_clone_text(getattr(cfg, "session_name", ""))
    secondary_session_name = clean_clone_text(getattr(cfg, "secondary_session_name", ""))
    if normalized == "secondary":
        if not secondary_session_name:
            raise RuntimeError("计划要求第二账号执行迁移，但未配置第二账号 session")
        if secondary_session_name == primary_session_name:
            raise RuntimeError("第二账号 session 与主账号相同，不能作为独立迁移账号")
        return _cfg_with_session_name(cfg, secondary_session_name)
    if not primary_session_name:
        raise RuntimeError("计划要求主账号执行迁移，但未配置主账号 session")
    return _cfg_with_session_name(cfg, primary_session_name)


def split_clone_text_chunks(text: str) -> list[str]:
    value = str(text or "")
    if not value:
        return []
    return [
        value[index : index + CLONE_TEXT_REPLAY_CHUNK_MAX_LEN]
        for index in range(0, len(value), CLONE_TEXT_REPLAY_CHUNK_MAX_LEN)
    ]


def clone_sent_message_id(result: Any) -> int | None:
    if isinstance(result, (list, tuple)):
        result = result[0] if result else None
    raw_id = getattr(result, "id", None)
    if raw_id in (None, ""):
        return None
    try:
        return int(raw_id)
    except (TypeError, ValueError):
        return None


def send_clone_text_chunk(client: Any, target_entity: Any, text: str) -> int | None:
    with bind_client_event_loop(client):
        result = client.send_message(target_entity, text)
    return clone_sent_message_id(result)
