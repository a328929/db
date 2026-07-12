from typing import Any

from tg_harvest.admin_jobs.clone import _cfg_with_session_name
from tg_harvest.admin_jobs.sessions import bind_client_event_loop
from tg_harvest.domain.clone_plan import CLONE_TEXT_REPLAY_CHUNK_MAX_LEN
from tg_harvest.domain.coerce import clean_text as clean_clone_text
from tg_harvest.domain.coerce import optional_int


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
    secondary_session_name = clean_clone_text(
        getattr(cfg, "secondary_session_name", "")
    )
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
    return optional_int(getattr(result, "id", None))


def _send_clone_text_with_random_id(
    client: Any,
    target_entity: Any,
    text: str,
    *,
    random_id: int,
) -> Any:
    from telethon.tl.functions.messages import SendMessageRequest

    with bind_client_event_loop(client):
        target_peer = client.get_input_entity(target_entity)
        request = SendMessageRequest(
            peer=target_peer,
            message=str(text),
            silent=True,
            random_id=int(random_id),
        )
        result = client(request)
        resolve_message = getattr(client, "_get_response_message", None)
        if not callable(resolve_message):
            raise RuntimeError("当前 Telegram 客户端不支持可恢复文本发送")
        return resolve_message(request, result, target_peer)


def send_clone_text_chunk(
    client: Any,
    target_entity: Any,
    text: str,
    *,
    random_id: int | None = None,
) -> int | None:
    if (
        random_id is not None
        and callable(getattr(client, "get_input_entity", None))
        and callable(client)
    ):
        return clone_sent_message_id(
            _send_clone_text_with_random_id(
                client,
                target_entity,
                text,
                random_id=int(random_id),
            )
        )

    with bind_client_event_loop(client):
        # The database stores literal text rather than Telegram entity spans.
        # Disabling Telethon's default Markdown parser prevents accidental
        # reinterpretation of source text during replay.
        result = client.send_message(target_entity, text, parse_mode=None, silent=True)
    return clone_sent_message_id(result)
