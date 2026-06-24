import json
import logging
import queue
import threading
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from tg_harvest.domain.coerce import clean_text, safe_int

logger = logging.getLogger(__name__)

BOT_API_BASE_URL = "https://api.telegram.org"
MAX_TELEGRAM_MESSAGE_LEN = 4096
DEFAULT_TIMEOUT_SECONDS = 3.0
_QUEUE_MAXSIZE = 100

_notify_queue: queue.Queue[tuple[Any, str, str | None]] | None = None
_notify_worker_started = False
_notify_lock = threading.Lock()


def _enabled_int(value: object) -> int:
    return 1 if safe_int(value) == 1 else 0


def bot_token(cfg: Any) -> str:
    return clean_text(getattr(cfg, "ops_bot_token", ""))


def notify_chat_id(cfg: Any) -> str:
    return clean_text(getattr(cfg, "ops_bot_notify_chat_id", ""))


def is_notify_enabled(cfg: Any) -> bool:
    return (
        _enabled_int(getattr(cfg, "ops_bot_enabled", 0)) == 1
        and bool(bot_token(cfg))
        and bool(notify_chat_id(cfg))
    )


def bot_timeout_seconds(cfg: Any) -> float:
    value = getattr(cfg, "ops_bot_timeout_seconds", DEFAULT_TIMEOUT_SECONDS)
    try:
        return max(0.5, float(value))
    except (TypeError, ValueError):
        return DEFAULT_TIMEOUT_SECONDS


def mask_bot_token(token: str) -> str:
    token = clean_text(token)
    if not token:
        return ""
    if len(token) <= 10:
        return "***"
    return f"{token[:4]}...{token[-4:]}"


def trim_message(text: str) -> str:
    message = clean_text(text)
    if len(message) <= MAX_TELEGRAM_MESSAGE_LEN:
        return message
    suffix = "\n...[truncated]"
    return message[: MAX_TELEGRAM_MESSAGE_LEN - len(suffix)] + suffix


def _bot_api_url(token: str, method: str) -> str:
    return f"{BOT_API_BASE_URL}/bot{token}/{method}"


def _post_bot_api(
    *,
    token: str,
    method: str,
    payload: dict[str, Any],
    timeout_seconds: float,
) -> dict[str, Any] | None:
    body = urlencode(payload).encode("utf-8")
    request = Request(
        _bot_api_url(token, method),
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            raw = response.read(8192)
    except HTTPError as exc:
        logger.warning("运维机器人请求失败：method=%s status=%s", method, exc.code)
        return None
    except URLError as exc:
        logger.warning("运维机器人网络异常：method=%s error=%s", method, exc.reason)
        return None
    except TimeoutError:
        logger.warning("运维机器人请求超时：method=%s", method)
        return None
    except Exception as exc:
        logger.warning("运维机器人请求异常：method=%s error=%r", method, exc)
        return None

    try:
        data = json.loads(raw.decode("utf-8"))
    except Exception:
        logger.warning("运维机器人返回无法解析：method=%s", method)
        return None
    if not bool(data.get("ok")):
        logger.warning(
            "运维机器人返回失败：method=%s description=%s",
            method,
            data.get("description"),
        )
        return None
    return data


def send_message_sync(
    cfg: Any,
    text: str,
    *,
    chat_id: str | None = None,
) -> bool:
    token = bot_token(cfg)
    target_chat_id = str(chat_id or notify_chat_id(cfg)).strip()
    if not token or not target_chat_id:
        return False

    payload = {
        "chat_id": target_chat_id,
        "text": trim_message(text),
        "disable_web_page_preview": "true",
    }
    return (
        _post_bot_api(
            token=token,
            method="sendMessage",
            payload=payload,
            timeout_seconds=bot_timeout_seconds(cfg),
        )
        is not None
    )


def _worker() -> None:
    assert _notify_queue is not None
    while True:
        cfg, text, chat_id = _notify_queue.get()
        try:
            send_message_sync(cfg, text, chat_id=chat_id)
        except Exception:
            logger.exception("运维机器人异步通知发送失败")
        finally:
            _notify_queue.task_done()


def _ensure_worker_started() -> queue.Queue[tuple[Any, str, str | None]]:
    global _notify_queue, _notify_worker_started

    with _notify_lock:
        if _notify_queue is None:
            _notify_queue = queue.Queue(maxsize=_QUEUE_MAXSIZE)
        if not _notify_worker_started:
            thread = threading.Thread(
                target=_worker,
                name="tg-ops-bot-notifier",
                daemon=True,
            )
            thread.start()
            _notify_worker_started = True
        return _notify_queue


def enqueue_message(cfg: Any, text: str, *, chat_id: str | None = None) -> bool:
    if not is_notify_enabled(cfg) and chat_id is None:
        return False

    if not bot_token(cfg):
        return False

    target_chat_id = str(chat_id or notify_chat_id(cfg)).strip()
    if not target_chat_id:
        return False

    notify_queue = _ensure_worker_started()
    try:
        notify_queue.put_nowait((cfg, trim_message(text), target_chat_id))
    except queue.Full:
        logger.warning("运维机器人通知队列已满，本条通知已丢弃")
        return False
    return True
