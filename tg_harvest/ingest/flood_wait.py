import logging
import random
import re
from dataclasses import dataclass
from typing import Any

DEFAULT_FLOOD_WAIT_SWITCH_THRESHOLD = 30
DEFAULT_FLOOD_RETRY_MIN_SECONDS = 2.0
DEFAULT_FLOOD_RETRY_MAX_SECONDS = 300.0


@dataclass
class AccountFloodWaitError(RuntimeError):
    seconds: int
    threshold_seconds: int
    account_label: str = ""
    scope: str = ""

    def __post_init__(self) -> None:
        account = self.account_label or "-"
        scope = self.scope or "-"
        RuntimeError.__init__(
            self,
            "Telegram FloodWait 超过切换阈值 "
            f"(account={account}, scope={scope}, wait={self.seconds}s, "
            f"threshold={self.threshold_seconds}s)",
        )


def flood_wait_threshold(value: object) -> int:
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return DEFAULT_FLOOD_WAIT_SWITCH_THRESHOLD


def flood_sleep_threshold_kwargs(cfg: object) -> dict[str, int]:
    return {
        "flood_sleep_threshold": flood_wait_threshold(
            getattr(cfg, "flood_wait_switch_threshold", DEFAULT_FLOOD_WAIT_SWITCH_THRESHOLD)
        )
    }


def is_flood_wait_error(exc: BaseException) -> bool:
    if isinstance(exc, AccountFloodWaitError):
        return True
    text = f"{exc.__class__.__name__}: {exc}".lower()
    return "floodwait" in text or "flood wait" in text


def flood_wait_seconds(exc: BaseException) -> int:
    if isinstance(exc, AccountFloodWaitError):
        return max(1, int(exc.seconds))
    raw_seconds = getattr(exc, "seconds", None)
    try:
        if raw_seconds is not None:
            return max(1, int(raw_seconds))
    except (TypeError, ValueError):
        pass

    text = f"{exc.__class__.__name__}: {exc}".lower()
    patterns = [
        r"wait of\s+(\d+)\s+seconds",
        r"wait\s+(\d+)\s*s",
        r"(\d+)\s+seconds",
        r"floodwait(?:error)?[_ ]?(\d+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            try:
                return max(1, int(match.group(1)))
            except (TypeError, ValueError):
                return 1
    return 1


def raise_if_long_flood_wait(
    exc: BaseException,
    *,
    threshold_seconds: int,
    account_label: str = "",
    scope: str = "",
) -> None:
    if not is_flood_wait_error(exc):
        return
    threshold = flood_wait_threshold(threshold_seconds)
    seconds = flood_wait_seconds(exc)
    if seconds > threshold:
        raise AccountFloodWaitError(
            seconds=seconds,
            threshold_seconds=threshold,
            account_label=account_label,
            scope=scope,
        ) from exc


def bounded_retry_count(value: object, default: int = 3) -> int:
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return max(1, int(default))


def _bounded_retry_seconds(value: object, default: float) -> float:
    try:
        return max(0.0, float(value))
    except (TypeError, ValueError):
        return max(0.0, float(default))


def exponential_backoff_seconds(
    retry_index: int,
    *,
    min_seconds: float = DEFAULT_FLOOD_RETRY_MIN_SECONDS,
    max_seconds: float = DEFAULT_FLOOD_RETRY_MAX_SECONDS,
    jitter_min_seconds: float = 0.5,
    jitter_max_seconds: float = 1.5,
    required_wait_seconds: int = 0,
) -> float:
    safe_retry_index = max(1, int(retry_index))
    safe_min_seconds = _bounded_retry_seconds(min_seconds, DEFAULT_FLOOD_RETRY_MIN_SECONDS)
    safe_max_seconds = max(
        safe_min_seconds,
        _bounded_retry_seconds(max_seconds, DEFAULT_FLOOD_RETRY_MAX_SECONDS),
    )
    base_backoff = min(safe_max_seconds, safe_min_seconds * (2.0 ** (safe_retry_index - 1)))
    effective_wait = max(float(max(0, int(required_wait_seconds))), base_backoff)
    safe_jitter_min = _bounded_retry_seconds(jitter_min_seconds, 0.5)
    safe_jitter_max = max(safe_jitter_min, _bounded_retry_seconds(jitter_max_seconds, 1.5))
    return effective_wait + random.uniform(safe_jitter_min, safe_jitter_max)


def short_retry_sleep_seconds(
    retry_index: int,
    *,
    min_seconds: float = DEFAULT_FLOOD_RETRY_MIN_SECONDS,
    max_seconds: float = DEFAULT_FLOOD_RETRY_MAX_SECONDS,
) -> float:
    return exponential_backoff_seconds(
        retry_index,
        min_seconds=min_seconds,
        max_seconds=max_seconds,
        jitter_min_seconds=0.2,
        jitter_max_seconds=1.0,
        required_wait_seconds=0,
    )


def is_transient_telegram_error(exc: BaseException) -> bool:
    if is_flood_wait_error(exc):
        return False
    name = exc.__class__.__name__.lower()
    if name in {
        "floodwaiterror",
        "filereferenceexpirederror",
        "channelprivateerror",
        "chatforbiddenerror",
        "userbannedinchannelerror",
    }:
        return False
    return isinstance(exc, (TimeoutError, ConnectionError, OSError))


def format_retry_context(*, retry_index: int, max_retries: int, wait_seconds: float) -> str:
    return (
        f"retry={max(1, int(retry_index))}/{bounded_retry_count(max_retries)} "
        f"wait={max(0.0, float(wait_seconds)):.2f}s"
    )


def maybe_refresh_entity_cursor(client: Any, entity: Any) -> bool:
    if client is None or not hasattr(client, "get_messages"):
        return False
    try:
        client.get_messages(entity, limit=1)
        return True
    except Exception:
        return False


def call_with_bounded_retry(
    fn: Any,
    /,
    *args: Any,
    max_retries: int = 3,
    flood_wait_threshold_seconds: int = DEFAULT_FLOOD_WAIT_SWITCH_THRESHOLD,
    account_label: str = "",
    scope: str = "",
    sleep_fn: Any = None,
    **kwargs: Any,
) -> Any:
    safe_sleep_fn = sleep_fn
    if safe_sleep_fn is None:
        import time

        safe_sleep_fn = time.sleep

    safe_max_retries = bounded_retry_count(max_retries)
    retry_count = 0
    while True:
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            if is_flood_wait_error(exc):
                raise_if_long_flood_wait(
                    exc,
                    threshold_seconds=flood_wait_threshold_seconds,
                    account_label=account_label,
                    scope=scope,
                )
                retry_count += 1
                if retry_count >= safe_max_retries:
                    raise
                wait_seconds = exponential_backoff_seconds(
                    retry_count,
                    required_wait_seconds=flood_wait_seconds(exc),
                )
                logging.warning(
                    "Telegram 调用触发 FloodWait，准备重试: scope=%s account=%s %s",
                    scope or "-",
                    account_label or "-",
                    format_retry_context(
                        retry_index=retry_count,
                        max_retries=safe_max_retries,
                        wait_seconds=wait_seconds,
                    ),
                )
                safe_sleep_fn(wait_seconds)
                continue
            if is_transient_telegram_error(exc):
                retry_count += 1
                if retry_count >= safe_max_retries:
                    raise
                wait_seconds = short_retry_sleep_seconds(retry_count)
                logging.warning(
                    "Telegram 调用出现瞬时错误，准备重试: scope=%s account=%s %s error=%s",
                    scope or "-",
                    account_label or "-",
                    format_retry_context(
                        retry_index=retry_count,
                        max_retries=safe_max_retries,
                        wait_seconds=wait_seconds,
                    ),
                    exc,
                )
                safe_sleep_fn(wait_seconds)
                continue
            raise
