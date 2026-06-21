import re
from dataclasses import dataclass

DEFAULT_FLOOD_WAIT_SWITCH_THRESHOLD = 30


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

