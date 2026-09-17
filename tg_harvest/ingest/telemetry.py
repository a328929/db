from contextlib import contextmanager
from dataclasses import dataclass


@dataclass
class TelegramRequestCounts:
    requests: int = 0


@contextmanager
def count_telegram_requests(client):
    """Count logical RPC requests on one isolated Telethon 1.x worker client."""
    counts = TelegramRequestCounts()
    original = getattr(client, "_call", None)
    if not callable(original):
        yield counts
        return
    had_override = "_call" in getattr(client, "__dict__", {})

    async def measured(sender, request, *args, **kwargs):
        counts.requests += len(request) if isinstance(request, (list, tuple)) else 1
        return await original(sender, request, *args, **kwargs)

    client._call = measured
    try:
        yield counts
    finally:
        if had_override:
            client._call = original
        else:
            del client._call
