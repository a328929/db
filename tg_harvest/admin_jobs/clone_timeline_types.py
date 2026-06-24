from dataclasses import dataclass, field
from typing import Any


@dataclass
class TimelineCounters:
    text_sent: int = 0
    text_skipped: int = 0
    text_failed: int = 0
    media_sent: int = 0
    media_skipped: int = 0
    media_failed: int = 0
    media_group_sent: int = 0
    media_group_skipped: int = 0
    media_group_failed: int = 0
    processed: int = 0

    def as_update_fields(self) -> dict[str, int]:
        return {
            "text_sent": self.text_sent,
            "text_skipped": self.text_skipped,
            "text_failed": self.text_failed,
            "media_sent": self.media_sent,
            "media_skipped": self.media_skipped,
            "media_failed": self.media_failed,
            "media_group_sent": self.media_group_sent,
            "media_group_skipped": self.media_group_skipped,
            "media_group_failed": self.media_group_failed,
        }


@dataclass
class TimelineExecutionState:
    job_id: str
    run_id: str
    plan_id: str
    migration_id: str
    source_chat_id: int
    target_chat_id: int
    target_title: str
    source_title: str
    preview: dict[str, Any]
    plan: dict[str, Any]
    run: dict[str, Any]
    accounts: dict[str, str]
    using_relay: bool
    normalized_message_limit: int
    normalized_send_delay_ms: int
    text_total: int
    media_total: int
    media_group_total: int
    progress_total: int
    counters: TimelineCounters = field(default_factory=TimelineCounters)
    after_ts: int | None = None
    after_message_id: int | None = None
    stopped: bool = False
    limit_reached: bool = False
    copied_api_group_keys: set[tuple[Any, ...]] = field(default_factory=set)


@dataclass(frozen=True)
class TimelineFinalStatus:
    status: str
    phase: str
    error_message: str
