# -*- coding: utf-8 -*-
"""Backward-compatible facade for the split harvest implementation.

This module intentionally keeps the old import surface (`tg_harvest.harvest`) while
routing all behavior to the maintained split modules:
- `harvest_parse.py`
- `harvest_store.py`
- `harvest_runner.py`
"""

from .harvest_parse import (
    HarvestCounters,
    build_msg_link,
    classify_msg_type,
    extract_media_meta,
    extract_message_text,
    log_parse_failure_summary,
    resolve_target_entities,
    resolve_target_entity,
    setup_logging,
)
from .harvest_runner import collect_target_entities, get_existing_chat_ids, run_harvest
from .harvest_store import (
    batch_upsert,
    get_chat_stats,
    get_last_message_id,
    refresh_media_groups_for_chat,
    upsert_chat,
)

__all__ = [
    "HarvestCounters",
    "setup_logging",
    "classify_msg_type",
    "extract_message_text",
    "extract_media_meta",
    "build_msg_link",
    "log_parse_failure_summary",
    "resolve_target_entities",
    "resolve_target_entity",
    "get_existing_chat_ids",
    "collect_target_entities",
    "upsert_chat",
    "get_last_message_id",
    "batch_upsert",
    "refresh_media_groups_for_chat",
    "get_chat_stats",
    "run_harvest",
]
