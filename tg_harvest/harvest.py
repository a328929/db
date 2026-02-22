# -*- coding: utf-8 -*-
"""兼容层：保留旧导入路径，将职责拆分到 parse/store/runner 子模块。"""

from .harvest_parse import (
    HarvestCounters,
    build_msg_link,
    classify_msg_type,
    extract_media_meta,
    extract_message_text,
    log_parse_failure_summary,
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
    "run_harvest",
    "setup_logging",
    "classify_msg_type",
    "extract_message_text",
    "extract_media_meta",
    "resolve_target_entity",
    "collect_target_entities",
    "get_existing_chat_ids",
    "build_msg_link",
    "upsert_chat",
    "batch_upsert",
    "get_last_message_id",
    "refresh_media_groups_for_chat",
    "get_chat_stats",
    "HarvestCounters",
    "log_parse_failure_summary",
]


if __name__ == "__main__":
    run_harvest()
