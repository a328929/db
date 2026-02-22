# -*- coding: utf-8 -*-
import os
from dataclasses import dataclass
from .db import resolve_db_path

# =========================
# 配置区（支持环境变量覆盖）
# =========================

def _env_str(name: str, default: str = "") -> str:
    v = os.getenv(name, None)
    return (v if v is not None else default).strip()


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name, None)
    if v is None:
        return int(default)
    try:
        return int(v.strip())
    except Exception:
        return int(default)


@dataclass
class AppConfig:
    api_id: int
    api_hash: str
    session_name: str

    db_name: str
    target_group: str
    scan_existing_chats: int

    dedup_mode: str
    dedup_threshold: int

    batch_size: int
    rescan_tail_ids: int
    media_caption_guard_len: int
    promo_score_threshold: int
    log_every: int

    @classmethod
    def load(cls) -> "AppConfig":
        return cls(
            api_id=_env_int("TG_API_ID", 2040),
            api_hash=_env_str("TG_API_HASH", "b18441a1ff607e10a989891a5462e627"),
            session_name=_env_str("TG_SESSION_NAME", "my_session"),

            db_name=resolve_db_path(_env_str("TG_DB_NAME", "tg_data.db")),
            target_group=_env_str("TG_TARGET_GROUP", "顶级萝莉内部群"),
            scan_existing_chats=_env_int("TG_SCAN_DB_CHATS", 0),

            dedup_mode=_env_str("TG_DEDUP_MODE", "PURGE_ALL").upper(),
            dedup_threshold=_env_int("TG_DEDUP_THRESHOLD", 2),

            batch_size=_env_int("TG_BATCH_SIZE", 1000),
            rescan_tail_ids=_env_int("TG_RESCAN_TAIL_IDS", 1000),
            media_caption_guard_len=_env_int("TG_MEDIA_CAPTION_GUARD_LEN", 58),
            promo_score_threshold=_env_int("TG_PROMO_SCORE_THRESHOLD", 3),
            log_every=_env_int("TG_LOG_EVERY", 1000),
        )


CFG = AppConfig.load()


def _is_enabled(v: int) -> bool:
    return int(v) == 1
