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


def _load_raw_config_values() -> dict:
    return {
        "api_id": _env_int("TG_API_ID", 2040),
        "api_hash": _env_str("TG_API_HASH", "b18441a1ff607e10a989891a5462e627"),
        "session_name": _env_str("TG_SESSION_NAME", "my_session"),
        "db_name": resolve_db_path(_env_str("TG_DB_NAME", "tg_data.db")),
        "target_group": _env_str("TG_TARGET_GROUP", "顶级萝莉内部群"),
        "scan_existing_chats": _env_int("TG_SCAN_DB_CHATS", 0),
        "dedup_mode": _env_str("TG_DEDUP_MODE", "PURGE_ALL").upper(),
        "dedup_threshold": _env_int("TG_DEDUP_THRESHOLD", 2),
        "batch_size": _env_int("TG_BATCH_SIZE", 1000),
        "rescan_tail_ids": _env_int("TG_RESCAN_TAIL_IDS", 1000),
        "media_caption_guard_len": _env_int("TG_MEDIA_CAPTION_GUARD_LEN", 58),
        "promo_score_threshold": _env_int("TG_PROMO_SCORE_THRESHOLD", 3),
        "log_every": _env_int("TG_LOG_EVERY", 1000),
    }


def _normalize_config_values(raw: dict) -> dict:
    normalized = dict(raw)

    # 统一配置收口（防止环境变量误配）
    normalized["scan_existing_chats"] = 1 if int(normalized["scan_existing_chats"]) == 1 else 0

    if normalized["dedup_mode"] not in {"PURGE_ALL", "KEEP_FIRST"}:
        normalized["dedup_mode"] = "PURGE_ALL"

    normalized["dedup_threshold"] = max(2, int(normalized["dedup_threshold"]))
    normalized["batch_size"] = max(1, int(normalized["batch_size"]))
    normalized["rescan_tail_ids"] = max(0, int(normalized["rescan_tail_ids"]))
    normalized["media_caption_guard_len"] = max(0, int(normalized["media_caption_guard_len"]))
    normalized["promo_score_threshold"] = max(0, int(normalized["promo_score_threshold"]))
    normalized["log_every"] = max(1, int(normalized["log_every"]))
    return normalized


def _build_app_config(values: dict) -> "AppConfig":
    return AppConfig(
        api_id=values["api_id"],
        api_hash=values["api_hash"],
        session_name=values["session_name"],
        db_name=values["db_name"],
        target_group=values["target_group"],
        scan_existing_chats=values["scan_existing_chats"],
        dedup_mode=values["dedup_mode"],
        dedup_threshold=values["dedup_threshold"],
        batch_size=values["batch_size"],
        rescan_tail_ids=values["rescan_tail_ids"],
        media_caption_guard_len=values["media_caption_guard_len"],
        promo_score_threshold=values["promo_score_threshold"],
        log_every=values["log_every"],
    )


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
        raw = _load_raw_config_values()
        normalized = _normalize_config_values(raw)
        return _build_app_config(normalized)


CFG = AppConfig.load()


def _is_enabled(v: int) -> bool:
    return int(v) == 1
