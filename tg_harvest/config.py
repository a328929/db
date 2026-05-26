# -*- coding: utf-8 -*-
import os
from dataclasses import dataclass
from pathlib import Path
from tg_harvest.runtime.paths import resolve_db_path
from tg_harvest.runtime.paths import resolve_session_name

# =========================
# 配置加载
# =========================


def _load_dotenv_if_available() -> None:
    try:
        from dotenv import load_dotenv
    except Exception:
        return

    env_path = Path(__file__).resolve().parents[1] / ".env"
    load_dotenv(env_path, override=False)


_load_dotenv_if_available()


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
        # 电报接口编号。允许从环境使用测试专用编号，但服务端会限制用途。
        "api_id": _env_int("TG_API_ID", 0),
        # 电报接口密钥。必须与接口编号配套，只从环境读取。
        "api_hash": _env_str("TG_API_HASH", ""),
        # 电报会话路径。用于复用登录状态。
        "session_name": resolve_session_name(_env_str("TG_SESSION_NAME", "my_session")),
        # 数据库路径。相对路径会放入运行目录。
        "db_name": resolve_db_path(_env_str("TG_DB_NAME", "tg_data.db")),
        # 默认采集目标。可填群名、用户名或链接。
        "target_group": _env_str("TG_TARGET_GROUP", "顶级萝莉内部群"),
        # 是否扫描数据库中已有聊天对象。仅数值一表示开启。
        "scan_existing_chats": _env_int("TG_SCAN_DB_CHATS", 0),
        # 去重策略。非法值会回退为清理全部重复项。
        "dedup_mode": _env_str("TG_DEDUP_MODE", "PURGE_ALL").upper(),
        # 去重触发阈值。最小值为二，避免单条内容被误删。
        "dedup_threshold": _env_int("TG_DEDUP_THRESHOLD", 2),
        # 采集写入批量大小。越大吞吐越高，但内存和事务耗时也会增加。
        "batch_size": _env_int("TG_BATCH_SIZE", 2000),
        # 增量采集时回扫尾部编号范围。数值零表示关闭。
        "rescan_tail_ids": _env_int("TG_RESCAN_TAIL_IDS", 0),
        # 媒体说明文字保护长度。用于降低短说明误判风险。
        "media_caption_guard_len": _env_int("TG_MEDIA_CAPTION_GUARD_LEN", 58),
        # 推广内容判定分数阈值。关闭推广过滤时不参与新消息判定。
        "promo_score_threshold": _env_int("TG_PROMO_SCORE_THRESHOLD", 0),
        # 是否关闭推广过滤。仅数值一表示关闭。
        "disable_promo_filter": _env_int("TG_DISABLE_PROMO_FILTER", 1),
        # 采集进度日志间隔。最小值为一。
        "log_every": _env_int("TG_LOG_EVERY", 1000),
        # 数据库页面缓存大小，单位为兆。
        "sqlite_cache_mb": _env_int("TG_SQLITE_CACHE_MB", 512),
        # 数据库内存映射大小，单位为兆。数值零表示关闭。
        "sqlite_mmap_mb": _env_int("TG_SQLITE_MMAP_MB", 1024),
        # 后台任务最大保留数量。
        "admin_job_max_count": _env_int("TG_ADMIN_JOB_MAX_COUNT", 100),
        # 单个后台任务最大日志行数。
        "admin_job_log_max_lines": _env_int("TG_ADMIN_JOB_LOG_MAX_LINES", 5000),
        # 全部群组更新时的并发数量。
        "admin_update_concurrency": _env_int("TG_ADMIN_UPDATE_CONCURRENCY", 10),
        # 启动时是否强制修复全文索引。仅数值一表示开启。
        "force_heal_fts": _env_int("TG_FORCE_HEAL_FTS", 0),
        # 是否跳过启动期 FTS 全量修复。恢复大库且磁盘紧张时可临时开启。
        "skip_fts_auto_heal": _env_int("TG_SKIP_FTS_AUTO_HEAL", 0),
        # 后台管理密码。未配置时拒绝后台登录。
        "admin_password": _env_str("TG_ADMIN_PASSWORD", ""),
        # 后台登录有效时间，单位为秒。
        "admin_session_expiry": _env_int("TG_ADMIN_SESSION_EXPIRY", 840000),
    }


def _normalize_config_values(raw: dict) -> dict:
    normalized = dict(raw)

    # 统一配置收口（防止环境变量误配）
    normalized["scan_existing_chats"] = (
        1 if int(normalized["scan_existing_chats"]) == 1 else 0
    )

    if normalized["dedup_mode"] not in {"PURGE_ALL", "KEEP_FIRST"}:
        normalized["dedup_mode"] = "PURGE_ALL"

    normalized["dedup_threshold"] = max(2, int(normalized["dedup_threshold"]))
    normalized["batch_size"] = max(1, int(normalized["batch_size"]))
    normalized["rescan_tail_ids"] = max(0, int(normalized["rescan_tail_ids"]))
    normalized["media_caption_guard_len"] = max(
        0, int(normalized["media_caption_guard_len"])
    )
    normalized["promo_score_threshold"] = max(
        0, int(normalized["promo_score_threshold"])
    )
    normalized["disable_promo_filter"] = (
        1 if int(normalized["disable_promo_filter"]) == 1 else 0
    )
    normalized["log_every"] = max(1, int(normalized["log_every"]))
    normalized["sqlite_cache_mb"] = max(16, int(normalized["sqlite_cache_mb"]))
    normalized["sqlite_mmap_mb"] = max(0, int(normalized["sqlite_mmap_mb"]))
    normalized["admin_job_max_count"] = max(10, int(normalized["admin_job_max_count"]))
    normalized["admin_job_log_max_lines"] = max(
        500, int(normalized["admin_job_log_max_lines"])
    )
    normalized["admin_update_concurrency"] = max(
        1, int(normalized["admin_update_concurrency"])
    )
    normalized["force_heal_fts"] = 1 if int(normalized["force_heal_fts"]) == 1 else 0
    normalized["skip_fts_auto_heal"] = (
        1 if int(normalized["skip_fts_auto_heal"]) == 1 else 0
    )
    normalized["admin_session_expiry"] = max(60, int(normalized["admin_session_expiry"]))
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
        disable_promo_filter=values["disable_promo_filter"],
        log_every=values["log_every"],
        sqlite_cache_mb=values["sqlite_cache_mb"],
        sqlite_mmap_mb=values["sqlite_mmap_mb"],
        admin_job_max_count=values["admin_job_max_count"],
        admin_job_log_max_lines=values["admin_job_log_max_lines"],
        admin_update_concurrency=values["admin_update_concurrency"],
        force_heal_fts=values["force_heal_fts"],
        skip_fts_auto_heal=values["skip_fts_auto_heal"],
        admin_password=values["admin_password"],
        admin_session_expiry=values["admin_session_expiry"],
    )


@dataclass
class AppConfig:
    # 电报连接
    api_id: int
    api_hash: str
    session_name: str

    # 数据来源
    db_name: str
    target_group: str
    scan_existing_chats: int

    # 去重策略
    dedup_mode: str
    dedup_threshold: int

    # 处理流程
    batch_size: int
    rescan_tail_ids: int
    media_caption_guard_len: int
    promo_score_threshold: int
    disable_promo_filter: int
    log_every: int

    # 数据库
    sqlite_cache_mb: int
    sqlite_mmap_mb: int

    # 后台任务
    admin_job_max_count: int
    admin_job_log_max_lines: int
    admin_update_concurrency: int

    # 索引维护
    force_heal_fts: int
    skip_fts_auto_heal: int

    # 后台验证
    admin_password: str
    admin_session_expiry: int

    @classmethod
    def load(cls) -> "AppConfig":
        raw = _load_raw_config_values()
        normalized = _normalize_config_values(raw)
        return _build_app_config(normalized)


CFG = AppConfig.load()


def _is_enabled(v: int) -> bool:
    return int(v) == 1
