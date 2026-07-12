import os
from dataclasses import dataclass
from pathlib import Path

from tg_harvest.domain.coerce import enabled_int, safe_int
from tg_harvest.runtime.paths import resolve_db_path, resolve_session_name

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
    return safe_int(os.getenv(name, None), default)


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name, None)
    if v is None:
        return float(default)
    try:
        return float(v.strip())
    except Exception:
        return float(default)


def _env_optional_int(name: str) -> int | None:
    v = os.getenv(name, None)
    if v is None:
        return None
    text = v.strip().lower()
    if not text or text in {"auto", "none", "default"}:
        return None
    try:
        return int(text)
    except Exception:
        return None


def _env_optional_float(name: str) -> float | None:
    v = os.getenv(name, None)
    if v is None:
        return None
    text = v.strip().lower()
    if not text or text in {"auto", "none", "default"}:
        return None
    try:
        return float(text)
    except Exception:
        return None


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
        # Telegram 历史消息请求间隔。空值表示使用 Telethon 默认保护策略。
        "history_wait_time": _env_optional_float("TG_HISTORY_WAIT_TIME"),
        # Telegram FloodWait 超过该秒数时，不再等待，改由上层尝试切换账号。
        "flood_wait_switch_threshold": _env_int("TG_FLOOD_WAIT_SWITCH_THRESHOLD", 30),
        # 可选第二账号会话路径。配置后，新增大群会尝试双账号区间拉取。
        "secondary_session_name": (
            resolve_session_name(_env_str("TG_SECONDARY_SESSION_NAME", ""))
            if _env_str("TG_SECONDARY_SESSION_NAME", "")
            else ""
        ),
        # 克隆媒体中转频道。两个账号都加入该频道后，可解决无单账号同时访问源和目标的问题。
        "clone_relay_chat_id": _env_int("TG_CLONE_RELAY_CHAT_ID", 0),
        # 中转频道公开用户名或本地实体兜底名称，可选。
        "clone_relay_chat_username": _env_str("TG_CLONE_RELAY_CHAT_USERNAME", ""),
        # 新增目标最后一条消息 ID 达到该阈值时，才考虑双账号区间拉取。
        "multi_account_min_message_id": _env_int("TG_MULTI_ACCOUNT_MIN_MESSAGE_ID", 5000),
        # 双账号区间拉取的 message_id 范围跨度。
        "multi_account_range_chunk_size": _env_int("TG_MULTI_ACCOUNT_RANGE_CHUNK_SIZE", 1000),
        # 数据库页面缓存大小，单位为兆。
        "sqlite_cache_mb": _env_int("TG_SQLITE_CACHE_MB", 512),
        # 数据库内存映射大小，单位为兆。数值零表示关闭。
        "sqlite_mmap_mb": _env_int("TG_SQLITE_MMAP_MB", 1024),
        # 管理页数据库容量健康阈值，单位均为字节。阈值只影响只读告警，不会触发维护操作。
        "db_health_size_warning_bytes": _env_int(
            "TG_DB_HEALTH_SIZE_WARNING_BYTES", 20 * 1024 * 1024 * 1024
        ),
        "db_health_size_critical_bytes": _env_int(
            "TG_DB_HEALTH_SIZE_CRITICAL_BYTES", 50 * 1024 * 1024 * 1024
        ),
        "db_health_wal_warning_bytes": _env_int(
            "TG_DB_HEALTH_WAL_WARNING_BYTES", 512 * 1024 * 1024
        ),
        "db_health_wal_critical_bytes": _env_int(
            "TG_DB_HEALTH_WAL_CRITICAL_BYTES", 2 * 1024 * 1024 * 1024
        ),
        # 磁盘余量的严重阈值必须小于等于预警阈值。
        "db_health_disk_free_warning_bytes": _env_int(
            "TG_DB_HEALTH_DISK_FREE_WARNING_BYTES", 10 * 1024 * 1024 * 1024
        ),
        "db_health_disk_free_critical_bytes": _env_int(
            "TG_DB_HEALTH_DISK_FREE_CRITICAL_BYTES", 3 * 1024 * 1024 * 1024
        ),
        "db_health_cjk_queue_warning": _env_int(
            "TG_DB_HEALTH_CJK_QUEUE_WARNING", 10000
        ),
        "db_health_cjk_queue_critical": _env_int(
            "TG_DB_HEALTH_CJK_QUEUE_CRITICAL", 100000
        ),
        # 后台任务最大保留数量。
        "admin_job_max_count": _env_int("TG_ADMIN_JOB_MAX_COUNT", 100),
        # 单个后台任务最大日志行数。
        "admin_job_log_max_lines": _env_int("TG_ADMIN_JOB_LOG_MAX_LINES", 5000),
        # 全部群组更新时的并发数量。
        "admin_update_concurrency": _env_int("TG_ADMIN_UPDATE_CONCURRENCY", 4),
        # 全部群组更新时，同一账号启动下一个群组前的最小间隔秒数。
        # 未配置时会按账号数自动取更保守的默认值。
        "admin_update_min_chat_start_gap_seconds": _env_optional_float(
            "TG_ADMIN_UPDATE_MIN_CHAT_START_GAP_SECONDS"
        ),
        # 全部群组更新时，第二账号执行公开 username 解析前的最小额外间隔秒数。
        # 空值表示自动采用比普通群组启动更保守的节流。
        "admin_update_secondary_username_gap_seconds": _env_optional_float(
            "TG_ADMIN_UPDATE_SECONDARY_USERNAME_GAP_SECONDS"
        ),
        # 全部群组更新时，允许第二账号主动按公开 username 解析的群组数量上限。
        # 0 表示彻底关闭主动预热；空值表示按缓存覆盖率自动渐进预热。
        "admin_update_secondary_public_resolve_limit": _env_optional_int(
            "TG_ADMIN_UPDATE_SECONDARY_PUBLIC_RESOLVE_LIMIT"
        ),
        # 风险扫描中主动解析未加入公开频道的单轮上限；缓存命中不计入该预算。
        "admin_restricted_public_resolve_limit": _env_int(
            "TG_ADMIN_RESTRICTED_PUBLIC_RESOLVE_LIMIT", 40
        ),
        # 同一账号连续主动解析公开 username 的最小间隔秒数。
        "admin_restricted_public_resolve_gap_seconds": _env_float(
            "TG_ADMIN_RESTRICTED_PUBLIC_RESOLVE_GAP_SECONDS", 1.0
        ),
        # 全部群组更新时，全部账号都处于 FloodWait 时最多等待多久。
        "admin_update_max_cooldown_wait_seconds": _env_int(
            "TG_ADMIN_UPDATE_MAX_COOLDOWN_WAIT_SECONDS", 45
        ),
        # 数据库内群组事件监听。仅监听已入库群组，不自动纳入新加入群。
        "db_listener_enabled": _env_int("TG_DB_LISTENER_ENABLED", 1),
        # 数据库内群组缓存刷新周期，单位秒。
        "db_listener_refresh_seconds": _env_int("TG_DB_LISTENER_REFRESH_SECONDS", 120),
        # 是否启用数据库群组的低频轮巡探测。
        "db_listener_public_probe_enabled": _env_int(
            "TG_DB_LISTENER_PUBLIC_PROBE_ENABLED", 1
        ),
        # 低频轮巡探测的轮询周期，单位秒。
        "db_listener_public_probe_interval_seconds": _env_int(
            "TG_DB_LISTENER_PUBLIC_PROBE_INTERVAL_SECONDS", 180
        ),
        # 每轮低频轮巡探测最多处理的群组数。
        "db_listener_public_probe_batch_size": _env_int(
            "TG_DB_LISTENER_PUBLIC_PROBE_BATCH_SIZE", 4
        ),
        # 未加入但可探测群组两次轮巡之间的最小间隔，单位秒。
        "db_listener_public_probe_chat_cooldown_seconds": _env_int(
            "TG_DB_LISTENER_PUBLIC_PROBE_CHAT_COOLDOWN_SECONDS", 3600
        ),
        # 已加入群组的低频补探测最小间隔，单位秒。
        "db_listener_joined_probe_chat_cooldown_seconds": _env_int(
            "TG_DB_LISTENER_JOINED_PROBE_CHAT_COOLDOWN_SECONDS", 10800
        ),
        # 长期不活跃群组的额外低频探测最小间隔，单位秒。
        "db_listener_inactive_probe_chat_cooldown_seconds": _env_int(
            "TG_DB_LISTENER_INACTIVE_PROBE_CHAT_COOLDOWN_SECONDS", 43200
        ),
        # 智能调度器。开启后事件先进入持久 pending，再按 quiet delay 批量拉取。
        "sync_scheduler_enabled": _env_int("TG_SYNC_SCHEDULER_ENABLED", 1),
        # 本地模型预测。默认关闭；依赖缺失时只记录不可用状态，不阻塞入库。
        "sync_ai_enabled": _env_int("TG_SYNC_AI_ENABLED", 0),
        # 模型 shadow 模式。开启时模型只记录建议，不接管调度。
        "sync_ai_shadow": _env_int("TG_SYNC_AI_SHADOW", 1),
        # 自动晋级。默认关闭，必须显式开启后才允许模型从观察转入接管。
        "sync_ai_auto_promote_enabled": _env_int(
            "TG_SYNC_AI_AUTO_PROMOTE_ENABLED", 0
        ),
        # 事件到拉取的最小延迟边界，单位秒。
        "sync_min_delay_seconds": _env_int("TG_SYNC_MIN_DELAY_SECONDS", 15),
        # 活跃群 quiet delay 上限，单位秒。
        "sync_max_active_delay_seconds": _env_int(
            "TG_SYNC_MAX_ACTIVE_DELAY_SECONDS", 600
        ),
        # 冷门群 quiet delay 上限，单位秒。
        "sync_max_cold_delay_seconds": _env_int(
            "TG_SYNC_MAX_COLD_DELAY_SECONDS", 7200
        ),
        # 在线训练节奏配置。依赖缺失时不会启动训练线程。
        "sync_model_train_interval_seconds": _env_int(
            "TG_SYNC_MODEL_TRAIN_INTERVAL_SECONDS", 1800
        ),
        "sync_model_kind": _env_str("TG_SYNC_MODEL_KIND", "torch_lite"),
        "sync_model_min_samples": _env_int("TG_SYNC_MODEL_MIN_SAMPLES", 200),
        "sync_model_max_train_samples": _env_int(
            "TG_SYNC_MODEL_MAX_TRAIN_SAMPLES", 16384
        ),
        "sync_model_train_batch_size": _env_int(
            "TG_SYNC_MODEL_TRAIN_BATCH_SIZE", 128
        ),
        "sync_model_train_epochs": _env_int("TG_SYNC_MODEL_TRAIN_EPOCHS", 4),
        "sync_model_learning_rate": _env_float(
            "TG_SYNC_MODEL_LEARNING_RATE", 0.001
        ),
        "sync_model_torch_threads": _env_int("TG_SYNC_MODEL_TORCH_THREADS", 1),
        "sync_model_min_eval_samples": _env_int(
            "TG_SYNC_MODEL_MIN_EVAL_SAMPLES", 50
        ),
        "sync_model_ready_delay_accuracy": _env_float(
            "TG_SYNC_MODEL_READY_DELAY_ACCURACY", 0.45
        ),
        "sync_model_ready_max_delay_mae_buckets": _env_float(
            "TG_SYNC_MODEL_READY_MAX_DELAY_MAE_BUCKETS", 1.25
        ),
        "sync_model_ready_max_added_mae_log": _env_float(
            "TG_SYNC_MODEL_READY_MAX_ADDED_MAE_LOG", 1.5
        ),
        "sync_model_ready_consecutive_runs": _env_int(
            "TG_SYNC_MODEL_READY_CONSECUTIVE_RUNS", 3
        ),
        "sync_model_min_confidence": _env_float(
            "TG_SYNC_MODEL_MIN_CONFIDENCE", 0.35
        ),
        "sync_model_max_active_delay_factor": _env_float(
            "TG_SYNC_MODEL_MAX_ACTIVE_DELAY_FACTOR", 2.0
        ),
        "sync_learning_retention_days": _env_int(
            "TG_SYNC_LEARNING_RETENTION_DAYS", 90
        ),
        "sync_learning_max_rows": _env_int("TG_SYNC_LEARNING_MAX_ROWS", 200000),
        "sync_scheduler_concurrency": _env_int("TG_SYNC_SCHEDULER_CONCURRENCY", 2),
        # 可选运维机器人；默认关闭，只用于任务通知，不参与历史消息采集。
        "ops_bot_enabled": _env_int("TG_OPS_BOT_ENABLED", 0),
        # 运维机器人 token，只从环境读取，禁止写入日志。
        "ops_bot_token": _env_str("TG_OPS_BOT_TOKEN", ""),
        # 运维机器人通知目标 chat_id；可填个人、群组或频道 ID。
        "ops_bot_notify_chat_id": _env_str("TG_OPS_BOT_NOTIFY_CHAT_ID", ""),
        # 运维机器人 HTTP 调用超时秒数。
        "ops_bot_timeout_seconds": _env_optional_float("TG_OPS_BOT_TIMEOUT_SECONDS"),
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
    normalized["scan_existing_chats"] = enabled_int(normalized["scan_existing_chats"])

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
    normalized["disable_promo_filter"] = enabled_int(normalized["disable_promo_filter"])
    normalized["log_every"] = max(1, int(normalized["log_every"]))
    if normalized["history_wait_time"] is not None:
        normalized["history_wait_time"] = max(0.0, float(normalized["history_wait_time"]))
    normalized["flood_wait_switch_threshold"] = max(
        1, int(normalized["flood_wait_switch_threshold"])
    )
    normalized["multi_account_min_message_id"] = max(
        0, int(normalized["multi_account_min_message_id"])
    )
    normalized["multi_account_range_chunk_size"] = max(
        100, int(normalized["multi_account_range_chunk_size"])
    )
    normalized["sqlite_cache_mb"] = max(16, int(normalized["sqlite_cache_mb"]))
    normalized["sqlite_mmap_mb"] = max(0, int(normalized["sqlite_mmap_mb"]))
    normalized["db_health_size_warning_bytes"] = max(
        1, int(normalized["db_health_size_warning_bytes"])
    )
    normalized["db_health_size_critical_bytes"] = max(
        int(normalized["db_health_size_warning_bytes"]),
        int(normalized["db_health_size_critical_bytes"]),
    )
    normalized["db_health_wal_warning_bytes"] = max(
        1, int(normalized["db_health_wal_warning_bytes"])
    )
    normalized["db_health_wal_critical_bytes"] = max(
        int(normalized["db_health_wal_warning_bytes"]),
        int(normalized["db_health_wal_critical_bytes"]),
    )
    normalized["db_health_disk_free_warning_bytes"] = max(
        1, int(normalized["db_health_disk_free_warning_bytes"])
    )
    normalized["db_health_disk_free_critical_bytes"] = min(
        int(normalized["db_health_disk_free_warning_bytes"]),
        max(1, int(normalized["db_health_disk_free_critical_bytes"])),
    )
    normalized["db_health_cjk_queue_warning"] = max(
        1, int(normalized["db_health_cjk_queue_warning"])
    )
    normalized["db_health_cjk_queue_critical"] = max(
        int(normalized["db_health_cjk_queue_warning"]),
        int(normalized["db_health_cjk_queue_critical"]),
    )
    normalized["admin_job_max_count"] = max(10, int(normalized["admin_job_max_count"]))
    normalized["admin_job_log_max_lines"] = max(
        500, int(normalized["admin_job_log_max_lines"])
    )
    normalized["admin_update_concurrency"] = max(
        1, int(normalized["admin_update_concurrency"])
    )
    if normalized["admin_update_min_chat_start_gap_seconds"] is not None:
        normalized["admin_update_min_chat_start_gap_seconds"] = max(
            0.0, float(normalized["admin_update_min_chat_start_gap_seconds"])
        )
    if normalized["admin_update_secondary_username_gap_seconds"] is not None:
        normalized["admin_update_secondary_username_gap_seconds"] = max(
            0.0, float(normalized["admin_update_secondary_username_gap_seconds"])
        )
    if normalized["admin_update_secondary_public_resolve_limit"] is not None:
        normalized["admin_update_secondary_public_resolve_limit"] = max(
            0, int(normalized["admin_update_secondary_public_resolve_limit"])
        )
    normalized["admin_update_max_cooldown_wait_seconds"] = max(
        0, int(normalized["admin_update_max_cooldown_wait_seconds"])
    )
    normalized["db_listener_enabled"] = enabled_int(normalized["db_listener_enabled"])
    normalized["db_listener_refresh_seconds"] = max(
        30, int(normalized["db_listener_refresh_seconds"])
    )
    normalized["db_listener_public_probe_enabled"] = enabled_int(
        normalized["db_listener_public_probe_enabled"]
    )
    normalized["db_listener_public_probe_interval_seconds"] = max(
        60, int(normalized["db_listener_public_probe_interval_seconds"])
    )
    normalized["db_listener_public_probe_batch_size"] = max(
        1, int(normalized["db_listener_public_probe_batch_size"])
    )
    normalized["db_listener_public_probe_chat_cooldown_seconds"] = max(
        300, int(normalized["db_listener_public_probe_chat_cooldown_seconds"])
    )
    normalized["db_listener_joined_probe_chat_cooldown_seconds"] = max(
        1800, int(normalized["db_listener_joined_probe_chat_cooldown_seconds"])
    )
    normalized["db_listener_inactive_probe_chat_cooldown_seconds"] = max(
        int(normalized["db_listener_joined_probe_chat_cooldown_seconds"]),
        int(normalized["db_listener_inactive_probe_chat_cooldown_seconds"]),
    )
    normalized["sync_scheduler_enabled"] = enabled_int(
        normalized["sync_scheduler_enabled"]
    )
    normalized["sync_ai_enabled"] = enabled_int(normalized["sync_ai_enabled"])
    normalized["sync_ai_shadow"] = enabled_int(normalized["sync_ai_shadow"])
    normalized["sync_ai_auto_promote_enabled"] = enabled_int(
        normalized["sync_ai_auto_promote_enabled"]
    )
    normalized["sync_min_delay_seconds"] = max(
        1, int(normalized["sync_min_delay_seconds"])
    )
    normalized["sync_max_active_delay_seconds"] = max(
        int(normalized["sync_min_delay_seconds"]),
        int(normalized["sync_max_active_delay_seconds"]),
    )
    normalized["sync_max_cold_delay_seconds"] = max(
        int(normalized["sync_max_active_delay_seconds"]),
        int(normalized["sync_max_cold_delay_seconds"]),
    )
    normalized["sync_model_train_interval_seconds"] = max(
        60, int(normalized["sync_model_train_interval_seconds"])
    )
    if normalized["sync_model_kind"] not in {"torch_lite", "torch"}:
        normalized["sync_model_kind"] = "torch_lite"
    normalized["sync_model_min_samples"] = max(
        1, int(normalized["sync_model_min_samples"])
    )
    normalized["sync_model_max_train_samples"] = max(
        int(normalized["sync_model_min_samples"]),
        int(normalized["sync_model_max_train_samples"]),
    )
    normalized["sync_model_train_batch_size"] = max(
        4, int(normalized["sync_model_train_batch_size"])
    )
    normalized["sync_model_train_epochs"] = max(
        1, int(normalized["sync_model_train_epochs"])
    )
    normalized["sync_model_learning_rate"] = max(
        0.00001, float(normalized["sync_model_learning_rate"])
    )
    normalized["sync_model_torch_threads"] = max(
        1, int(normalized["sync_model_torch_threads"])
    )
    normalized["sync_model_min_eval_samples"] = max(
        1, int(normalized["sync_model_min_eval_samples"])
    )
    normalized["sync_model_ready_delay_accuracy"] = min(
        1.0, max(0.0, float(normalized["sync_model_ready_delay_accuracy"]))
    )
    normalized["sync_model_ready_max_delay_mae_buckets"] = max(
        0.0, float(normalized["sync_model_ready_max_delay_mae_buckets"])
    )
    normalized["sync_model_ready_max_added_mae_log"] = max(
        0.0, float(normalized["sync_model_ready_max_added_mae_log"])
    )
    normalized["sync_model_ready_consecutive_runs"] = max(
        1, int(normalized["sync_model_ready_consecutive_runs"])
    )
    normalized["sync_model_min_confidence"] = min(
        1.0, max(0.0, float(normalized["sync_model_min_confidence"]))
    )
    normalized["sync_model_max_active_delay_factor"] = max(
        1.0, float(normalized["sync_model_max_active_delay_factor"])
    )
    normalized["sync_learning_retention_days"] = max(
        1, int(normalized["sync_learning_retention_days"])
    )
    normalized["sync_learning_max_rows"] = max(
        1000, int(normalized["sync_learning_max_rows"])
    )
    normalized["sync_scheduler_concurrency"] = max(
        1, int(normalized["sync_scheduler_concurrency"])
    )
    normalized["ops_bot_enabled"] = enabled_int(normalized["ops_bot_enabled"])
    if normalized["ops_bot_timeout_seconds"] is None:
        normalized["ops_bot_timeout_seconds"] = 3.0
    else:
        normalized["ops_bot_timeout_seconds"] = max(
            0.5, float(normalized["ops_bot_timeout_seconds"])
        )
    normalized["force_heal_fts"] = enabled_int(normalized["force_heal_fts"])
    normalized["skip_fts_auto_heal"] = enabled_int(normalized["skip_fts_auto_heal"])
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
        history_wait_time=values["history_wait_time"],
        flood_wait_switch_threshold=values["flood_wait_switch_threshold"],
        secondary_session_name=values["secondary_session_name"],
        clone_relay_chat_id=values["clone_relay_chat_id"],
        clone_relay_chat_username=values["clone_relay_chat_username"],
        multi_account_min_message_id=values["multi_account_min_message_id"],
        multi_account_range_chunk_size=values["multi_account_range_chunk_size"],
        sqlite_cache_mb=values["sqlite_cache_mb"],
        sqlite_mmap_mb=values["sqlite_mmap_mb"],
        db_health_size_warning_bytes=values["db_health_size_warning_bytes"],
        db_health_size_critical_bytes=values["db_health_size_critical_bytes"],
        db_health_wal_warning_bytes=values["db_health_wal_warning_bytes"],
        db_health_wal_critical_bytes=values["db_health_wal_critical_bytes"],
        db_health_disk_free_warning_bytes=values[
            "db_health_disk_free_warning_bytes"
        ],
        db_health_disk_free_critical_bytes=values[
            "db_health_disk_free_critical_bytes"
        ],
        db_health_cjk_queue_warning=values["db_health_cjk_queue_warning"],
        db_health_cjk_queue_critical=values["db_health_cjk_queue_critical"],
        admin_job_max_count=values["admin_job_max_count"],
        admin_job_log_max_lines=values["admin_job_log_max_lines"],
        admin_update_concurrency=values["admin_update_concurrency"],
        admin_update_min_chat_start_gap_seconds=values[
            "admin_update_min_chat_start_gap_seconds"
        ],
        admin_update_secondary_username_gap_seconds=values[
            "admin_update_secondary_username_gap_seconds"
        ],
        admin_update_secondary_public_resolve_limit=values[
            "admin_update_secondary_public_resolve_limit"
        ],
        admin_update_max_cooldown_wait_seconds=values[
            "admin_update_max_cooldown_wait_seconds"
        ],
        db_listener_enabled=values["db_listener_enabled"],
        db_listener_refresh_seconds=values["db_listener_refresh_seconds"],
        db_listener_public_probe_enabled=values["db_listener_public_probe_enabled"],
        db_listener_public_probe_interval_seconds=values[
            "db_listener_public_probe_interval_seconds"
        ],
        db_listener_public_probe_batch_size=values[
            "db_listener_public_probe_batch_size"
        ],
        db_listener_public_probe_chat_cooldown_seconds=values[
            "db_listener_public_probe_chat_cooldown_seconds"
        ],
        db_listener_joined_probe_chat_cooldown_seconds=values[
            "db_listener_joined_probe_chat_cooldown_seconds"
        ],
        db_listener_inactive_probe_chat_cooldown_seconds=values[
            "db_listener_inactive_probe_chat_cooldown_seconds"
        ],
        sync_scheduler_enabled=values["sync_scheduler_enabled"],
        sync_ai_enabled=values["sync_ai_enabled"],
        sync_ai_shadow=values["sync_ai_shadow"],
        sync_ai_auto_promote_enabled=values["sync_ai_auto_promote_enabled"],
        sync_min_delay_seconds=values["sync_min_delay_seconds"],
        sync_max_active_delay_seconds=values["sync_max_active_delay_seconds"],
        sync_max_cold_delay_seconds=values["sync_max_cold_delay_seconds"],
        sync_model_train_interval_seconds=values[
            "sync_model_train_interval_seconds"
        ],
        sync_model_kind=values["sync_model_kind"],
        sync_model_min_samples=values["sync_model_min_samples"],
        sync_model_max_train_samples=values["sync_model_max_train_samples"],
        sync_model_train_batch_size=values["sync_model_train_batch_size"],
        sync_model_train_epochs=values["sync_model_train_epochs"],
        sync_model_learning_rate=values["sync_model_learning_rate"],
        sync_model_torch_threads=values["sync_model_torch_threads"],
        sync_model_min_eval_samples=values["sync_model_min_eval_samples"],
        sync_model_ready_delay_accuracy=values["sync_model_ready_delay_accuracy"],
        sync_model_ready_max_delay_mae_buckets=values[
            "sync_model_ready_max_delay_mae_buckets"
        ],
        sync_model_ready_max_added_mae_log=values[
            "sync_model_ready_max_added_mae_log"
        ],
        sync_model_ready_consecutive_runs=values[
            "sync_model_ready_consecutive_runs"
        ],
        sync_model_min_confidence=values["sync_model_min_confidence"],
        sync_model_max_active_delay_factor=values[
            "sync_model_max_active_delay_factor"
        ],
        sync_learning_retention_days=values["sync_learning_retention_days"],
        sync_learning_max_rows=values["sync_learning_max_rows"],
        sync_scheduler_concurrency=values["sync_scheduler_concurrency"],
        ops_bot_enabled=values["ops_bot_enabled"],
        ops_bot_token=values["ops_bot_token"],
        ops_bot_notify_chat_id=values["ops_bot_notify_chat_id"],
        ops_bot_timeout_seconds=values["ops_bot_timeout_seconds"],
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
    history_wait_time: float | None
    flood_wait_switch_threshold: int
    secondary_session_name: str
    clone_relay_chat_id: int
    clone_relay_chat_username: str
    multi_account_min_message_id: int
    multi_account_range_chunk_size: int

    # 数据库
    sqlite_cache_mb: int
    sqlite_mmap_mb: int

    # 后台任务
    admin_job_max_count: int
    admin_job_log_max_lines: int
    admin_update_concurrency: int
    admin_update_min_chat_start_gap_seconds: float | None
    admin_update_secondary_username_gap_seconds: float | None
    admin_update_secondary_public_resolve_limit: int | None
    admin_update_max_cooldown_wait_seconds: int
    db_listener_enabled: int
    db_listener_refresh_seconds: int
    db_listener_public_probe_enabled: int
    db_listener_public_probe_interval_seconds: int
    db_listener_public_probe_batch_size: int
    db_listener_public_probe_chat_cooldown_seconds: int
    db_listener_joined_probe_chat_cooldown_seconds: int
    db_listener_inactive_probe_chat_cooldown_seconds: int
    sync_scheduler_enabled: int
    sync_ai_enabled: int
    sync_ai_shadow: int
    sync_ai_auto_promote_enabled: int
    sync_min_delay_seconds: int
    sync_max_active_delay_seconds: int
    sync_max_cold_delay_seconds: int
    sync_model_train_interval_seconds: int
    sync_model_kind: str
    sync_model_min_samples: int
    sync_model_max_train_samples: int
    sync_model_train_batch_size: int
    sync_model_train_epochs: int
    sync_model_learning_rate: float
    sync_model_torch_threads: int
    sync_model_min_eval_samples: int
    sync_model_ready_delay_accuracy: float
    sync_model_ready_max_delay_mae_buckets: float
    sync_model_ready_max_added_mae_log: float
    sync_model_ready_consecutive_runs: int
    sync_model_min_confidence: float
    sync_model_max_active_delay_factor: float
    sync_learning_retention_days: int
    sync_learning_max_rows: int
    sync_scheduler_concurrency: int
    ops_bot_enabled: int
    ops_bot_token: str
    ops_bot_notify_chat_id: str
    ops_bot_timeout_seconds: float

    # 索引维护
    force_heal_fts: int
    skip_fts_auto_heal: int

    # 后台验证
    admin_password: str
    admin_session_expiry: int

    # 数据库容量健康阈值。默认值放在字段上，方便旧测试和嵌入调用逐步迁移。
    db_health_size_warning_bytes: int = 20 * 1024 * 1024 * 1024
    db_health_size_critical_bytes: int = 50 * 1024 * 1024 * 1024
    db_health_wal_warning_bytes: int = 512 * 1024 * 1024
    db_health_wal_critical_bytes: int = 2 * 1024 * 1024 * 1024
    db_health_disk_free_warning_bytes: int = 10 * 1024 * 1024 * 1024
    db_health_disk_free_critical_bytes: int = 3 * 1024 * 1024 * 1024
    db_health_cjk_queue_warning: int = 10000
    db_health_cjk_queue_critical: int = 100000

    @classmethod
    def load(cls) -> "AppConfig":
        raw = _load_raw_config_values()
        normalized = _normalize_config_values(raw)
        return _build_app_config(normalized)


CFG = AppConfig.load()


def _is_enabled(v: int) -> bool:
    return enabled_int(v) == 1
