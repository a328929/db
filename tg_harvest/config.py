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
        # Telegram 开发者 API ID（环境变量：TG_API_ID）。
        # 用途：用于初始化 Telegram 客户端身份认证，必须与 TG_API_HASH 成对使用。
        # 默认值 2040 仅用于本地演示，生产环境请替换为你自己的开发者配置。
        "api_id": _env_int("TG_API_ID", 2040),
        # Telegram 开发者 API Hash（环境变量：TG_API_HASH）。
        # 用途：配合 API ID 进行客户端鉴权；若错误会导致登录/连接失败。
        # 该值属于敏感信息，建议通过环境变量注入，不要硬编码在公开仓库中。
        "api_hash": _env_str("TG_API_HASH", "b18441a1ff607e10a989891a5462e627"),
        # Telegram 会话名称（环境变量：TG_SESSION_NAME）。
        # 用途：决定本地会话文件名，用于复用登录状态，避免每次启动重新扫码/验证码登录。
        # 多账号场景可通过不同 session_name 隔离会话文件。
        "session_name": _env_str("TG_SESSION_NAME", "my_session"),
        # SQLite 数据库路径/名称（环境变量：TG_DB_NAME）。
        # 用途：指定采集数据落盘位置；内部会通过 resolve_db_path 解析为实际可用路径。
        # 既可传相对路径也可传绝对路径，建议在部署时显式指定以便运维。
        "db_name": resolve_db_path(_env_str("TG_DB_NAME", "tg_data.db")),
        # 目标群组名称或标识（环境变量：TG_TARGET_GROUP）。
        # 用途：指定采集/监听的主群组；程序会以此作为消息来源过滤条件之一。
        # 若配置错误，可能导致无法匹配到目标会话。
        "target_group": _env_str("TG_TARGET_GROUP", "顶级萝莉内部群"),
        # 是否扫描数据库中已有聊天对象（环境变量：TG_SCAN_DB_CHATS，0/1）。
        # 用途：开启后会从数据库历史记录中补扫聊天来源，适合增量修复或历史数据补全。
        # 规范化时仅 1 视为开启，其他值一律按 0 关闭处理。
        "scan_existing_chats": _env_int("TG_SCAN_DB_CHATS", 0),
        # 去重策略（环境变量：TG_DEDUP_MODE，可选 PURGE_ALL / KEEP_FIRST）。
        # 用途：控制命中重复内容时的处理方式：
        # - PURGE_ALL：清理所有重复项；
        # - KEEP_FIRST：保留第一条，删除后续重复。
        # 非法值会在规范化阶段回退为 PURGE_ALL。
        "dedup_mode": _env_str("TG_DEDUP_MODE", "PURGE_ALL").upper(),
        # 去重触发阈值（环境变量：TG_DEDUP_THRESHOLD）。
        # 用途：同类内容累计达到该数量后触发去重逻辑。
        # 系统最小值为 2，避免“单条数据也触发去重”的误删风险。
        "dedup_threshold": _env_int("TG_DEDUP_THRESHOLD", 2),
        # 批处理大小（环境变量：TG_BATCH_SIZE）。
        # 用途：控制单次读取/处理消息数量，影响吞吐与内存占用。
        # 值越大通常吞吐更高，但峰值内存和单次事务耗时也会增加。
        "batch_size": _env_int("TG_BATCH_SIZE", 1000),
        # 重扫尾部消息 ID 数量（环境变量：TG_RESCAN_TAIL_IDS）。
        # 用途：在增量采集时回看最近 N 条 ID 区间，降低漏采/乱序导致的数据缺口。
        # 允许为 0（关闭尾部重扫）。
        "rescan_tail_ids": _env_int("TG_RESCAN_TAIL_IDS", 1000),
        # 媒体消息 caption 保护长度（环境变量：TG_MEDIA_CAPTION_GUARD_LEN）。
        # 用途：当 caption 长度低于该阈值时可触发额外保护策略，减少短文本被误判/误清理。
        # 允许为 0（不启用长度保护）。
        "media_caption_guard_len": _env_int("TG_MEDIA_CAPTION_GUARD_LEN", 58),
        # 推广内容判定分数阈值（环境变量：TG_PROMO_SCORE_THRESHOLD）。
        # 用途：用于规则打分模型中识别推广/广告信息，分数达到阈值视为命中。
        # 允许为 0（最宽松，几乎不做拦截）。
        "promo_score_threshold": _env_int("TG_PROMO_SCORE_THRESHOLD", 3),
        # 日志输出间隔（环境变量：TG_LOG_EVERY）。
        # 用途：每处理 N 条数据输出一次进度日志，便于观测运行状态。
        # 最小值为 1，避免因 0 或负数导致日志逻辑异常。
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
    # Telegram 鉴权参数
    api_id: int
    api_hash: str
    session_name: str

    # 数据源/数据落盘参数
    db_name: str
    target_group: str
    scan_existing_chats: int

    # 去重策略参数
    dedup_mode: str
    dedup_threshold: int

    # 处理流程参数
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
