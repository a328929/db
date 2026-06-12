import json
import logging
import re
import unicodedata
from pathlib import Path
from typing import Any

from tg_harvest.config import AppConfig
from tg_harvest.domain.normalize import (
    CONTACT_ID_RE,
    INVITE_RE,
    MENTION_RE,
    OBF_TME_RE,
    PHONE_RE,
    QQ_RE,
    URL_RE,
    WECHAT_RE,
    _compact_for_detection,
    _light_normalize,
    _safe_lower_nfkc,
    make_hash,
    normalize_text_for_hash,
    normalize_text_light,
)

logger = logging.getLogger(__name__)

# =========================
# 常量与模式定义
# =========================

# 混淆检测模式：匹配带干扰字符的常见关键词（如 v-x, t.m.e）
RE_OBFUSCATED_CONTACT = re.compile(
    r"(?:v[\s\W_]*x|w[\s\W_]*x|t[\s\W_]*\.?[\s\W_]*m[\s\W_]*\.?[\s\W_]*e)", re.I
)

_DEFAULT_PROMO_SCORES = {
    "url_base": 3,
    "url_max_extra": 2,
    "obf_tg_base": 3,
    "obf_tg_max_extra": 2,
    "invite_base": 4,
    "invite_max_extra": 2,
    "mention_single": 1,
    "mention_multi_base": 2,
    "mention_max_extra": 2,
    "wechat_base": 3,
    "wechat_max_extra": 2,
    "qq_base": 2,
    "qq_max_extra": 2,
    "phone": 2,
    "contact_id_base": 2,
    "contact_id_max_extra": 2,
    "kw_multiplier": 2,
    "kw_max": 10,
    "cta_max": 3,
    "compact_tg": 2,
    "compact_wechat": 1,
    "compact_qq": 1,
    "obfuscation": 1,
    "combo_contact_cta": 1,
    "combo_kw_contact": 1,
    "group_bonus": 1,
}

_DEFAULT_PROMO_RULES = {
    "promo_keywords": [
        "防失联",
        "失联",
        "备用",
        "车队",
        "备份群",
        "满足你的",
        "备用群",
        "新群",
        "新频道",
        "频道",
        "频道号",
        "频道链接",
        "永久地址",
        "最新地址",
        "发布页",
        "导航",
        "进群",
        "加群",
        "拉群",
        "群主",
        "私聊",
        "联系",
        "引擎",
        "联系方式",
        "联系客服",
        "客服",
        "商务",
        "酒馆",
        "体验",
        "商务合作",
        "合作",
        "代理",
        "推广",
        "资源群",
        "福利群",
        "免费进群",
        "搜索",
        "入口",
        "加我",
        "找我",
        "金品",
        "咨询",
        "群里见",
        "搜群",
        "飞机",
        "电报",
        "纸飞机",
        "telegram",
        "tg",
        "channel",
        "group",
        "join",
        "contact",
        "wechat",
        "whatsapp",
        "business",
        "support",
        "微信",
        "微 信",
        "vx",
        "wx",
        "QQ",
        "qq",
        "q群",
        "qq群",
        "飞机号",
        "频道号",
        "JISOU帮你精准找到",
    ],
    "hard_promo_markers": [
        "t.me/",
        "telegram.me",
        "joinchat",
        "/+",
        "私聊",
        "联系客服",
        "联系我",
        "加群",
        "进群",
        "vx",
        "wx",
        "微信",
        "wechat",
        "qq",
        "@",
        "频道链接",
        "发布页",
        "导航",
        "jiso",
    ],
    "cta_words": [
        "点击",
        "加入",
        "进群",
        "加群",
        "私信",
        "联系我",
        "车队",
        "酒馆",
        "体验",
        "联系客服",
        "搜索",
        "满足你的",
        "引擎",
        "合作",
        "进入",
        "JISOU帮你精准找到",
        "查看",
        "金品",
        "复制",
        "搜索",
        "加我",
        "咨询",
        "订阅",
        "关注",
        "打开",
        "扫码",
    ],
    "compact_markers": [
        "tme",
        "telegramme",
        "joinchat",
        "vx",
        "wx",
        "wechat",
        "微信",
        "qq群",
        "q群",
        "客服",
        "加群",
        "进群",
        "发布页",
        "导航",
    ],
    "promo_scores": _DEFAULT_PROMO_SCORES,
}

# =========================
# 规则加载与初始化
# =========================


def _find_promo_rules_file() -> Path | None:
    current_file = Path(__file__).resolve()
    candidates = [
        current_file.parent.parent / "promo_rules.json",
        current_file.with_name("promo_rules.json"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _load_rules_safely() -> dict[str, Any]:
    rules_file = _find_promo_rules_file()
    if rules_file is None:
        return _DEFAULT_PROMO_RULES
    try:
        with open(rules_file, encoding="utf-8") as f:
            raw = json.load(f)

        merged = dict(_DEFAULT_PROMO_RULES)
        for key in [
            "promo_keywords",
            "hard_promo_markers",
            "cta_words",
            "compact_markers",
        ]:
            if isinstance(raw.get(key), list):
                merged[key] = [str(x) for x in raw[key] if str(x).strip()]

        if isinstance(raw.get("promo_scores"), dict):
            promo_scores = dict(merged["promo_scores"]) # type: ignore
            promo_scores.update(
                {
                    k: v
                    for k, v in raw["promo_scores"].items()
                    if isinstance(v, (int, float))
                }
            )
            merged["promo_scores"] = promo_scores
        return merged
    except Exception as e:
        logger.warning(f"加载外部规则失败，使用默认配置: {e}")
        return _DEFAULT_PROMO_RULES


_RULES = _load_rules_safely()
PROMO_KEYWORDS = _RULES["promo_keywords"]
HARD_PROMO_MARKERS = _RULES["hard_promo_markers"]
CTA_WORDS = _RULES["cta_words"]
COMPACT_MARKERS = _RULES["compact_markers"]
PROMO_SCORES = _RULES["promo_scores"]


# 预处理紧凑型关键词用于快速匹配
def _make_compact_set(words: list[str]) -> list[str]:
    return sorted(
        {
            re.sub(r"[\W_]+", "", unicodedata.normalize("NFKC", w).lower())
            for w in words
            if w.strip()
        }
    )


PROMO_KEYWORDS_COMPACT = _make_compact_set(PROMO_KEYWORDS)
CTA_WORDS_COMPACT = _make_compact_set(CTA_WORDS)

# =========================
# 引流判定核心逻辑 (DRY 优化)
# =========================


def _get_hit_stats(text: str) -> dict[str, int]:
    """提取文本中的各类引流信号命中统计"""
    if not text:
        return {
            k: 0
            for k in [
                "url",
                "obf_tg",
                "invite",
                "mention",
                "wechat",
                "qq",
                "phone",
                "contact_id",
                "kw",
                "cta",
                "comp_tg",
                "comp_wx",
                "comp_qq",
                "obf_raw",
            ]
        }

    s_light = _light_normalize(text)
    s_compact = _compact_for_detection(text)

    hits = {
        "url": len(URL_RE.findall(s_light)),
        "obf_tg": len(OBF_TME_RE.findall(s_light)),
        "invite": len(INVITE_RE.findall(s_light)),
        "mention": len(MENTION_RE.findall(s_light)),
        "wechat": len(WECHAT_RE.findall(s_light)),
        "qq": len(QQ_RE.findall(s_light)),
        "phone": len(PHONE_RE.findall(s_light)),
        "contact_id": len(CONTACT_ID_RE.findall(s_light)),
        "kw": _count_compact_hits(s_compact, PROMO_KEYWORDS_COMPACT),
        "cta": _count_compact_hits(s_compact, CTA_WORDS_COMPACT),
        "comp_tg": 1
        if any(x in s_compact for x in ["tme", "telegramme", "joinchat"])
        else 0,
        "comp_wx": 1
        if any(x in s_compact for x in ["vx", "wx", "wechat", "微信"])
        else 0,
        "comp_qq": 1 if any(x in s_compact for x in ["qq", "qq群", "q群"]) else 0,
        "obf_raw": 1 if RE_OBFUSCATED_CONTACT.search(s_light) else 0,
    }
    return hits


def _count_compact_hits(compact_text: str, compact_keywords: list[str]) -> int:
    if not compact_text:
        return 0
    # 仅统计长度 > 2 的词，防止 vx/tg 等短词在普通文本中误命中（逻辑保持与原版一致但更清晰）
    return len({k for k in compact_keywords if len(k) > 2 and k in compact_text})


def _has_hard_signals(text: str, hits: dict[str, int] | None = None) -> bool:
    """判定是否包含明确的引流硬信号"""
    if not hits:
        hits = _get_hit_stats(text)

    # 组合判定：正则直接命中 OR 统计命中
    if (
        hits["url"]
        + hits["obf_tg"]
        + hits["invite"]
        + hits["wechat"]
        + hits["qq"]
        + hits["phone"]
        + hits["contact_id"]
        > 0
    ):
        return True

    s_lower = _safe_lower_nfkc(text)
    if any(m.lower() in s_lower for m in HARD_PROMO_MARKERS):
        return True

    s_compact = _compact_for_detection(text)
    return bool(any(m in s_compact for m in COMPACT_MARKERS))


# =========================
# 保护逻辑 (防误删)
# =========================


def _is_protected_caption(
    text: str, msg_type: str, has_media: bool, hits: dict[str, int], guard_len: int
) -> bool:
    """判定是否属于受保护的短媒体标题/普通说明"""
    if not has_media or not text:
        return True

    if _has_hard_signals(text, hits):
        return False

    # 逻辑 1: 长度极短且无明显诱导的媒体标题 (如 "第01集", "预览图")
    clean_text = normalize_text_for_hash(text)
    if 0 < len(clean_text) <= 12 and msg_type in {
        "PHOTO",
        "VIDEO",
        "GIF",
        "AUDIO",
        "FILE",
    }:
        return True

    # 逻辑 2: 文案长度在阈值内，且关键词/提及数极低
    return len(text) <= guard_len and hits["kw"] <= 1 and hits["mention"] <= 1


# =========================
# 公开接口
# =========================


def build_single_promo_features(
    text: str, msg_type: str, has_media: bool, cfg: AppConfig
) -> dict[str, Any]:
    if int(getattr(cfg, "disable_promo_filter", 0)) == 1:
        norm_hash = normalize_text_for_hash(text)
        return {
            "is_promo": 0,
            "promo_score": 0,
            "promo_reasons": [],
            "dedupe_eligible": 0,
            "guard_reason": None,
            "content_norm": normalize_text_light(text),
            "pure_hash": make_hash(norm_hash) if norm_hash else "",
            "text_len": len(text or ""),
        }

    hits = _get_hit_stats(text)
    score = 0
    reasons = []
    s_cfg = PROMO_SCORES

    # 1. 基础信号打分
    def add_score(key: str, base_cfg: str, max_extra_cfg: str, label: str):
        nonlocal score
        count = hits[key]
        if count > 0:
            val = s_cfg[base_cfg] + min(count - 1, s_cfg[max_extra_cfg])
            score += val
            reasons.append(f"{label}:{count}")

    add_score("url", "url_base", "url_max_extra", "url")
    add_score("obf_tg", "obf_tg_base", "obf_tg_max_extra", "obf_tg")
    add_score("invite", "invite_base", "invite_max_extra", "invite")
    add_score("wechat", "wechat_base", "wechat_max_extra", "wechat")
    add_score("qq", "qq_base", "qq_max_extra", "qq")
    add_score("contact_id", "contact_id_base", "contact_id_max_extra", "contact_id")

    if hits["mention"] == 1:
        score += s_cfg["mention_single"]
        reasons.append("mention:1")
    elif hits["mention"] > 1:
        score += s_cfg["mention_multi_base"] + min(
            hits["mention"] - 2, s_cfg["mention_max_extra"]
        )
        reasons.append(f"mention:{hits['mention']}")

    if hits["phone"] > 0:
        score += s_cfg["phone"]
        reasons.append(f"phone:{hits['phone']}")

    if hits["kw"] > 0:
        kw_val = min(hits["kw"] * s_cfg["kw_multiplier"], s_cfg["kw_max"])
        score += kw_val
        reasons.append(f"kw:{hits['kw']}")

    if hits["cta"] > 0:
        score += min(hits["cta"], s_cfg["cta_max"])
        reasons.append(f"cta:{hits['cta']}")

    # 2. 紧凑型/混淆信号
    for k, label in [
        ("comp_tg", "compact_tg"),
        ("comp_wx", "compact_wechat"),
        ("comp_qq", "compact_qq"),
        ("obf_raw", "obfuscation"),
    ]:
        if hits[k] > 0:
            score += s_cfg[label]
            reasons.append(label)

    # 3. 组合逻辑
    contact_count = (
        hits["url"]
        + hits["obf_tg"]
        + hits["invite"]
        + hits["wechat"]
        + hits["qq"]
        + hits["phone"]
        + hits["contact_id"]
    )
    if contact_count > 0 and hits["cta"] > 0:
        score += s_cfg["combo_contact_cta"]
        reasons.append("combo:contact+cta")
    if hits["kw"] >= 2 and contact_count > 0:
        score += s_cfg["combo_kw_contact"]
        reasons.append("combo:kw+contact")

    # 最终结果
    is_promo = 1 if score >= cfg.promo_score_threshold else 0
    protected = _is_protected_caption(
        text, msg_type, has_media, hits, cfg.media_caption_guard_len
    )

    dedupe_eligible = 1 if (is_promo and not protected) else 0
    guard_reason = "GENERIC_MEDIA_CAPTION_GUARD" if (is_promo and protected) else None

    norm_hash = normalize_text_for_hash(text)
    return {
        "is_promo": is_promo,
        "promo_score": int(score),
        "promo_reasons": reasons,
        "dedupe_eligible": dedupe_eligible,
        "guard_reason": guard_reason,
        "content_norm": normalize_text_light(text),
        "pure_hash": make_hash(norm_hash) if norm_hash else "",
        "text_len": len(text or ""),
    }


def build_group_promo_features(
    captions_concat: str, item_count: int, media_sig_hash: str, cfg: AppConfig
) -> dict[str, Any]:
    """媒体组广告识别"""
    if int(getattr(cfg, "disable_promo_filter", 0)) == 1:
        text = (captions_concat or "").strip()
        pure_hash = ""
        if text:
            pure_hash = build_single_promo_features(text, "GROUP", True, cfg)["pure_hash"]
        dedupe_hash = (
            pure_hash if pure_hash else (("gm:" + media_sig_hash) if media_sig_hash else "")
        )
        return {
            "is_promo": 0,
            "promo_score": 0,
            "promo_reasons": [],
            "dedupe_eligible": 0,
            "guard_reason": None,
            "caption_norm": normalize_text_light(text),
            "pure_hash": pure_hash,
            "dedupe_hash": dedupe_hash,
        }

    if not captions_concat or not captions_concat.strip():
        return {
            "is_promo": 0,
            "promo_score": 0,
            "promo_reasons": [],
            "dedupe_eligible": 0,
            "guard_reason": "EMPTY_MEDIA_GROUP_CAPTION",
            "caption_norm": "",
            "pure_hash": "",
            "dedupe_hash": ("gm:" + media_sig_hash) if media_sig_hash else "",
        }

    text = captions_concat.strip()
    hits = _get_hit_stats(text)
    # 调用单条逻辑获取基础分数
    base_feat = build_single_promo_features(text, "GROUP", True, cfg)

    score = base_feat["promo_score"]
    reasons = base_feat["promo_reasons"]

    # 组额外奖励
    if item_count >= 2 and _has_hard_signals(text, hits):
        score += PROMO_SCORES["group_bonus"]
        reasons.append(f"group_bonus:{item_count}")

    is_promo = 1 if score >= cfg.promo_score_threshold else 0
    protected = _is_protected_caption(
        text, "GROUP", True, hits, cfg.media_caption_guard_len
    )

    dedupe_eligible = 1 if (is_promo and not protected) else 0
    guard_reason = (
        "GENERIC_MEDIA_GROUP_CAPTION_GUARD" if (is_promo and protected) else None
    )

    pure_hash = base_feat["pure_hash"]
    dedupe_hash = (
        pure_hash if pure_hash else (("gm:" + media_sig_hash) if media_sig_hash else "")
    )

    return {
        "is_promo": is_promo,
        "promo_score": int(score),
        "promo_reasons": reasons,
        "dedupe_eligible": dedupe_eligible,
        "guard_reason": guard_reason,
        "caption_norm": base_feat["content_norm"],
        "pure_hash": pure_hash,
        "dedupe_hash": dedupe_hash,
    }


def contains_hard_promo_markers(text: str) -> bool:
    return _has_hard_signals(text)


def is_generic_media_caption(
    text: str,
    msg_type: str,
    has_media: bool,
    promo_stats: dict[str, int] | None = None,
    guard_len: int = 58,
) -> bool:
    # 保持稳定的公共接口
    hits = promo_stats if promo_stats else _get_hit_stats(text)
    return _is_protected_caption(text, msg_type, has_media, hits, guard_len)
