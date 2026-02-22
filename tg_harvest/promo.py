# -*- coding: utf-8 -*-
import json
import re
import unicodedata
from pathlib import Path
from typing import List, Dict, Tuple, Optional, Any

from .config import AppConfig
from .normalize import (
    _safe_lower_nfkc, _compact_for_detection, _light_normalize, normalize_text_for_hash,
    normalize_text_light, make_hash, _safe_json,
    URL_RE, INVITE_RE, OBF_TME_RE, MENTION_RE, WECHAT_RE, QQ_RE, PHONE_RE, CONTACT_ID_RE
)

# =========================
# 引流规则（支持 JSON 外置）
# =========================

_DEFAULT_PROMO_RULES = {
    "promo_keywords": [
        "防失联", "失联", "备用", "车队", "备份群", "满足你的", "备用群", "新群", "新频道", "频道", "频道号", "频道链接",
        "永久地址", "最新地址", "发布页", "导航", "进群", "加群", "拉群", "群主", "私聊", "联系", "引擎", "联系方式",
        "联系客服", "客服", "商务", "酒馆", "体验", "商务合作", "合作", "代理", "推广", "资源群", "福利群", "免费进群",
        "搜索", "入口", "加我", "找我", "金品", "咨询", "群里见", "搜群", "飞机", "电报", "纸飞机", "telegram", "tg",
        "channel", "group", "join", "contact", "wechat", "whatsapp", "business", "support", "微信", "微 信", "vx", "wx",
        "QQ", "qq", "q群", "qq群", "飞机号", "频道号", "JISOU帮你精准找到",
    ],
    "hard_promo_markers": [
        "t.me/", "telegram.me", "joinchat", "/+", "私聊", "联系客服", "联系我", "加群", "进群", "vx", "wx", "微信",
        "wechat", "qq", "@", "频道链接", "发布页", "导航", "jiso",
    ],
    "cta_words": [
        "点击", "加入", "进群", "加群", "私信", "联系我", "车队", "酒馆", "体验", "联系客服", "搜索", "满足你的", "引擎", "合作",
        "进入", "JISOU帮你精准找到", "查看", "金品", "复制", "搜索", "加我", "咨询", "订阅", "关注", "打开", "扫码",
    ],
    "compact_markers": ["tme", "telegramme", "joinchat", "vx", "wx", "wechat", "微信", "qq群", "q群", "客服", "加群", "进群", "发布页", "导航"],
}


def _load_promo_rules() -> Dict[str, List[str]]:
    rules_file = Path(__file__).resolve().with_name("promo_rules.json")
    if not rules_file.exists():
        return _DEFAULT_PROMO_RULES
    try:
        raw = json.loads(rules_file.read_text(encoding="utf-8"))
        merged = dict(_DEFAULT_PROMO_RULES)
        for key in merged.keys():
            value = raw.get(key)
            if isinstance(value, list):
                merged[key] = [str(x) for x in value if str(x).strip()]
        return merged
    except Exception:
        return _DEFAULT_PROMO_RULES


_PROMO_RULES = _load_promo_rules()
PROMO_KEYWORDS = _PROMO_RULES["promo_keywords"]
HARD_PROMO_MARKERS = _PROMO_RULES["hard_promo_markers"]
CTA_WORDS = _PROMO_RULES["cta_words"]
COMPACT_MARKERS = _PROMO_RULES["compact_markers"]

PROMO_KEYWORDS_COMPACT = sorted({re.sub(r"[\W_]+", "", unicodedata.normalize("NFKC", k).lower()) for k in PROMO_KEYWORDS if k.strip()})
CTA_WORDS_COMPACT = sorted({re.sub(r"[\W_]+", "", unicodedata.normalize("NFKC", k).lower()) for k in CTA_WORDS if k.strip()})

def contains_hard_promo_markers(text: str) -> bool:
    if not text:
        return False
    s = _safe_lower_nfkc(text)
    compact = _compact_for_detection(text)

    # 原始标记
    if any(x.lower() in s for x in HARD_PROMO_MARKERS):
        return True

    # 压缩串标记（抗插符号/拆字）
    if any(m in compact for m in COMPACT_MARKERS):
        return True

    # 宽松 regex
    if OBF_TME_RE.search(s):
        return True
    if WECHAT_RE.search(s) or QQ_RE.search(s) or CONTACT_ID_RE.search(s):
        return True
    return False


def _count_compact_keyword_hits(compact: str, keywords_compact: List[str]) -> int:
    if not compact:
        return 0
    hits = 0
    seen = set()
    for k in keywords_compact:
        if not k or len(k) < 2:
            continue
        # 短词（vx/wx/tg/qq）要求更谨慎
        if len(k) <= 2:
            continue
        if k in compact:
            seen.add(k)
    hits = len(seen)
    return hits


def _score_promo_signals(text: str) -> Tuple[int, List[str], Dict[str, int]]:
    """
    广告打分（增强版）
    同时看：
    - 原文（raw）
    - 轻归一化（light）
    - 压缩串（compact，抗插符号/拆字）
    """
    raw = text or ""
    s = _light_normalize(raw)
    compact = _compact_for_detection(raw)

    url_hits = len(URL_RE.findall(s))
    obf_tg_hits = len(OBF_TME_RE.findall(s))
    invite_hits = len(INVITE_RE.findall(s))
    mention_hits = len(MENTION_RE.findall(s))
    wechat_hits = len(WECHAT_RE.findall(s))
    qq_hits = len(QQ_RE.findall(s))
    phone_hits = len(PHONE_RE.findall(s))
    contact_id_hits = len(CONTACT_ID_RE.findall(s))

    kw_hits = _count_compact_keyword_hits(compact, PROMO_KEYWORDS_COMPACT)
    cta_hits = _count_compact_keyword_hits(compact, CTA_WORDS_COMPACT)

    # 额外模式：压缩串中直接出现 tg/wechat/qq 形态
    compact_tg = int(any(x in compact for x in ["tme", "telegramme", "joinchat"]))
    compact_wechat = int(any(x in compact for x in ["vx", "wx", "wechat", "微信"]))
    compact_qq = int(any(x in compact for x in ["qq", "qq群", "q群"]))

    # 可疑“插符号拆字”模式，如 v-x / t . m e / w x
    obfuscation_hits = 0
    if re.search(r"(?:v[\s\W_]*x|w[\s\W_]*x|t[\s\W_]*\.?[\s\W_]*m[\s\W_]*\.?[\s\W_]*e)", s, re.I):
        obfuscation_hits += 1

    # 链接/联系方式强信号
    score = 0
    reasons: List[str] = []

    if url_hits:
        score += 3 + min(url_hits - 1, 2)
        reasons.append(f"url:{url_hits}")

    if obf_tg_hits:
        score += 3 + min(obf_tg_hits - 1, 2)
        reasons.append(f"obf_tg:{obf_tg_hits}")

    if invite_hits:
        score += 4 + min(invite_hits - 1, 2)
        reasons.append(f"invite:{invite_hits}")

    if mention_hits:
        score += 1 if mention_hits == 1 else (2 + min(mention_hits - 2, 2))
        reasons.append(f"mention:{mention_hits}")

    if wechat_hits:
        score += 3 + min(wechat_hits - 1, 2)
        reasons.append(f"wechat:{wechat_hits}")

    if qq_hits:
        score += 2 + min(qq_hits - 1, 2)
        reasons.append(f"qq:{qq_hits}")

    if phone_hits:
        score += 2
        reasons.append(f"phone:{phone_hits}")

    if contact_id_hits:
        score += 2 + min(contact_id_hits, 2)
        reasons.append(f"contact_id:{contact_id_hits}")

    if kw_hits:
        kw_score = min(kw_hits * 2, 10)
        score += kw_score
        reasons.append(f"kw:{kw_hits}")

    if cta_hits:
        score += min(cta_hits, 3)
        reasons.append(f"cta:{cta_hits}")

    if compact_tg:
        score += 2
        reasons.append("compact_tg")
    if compact_wechat:
        score += 1
        reasons.append("compact_wechat")
    if compact_qq:
        score += 1
        reasons.append("compact_qq")
    if obfuscation_hits:
        score += 1
        reasons.append("obfuscation")

    # 组合加权：联系方式/链接 + CTA
    contact_total = (url_hits + obf_tg_hits + invite_hits + wechat_hits + qq_hits + phone_hits + contact_id_hits)
    if contact_total > 0 and cta_hits > 0:
        score += 1
        reasons.append("combo:contact+cta")

    # 组合加权：tg/群/频道关键词 + 联系方式
    if kw_hits >= 2 and contact_total > 0:
        score += 1
        reasons.append("combo:kw+contact")

    stats = {
        "url_hits": url_hits,
        "obf_tg_hits": obf_tg_hits,
        "invite_hits": invite_hits,
        "mention_hits": mention_hits,
        "wechat_hits": wechat_hits,
        "qq_hits": qq_hits,
        "phone_hits": phone_hits,
        "contact_id_hits": contact_id_hits,
        "kw_hits": kw_hits,
        "cta_hits": cta_hits,
        "compact_tg": compact_tg,
        "compact_wechat": compact_wechat,
        "compact_qq": compact_qq,
        "obfuscation_hits": obfuscation_hits,
    }
    return score, reasons, stats


def is_generic_media_caption(text: str,
                             msg_type: str,
                             has_media: bool,
                             promo_stats: Optional[Dict[str, int]] = None,
                             guard_len: int = 58) -> bool:
    """
    保护“普通媒体标题”，避免误删：
    - 媒体消息
    - 文案短
    - 没明显引流痕迹
    """
    if not has_media:
        return False

    s = (text or "").strip()
    if not s:
        return True  # 纯媒体无caption，默认不参与文本去重（防误杀）

    low = _light_normalize(s)

    has_hard = bool(
        URL_RE.search(low) or OBF_TME_RE.search(low) or INVITE_RE.search(low) or
        WECHAT_RE.search(low) or QQ_RE.search(low) or PHONE_RE.search(low) or CONTACT_ID_RE.search(low)
    )

    if promo_stats is not None:
        if (
            promo_stats["url_hits"] + promo_stats["obf_tg_hits"] + promo_stats["invite_hits"] +
            promo_stats["wechat_hits"] + promo_stats["qq_hits"] + promo_stats["phone_hits"] +
            promo_stats["contact_id_hits"]
        ) > 0:
            has_hard = True

    if len(s) <= guard_len and not has_hard:
        kw_hits = 0
        if promo_stats is not None:
            kw_hits = int(promo_stats.get("kw_hits", 0))
        else:
            kw_hits = _count_compact_keyword_hits(_compact_for_detection(low), PROMO_KEYWORDS_COMPACT)

        mention_hits = int((promo_stats or {}).get("mention_hits", len(MENTION_RE.findall(low))))
        if kw_hits <= 1 and mention_hits <= 1 and not contains_hard_promo_markers(s):
            return True

    # 很短标题保护（比如“第12集”“预告”“花絮”）
    if msg_type in {"PHOTO", "VIDEO", "GIF", "AUDIO", "FILE"} and not contains_hard_promo_markers(s):
        plain = normalize_text_for_hash(s)
        if 0 < len(plain) <= 12:
            return True

    return False


def build_single_promo_features(text: str, msg_type: str, has_media: bool, cfg: AppConfig) -> Dict[str, Any]:
    score, reasons, stats = _score_promo_signals(text)
    raw = text or ""

    is_promo = 1 if score >= cfg.promo_score_threshold else 0
    guard_reason = None
    dedupe_eligible = 0

    if is_promo:
        generic_guard = is_generic_media_caption(
            raw, msg_type=msg_type, has_media=has_media, promo_stats=stats, guard_len=cfg.media_caption_guard_len
        )

        if generic_guard and not contains_hard_promo_markers(raw):
            guard_reason = "GENERIC_MEDIA_CAPTION_GUARD"
            dedupe_eligible = 0
        else:
            dedupe_eligible = 1
    else:
        dedupe_eligible = 0

    norm_for_hash = normalize_text_for_hash(raw)
    pure_hash = make_hash(norm_for_hash) if norm_for_hash else ""

    return {
        "is_promo": is_promo,
        "promo_score": score,
        "promo_reasons": reasons,
        "dedupe_eligible": dedupe_eligible,
        "guard_reason": guard_reason,
        "content_norm": normalize_text_light(raw),
        "pure_hash": pure_hash,
        "text_len": len(raw),
    }


def build_group_promo_features(captions_concat: str,
                               item_count: int,
                               types_csv: str,
                               media_sig_hash: str,
                               cfg: AppConfig) -> Dict[str, Any]:
    """
    媒体组级广告识别（解决“相册广告”）
    """
    raw = (captions_concat or "").strip()

    # caption 可能为空，但媒体组仍可有媒体签名
    if not raw:
        return {
            "is_promo": 0,
            "promo_score": 0,
            "promo_reasons": [],
            "dedupe_eligible": 0,
            "guard_reason": "EMPTY_MEDIA_GROUP_CAPTION",
            "caption_norm": "",
            "pure_hash": "",
            "dedupe_hash": "",
        }

    score, reasons, stats = _score_promo_signals(raw)

    # 多媒体组 + 明显导流特征，加一点权重
    if item_count >= 2 and (
        contains_hard_promo_markers(raw)
        or stats["url_hits"] > 0
        or stats["obf_tg_hits"] > 0
        or stats["invite_hits"] > 0
        or stats["wechat_hits"] > 0
        or stats["qq_hits"] > 0
        or stats["contact_id_hits"] > 0
    ):
        score += 1
        reasons.append(f"group_bonus:{item_count}")

    is_promo = 1 if score >= cfg.promo_score_threshold else 0
    guard_reason = None
    dedupe_eligible = 0

    if is_promo:
        short_plain = normalize_text_for_hash(raw)
        no_hard = not contains_hard_promo_markers(raw)
        no_contacts = (stats["wechat_hits"] + stats["qq_hits"] + stats["phone_hits"] + stats["contact_id_hits"] == 0)
        no_links = (stats["url_hits"] + stats["obf_tg_hits"] + stats["invite_hits"] == 0)
        mentions_ok = (stats["mention_hits"] <= 1)
        kw_few = (stats["kw_hits"] <= 1)

        if len(raw) <= cfg.media_caption_guard_len and no_hard and no_contacts and no_links and mentions_ok and kw_few:
            guard_reason = "GENERIC_MEDIA_GROUP_CAPTION_GUARD"
            dedupe_eligible = 0
        elif 0 < len(short_plain) <= 12 and no_hard and no_contacts and no_links:
            guard_reason = "SHORT_MEDIA_GROUP_TITLE_GUARD"
            dedupe_eligible = 0
        else:
            dedupe_eligible = 1

    caption_norm = normalize_text_light(raw)
    pure_hash = make_hash(normalize_text_for_hash(raw)) if raw else ""

    # 组主 dedupe 键：当前仅在有文案时使用文本模板（空文案由上游保守处理）
    if pure_hash:
        dedupe_hash = pure_hash
    elif media_sig_hash:
        dedupe_hash = "gm:" + media_sig_hash
    else:
        dedupe_hash = ""

    return {
        "is_promo": is_promo,
        "promo_score": score,
        "promo_reasons": reasons,
        "dedupe_eligible": dedupe_eligible,
        "guard_reason": guard_reason,
        "caption_norm": caption_norm,
        "pure_hash": pure_hash,
        "dedupe_hash": dedupe_hash,
    }
