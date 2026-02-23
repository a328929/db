# -*- coding: utf-8 -*-
import re
import html
import json
import hashlib
import unicodedata
from typing import Any, List, Optional

# =========================
# 文本清洗 / 归一化工具
# =========================

# ---- 基础正则 ----
ZERO_WIDTH_RE = re.compile(r"[\u200b-\u200f\u2060\ufeff\u180e]")
CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")
MULTISPACE_RE = re.compile(r"\s+")
DIGIT_RE = re.compile(r"\d+")

# 标准 URL / tg
URL_RE = re.compile(r"(https?://\S+|t\.me/\S+|telegram\.me/\S+)", re.I)
INVITE_RE = re.compile(r"(?:https?://)?t\.me/(?:joinchat/|\+)[A-Za-z0-9_-]+", re.I)

# 更宽松的“拆字/插符号” tg 链接检测
OBF_TME_RE = re.compile(
    r"(?:h\s*t\s*t\s*p\s*s?\s*[:：]?\s*/\s*/\s*)?"
    r"(?:t\s*[\.\-_/\\]?\s*m\s*[\.\-_/\\]?\s*e|telegram\s*[\.\-_/\\]?\s*me)"
    r"\s*/\s*[A-Za-z0-9_+\-/]+",
    re.I
)

# @mention（允许 @ 后有轻微空格）
MENTION_RE = re.compile(r"(?<!\w)@\s*[A-Za-z0-9_]{3,}")

# 联系方式（增强版，容忍插空格/符号）
WECHAT_RE = re.compile(
    r"(?:"
    r"v[\s\W_]*x|w[\s\W_]*x|we[\s\W_]*chat|微[\s\W_]*信"
    r")"
    r"(?:号|id|ID)?"
    r"\s*[:：]?\s*"
    r"[A-Za-z0-9][A-Za-z0-9_\-]{3,}",
    re.I
)

QQ_RE = re.compile(
    r"(?:q[\s\W_]*q|q[\s\W_]*群|q[\s\W_]*q[\s\W_]*群|扣扣)"
    r"(?:号|群|群号)?"
    r"\s*[:：]?\s*"
    r"\d{5,}",
    re.I
)

PHONE_RE = re.compile(r"(?<!\d)(?:\+?86[\s\-]?)?1[3-9]\d(?:[\s\-]?\d){8}(?!\d)")

# “关键词 + 账号”组合（比如：联系：abc12345）
CONTACT_ID_RE = re.compile(
    r"(?:联系(?:方式)?|联系我|客服(?:微信)?|商务(?:合作)?|投稿|咨询|加我|找我|vx|wx|微信|wechat|qq|tg|telegram)"
    r"\s*[:：]?\s*"
    r"[A-Za-z0-9_@\-\+]{4,}",
    re.I
)

# 常见乱码/花字分隔符（用于压缩）
NOISE_SEP_RE = re.compile(r"[\s\W_]+", re.UNICODE)
NON_WORD_CJK_RE = re.compile(r"[^\w\u4e00-\u9fff]+", re.UNICODE)

# 超长疑似随机串（抗机器人扰动）
LONG_MIXED_TOKEN_RE = re.compile(r"(?i)^(?=.*[a-z])(?=.*\d)[a-z0-9]{8,}$")
LONG_ALPHA_GIBBERISH_RE = re.compile(r"(?i)^[bcdfghjklmnpqrstvwxyz]{10,}$")

# 重复字符压缩（如 vxxxxxx / 微微微微）
REPEAT_CHAR_RE = re.compile(r"(.)\1{3,}", re.UNICODE)

# emoji / pictograph 粗略过滤（不追求完美）
EMOJI_BLOCK_RE = re.compile(
    r"[\U0001F000-\U0001FAFF\U00002700-\U000027BF\U00002600-\U000026FF]+",
    re.UNICODE
)


# 常见同形字符折叠（只放一小批高频，别搞太激进）
CONFUSABLE_MAP = str.maketrans({
    # 西里尔/希腊常见混淆 -> 拉丁
    "а": "a", "е": "e", "о": "o", "р": "p", "с": "c", "у": "y", "х": "x", "і": "i", "ј": "j",
    "Α": "A", "Β": "B", "Ε": "E", "Ζ": "Z", "Η": "H", "Ι": "I", "Κ": "K", "Μ": "M", "Ν": "N",
    "Ο": "O", "Ρ": "P", "Τ": "T", "Υ": "Y", "Χ": "X",
    "а": "a", "А": "A", "В": "B", "С": "C", "Е": "E", "Н": "H", "К": "K", "М": "M", "О": "O", "Р": "P", "Т": "T", "Х": "X",
    # 常见全角符号在 NFKC 里大多会处理，这里补一点
    "＠": "@", "／": "/", "：": ":", "．": ".", "。": ".",
})


def _safe_json(obj: Any) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return "[]"


def _safe_lower_nfkc(text: str) -> str:
    s = unicodedata.normalize("NFKC", text or "")
    s = html.unescape(s)
    s = s.translate(CONFUSABLE_MAP)
    s = ZERO_WIDTH_RE.sub("", s)
    s = CONTROL_RE.sub(" ", s)
    return s.lower()


def _collapse_repeats(s: str) -> str:
    # 连续重复字符压到最多2个，抗“垃垃垃垃圾圾圾”
    return REPEAT_CHAR_RE.sub(r"\1\1", s)


def _clean_visual_noise(s: str) -> str:
    # 去 emoji / 花字块，再做轻度重复压缩
    s = EMOJI_BLOCK_RE.sub(" ", s)
    s = _collapse_repeats(s)
    return s


def _compact_for_detection(s: str) -> str:
    """
    检测用压缩串：
    - 去空格和大部分符号，把“微 x 信”“t . me / +xxx”压成连续串
    """
    s = _safe_lower_nfkc(s)
    s = _clean_visual_noise(s)
    s = NOISE_SEP_RE.sub("", s)
    return s


def _light_normalize(s: str) -> str:
    s = _safe_lower_nfkc(s)
    s = _clean_visual_noise(s)
    s = MULTISPACE_RE.sub(" ", s).strip()
    return s


def _should_keep_non_noise_token(tok: str) -> Optional[str]:
    if not tok:
        return None

    # 含中文通常不算噪音
    if re.search(r"[\u4e00-\u9fff]", tok):
        return None

    t = tok.strip().lower()
    if len(t) < 6:
        return None

    # 保留可能有意义的常见 token
    whitelist = {
        "telegram", "wechat", "whatsapp", "channel", "group", "contact", "support",
        "tme", "joinchat", "http", "https"
    }
    if t in whitelist:
        return None

    return t


def _is_structural_noise_token(t: str) -> bool:
    # 纯字母但像一串辅音轰炸
    if LONG_ALPHA_GIBBERISH_RE.match(t):
        return True

    # 字母数字混合长串
    if LONG_MIXED_TOKEN_RE.match(t):
        return True

    return False


def _is_ratio_noise_token(t: str) -> bool:
    # 超长且元音极少
    if re.fullmatch(r"[a-z]{10,}", t):
        vowels = sum(1 for c in t if c in "aeiou")
        if vowels <= 1:
            return True
    return False


def _is_noise_token(tok: str) -> bool:
    """
    用于 hash 归一化时剔除“随机扰动串”
    目标：像 a8f3k2m1 / xqzptklllj 这种。
    """
    t = _should_keep_non_noise_token(tok)
    if t is None:
        return False

    if _is_structural_noise_token(t):
        return True

    if _is_ratio_noise_token(t):
        return True

    return False


def _replace_strong_signals(s: str) -> str:
    # 先替换强信号（顺序重要：先具体再通用）
    s = OBF_TME_RE.sub(" TG_LINK ", s)
    s = INVITE_RE.sub(" TG_INVITE ", s)
    s = URL_RE.sub(" URL ", s)

    s = MENTION_RE.sub(" MENTION ", s)
    s = WECHAT_RE.sub(" WECHAT_ID ", s)
    s = QQ_RE.sub(" QQ_ID ", s)
    s = PHONE_RE.sub(" PHONE ", s)
    s = CONTACT_ID_RE.sub(" CONTACT_ID ", s)
    return s


def _inject_compact_markers(s: str) -> str:
    # 一些被拆开的弱形式再兜一层（压缩串级别）
    compact = _compact_for_detection(s)
    # 如果压缩串里出现明显 tg/wechat/qq 形态，给原串注入占位，增强稳定性
    marker_tokens: List[str] = []
    if "tme" in compact or "telegramme" in compact or "joinchat" in compact:
        marker_tokens.append("TGLINK")
    if "vx" in compact or "wx" in compact or "wechat" in compact or "微信" in compact:
        marker_tokens.append("WECHAT")
    if "qq" in compact or "qq群" in compact or "q群" in compact:
        marker_tokens.append("QQ")
    if marker_tokens:
        s += " " + " ".join(marker_tokens)
    return s


def _filter_and_denoise_tokens(s: str) -> str:
    # token 级去噪
    tokens = []
    for tok in re.split(r"\s+", s):
        if not tok:
            continue
        # 清掉 token 级多余符号
        t = re.sub(r"^[^\w\u4e00-\u9fff]+|[^\w\u4e00-\u9fff]+$", "", tok, flags=re.UNICODE)
        if not t:
            continue

        # 去数字（模板去重里通常是噪音）
        t = DIGIT_RE.sub("", t)

        # 去随机干扰串
        if _is_noise_token(t):
            continue

        if t:
            tokens.append(t)

    return " ".join(tokens)


def _finalize_template_text(s: str) -> str:
    # 最后再做一次“只留字母数字中文”的收缩，形成稳定模板
    s = NON_WORD_CJK_RE.sub("", s)
    s = _collapse_repeats(s)
    return s.strip()


def normalize_text_for_hash(text: str) -> str:
    """
    模板识别用“强归一化”：
    - 抗零宽/全角/emoji/同形字
    - 去链接/去联系方式/去数字
    - 去随机噪声 token
    - 去大部分符号
    """
    if not text:
        return ""

    s = _safe_lower_nfkc(text)
    s = _clean_visual_noise(s)

    s = _replace_strong_signals(s)
    s = _inject_compact_markers(s)

    # 统一空白
    s = MULTISPACE_RE.sub(" ", s).strip()
    s = _filter_and_denoise_tokens(s)
    return _finalize_template_text(s)


def normalize_text_light(text: str) -> str:
    if not text:
        return ""
    s = _light_normalize(text)
    return s


def make_hash(text: str) -> str:
    if not text:
        return ""
    return hashlib.blake2b(text.encode("utf-8"), digest_size=16).hexdigest()
