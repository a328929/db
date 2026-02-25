# -*- coding: utf-8 -*-
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .dedupe import build_media_fingerprint
from .normalize import _safe_json


def setup_logging():
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("telethon").setLevel(logging.WARNING)
    logging.getLogger("werkzeug").setLevel(logging.WARNING)


def classify_msg_type(message) -> str:
    try:
        if getattr(message, "sticker", None):
            return "STICKER"
        if getattr(message, "gif", None):
            return "GIF"
        if getattr(message, "voice", None):
            return "VOICE"
        if getattr(message, "video_note", None):
            return "VIDEO_NOTE"
        if getattr(message, "audio", None):
            return "AUDIO"
        if getattr(message, "video", None):
            return "VIDEO"
        if getattr(message, "photo", None):
            return "PHOTO"
        if getattr(message, "document", None):
            return "FILE"
        if getattr(message, "poll", None):
            return "POLL"
        if getattr(message, "contact", None):
            return "CONTACT"
        if getattr(message, "geo", None):
            return "GEO"
        return "TEXT"
    except Exception:
        return "TEXT"


def extract_message_text(message) -> str:
    for attr in _text_source_attrs():
        v = _read_text_attr(message, attr)
        if v is None:
            continue
        normalized = _normalize_text(v)
        if normalized:
            return normalized
    return ""


def _text_source_attrs() -> tuple:
    return ("raw_text", "message", "text")


def _read_text_attr(message, attr: str):
    try:
        return getattr(message, attr, None)
    except Exception:
        return None


def _normalize_text(value) -> str:
    return str(value).strip()


def extract_media_meta(message, msg_type: str) -> Dict[str, Any]:
    out = _init_media_meta(msg_type)
    if msg_type == "TEXT":
        return out

    extra = _extract_file_wrapper_fields(message, out)
    _fill_fallback_file_unique_id(message, out)
    _append_message_level_meta(message, extra)
    _finalize_media_meta(out, extra)
    return out


def _init_media_meta(msg_type: str) -> Dict[str, Any]:
    return {
        "media_kind": msg_type if msg_type != "TEXT" else None,
        "file_unique_id": None,
        "file_name": None,
        "file_ext": None,
        "mime_type": None,
        "file_size": None,
        "width": None,
        "height": None,
        "duration_sec": None,
        "media_fingerprint": None,
        "meta_json": None,
    }


def _extract_file_wrapper_fields(message, out: Dict[str, Any]) -> Dict[str, Any]:
    extra: Dict[str, Any] = {}
    try:
        f = getattr(message, "file", None)
        if f is not None:
            for k in ("id", "name", "ext", "mime_type", "size", "width", "height", "duration", "title", "performer", "emoji"):
                try:
                    v = getattr(f, k, None)
                except Exception:
                    v = None
                if v is None:
                    continue
                if k == "id":
                    out["file_unique_id"] = str(v)
                elif k == "name":
                    out["file_name"] = str(v)
                elif k == "ext":
                    out["file_ext"] = str(v)
                elif k == "mime_type":
                    out["mime_type"] = str(v)
                elif k == "size":
                    try:
                        out["file_size"] = int(v)
                    except Exception:
                        pass
                elif k == "width":
                    try:
                        out["width"] = int(v)
                    except Exception:
                        pass
                elif k == "height":
                    try:
                        out["height"] = int(v)
                    except Exception:
                        pass
                elif k == "duration":
                    try:
                        out["duration_sec"] = int(v)
                    except Exception:
                        pass
                else:
                    extra[k] = v
    except Exception as e:
        extra["file_wrapper_error"] = str(e)
    return extra


def _fill_fallback_file_unique_id(message, out: Dict[str, Any]):
    if not out["file_unique_id"]:
        try:
            p = getattr(message, "photo", None)
            if p is not None and hasattr(p, "id"):
                out["file_unique_id"] = str(getattr(p, "id"))
        except Exception:
            pass
    if not out["file_unique_id"]:
        try:
            d = getattr(message, "document", None)
            if d is not None and hasattr(d, "id"):
                out["file_unique_id"] = str(getattr(d, "id"))
        except Exception:
            pass


def _append_message_level_meta(message, extra: Dict[str, Any]):
    try:
        extra["views"] = getattr(message, "views", None)
        extra["forwards"] = getattr(message, "forwards", None)
        extra["edit_date"] = str(getattr(message, "edit_date", None)) if getattr(message, "edit_date", None) else None
    except Exception:
        pass


def _finalize_media_meta(out: Dict[str, Any], extra: Dict[str, Any]):
    extra = {k: v for k, v in extra.items() if v is not None}
    out["meta_json"] = _safe_json(extra) if extra else None
    out["media_fingerprint"] = build_media_fingerprint(
        file_unique_id=out["file_unique_id"],
        mime_type=out["mime_type"],
        file_size=out["file_size"],
        width=out["width"],
        height=out["height"],
        duration_sec=out["duration_sec"],
    )


def _parse_target_identifier(target: str) -> tuple[str, bool]:
    t = (target or "").strip()
    cleaned = t.replace("https://t.me/", "").replace("http://t.me/", "").strip("/")
    if cleaned.startswith("@"):
        cleaned = cleaned.lstrip("@")
    explicit_identifier = bool(cleaned and (cleaned != t or t.startswith("@") or re.fullmatch(r"-?\d+", t)))
    return cleaned, explicit_identifier


def resolve_target_entities(client: Any, target: str) -> List[Any]:
    t = (target or "").strip()
    if not t:
        return []

    try:
        cleaned, explicit_identifier = _parse_target_identifier(t)
        if explicit_identifier:
            entity = client.get_entity(cleaned)
            return [entity] if entity is not None else []
    except Exception:
        return []

    try:
        exact_matches: List[Any] = []
        partial_match = None
        for d in client.get_dialogs():
            title = (d.title or "")
            if title.strip() == t:
                exact_matches.append(d.entity)
                continue
            if t and partial_match is None and t in title:
                partial_match = d.entity

        if len(exact_matches) > 1:
            logging.info("发现多个同名群组/频道（target=%s, count=%s），将全部纳入导入", t, len(exact_matches))
            return exact_matches
        if len(exact_matches) == 1:
            return [exact_matches[0]]
        return [partial_match] if partial_match is not None else []
    except Exception:
        return []


def resolve_target_entity(client: Any, target: str):
    entities = resolve_target_entities(client, target)
    return entities[0] if entities else None


def build_msg_link(entity, msg_id: int) -> str:
    username = getattr(entity, "username", None)
    if username:
        return f"https://t.me/{username}/{msg_id}"
    raw_id = str(getattr(entity, "id", ""))
    if raw_id.startswith("-100"):
        raw_id = raw_id[4:]
    else:
        raw_id = raw_id.lstrip("-")
    return f"https://t.me/c/{raw_id}/{msg_id}"


@dataclass
class HarvestCounters:
    seen: int = 0
    written: int = 0
    parse_failures: int = 0
    parse_failure_samples: Optional[List[str]] = None
    parse_failures_by_type: Optional[Dict[str, int]] = None

    def __post_init__(self):
        if self.parse_failure_samples is None:
            self.parse_failure_samples = []
        if self.parse_failures_by_type is None:
            self.parse_failures_by_type = {}

    def note_parse_failure(self, err: Exception, message: Any = None):
        self.parse_failures += 1
        key = err.__class__.__name__
        self.parse_failures_by_type[key] = self.parse_failures_by_type.get(key, 0) + 1
        if len(self.parse_failure_samples) >= 5:
            return
        msg_id = getattr(message, "id", None)
        self.parse_failure_samples.append(f"id={msg_id}, err={key}: {err}")


def log_parse_failure_summary(counters: HarvestCounters):
    if counters.parse_failures == 0:
        return
    logging.warning("解析失败统计: total=%s by_type=%s", counters.parse_failures, counters.parse_failures_by_type)
    for sample in counters.parse_failure_samples:
        logging.warning("解析失败样例: %s", sample)
