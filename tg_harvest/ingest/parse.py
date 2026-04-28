# -*- coding: utf-8 -*-
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from tg_harvest.domain.dedupe import build_media_fingerprint
from tg_harvest.domain.normalize import _safe_json


# 设置日志
def setup_logging():
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
        )
    logging.getLogger("telethon").setLevel(logging.WARNING)
    logging.getLogger("werkzeug").setLevel(logging.WARNING)


@dataclass
class ParsedMessage:
    msg_id: int
    date_text: str
    date_ts: int
    sender_id: int
    msg_type: str
    content: str
    has_media: bool
    grouped_id: Optional[int]
    media_meta: Optional[Dict[str, Any]] = None


class MessageParseError(RuntimeError):
    def __init__(self, message_id: Any, cause: Exception) -> None:
        self.message_id = message_id
        self.cause = cause
        super().__init__(f"id={message_id}, err={cause.__class__.__name__}: {cause}")


class MessageParser:
    """负责将 Telethon 消息对象转化为内部结构化数据"""

    TEXT_ATTRS = ("raw_text", "message", "text")
    MEDIA_FIELDS = (
        "id",
        "name",
        "ext",
        "mime_type",
        "size",
        "width",
        "height",
        "duration",
        "title",
        "performer",
        "emoji",
    )

    @classmethod
    def parse(cls, message: Any) -> Optional[ParsedMessage]:
        dt = getattr(message, "date", None)
        if dt is None:
            return None

        raw_msg_id = getattr(message, "id", 0)
        try:
            msg_id = int(raw_msg_id or 0)
            if not msg_id:
                return None

            msg_type = cls.classify_type(message)
            content = cls.extract_text(message)
            has_media = msg_type != "TEXT"

            # 处理媒体组 ID
            gid = getattr(message, "grouped_id", None)
            grouped_id = int(gid) if gid is not None else None

            media_meta = None
            if has_media:
                media_meta = cls.extract_media_meta(message, msg_type)
                # 如果消息没有文本但有媒体文件名，则使用文件名作为内容，以便搜索
                if not content and media_meta.get("file_name"):
                    content = media_meta["file_name"]

            return ParsedMessage(
                msg_id=msg_id,
                date_text=dt.strftime("%Y-%m-%d %H:%M:%S"),
                date_ts=int(dt.timestamp()),
                sender_id=int(getattr(message, "sender_id", 0) or 0),
                msg_type=msg_type,
                content=content,
                has_media=has_media,
                grouped_id=grouped_id,
                media_meta=media_meta,
            )
        except Exception as exc:
            raise MessageParseError(raw_msg_id or "?", exc) from exc

    @classmethod
    def classify_type(cls, message: Any) -> str:
        for attr, label in [
            ("sticker", "STICKER"),
            ("gif", "GIF"),
            ("voice", "VOICE"),
            ("video_note", "VIDEO_NOTE"),
            ("audio", "AUDIO"),
            ("video", "VIDEO"),
            ("photo", "PHOTO"),
            ("document", "FILE"),
            ("poll", "POLL"),
            ("contact", "CONTACT"),
            ("geo", "GEO"),
        ]:
            if getattr(message, attr, None):
                return label
        return "TEXT"

    @classmethod
    def extract_text(cls, message: Any) -> str:
        for attr in cls.TEXT_ATTRS:
            val = getattr(message, attr, None)
            if val:
                return str(val).strip()
        return ""

    @classmethod
    def extract_media_meta(cls, message: Any, msg_type: str) -> Dict[str, Any]:
        meta: Dict[str, Any] = {
            "media_kind": msg_type,
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

        extra = {}
        # 1. 从 .file 封装提取
        f = getattr(message, "file", None)
        if f:
            for k in cls.MEDIA_FIELDS:
                v = getattr(f, k, None)
                if v is None:
                    continue
                if k == "id":
                    meta["file_unique_id"] = str(v)
                elif k == "name":
                    meta["file_name"] = str(v)
                elif k == "ext":
                    meta["file_ext"] = str(v)
                elif k == "mime_type":
                    meta["mime_type"] = str(v)
                elif k == "size":
                    meta["file_size"] = int(v)
                elif k == "width":
                    meta["width"] = int(v)
                elif k == "height":
                    meta["height"] = int(v)
                elif k == "duration":
                    meta["duration_sec"] = int(v)
                else:
                    extra[k] = v

        # 2. 回退机制：从原始对象提取 ID
        if not meta["file_unique_id"]:
            for attr in ("photo", "document"):
                obj = getattr(message, attr, None)
                if obj and hasattr(obj, "id"):
                    meta["file_unique_id"] = str(obj.id)
                    break

        # 3. 提取消息级统计
        for attr in ("views", "forwards"):
            val = getattr(message, attr, None)
            if val is not None:
                extra[attr] = val

        ed = getattr(message, "edit_date", None)
        if ed:
            extra["edit_date"] = str(ed)

        # 4. 终结化处理
        meta["meta_json"] = _safe_json(extra) if extra else None
        meta["media_fingerprint"] = build_media_fingerprint(
            file_unique_id=meta["file_unique_id"],
            mime_type=meta["mime_type"],
            file_size=meta["file_size"],
            width=meta["width"],
            height=meta["height"],
            duration_sec=meta["duration_sec"],
        )
        return meta


def _match_entities_from_dialog_titles(client: Any, target: str) -> List[Any]:
    exact_matches = []
    partial_match = None

    for d in client.get_dialogs():
        title = (getattr(d, "title", None) or "").strip()
        if not title:
            continue
        if title == target:
            exact_matches.append(d.entity)
        elif target in title and partial_match is None:
            partial_match = d.entity

    return exact_matches if exact_matches else ([partial_match] if partial_match else [])


def resolve_target_entities(client: Any, target: str) -> List[Any]:
    t = (target or "").strip()
    if not t:
        return []
    try:
        cleaned = t.replace("https://t.me/", "").replace("http://t.me/", "").strip("/")
        is_explicit_username = t.startswith("@")
        if cleaned.startswith("@"):
            cleaned = cleaned.lstrip("@")

        # 链接和 @username 属于显式标识符，直接按实体解析。
        if cleaned != t or is_explicit_username:
            entity = client.get_entity(cleaned)
            return [entity] if entity else []

        is_numeric_target = re.fullmatch(r"-?\d+", t) is not None
        if is_numeric_target:
            # 纯数字输入存在歧义：它既可能是 chat_id，也可能只是频道标题。
            # 先在当前账号已加入的会话标题里精确匹配，找不到再回退到 ID 解析。
            title_matches = _match_entities_from_dialog_titles(client, t)
            if title_matches:
                return title_matches

            entity = client.get_entity(int(cleaned))
            return [entity] if entity else []

        return _match_entities_from_dialog_titles(client, t)
    except Exception:
        return []

@dataclass
class HarvestCounters:
    seen: int = 0
    written: int = 0
    parse_failures: int = 0
    parse_failure_samples: List[str] = field(default_factory=list)
    parse_failures_by_type: Dict[str, int] = field(default_factory=dict)

    def note_parse_failure(self, err: Exception, message: Any = None):
        self.parse_failures += 1
        key = err.__class__.__name__
        self.parse_failures_by_type[key] = self.parse_failures_by_type.get(key, 0) + 1
        if len(self.parse_failure_samples) < 5:
            mid = getattr(message, "id", "?")
            self.parse_failure_samples.append(f"id={mid}, err={key}: {err}")


def log_parse_failure_summary(counters: HarvestCounters):
    if counters.parse_failures > 0:
        logging.warning(
            f"解析失败统计: total={counters.parse_failures} by_type={counters.parse_failures_by_type}"
        )
        for s in counters.parse_failure_samples:
            logging.warning(f"解析失败样例: {s}")
