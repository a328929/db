import logging
import mimetypes
import re
from dataclasses import dataclass, field
from typing import Any

from tg_harvest.domain.coerce import optional_int
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
    grouped_id: int | None
    media_meta: dict[str, Any] | None = None


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

    @staticmethod
    def _coerce_int(value: Any) -> int | None:
        return optional_int(value)

    @classmethod
    def _set_text_if_empty(cls, meta: dict[str, Any], key: str, value: Any) -> None:
        if meta.get(key) or value is None:
            return
        text = str(value).strip()
        if text:
            meta[key] = text

    @classmethod
    def _set_int_if_empty(cls, meta: dict[str, Any], key: str, value: Any) -> None:
        if meta.get(key) is not None:
            return
        parsed = cls._coerce_int(value)
        if parsed is not None:
            meta[key] = parsed

    @classmethod
    def _set_extension_if_empty(cls, meta: dict[str, Any]) -> None:
        if meta.get("file_ext"):
            return
        file_name = str(meta.get("file_name") or "").strip()
        if "." in file_name:
            ext = "." + file_name.rsplit(".", 1)[-1].strip()
            if len(ext) > 1:
                meta["file_ext"] = ext
                return
        mime_type = str(meta.get("mime_type") or "").strip()
        if mime_type:
            guessed_ext = mimetypes.guess_extension(mime_type)
            if guessed_ext:
                meta["file_ext"] = guessed_ext

    @classmethod
    def _extract_document_attribute_meta(
        cls, meta: dict[str, Any], extra: dict[str, Any], document: Any
    ) -> None:
        for attr in getattr(document, "attributes", None) or []:
            cls._set_text_if_empty(meta, "file_name", getattr(attr, "file_name", None))
            cls._set_int_if_empty(meta, "duration_sec", getattr(attr, "duration", None))
            cls._set_int_if_empty(meta, "width", getattr(attr, "w", None))
            cls._set_int_if_empty(meta, "height", getattr(attr, "h", None))
            for key in ("title", "performer", "emoji"):
                value = getattr(attr, key, None)
                if value is not None and key not in extra:
                    extra[key] = value

    @classmethod
    def _extract_photo_size_meta(cls, meta: dict[str, Any], photo: Any) -> None:
        best_size = None
        best_area = -1
        for size in getattr(photo, "sizes", None) or []:
            width = cls._coerce_int(getattr(size, "w", None))
            height = cls._coerce_int(getattr(size, "h", None))
            area = (width or 0) * (height or 0)
            if area > best_area:
                best_area = area
                best_size = size
        if best_size is None:
            return
        cls._set_int_if_empty(meta, "width", getattr(best_size, "w", None))
        cls._set_int_if_empty(meta, "height", getattr(best_size, "h", None))
        cls._set_int_if_empty(meta, "file_size", getattr(best_size, "size", None))

    @classmethod
    def _extract_raw_media_meta(
        cls, message: Any, meta: dict[str, Any], extra: dict[str, Any]
    ) -> None:
        document = getattr(message, "document", None)
        if document:
            cls._set_text_if_empty(meta, "file_unique_id", getattr(document, "id", None))
            cls._set_text_if_empty(meta, "mime_type", getattr(document, "mime_type", None))
            cls._set_int_if_empty(meta, "file_size", getattr(document, "size", None))
            cls._extract_document_attribute_meta(meta, extra, document)

        photo = getattr(message, "photo", None)
        if photo:
            cls._set_text_if_empty(meta, "file_unique_id", getattr(photo, "id", None))
            cls._extract_photo_size_meta(meta, photo)

        cls._set_extension_if_empty(meta)

    @classmethod
    def parse(cls, message: Any) -> ParsedMessage | None:
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
    def extract_media_meta(cls, message: Any, msg_type: str) -> dict[str, Any]:
        meta: dict[str, Any] = {
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

        # Telethon's convenience .file wrapper is not always populated. The raw
        # document/photo payload usually still carries size, mime type,
        # dimensions and duration.
        cls._extract_raw_media_meta(message, meta, extra)

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


def _match_entities_from_dialog_titles(client: Any, target: str) -> list[Any]:
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


def _is_entity_lookup_miss(exc: Exception) -> bool:
    exc_name = exc.__class__.__name__.lower()
    exc_message = str(exc).lower()
    if exc_name in {"usernameinvaliderror", "usernamenotoccupiederror"}:
        return True
    return isinstance(exc, ValueError) and (
        "could not find the input entity" in exc_message
        or "cannot find any entity" in exc_message
        or "no user has" in exc_message
    )


def _resolve_entity_or_empty(client: Any, key: Any) -> list[Any]:
    try:
        entity = client.get_entity(key)
    except Exception as exc:
        if _is_entity_lookup_miss(exc):
            return []
        raise
    return [entity] if entity else []


def resolve_target_entities(client: Any, target: str) -> list[Any]:
    t = (target or "").strip()
    if not t:
        return []
    cleaned = t.replace("https://t.me/", "").replace("http://t.me/", "").strip("/")
    is_explicit_username = t.startswith("@")
    if cleaned.startswith("@"):
        cleaned = cleaned.lstrip("@")

    # 链接和 @username 属于显式标识符，直接按实体解析。
    if cleaned != t or is_explicit_username:
        return _resolve_entity_or_empty(client, cleaned)

    is_numeric_target = re.fullmatch(r"-?\d+", t) is not None
    if is_numeric_target:
        # 纯数字输入存在歧义：它既可能是 chat_id，也可能只是频道标题。
        # 先在当前账号已加入的会话标题里精确匹配，找不到再回退到 ID 解析。
        title_matches = _match_entities_from_dialog_titles(client, t)
        if title_matches:
            return title_matches

        return _resolve_entity_or_empty(client, int(cleaned))

    return _match_entities_from_dialog_titles(client, t)

@dataclass
class HarvestCounters:
    seen: int = 0
    written: int = 0
    parse_failures: int = 0
    parse_failure_samples: list[str] = field(default_factory=list)
    parse_failures_by_type: dict[str, int] = field(default_factory=dict)

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
