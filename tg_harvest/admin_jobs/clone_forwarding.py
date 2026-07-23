import os
import tempfile
from typing import Any

from tg_harvest.admin_jobs.sessions import bind_client_event_loop


class CloneForwardOutcomeAmbiguousError(RuntimeError):
    """Telegram may have accepted the forward but no recoverable response remains."""


def _raise_if_random_id_was_consumed(exc: Exception, *, operation: str) -> None:
    error_text = f"{type(exc).__name__}: {exc}".lower()
    if "randomidduplicate" not in error_text and not (
        "random id" in error_text
        and ("duplicate" in error_text or "already used" in error_text)
    ):
        return
    raise CloneForwardOutcomeAmbiguousError(
        f"Telegram 报告该随机 ID 已被使用，说明此前{operation}可能已经成功，"
        "但本地没有保存对应消息 ID；任务已停止以避免重复消息"
    ) from exc


def _message_id_list(messages: Any) -> tuple[list[int], bool]:
    if isinstance(messages, (list, tuple)):
        return [int(message_id) for message_id in messages], False
    return [int(messages)], True


def _forward_with_random_ids(
    client: Any,
    target_entity: Any,
    messages: Any,
    *,
    from_peer: Any,
    random_ids: list[int],
    silent: bool,
) -> Any:
    """Use MTProto random IDs so a resumed delivery cannot create duplicates."""
    from telethon.tl.functions.messages import ForwardMessagesRequest

    message_ids, single = _message_id_list(messages)
    if len(message_ids) != len(random_ids):
        raise ValueError("媒体转发随机 ID 数量与消息数量不一致")

    with bind_client_event_loop(client):
        target_peer = client.get_input_entity(target_entity)
        source_peer = client.get_input_entity(from_peer)
        request = ForwardMessagesRequest(
            from_peer=source_peer,
            id=message_ids,
            to_peer=target_peer,
            silent=bool(silent),
            drop_author=True,
            random_id=[int(random_id) for random_id in random_ids],
        )
        previous_raise_last_error = getattr(client, "_raise_last_call_error", None)
        if previous_raise_last_error is not None:
            client._raise_last_call_error = True
        try:
            result = client(request)
        except Exception as exc:
            _raise_if_random_id_was_consumed(exc, operation="转发")
            raise
        finally:
            if previous_raise_last_error is not None:
                client._raise_last_call_error = previous_raise_last_error
        resolve_messages = getattr(client, "_get_response_message", None)
        if not callable(resolve_messages):
            raise RuntimeError("当前 Telegram 客户端不支持可恢复媒体转发")
        sent = resolve_messages(request, result, target_peer)
    return sent[0] if single else sent


def _independent_uploaded_media(message: Any, uploaded_file: Any) -> Any:
    """Build newly uploaded media without relay lifetime or forward identity."""
    from telethon.tl import types

    media = getattr(message, "media", None)
    if isinstance(media, types.MessageMediaPhoto):
        return types.InputMediaUploadedPhoto(
            file=uploaded_file,
            spoiler=bool(getattr(media, "spoiler", False)),
            ttl_seconds=None,
        )
    if isinstance(media, types.MessageMediaDocument):
        document = getattr(media, "document", None)
        if document is None:
            raise RuntimeError("中转消息缺少可重新上传的文档媒体")
        return types.InputMediaUploadedDocument(
            file=uploaded_file,
            mime_type=str(getattr(document, "mime_type", "") or "application/octet-stream"),
            attributes=list(getattr(document, "attributes", None) or []),
            spoiler=bool(getattr(media, "spoiler", False)),
            ttl_seconds=None,
        )
    raise RuntimeError("中转消息不包含可独立发送的照片或文档媒体")


def _uploaded_media_reference(message: Any, uploaded_result: Any) -> Any:
    """Convert messages.uploadMedia output into reusable album input media."""
    from telethon import utils
    from telethon.tl import types

    original_media = getattr(message, "media", None)
    if isinstance(original_media, types.MessageMediaPhoto):
        photo = getattr(uploaded_result, "photo", None)
        if photo is None:
            raise RuntimeError("照片重新上传后未返回可用媒体引用")
        return types.InputMediaPhoto(
            id=utils.get_input_photo(photo),
            spoiler=bool(getattr(original_media, "spoiler", False)),
            ttl_seconds=None,
        )
    if isinstance(original_media, types.MessageMediaDocument):
        document = getattr(uploaded_result, "document", None)
        if document is None:
            raise RuntimeError("文档重新上传后未返回可用媒体引用")
        return types.InputMediaDocument(
            id=utils.get_input_document(document),
            spoiler=bool(getattr(original_media, "spoiler", False)),
            ttl_seconds=None,
        )
    raise RuntimeError("重新上传结果与中转媒体类型不一致")


def _send_independent_media_with_random_ids(
    client: Any,
    target_entity: Any,
    relay_messages: list[Any],
    *,
    random_ids: list[int],
    silent: bool,
) -> Any:
    from telethon.tl.functions.messages import (
        SendMediaRequest,
        SendMultiMediaRequest,
        UploadMediaRequest,
    )
    from telethon.tl.types import InputSingleMedia

    if len(relay_messages) != len(random_ids):
        raise ValueError("独立媒体发送随机 ID 数量与消息数量不一致")

    download_media = getattr(client, "download_media", None)
    upload_file = getattr(client, "upload_file", None)
    if not callable(download_media) or not callable(upload_file):
        raise RuntimeError("当前 Telegram 客户端不支持媒体下载后重新上传")

    with (
        tempfile.TemporaryDirectory(prefix="tg_clone_media_") as temp_dir,
        bind_client_event_loop(client),
    ):
        target_peer = client.get_input_entity(target_entity)
        uploaded_media: list[Any] = []
        for index, message in enumerate(relay_messages):
            download_target = os.path.join(temp_dir, f"media-{index:04d}")
            downloaded_path = download_media(message, file=download_target)
            if not downloaded_path or not os.path.isfile(str(downloaded_path)):
                raise RuntimeError("中转媒体下载失败，未生成可重新上传的临时文件")
            uploaded_file = upload_file(str(downloaded_path))
            uploaded_media.append(
                _independent_uploaded_media(message, uploaded_file)
            )

        if len(uploaded_media) > 1:
            uploaded_media = [
                _uploaded_media_reference(
                    message,
                    client(
                        UploadMediaRequest(
                            peer=target_peer,
                            media=media,
                        )
                    ),
                )
                for message, media in zip(
                    relay_messages,
                    uploaded_media,
                    strict=True,
                )
            ]

        media_items = [
            InputSingleMedia(
                media=media,
                message=str(getattr(message, "message", "") or ""),
                random_id=int(random_id),
                entities=list(getattr(message, "entities", None) or []),
            )
            for message, media, random_id in zip(
                relay_messages,
                uploaded_media,
                random_ids,
                strict=True,
            )
        ]
        if len(media_items) == 1:
            item = media_items[0]
            request = SendMediaRequest(
                peer=target_peer,
                media=item.media,
                message=item.message,
                random_id=item.random_id,
                entities=item.entities,
                silent=bool(silent),
            )
            single = True
        else:
            request = SendMultiMediaRequest(
                peer=target_peer,
                multi_media=media_items,
                silent=bool(silent),
            )
            single = False

        previous_raise_last_error = getattr(client, "_raise_last_call_error", None)
        if previous_raise_last_error is not None:
            client._raise_last_call_error = True
        try:
            result = client(request)
        except Exception as exc:
            _raise_if_random_id_was_consumed(exc, operation="独立媒体发送")
            raise
        finally:
            if previous_raise_last_error is not None:
                client._raise_last_call_error = previous_raise_last_error

        resolve_messages = getattr(client, "_get_response_message", None)
        if not callable(resolve_messages):
            raise RuntimeError("当前 Telegram 客户端不支持可恢复的独立媒体发送")
        sent = resolve_messages(request, result, target_peer)

    if single:
        if isinstance(sent, (list, tuple)):
            return sent[0] if sent else None
        return sent
    if isinstance(sent, (list, tuple)):
        return list(sent)
    try:
        return list(sent)
    except TypeError:
        return [sent]


def clone_send_independent_media(
    client: Any,
    target_entity: Any,
    relay_messages: list[Any],
    *,
    random_ids: list[int],
    silent: bool = True,
) -> Any:
    """Create target-owned media messages independent from relay messages."""
    messages = list(relay_messages)
    if not messages:
        raise ValueError("独立媒体发送缺少中转消息")
    if len(messages) != len(random_ids):
        raise ValueError("独立媒体发送随机 ID 数量与消息数量不一致")

    if callable(getattr(client, "get_input_entity", None)) and callable(client):
        return _send_independent_media_with_random_ids(
            client,
            target_entity,
            messages,
            random_ids=random_ids,
            silent=silent,
        )

    send_file = getattr(client, "send_file", None)
    if not callable(send_file):
        raise RuntimeError("当前 Telegram 客户端不支持独立媒体发送")
    files = [getattr(message, "media", None) for message in messages]
    if any(file is None for file in files):
        raise RuntimeError("中转消息不包含可独立发送的媒体")
    captions = [str(getattr(message, "message", "") or "") for message in messages]
    formatting_entities = [
        list(getattr(message, "entities", None) or []) for message in messages
    ]
    single = len(messages) == 1
    with bind_client_event_loop(client):
        return send_file(
            target_entity,
            files[0] if single else files,
            caption=captions[0] if single else captions,
            formatting_entities=(
                formatting_entities[0] if single else formatting_entities
            ),
            parse_mode=None,
            silent=bool(silent),
        )


def clone_forward_without_source_attribution(
    client: Any,
    target_entity: Any,
    messages: Any,
    *,
    from_peer: Any,
    random_ids: list[int] | None = None,
    silent: bool = True,
) -> Any:
    """Forward as a copied message, hiding Telegram's source attribution."""
    if (
        random_ids is not None
        and callable(getattr(client, "get_input_entity", None))
        and callable(client)
    ):
        return _forward_with_random_ids(
            client,
            target_entity,
            messages,
            from_peer=from_peer,
            random_ids=random_ids,
            silent=silent,
        )

    kwargs: dict[str, Any] = {
        "from_peer": from_peer,
        "drop_author": True,
        "silent": bool(silent),
    }

    with bind_client_event_loop(client):
        return client.forward_messages(target_entity, messages, **kwargs)


def clone_delete_copied_relay_messages(
    client: Any,
    relay_entity: Any,
    message_ids: list[int],
) -> Any:
    """Best-effort cleanup for temporary relay copies after a target copy attempt."""
    with bind_client_event_loop(client):
        return client.delete_messages(relay_entity, message_ids, revoke=True)
