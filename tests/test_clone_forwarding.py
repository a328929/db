from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from telethon.tl import types
from telethon.tl.functions.messages import (
    SendMediaRequest,
    SendMultiMediaRequest,
    UploadMediaRequest,
)

from tg_harvest.admin_jobs.clone_forwarding import (
    CloneForwardOutcomeAmbiguousError,
    clone_forward_without_source_attribution,
    clone_send_independent_media,
)

ROOT = Path(__file__).resolve().parent.parent


class _ForwardClient:
    def __init__(self):
        self.calls = []

    def forward_messages(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return ["sent"]


class _DurableForwardClient:
    def __init__(self):
        self.request = None

    def get_input_entity(self, value):
        return f"input:{value}"

    def __call__(self, request):
        self.request = request
        return "updates"

    def _get_response_message(self, request, _updates, _target):
        return [SimpleNamespace(id=7000 + index) for index, _ in enumerate(request.id)]


def test_clone_forward_without_source_attribution_forces_drop_author_and_silence():
    client = _ForwardClient()

    result = clone_forward_without_source_attribution(
        client,
        "target",
        [1, 2],
        from_peer="source",
    )

    assert result == ["sent"]
    assert client.calls == [
        (
            ("target", [1, 2]),
            {
                "from_peer": "source",
                "drop_author": True,
                "silent": True,
            },
        )
    ]


def test_clone_forward_reuses_persisted_random_ids_when_available():
    client = _DurableForwardClient()

    result = clone_forward_without_source_attribution(
        client,
        "target",
        [11, 12],
        from_peer="source",
        random_ids=[101, 102],
    )

    assert [message.id for message in result] == [7000, 7001]
    assert client.request is not None
    assert client.request.id == [11, 12]
    assert client.request.random_id == [101, 102]
    assert client.request.drop_author is True
    assert client.request.silent is True


def test_clone_forward_turns_duplicate_random_id_into_ambiguous_outcome():
    class RandomIdDuplicateError(RuntimeError):
        pass

    class _DuplicateClient(_DurableForwardClient):
        def __call__(self, request):
            self.request = request
            raise RandomIdDuplicateError("random ID was already used")

    with pytest.raises(CloneForwardOutcomeAmbiguousError, match="避免重复消息"):
        clone_forward_without_source_attribution(
            _DuplicateClient(),
            "target",
            11,
            from_peer="source",
            random_ids=[101],
        )


class _DurableMediaClient:
    def __init__(self):
        self.request = None
        self.requests = []
        self.download_calls = []
        self.upload_calls = []

    def get_input_entity(self, value):
        return f"input:{value}"

    def __call__(self, request):
        self.request = request
        self.requests.append(request)
        if isinstance(request, UploadMediaRequest):
            media_id = 500 + len(self.requests)
            if isinstance(request.media, types.InputMediaUploadedPhoto):
                return types.MessageMediaPhoto(
                    photo=types.Photo(
                        id=media_id,
                        access_hash=media_id + 100,
                        file_reference=f"uploaded-photo-{media_id}".encode(),
                        date=datetime.now(UTC),
                        sizes=[],
                        dc_id=2,
                    )
                )
            return types.MessageMediaDocument(
                document=types.Document(
                    id=media_id,
                    access_hash=media_id + 100,
                    file_reference=f"uploaded-document-{media_id}".encode(),
                    date=datetime.now(UTC),
                    mime_type="video/mp4",
                    size=100,
                    dc_id=2,
                    attributes=[],
                )
            )
        return "updates"

    def download_media(self, message, *, file):
        self.download_calls.append((message, file))
        path = Path(f"{file}.bin")
        path.write_bytes(b"independent-upload")
        return str(path)

    def upload_file(self, path):
        self.upload_calls.append(path)
        return SimpleNamespace(name=Path(path).name)

    def _get_response_message(self, request, _updates, _target):
        count = len(request.multi_media) if hasattr(request, "multi_media") else 1
        return [SimpleNamespace(id=8100 + index) for index in range(count)]


def _photo_media(media_id: int, *, ttl_seconds: int | None = None):
    return types.MessageMediaPhoto(
        photo=types.Photo(
            id=media_id,
            access_hash=media_id + 100,
            file_reference=f"photo-{media_id}".encode(),
            date=datetime.now(UTC),
            sizes=[],
            dc_id=2,
        ),
        spoiler=True,
        ttl_seconds=ttl_seconds,
    )


def _document_media(media_id: int, *, ttl_seconds: int | None = None):
    return types.MessageMediaDocument(
        document=types.Document(
            id=media_id,
            access_hash=media_id + 100,
            file_reference=f"document-{media_id}".encode(),
            date=datetime.now(UTC),
            mime_type="video/mp4",
            size=100,
            dc_id=2,
            attributes=[],
        ),
        spoiler=False,
        ttl_seconds=ttl_seconds,
    )


def test_clone_independent_single_media_uses_durable_send_request():
    client = _DurableMediaClient()
    entity = types.MessageEntityBold(offset=0, length=7)

    result = clone_send_independent_media(
        client,
        "target",
        [
            SimpleNamespace(
                media=_photo_media(101, ttl_seconds=30),
                message="caption",
                entities=[entity],
            )
        ],
        random_ids=[901],
    )

    assert result.id == 8100
    assert isinstance(client.request, SendMediaRequest)
    assert client.request.peer == "input:target"
    assert client.request.random_id == 901
    assert client.request.message == "caption"
    assert client.request.entities == [entity]
    assert client.request.media.spoiler is True
    assert client.request.media.ttl_seconds is None
    assert client.request.silent is True
    assert isinstance(client.request.media, types.InputMediaUploadedPhoto)
    assert len(client.download_calls) == 1
    assert len(client.upload_calls) == 1
    assert all(not Path(path).exists() for path in client.upload_calls)


def test_clone_independent_album_preserves_order_captions_and_random_ids():
    client = _DurableMediaClient()
    first_entity = types.MessageEntityItalic(offset=0, length=5)

    result = clone_send_independent_media(
        client,
        "target",
        [
            SimpleNamespace(
                media=_photo_media(101, ttl_seconds=20),
                message="first",
                entities=[first_entity],
            ),
            SimpleNamespace(
                media=_document_media(202, ttl_seconds=20),
                message="second",
                entities=[],
            ),
        ],
        random_ids=[901, 902],
    )

    assert [message.id for message in result] == [8100, 8101]
    assert isinstance(client.request, SendMultiMediaRequest)
    assert [item.random_id for item in client.request.multi_media] == [901, 902]
    assert [item.message for item in client.request.multi_media] == ["first", "second"]
    assert client.request.multi_media[0].entities == [first_entity]
    assert client.request.multi_media[1].entities == []
    assert isinstance(client.request.multi_media[0].media, types.InputMediaPhoto)
    assert isinstance(client.request.multi_media[1].media, types.InputMediaDocument)
    assert all(
        item.media.ttl_seconds is None for item in client.request.multi_media
    )
    assert client.request.silent is True
    assert len(client.download_calls) == 2
    assert len(client.upload_calls) == 2
    assert sum(isinstance(request, UploadMediaRequest) for request in client.requests) == 2
    assert all(not Path(path).exists() for path in client.upload_calls)


def test_clone_independent_media_turns_duplicate_random_id_into_ambiguous_outcome():
    class RandomIdDuplicateError(RuntimeError):
        pass

    class _DuplicateClient(_DurableMediaClient):
        def __call__(self, request):
            self.request = request
            raise RandomIdDuplicateError("random ID was already used")

    with pytest.raises(CloneForwardOutcomeAmbiguousError, match="避免重复消息"):
        clone_send_independent_media(
            _DuplicateClient(),
            "target",
            [SimpleNamespace(media=_photo_media(101), message="", entities=[])],
            random_ids=[901],
        )


def test_clone_forward_messages_calls_are_centralized():
    offenders = []
    for path in (ROOT / "tg_harvest").rglob("*.py"):
        if path.name == "clone_forwarding.py":
            continue
        source = path.read_text(encoding="utf-8")
        if "forward_messages(" in source:
            offenders.append(str(path.relative_to(ROOT)))

    assert offenders == []
