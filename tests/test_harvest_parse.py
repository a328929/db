import unittest
from datetime import datetime
from types import SimpleNamespace

from tg_harvest.ingest.parse import (
    MessageParseError,
    MessageParser,
    resolve_target_entities,
)


class _FakeDialog:
    def __init__(self, title, entity):
        self.title = title
        self.entity = entity


class _FakeClient:
    def __init__(self, dialogs=None, entities=None, dialogs_exc=None, entity_exc=None):
        self._dialogs = list(dialogs or [])
        self._entities = dict(entities or {})
        self._dialogs_exc = dialogs_exc
        self._entity_exc = entity_exc
        self.get_entity_calls = []

    def get_dialogs(self):
        if self._dialogs_exc is not None:
            raise self._dialogs_exc
        return list(self._dialogs)

    def get_entity(self, key):
        self.get_entity_calls.append(key)
        if self._entity_exc is not None:
            raise self._entity_exc
        return self._entities.get(key)


class ResolveTargetEntitiesTests(unittest.TestCase):
    def test_numeric_title_prefers_dialog_title_match(self) -> None:
        target_entity = object()
        client = _FakeClient(dialogs=[_FakeDialog("666", target_entity)])

        entities = resolve_target_entities(client, "666")

        self.assertEqual([target_entity], entities)
        self.assertEqual([], client.get_entity_calls)

    def test_numeric_target_falls_back_to_entity_lookup_when_title_missing(self) -> None:
        target_entity = object()
        client = _FakeClient(entities={666: target_entity})

        entities = resolve_target_entities(client, "666")

        self.assertEqual([target_entity], entities)
        self.assertEqual([666], client.get_entity_calls)

    def test_username_target_uses_explicit_entity_lookup(self) -> None:
        target_entity = object()
        client = _FakeClient(entities={"channel_name": target_entity})

        entities = resolve_target_entities(client, "@channel_name")

        self.assertEqual([target_entity], entities)
        self.assertEqual(["channel_name"], client.get_entity_calls)

    def test_explicit_target_returns_empty_for_entity_lookup_miss(self) -> None:
        client = _FakeClient(
            entity_exc=ValueError("Could not find the input entity for PeerUser")
        )

        entities = resolve_target_entities(client, "@missing_channel")

        self.assertEqual([], entities)

    def test_explicit_target_propagates_unexpected_lookup_failure(self) -> None:
        client = _FakeClient(entity_exc=RuntimeError("network down"))

        with self.assertRaisesRegex(RuntimeError, "network down"):
            resolve_target_entities(client, "@channel_name")

    def test_title_target_propagates_dialog_listing_failure(self) -> None:
        client = _FakeClient(dialogs_exc=RuntimeError("session database locked"))

        with self.assertRaisesRegex(RuntimeError, "session database locked"):
            resolve_target_entities(client, "Some Channel")


class MessageParserTests(unittest.TestCase):
    def test_parse_video_meta_falls_back_to_raw_document_attributes(self) -> None:
        message = SimpleNamespace(
            id=124,
            date=datetime(2024, 1, 1, 0, 0, 0),
            sender_id=1,
            raw_text="",
            message="",
            text="",
            grouped_id=None,
            sticker=None,
            gif=None,
            voice=None,
            video_note=None,
            audio=None,
            video=object(),
            photo=None,
            document=SimpleNamespace(
                id=987654321,
                mime_type="video/mp4",
                size=123456789,
                attributes=[
                    SimpleNamespace(file_name="clip.mp4"),
                    SimpleNamespace(duration=61, w=1920, h=1080),
                ],
            ),
            poll=None,
            contact=None,
            geo=None,
            file=None,
        )

        parsed = MessageParser.parse(message)

        self.assertIsNotNone(parsed)
        self.assertEqual("VIDEO", parsed.msg_type)
        self.assertEqual("clip.mp4", parsed.content)
        self.assertEqual("987654321", parsed.media_meta["file_unique_id"])
        self.assertEqual("clip.mp4", parsed.media_meta["file_name"])
        self.assertEqual(".mp4", parsed.media_meta["file_ext"])
        self.assertEqual("video/mp4", parsed.media_meta["mime_type"])
        self.assertEqual(123456789, parsed.media_meta["file_size"])
        self.assertEqual(61, parsed.media_meta["duration_sec"])
        self.assertEqual(1920, parsed.media_meta["width"])
        self.assertEqual(1080, parsed.media_meta["height"])

    def test_parse_raises_on_unexpected_media_metadata_error(self) -> None:
        message = SimpleNamespace(
            id=123,
            date=datetime(2024, 1, 1, 0, 0, 0),
            sender_id=1,
            raw_text="",
            message="",
            text="",
            grouped_id=None,
            sticker=None,
            gif=None,
            voice=None,
            video_note=None,
            audio=None,
            video=None,
            photo=object(),
            document=None,
            poll=None,
            contact=None,
            geo=None,
            file=SimpleNamespace(
                id="fid-1",
                name="demo.jpg",
                ext=".jpg",
                mime_type="image/jpeg",
                size="bad-size",
                width=100,
                height=100,
                duration=None,
                title=None,
                performer=None,
                emoji=None,
            ),
        )

        with self.assertRaises(MessageParseError) as captured:
            MessageParser.parse(message)

        self.assertEqual(123, captured.exception.message_id)
        self.assertIsInstance(captured.exception.cause, ValueError)


if __name__ == "__main__":
    unittest.main()
