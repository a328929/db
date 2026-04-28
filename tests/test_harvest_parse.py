import unittest
from datetime import datetime
from types import SimpleNamespace

from tg_harvest.ingest.parse import MessageParseError
from tg_harvest.ingest.parse import MessageParser
from tg_harvest.ingest.parse import resolve_target_entities


class _FakeDialog:
    def __init__(self, title, entity):
        self.title = title
        self.entity = entity


class _FakeClient:
    def __init__(self, dialogs=None, entities=None):
        self._dialogs = list(dialogs or [])
        self._entities = dict(entities or {})
        self.get_entity_calls = []

    def get_dialogs(self):
        return list(self._dialogs)

    def get_entity(self, key):
        self.get_entity_calls.append(key)
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


class MessageParserTests(unittest.TestCase):
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
