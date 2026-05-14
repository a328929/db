import sqlite3
import unittest
from types import SimpleNamespace

from tg_harvest.admin_jobs.common import resolve_chat_entity
from tg_harvest.ingest.runner import collect_target_entities


class _EntityClient:
    def __init__(self) -> None:
        self.calls = []

    def get_entity(self, value):
        self.calls.append(value)
        if isinstance(value, list):
            raise RuntimeError("bulk lookup unavailable")
        if int(value) == -100123:
            return SimpleNamespace(id=123, title="chat-123")
        raise RuntimeError(f"unknown entity {value}")


class _LookupMissClient:
    def __init__(self, results=None) -> None:
        self.calls = []
        self.results = dict(results or {})

    def get_entity(self, value):
        self.calls.append(value)
        if value in self.results:
            return self.results[value]
        raise ValueError("Could not find the input entity")


class CollectTargetEntitiesTests(unittest.TestCase):
    def test_existing_chat_ids_fallback_to_channel_peer_id(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            conn.execute(
                "CREATE TABLE chats(chat_id INTEGER PRIMARY KEY, last_seen_at TEXT NOT NULL)"
            )
            conn.execute(
                "INSERT INTO chats(chat_id, last_seen_at) VALUES (123, '2026-01-01')"
            )
            conn.commit()

            client = _EntityClient()
            entities = collect_target_entities(
                conn,
                client,
                SimpleNamespace(scan_existing_chats=1, target_group=""),
            )
        finally:
            conn.close()

        self.assertEqual([123], [int(entity.id) for entity in entities])
        self.assertIn(-100123, client.calls)

    def test_existing_negative_channel_ids_fallback_to_public_channel_id(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            conn.execute(
                "CREATE TABLE chats(chat_id INTEGER PRIMARY KEY, last_seen_at TEXT NOT NULL)"
            )
            conn.execute(
                "INSERT INTO chats(chat_id, last_seen_at) VALUES (-100123, '2026-01-01')"
            )
            conn.commit()

            client = _LookupMissClient({123: SimpleNamespace(id=123, title="chat-123")})
            entities = collect_target_entities(
                conn,
                client,
                SimpleNamespace(scan_existing_chats=1, target_group=""),
            )
        finally:
            conn.close()

        self.assertEqual([123], [int(entity.id) for entity in entities])
        self.assertIn(123, client.calls)

    def test_resolve_chat_entity_falls_back_to_username_after_negative_id_misses(self) -> None:
        entity = SimpleNamespace(id=777, title="by-username")
        client = _LookupMissClient({"public_name": entity})

        resolved = resolve_chat_entity(client, -100123, chat_username="public_name")

        self.assertIs(entity, resolved)
        self.assertIn("public_name", client.calls)


if __name__ == "__main__":
    unittest.main()
