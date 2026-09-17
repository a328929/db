import asyncio
import sqlite3
from types import SimpleNamespace

import pytest

from tg_harvest.ingest.parse import ParsedMessage
from tg_harvest.ingest.runner import _prepare_db_rows
from tg_harvest.ingest.store import batch_upsert
from tg_harvest.ingest.telemetry import count_telegram_requests
from tg_harvest.storage.connection import detect_sqlite_features
from tg_harvest.storage.schema import create_schema


def test_rpc_counter_counts_batches_and_errors_and_restores_client():
    async def call(sender, request, **kwargs):
        if request == "error":
            raise TimeoutError("test")
        return request

    client = SimpleNamespace(_call=call)
    with count_telegram_requests(client) as counts:
        assert asyncio.run(client._call(None, [1, 2])) == [1, 2]
        with pytest.raises(TimeoutError):
            asyncio.run(client._call(None, "error"))
    assert counts.requests == 3
    assert client._call is call


def test_rpc_counter_does_not_leave_a_bound_method_on_the_client():
    class Client:
        async def _call(self, sender, request):
            return request

    client = Client()
    with count_telegram_requests(client) as counts:
        assert asyncio.run(client._call(None, "request")) == "request"
    assert counts.requests == 1
    assert "_call" not in vars(client)


def test_ingest_distinguishes_inserts_updates_and_unchanged_tail_rescans():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        create_schema(conn, detect_sqlite_features(conn))
        conn.execute("INSERT INTO chats(chat_id,chat_title) VALUES (1,'Example')")
        conn.commit()
        parsed = ParsedMessage(
            msg_id=1,
            date_text="date",
            date_ts=1,
            sender_id=1,
            msg_type="TEXT",
            content="first",
            has_media=False,
            grouped_id=None,
        )
        row, _ = _prepare_db_rows(SimpleNamespace(id=1), 1, parsed)
        first = batch_upsert(conn, [row], [])
        unchanged = batch_upsert(conn, [row], [])
        parsed.content = "edited"
        row, _ = _prepare_db_rows(SimpleNamespace(id=1), 1, parsed)
        updated = batch_upsert(conn, [row], [])
        assert (first.persisted_change_count, first.new_message_count) == (1, 1)
        assert (unchanged.persisted_change_count, unchanged.new_message_count) == (0, 0)
        assert (updated.persisted_change_count, updated.new_message_count) == (1, 0)
    finally:
        conn.close()
