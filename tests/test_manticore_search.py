import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tg_harvest.search.browse_service import sqlite_browse_payload_service
from tg_harvest.search.expression import parse_query
from tg_harvest.search.manticore_client import ManticoreClient, ManticoreError
from tg_harvest.search.manticore_service import (
    compile_manticore_match,
    manticore_search_payload_service,
)
from tg_harvest.search.manticore_sync import (
    drain_manticore_outbox,
    validate_manticore_state,
)
from tg_harvest.search.params import SearchParams
from tg_harvest.storage.connection import detect_sqlite_features
from tg_harvest.storage.manticore_outbox import (
    OUTBOX_TABLE,
    get_manticore_index_status,
    get_manticore_meta,
    manticore_index_is_ready,
    set_manticore_index_status,
)
from tg_harvest.storage.schema import create_schema


class _Response:
    def __init__(self, payload):
        self.payload = json.dumps(payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def read(self):
        return self.payload


class _FakeManticoreClient(ManticoreClient):
    def __init__(self, responses=None):
        super().__init__(table="tg_messages")
        object.__setattr__(self, "responses", list(responses or []))
        object.__setattr__(self, "sql", [])
        object.__setattr__(self, "operations", [])

    def execute_select(self, sql):
        self.sql.append(sql)
        return self.responses.pop(0)

    def bulk(self, operations):
        self.operations.extend(operations)


class ManticoreClientTests(unittest.TestCase):
    def test_bulk_uses_ndjson_and_bearer_auth(self):
        client = ManticoreClient(bearer_token="secret")
        operation = client.replace_operation(7, {"content": "中文"})
        with patch(
            "urllib.request.urlopen",
            return_value=_Response({"errors": False}),
        ) as urlopen:
            client.bulk([operation])

        request = urlopen.call_args.args[0]
        self.assertEqual("Bearer secret", request.headers["Authorization"])
        self.assertEqual("/bulk", request.full_url.removeprefix(client.base_url))
        self.assertEqual(operation, json.loads(request.data.decode().strip()))

    def test_bulk_error_is_not_accepted_as_success(self):
        client = ManticoreClient()
        with patch(
            "urllib.request.urlopen",
            return_value=_Response({"errors": True, "error": "bad", "current_line": 2}),
        ), self.assertRaisesRegex(ManticoreError, "bad at line 2"):
            client.bulk([client.delete_operation(1)])

    def test_table_name_is_validated(self):
        with self.assertRaises(ValueError):
            ManticoreClient(table="messages; DROP TABLE messages")


class ManticoreExpressionTests(unittest.TestCase):
    def test_boolean_expression_is_compiled_to_manticore_syntax(self):
        compiled = compile_manticore_match(parse_query("福利/会员+onlyfans-广告"))
        self.assertEqual(
            '((("福利" | "会员") *onlyfans*) !("广告"))',
            compiled,
        )

    def test_phrase_and_user_wildcards_are_escaped(self):
        self.assertEqual(
            '"foo bar"', compile_manticore_match(parse_query('"foo bar"'))
        )
        self.assertEqual(
            "*100\\%_done*",
            compile_manticore_match(parse_query("100%_done")),
        )


class ManticoreOutboxTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        create_schema(
            self.conn,
            detect_sqlite_features(self.conn),
        )
        self.conn.execute("INSERT INTO chats(chat_id, chat_title) VALUES (1, 'Chat')")
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def _insert_message(self):
        self.conn.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts,
                content, content_norm, msg_type
            ) VALUES (1, 10, 'date', 100, 'Raw', 'normalized', 'TEXT')
            """
        )
        self.conn.commit()
        return int(self.conn.execute("SELECT pk FROM messages").fetchone()[0])

    def test_message_and_media_changes_collapse_to_one_revisioned_item(self):
        pk = self._insert_message()
        self.conn.execute(
            """
            INSERT INTO message_media(chat_id, message_id, file_size, duration_sec)
            VALUES (1, 10, 123, 9)
            """
        )
        self.conn.execute(
            "UPDATE messages SET content_norm = 'changed' WHERE pk = ?", (pk,)
        )
        self.conn.commit()

        row = self.conn.execute(
            f"SELECT * FROM {OUTBOX_TABLE} WHERE pk = ?", (pk,)
        ).fetchone()
        self.assertEqual("upsert", row["operation"])
        self.assertEqual(3, row["revision"])

    def test_index_status_requires_an_explicit_completed_rebuild(self):
        self.assertFalse(manticore_index_is_ready(self.conn, "tg_messages"))
        set_manticore_index_status(self.conn, "tg_messages", "building")
        self.assertEqual(
            "building", get_manticore_index_status(self.conn, "tg_messages")
        )
        set_manticore_index_status(self.conn, "tg_messages", "ready")
        self.assertTrue(manticore_index_is_ready(self.conn, "tg_messages"))

    def test_delete_overwrites_pending_upsert(self):
        pk = self._insert_message()
        self.conn.execute("DELETE FROM messages WHERE pk = ?", (pk,))
        self.conn.commit()
        row = self.conn.execute(
            f"SELECT operation, revision FROM {OUTBOX_TABLE} WHERE pk = ?", (pk,)
        ).fetchone()
        self.assertEqual(("delete", 2), tuple(row))

    def test_drain_writes_current_document_and_acknowledges_revision(self):
        pk = self._insert_message()
        self.conn.execute(
            """
            INSERT INTO message_media(chat_id, message_id, file_size, duration_sec)
            VALUES (1, 10, 123, 9)
            """
        )
        self.conn.commit()
        client = _FakeManticoreClient()

        self.assertEqual(1, drain_manticore_outbox(self.conn, client))
        self.assertEqual(0, self.conn.execute(f"SELECT COUNT(*) FROM {OUTBOX_TABLE}").fetchone()[0])
        operation = client.operations[0]["replace"]
        self.assertEqual(pk, operation["id"])
        self.assertEqual("normalized", operation["doc"]["content"])
        self.assertEqual(123, operation["doc"]["file_size"])

    def test_drain_does_not_ack_a_newer_revision(self):
        pk = self._insert_message()
        conn = self.conn

        class ConcurrentUpdateClient(_FakeManticoreClient):
            def bulk(self, operations):
                super().bulk(operations)
                conn.execute(
                    "UPDATE messages SET content_norm = 'newer' WHERE pk = ?", (pk,)
                )
                conn.commit()

        self.assertEqual(
            1, drain_manticore_outbox(self.conn, ConcurrentUpdateClient())
        )
        row = self.conn.execute(
            f"SELECT operation, revision FROM {OUTBOX_TABLE} WHERE pk = ?", (pk,)
        ).fetchone()
        self.assertEqual(("upsert", 2), tuple(row))

    def test_empty_content_is_kept_as_a_filterable_document(self):
        self.conn.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts,
                content, content_norm, msg_type
            ) VALUES (1, 11, 'date', 101, '', '', 'VIDEO')
            """
        )
        self.conn.commit()
        client = _FakeManticoreClient()

        self.assertEqual(1, drain_manticore_outbox(self.conn, client))
        self.assertEqual("", client.operations[0]["replace"]["doc"]["content"])

    def test_schema_removes_legacy_sqlite_search_objects(self):
        self.conn.executescript(
            """
            DROP TRIGGER trg_manticore_messages_insert;
            CREATE TABLE message_search_terms(pk INTEGER, term TEXT);
            CREATE TABLE message_search_terms_rebuild_queue(pk INTEGER PRIMARY KEY);
            CREATE TABLE message_search_terms_meta(key TEXT PRIMARY KEY, value TEXT);
            CREATE TABLE messages_fts(content TEXT);
            CREATE TRIGGER trg_message_terms_queue_insert
            AFTER INSERT ON messages BEGIN SELECT 1; END;
            """
        )
        create_schema(self.conn, detect_sqlite_features(self.conn))

        names = {
            row[0]
            for row in self.conn.execute(
                """
                SELECT name FROM sqlite_master
                WHERE name IN (
                    'message_search_terms', 'message_search_terms_rebuild_queue',
                    'message_search_terms_meta', 'messages_fts',
                    'trg_message_terms_queue_insert'
                )
                """
            )
        }
        self.assertEqual(set(), names)


class ManticoreValidationTests(unittest.TestCase):
    class Client:
        table = "tg_messages"

        def __init__(self, count):
            self.count = count

        def ensure_table(self):
            return None

        def document_count(self):
            return self.count

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "validation.db"
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        create_schema(conn, detect_sqlite_features(conn))
        conn.execute("INSERT INTO chats(chat_id, chat_title) VALUES (1, 'Chat')")
        conn.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type
            ) VALUES (1, 1, 'date', 1, 'TEXT')
            """
        )
        conn.execute(f"DELETE FROM {OUTBOX_TABLE}")
        conn.commit()
        conn.close()

    def tearDown(self):
        self.temp_dir.cleanup()

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def test_matching_counts_restore_ready_status(self):
        self.assertTrue(validate_manticore_state(self._connect, self.Client(1)))
        conn = self._connect()
        try:
            self.assertTrue(manticore_index_is_ready(conn, "tg_messages"))
            self.assertEqual(
                "1",
                get_manticore_meta(
                    conn, "manticore:tg_messages:manticore_document_count"
                ),
            )
        finally:
            conn.close()

    def test_missing_documents_invalidate_ready_status(self):
        conn = self._connect()
        set_manticore_index_status(conn, "tg_messages", "ready")
        conn.close()

        self.assertFalse(validate_manticore_state(self._connect, self.Client(0)))
        conn = self._connect()
        try:
            self.assertFalse(manticore_index_is_ready(conn, "tg_messages"))
            self.assertEqual(
                "0",
                get_manticore_meta(
                    conn, "manticore:tg_messages:manticore_document_count"
                ),
            )
        finally:
            conn.close()


class ManticoreServiceTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        create_schema(
            self.conn,
            detect_sqlite_features(self.conn),
        )
        self.conn.execute("INSERT INTO chats(chat_id, chat_title) VALUES (1, 'Chat')")
        self.conn.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts,
                content, content_norm, msg_type
            ) VALUES (1, 10, 'date', 100, 'Visible', 'visible', 'TEXT')
            """
        )
        self.conn.commit()
        self.pk = int(self.conn.execute("SELECT pk FROM messages").fetchone()[0])

    def tearDown(self):
        self.conn.close()

    def _params(self, **overrides):
        values = {
            "raw_query": "福利",
            "search_type": "all",
            "sort_by_req": "time",
            "order_req": "desc",
            "page": 1,
            "chat_id": None,
            "skip_count": True,
        }
        values.update(overrides)
        return SearchParams(**values)

    def test_search_hydrates_manticore_ids_from_sqlite_in_hit_order(self):
        client = _FakeManticoreClient(
            [
                {
                    "hits": {
                        "total": 1,
                        "total_relation": "eq",
                        "hits": [{"_id": self.pk, "_source": {}}],
                    }
                }
            ]
        )
        payload = manticore_search_payload_service(
            self.conn,
            self._params(),
            client=client,
            page_size=100,
            max_count=50000000,
            max_matches=1000000,
            map_search_items_fn=lambda rows: rows,
        )

        self.assertEqual("manticore", payload["search_backend"])
        self.assertEqual("Visible", payload["items"][0]["content"])
        self.assertIn('MATCH(\'"福利"\')', client.sql[0])
        self.assertIn("ORDER BY msg_date_ts DESC", client.sql[0])

    def test_page_query_reuses_manticore_total_when_count_is_deferred(self):
        client = _FakeManticoreClient(
            [
                {
                    "hits": {
                        "total": 12,
                        "total_relation": "eq",
                        "hits": [{"_id": self.pk, "_source": {}}],
                    }
                }
            ]
        )
        payload = manticore_search_payload_service(
            self.conn,
            self._params(skip_count=True),
            client=client,
            page_size=100,
            max_count=50000000,
            max_matches=1000000,
            map_search_items_fn=lambda rows: rows,
        )

        self.assertEqual(12, payload["total"])
        self.assertEqual(1, len(client.sql))

    def test_count_only_returns_total_without_expensive_group_facets(self):
        client = _FakeManticoreClient(
            [
                {"hits": {"total": 4, "total_relation": "eq", "hits": []}},
            ]
        )
        payload = manticore_search_payload_service(
            self.conn,
            self._params(skip_count=False, count_only=True),
            client=client,
            page_size=100,
            max_count=50000000,
            max_matches=1000000,
            map_search_items_fn=lambda rows: rows,
        )
        self.assertEqual(4, payload["total"])
        self.assertEqual([], payload["chat_facets"])

    def test_relevance_sort_uses_manticore_weight_for_each_message_type(self):
        expected_type_codes = {
            "text": 1,
            "image": 2,
            "video": 3,
            "audio": 4,
        }
        for search_type, type_code in expected_type_codes.items():
            with self.subTest(search_type=search_type):
                client = _FakeManticoreClient(
                    [{"hits": {"total": 0, "total_relation": "eq", "hits": []}}]
                )
                payload = manticore_search_payload_service(
                    self.conn,
                    self._params(
                        search_type=search_type,
                        sort_by_req="relevance",
                    ),
                    client=client,
                    page_size=100,
                    max_count=50000000,
                    max_matches=1000000,
                    map_search_items_fn=lambda rows: rows,
                )

                self.assertEqual("relevance", payload["effective_sort"])
                self.assertIn("ORDER BY WEIGHT() DESC", client.sql[0])
                self.assertIn(f"type_code = {type_code}", client.sql[0])

    def test_relevance_sort_respects_ascending_order(self):
        client = _FakeManticoreClient(
            [{"hits": {"total": 0, "total_relation": "eq", "hits": []}}]
        )
        payload = manticore_search_payload_service(
            self.conn,
            self._params(sort_by_req="relevance", order_req="asc"),
            client=client,
            page_size=100,
            max_count=50000000,
            max_matches=1000000,
            map_search_items_fn=lambda rows: rows,
        )

        self.assertEqual("asc", payload["effective_order"])
        self.assertIn("ORDER BY WEIGHT() ASC", client.sql[0])

    def test_non_relevance_page_sort_does_not_compute_bm25_scores(self):
        client = _FakeManticoreClient(
            [{"hits": {"total": 0, "total_relation": "eq", "hits": []}}]
        )
        manticore_search_payload_service(
            self.conn,
            self._params(sort_by_req="time"),
            client=client,
            page_size=100,
            max_count=50000000,
            max_matches=1000000,
            map_search_items_fn=lambda rows: rows,
        )

        self.assertIn("OPTION ranker=none", client.sql[0])

    def test_empty_query_browses_with_sqlite_ordering_indexes(self):
        payload = sqlite_browse_payload_service(
            self.conn,
            self._params(raw_query="", text_query=""),
            page_size=100,
            max_count=50000000,
            map_search_items_fn=lambda rows: [dict(row) for row in rows],
        )

        self.assertEqual("sqlite_browse", payload["search_backend"])
        self.assertEqual([self.pk], [row["pk"] for row in payload["items"]])

    def test_empty_query_relevance_sort_falls_back_to_time(self):
        payload = sqlite_browse_payload_service(
            self.conn,
            self._params(raw_query="", text_query="", sort_by_req="relevance"),
            page_size=100,
            max_count=50000000,
            map_search_items_fn=lambda rows: [dict(row) for row in rows],
        )

        self.assertEqual("time", payload["effective_sort"])

    def test_empty_query_count_uses_the_browse_path(self):
        payload = sqlite_browse_payload_service(
            self.conn,
            self._params(raw_query="", text_query="", count_only=True),
            page_size=100,
            max_count=50000000,
            map_search_items_fn=lambda rows: rows,
        )

        self.assertEqual(1, payload["total"])
        self.assertEqual([], payload["chat_facets"])


if __name__ == "__main__":
    unittest.main()
