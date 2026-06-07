import os
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from tg_harvest.search.params import SearchParams
from tg_harvest.search.params import MAX_SEARCH_PAGE
from tg_harvest.search.params import _parse_search_params
from tg_harvest.search.sql_builder import _build_search_query_spec
from tg_harvest.search.service import _search_payload_service
from tg_harvest.search.service import _build_payload_from_spec
from tg_harvest.search.service import _try_fast_count
from tg_harvest.search.cache import _format_data_version
from tg_harvest.search.cache import _make_count_cache_key
from tg_harvest.search.cache import _read_database_fingerprint


class SearchServiceFastCountTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE chats (
                chat_id INTEGER PRIMARY KEY,
                chat_title TEXT NOT NULL,
                message_count INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE messages (
                pk INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                msg_type TEXT NOT NULL
            )
            """
        )
        cur.executemany(
            "INSERT INTO chats(chat_id, chat_title, message_count) VALUES (?, ?, ?)",
            [
                (1, "A", 3),
                (2, "B", 2),
            ],
        )
        cur.executemany(
            "INSERT INTO messages(chat_id, msg_type) VALUES (?, ?)",
            [
                (1, "TEXT"),
                (1, "PHOTO"),
                (1, "VIDEO"),
                (2, "TEXT"),
                (2, "PHOTO"),
            ],
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()

    def test_fast_count_uses_chat_summary_for_all_scope(self) -> None:
        params = SearchParams(
            raw_query="",
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
        )
        result = _try_fast_count(self.conn, params, page_size=100, max_count=50000000)
        self.assertEqual((5, False, 1), result)

    def test_fast_count_uses_message_table_for_type_scope(self) -> None:
        params = SearchParams(
            raw_query="",
            search_type="image",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
        )
        result = _try_fast_count(self.conn, params, page_size=100, max_count=50000000)
        self.assertEqual((2, False, 1), result)

    def test_fast_count_disabled_when_text_query_exists(self) -> None:
        params = SearchParams(
            raw_query="hello",
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
        )
        result = _try_fast_count(self.conn, params, page_size=100, max_count=50000000)
        self.assertIsNone(result)

    def test_fast_count_disabled_when_duration_filter_exists(self) -> None:
        params = SearchParams(
            raw_query="",
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
            duration_sec=480,
        )
        result = _try_fast_count(self.conn, params, page_size=100, max_count=50000000)
        self.assertIsNone(result)

    def test_fast_count_disabled_when_date_filter_exists(self) -> None:
        params = SearchParams(
            raw_query="",
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
            start_ts=100,
        )
        result = _try_fast_count(self.conn, params, page_size=100, max_count=50000000)
        self.assertIsNone(result)

    def test_search_payload_service_schedules_async_maintenance_instead_of_draining_inline(
        self,
    ) -> None:
        params = SearchParams(
            raw_query="hello",
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
        )
        spec = {
            "count_sql": "SELECT 1 AS c",
            "query_sql": "SELECT 1",
            "query_sql_skip": None,
            "prefer_skip_query": False,
            "sql_params": [],
            "count_limit": 1,
            "match_query": "",
            "raw_query": "hello",
            "effective_sort": "time",
            "effective_order": "desc",
            "has_text_filter": True,
        }
        payload = {
            "ok": True,
            "query": "hello",
            "fts_query": "",
            "page": 1,
            "page_size": 100,
            "total": 1,
            "total_pages": 1,
            "total_is_capped": False,
            "effective_sort": "time",
            "effective_order": "desc",
            "items": [{"pk": 1}],
        }

        with patch(
            "tg_harvest.search.maintenance.schedule_message_search_maintenance"
        ) as schedule_mock, patch(
            "tg_harvest.storage.search_terms.drain_message_search_terms_rebuild_queue",
            side_effect=AssertionError("search path must not drain inline"),
        ), patch(
            "tg_harvest.search.service._build_search_query_spec",
            return_value=spec,
        ), patch(
            "tg_harvest.search.service._build_payload_from_spec",
            return_value=payload,
        ):
            result = _search_payload_service(
                self.conn,
                params,
                fts_enabled=False,
                from_sql="FROM messages m",
                page_size=100,
                max_count=50000000,
                map_search_items_fn=lambda rows: rows,
            )

        schedule_mock.assert_called_once_with()
        self.assertEqual(payload, result)

    def test_skip_count_empty_out_of_range_page_falls_back_to_precise_count(self) -> None:
        params = SearchParams(
            raw_query="",
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=4,
            chat_id=None,
            skip_count=True,
        )
        spec = {
            "count_sql": "SELECT COUNT(*) AS c FROM (SELECT 1 FROM messages LIMIT ?)",
            "query_sql": "SELECT pk FROM messages ORDER BY pk ASC LIMIT ? OFFSET ?",
            "query_sql_skip": "SELECT pk FROM messages ORDER BY pk ASC LIMIT ? OFFSET ?",
            "prefer_skip_query": True,
            "sql_params": [],
            "count_limit": 1000,
            "match_query": "",
            "raw_query": "",
            "effective_sort": "time",
            "effective_order": "asc",
            "has_text_filter": False,
        }

        payload = _build_payload_from_spec(
            self.conn,
            params,
            spec,
            page_size=2,
            max_count=50000000,
            map_search_items_fn=lambda rows: [dict(row) for row in rows],
        )

        self.assertEqual(3, payload["page"])
        self.assertEqual(5, payload["total"])
        self.assertEqual(3, payload["total_pages"])
        self.assertTrue(payload["data_version"])
        self.assertEqual([{"pk": 5}], payload["items"])

    def test_skip_count_empty_first_page_reports_zero_without_precise_count(self) -> None:
        params = SearchParams(
            raw_query="missing",
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
            skip_count=True,
        )
        spec = {
            "count_sql": "SELECT COUNT(*) AS c FROM (SELECT 1 FROM messages LIMIT ?)",
            "query_sql": "SELECT pk FROM messages WHERE 0 LIMIT ? OFFSET ?",
            "query_sql_skip": None,
            "prefer_skip_query": False,
            "sql_params": [],
            "count_limit": 1000,
            "match_query": "",
            "raw_query": "missing",
            "effective_sort": "time",
            "effective_order": "desc",
            "has_text_filter": True,
        }

        payload = _build_payload_from_spec(
            self.conn,
            params,
            spec,
            page_size=100,
            max_count=50000000,
            map_search_items_fn=lambda rows: [dict(row) for row in rows],
        )

        self.assertEqual(0, payload["total"])
        self.assertEqual(0, payload["total_pages"])
        self.assertEqual([], payload["items"])

    def test_count_only_ignores_skip_count_flag(self) -> None:
        params = SearchParams(
            raw_query="",
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
            skip_count=True,
            count_only=True,
        )
        spec = {
            "count_sql": "SELECT COUNT(*) AS c FROM (SELECT 1 FROM messages LIMIT ?)",
            "query_sql": "SELECT pk FROM messages ORDER BY pk ASC LIMIT ? OFFSET ?",
            "query_sql_skip": None,
            "prefer_skip_query": False,
            "sql_params": [],
            "count_limit": 1000,
            "match_query": "",
            "raw_query": "",
            "effective_sort": "time",
            "effective_order": "desc",
            "has_text_filter": False,
        }

        payload = _build_payload_from_spec(
            self.conn,
            params,
            spec,
            page_size=100,
            max_count=50000000,
            map_search_items_fn=lambda rows: [dict(row) for row in rows],
        )

        self.assertEqual(5, payload["total"])
        self.assertEqual(1, payload["total_pages"])
        self.assertEqual([], payload["items"])

    def test_count_only_payload_can_include_top_chat_facets(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        try:
            conn.execute(
                "CREATE TABLE chats(chat_id INTEGER PRIMARY KEY, chat_title TEXT NOT NULL)"
            )
            conn.execute(
                """
                CREATE TABLE messages(
                    pk INTEGER PRIMARY KEY,
                    chat_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    msg_date_ts INTEGER NOT NULL,
                    msg_type TEXT NOT NULL,
                    content TEXT,
                    content_norm TEXT
                )
                """
            )
            conn.executemany(
                "INSERT INTO chats(chat_id, chat_title) VALUES (?, ?)",
                [(1, "Alpha"), (2, "Beta")],
            )
            conn.executemany(
                "INSERT INTO messages(pk, chat_id, message_id, msg_date_ts, msg_type, content, content_norm) VALUES (?, ?, ?, ?, 'TEXT', ?, ?)",
                [
                    (1, 1, 10, 100, "hello", "hello"),
                    (2, 1, 11, 101, "hello again", "hello again"),
                    (3, 2, 20, 102, "hello", "hello"),
                ],
            )
            conn.commit()
            params = SearchParams(
                raw_query="hello",
                search_type="all",
                sort_by_req="time",
                order_req="desc",
                page=1,
                chat_id=None,
                count_only=True,
            )
            spec = _build_search_query_spec(
                params,
                from_sql="FROM messages m",
                fts_enabled=False,
                max_count=1000,
                force_like=True,
            )

            payload = _build_payload_from_spec(
                conn,
                params,
                spec,
                page_size=100,
                max_count=1000,
                map_search_items_fn=lambda rows: [dict(row) for row in rows],
                include_chat_facets=True,
            )

            self.assertEqual([], payload["items"])
            self.assertEqual(
                [
                    {"chat_id": 1, "chat_title": "Alpha", "count": 2},
                    {"chat_id": 2, "chat_title": "Beta", "count": 1},
                ],
                payload["chat_facets"],
            )
        finally:
            conn.close()

    def test_indexed_no_result_does_not_run_like_fallback_by_default(self) -> None:
        params = SearchParams(
            raw_query="missing",
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
            skip_count=True,
        )
        spec = {
            "count_sql": "SELECT 1 AS c",
            "query_sql": "SELECT 1 WHERE 0",
            "query_sql_skip": None,
            "prefer_skip_query": False,
            "sql_params": [],
            "count_limit": 1,
            "match_query": '"missing"',
            "raw_query": "missing",
            "effective_sort": "time",
            "effective_order": "desc",
            "has_text_filter": True,
            "uses_text_index": True,
            "uses_auxiliary_terms": False,
        }
        payload = {
            "ok": True,
            "query": "missing",
            "fts_query": '"missing"',
            "page": 1,
            "page_size": 100,
            "data_version": "v1",
            "total": 0,
            "total_pages": 0,
            "total_is_capped": False,
            "effective_sort": "time",
            "effective_order": "desc",
            "items": [],
        }

        with patch(
            "tg_harvest.search.maintenance.schedule_message_search_maintenance"
        ), patch(
            "tg_harvest.search.service._build_search_query_spec",
            return_value=spec,
        ) as build_mock, patch(
            "tg_harvest.search.service._build_payload_from_spec",
            return_value=payload,
        ) as payload_mock:
            result = _search_payload_service(
                self.conn,
                params,
                fts_enabled=True,
                from_sql="FROM messages m",
                page_size=100,
                max_count=50000000,
                map_search_items_fn=lambda rows: rows,
            )

        self.assertEqual(payload, result)
        self.assertEqual(1, build_mock.call_count)
        self.assertEqual(1, payload_mock.call_count)

    def test_auxiliary_index_no_result_can_fallback_when_rebuilds_are_pending(self) -> None:
        params = SearchParams(
            raw_query="福利",
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
            skip_count=True,
        )
        primary_spec = {
            "count_sql": "SELECT 1 AS c",
            "query_sql": "SELECT 1 WHERE 0",
            "query_sql_skip": None,
            "prefer_skip_query": False,
            "sql_params": ["福利", "%福利%"],
            "count_limit": 1,
            "match_query": "",
            "raw_query": "福利",
            "effective_sort": "time",
            "effective_order": "desc",
            "has_text_filter": True,
            "uses_text_index": True,
            "uses_auxiliary_terms": True,
        }
        fallback_spec = {
            **primary_spec,
            "uses_text_index": False,
            "uses_auxiliary_terms": False,
        }
        empty_payload = {
            "ok": True,
            "query": "福利",
            "fts_query": "",
            "page": 1,
            "page_size": 100,
            "data_version": "v1",
            "total": 0,
            "total_pages": 0,
            "total_is_capped": False,
            "effective_sort": "time",
            "effective_order": "desc",
            "items": [],
        }
        fallback_payload = {
            **empty_payload,
            "total": -1,
            "items": [{"pk": 1}],
        }

        with patch(
            "tg_harvest.search.maintenance.schedule_message_search_maintenance"
        ), patch(
            "tg_harvest.search.service._has_pending_message_search_term_rebuilds",
            return_value=True,
        ), patch(
            "tg_harvest.search.service._build_search_query_spec",
            side_effect=[primary_spec, fallback_spec],
        ) as build_mock, patch(
            "tg_harvest.search.service._build_payload_from_spec",
            side_effect=[empty_payload, fallback_payload],
        ) as payload_mock:
            result = _search_payload_service(
                self.conn,
                params,
                fts_enabled=True,
                from_sql="FROM messages m",
                page_size=100,
                max_count=50000000,
                map_search_items_fn=lambda rows: rows,
            )

        self.assertEqual(fallback_payload, result)
        self.assertEqual(2, build_mock.call_count)
        self.assertEqual(2, payload_mock.call_count)

    def test_auxiliary_index_partial_result_fallbacks_when_rebuilds_are_pending(self) -> None:
        params = SearchParams(
            raw_query="福",
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
            skip_count=True,
        )
        primary_spec = {
            "count_sql": "SELECT 1 AS c",
            "query_sql": "SELECT 1",
            "query_sql_skip": None,
            "prefer_skip_query": False,
            "sql_params": ["福", "%福%"],
            "count_limit": 1,
            "match_query": "",
            "raw_query": "福",
            "effective_sort": "time",
            "effective_order": "desc",
            "has_text_filter": True,
            "uses_text_index": True,
            "uses_auxiliary_terms": True,
        }
        fallback_spec = {
            **primary_spec,
            "uses_text_index": False,
            "uses_auxiliary_terms": False,
        }
        primary_payload = {
            "ok": True,
            "query": "福",
            "fts_query": "",
            "page": 1,
            "page_size": 100,
            "data_version": "v1",
            "total": -1,
            "total_pages": 0,
            "total_is_capped": False,
            "effective_sort": "time",
            "effective_order": "desc",
            "items": [{"pk": 1}],
        }
        fallback_payload = {
            **primary_payload,
            "items": [{"pk": 1}, {"pk": 2}],
        }

        with patch(
            "tg_harvest.search.maintenance.schedule_message_search_maintenance"
        ), patch(
            "tg_harvest.search.service._has_pending_message_search_term_rebuilds",
            return_value=True,
        ), patch(
            "tg_harvest.search.service._build_search_query_spec",
            side_effect=[primary_spec, fallback_spec],
        ), patch(
            "tg_harvest.search.service._build_payload_from_spec",
            side_effect=[primary_payload, fallback_payload],
        ):
            result = _search_payload_service(
                self.conn,
                params,
                fts_enabled=True,
                from_sql="FROM messages m",
                page_size=100,
                max_count=50000000,
                map_search_items_fn=lambda rows: rows,
            )

        self.assertEqual(fallback_payload, result)

    def test_count_cache_key_changes_after_database_file_write(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite3")
        os.close(fd)
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            conn.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY, value TEXT)")
            conn.commit()
            first_key = _make_count_cache_key(
                conn,
                count_sql="SELECT COUNT(*) AS c FROM sample LIMIT ?",
                sql_params=[],
                count_limit=1000,
                page_size=100,
            )
            conn.close()

            time.sleep(0.01)
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            conn.execute("INSERT INTO sample(value) VALUES ('changed')")
            conn.commit()
            conn.close()

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            second_key = _make_count_cache_key(
                conn,
                count_sql="SELECT COUNT(*) AS c FROM sample LIMIT ?",
                sql_params=[],
                count_limit=1000,
                page_size=100,
            )
            conn.close()

            self.assertNotEqual(first_key, second_key)
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_file_database_fingerprint_ignores_connection_local_data_version(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite3")
        os.close(fd)
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            conn.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY, value TEXT)")
            conn.commit()
            conn.close()

            with patch(
                "tg_harvest.search.cache._read_data_version",
                side_effect=[101, 202],
            ):
                first_conn = sqlite3.connect(db_path)
                first_conn.row_factory = sqlite3.Row
                first_version = _format_data_version(
                    _read_database_fingerprint(first_conn)
                )
                first_conn.close()

                second_conn = sqlite3.connect(db_path)
                second_conn.row_factory = sqlite3.Row
                second_version = _format_data_version(
                    _read_database_fingerprint(second_conn)
                )
                second_conn.close()

            self.assertEqual(first_version, second_version)
        finally:
            Path(db_path).unlink(missing_ok=True)

    def test_file_database_fingerprint_ignores_empty_wal_timestamp_churn(self) -> None:
        fd, db_path = tempfile.mkstemp(suffix=".sqlite3")
        os.close(fd)
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            conn.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY, value TEXT)")
            conn.commit()
            conn.close()

            versions = []
            for _ in range(3):
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                conn.execute("PRAGMA journal_mode=WAL")
                versions.append(
                    _format_data_version(_read_database_fingerprint(conn))
                )
                conn.close()

            self.assertEqual(1, len(set(versions)))
        finally:
            Path(db_path).unlink(missing_ok=True)
            Path(f"{db_path}-wal").unlink(missing_ok=True)
            Path(f"{db_path}-shm").unlink(missing_ok=True)


class SearchParamsParsingTests(unittest.TestCase):
    def test_parse_search_params_treats_false_strings_as_false(self) -> None:
        params = _parse_search_params(
            {
                "query": "hello",
                "skip_count": "false",
                "count_only": "0",
            }
        )

        self.assertFalse(params.skip_count)
        self.assertFalse(params.count_only)

    def test_parse_search_params_treats_true_strings_as_true(self) -> None:
        params = _parse_search_params(
            {
                "query": "hello",
                "skip_count": "true",
                "count_only": "1",
            }
        )

        self.assertTrue(params.skip_count)
        self.assertTrue(params.count_only)

    def test_parse_search_params_rejects_pathological_deep_pages(self) -> None:
        with self.assertRaisesRegex(ValueError, "page 不能超过"):
            _parse_search_params({"page": MAX_SEARCH_PAGE + 1})

    def test_parse_search_params_accepts_compact_and_separated_date_bounds(self) -> None:
        params = _parse_search_params(
            {
                "start_date": "20260101",
                "end_date": "2026-01-02",
            }
        )

        self.assertIsNotNone(params.start_ts)
        self.assertIsNotNone(params.end_ts_exclusive)
        assert params.start_ts is not None
        assert params.end_ts_exclusive is not None
        self.assertEqual(2 * 24 * 60 * 60, params.end_ts_exclusive - params.start_ts)

    def test_parse_search_params_ignores_blank_date_bounds(self) -> None:
        params = _parse_search_params({"start_date": " ", "end_date": ""})

        self.assertIsNone(params.start_ts)
        self.assertIsNone(params.end_ts_exclusive)

    def test_parse_search_params_rejects_invalid_date_bounds(self) -> None:
        with self.assertRaisesRegex(ValueError, "开始日期"):
            _parse_search_params({"start_date": "20261301"})

    def test_parse_search_params_rejects_reversed_date_bounds(self) -> None:
        with self.assertRaisesRegex(ValueError, "开始日期不能晚于结束日期"):
            _parse_search_params(
                {"start_date": "2026-01-03", "end_date": "2026-01-01"}
            )

    def test_parse_search_params_extracts_duration_filter_from_query(self) -> None:
        params = _parse_search_params({"query": "女孩+【00:08:00】"})

        self.assertEqual("女孩", params.text_query)
        self.assertEqual(8 * 60, params.duration_sec)
        self.assertEqual("女孩+【00:08:00】", params.raw_query)


if __name__ == "__main__":
    unittest.main()
