import unittest

from tg_harvest.search.expression import expr_to_debug_dict
from tg_harvest.search.expression import parse_query
from tg_harvest.search.expression import to_fts_match
from tg_harvest.search.params import SearchParams
from tg_harvest.search.sql_builder import _build_search_query_spec


class SearchSqlBuilderTests(unittest.TestCase):
    def test_parenthesized_boolean_expression_is_parsed(self) -> None:
        expr = parse_query('(foo/bar)+"baz qux"-spam')
        self.assertEqual(
            {
                "kind": "AND",
                "value": "",
                "left": {
                    "kind": "AND",
                    "value": "",
                    "left": {
                        "kind": "OR",
                        "value": "",
                        "left": {"kind": "TERM", "value": "foo", "left": None, "right": None},
                        "right": {"kind": "TERM", "value": "bar", "left": None, "right": None},
                    },
                    "right": {"kind": "PHRASE", "value": "baz qux", "left": None, "right": None},
                },
                "right": {
                    "kind": "NOT",
                    "value": "",
                    "left": {"kind": "TERM", "value": "spam", "left": None, "right": None},
                    "right": None,
                },
            },
            expr_to_debug_dict(expr),
        )

    def test_invalid_expression_raises_value_error(self) -> None:
        with self.assertRaises(ValueError):
            parse_query("foo+(bar")

    def test_query_selects_promo_flag(self) -> None:
        params = SearchParams(
            raw_query="hello",
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
        )

        spec = _build_search_query_spec(
            params,
            from_sql="FROM messages m LEFT JOIN chats c ON c.chat_id = m.chat_id LEFT JOIN message_media mm ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id",
            fts_enabled=False,
            max_count=1000,
        )

        self.assertIn("m.is_promo", spec["query_sql"])

    def test_raw_query_is_truncated_without_mutating_params(self) -> None:
        raw_query = "a" * 120
        params = SearchParams(
            raw_query=raw_query,
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
        )

        spec = _build_search_query_spec(
            params,
            from_sql="FROM messages m",
            fts_enabled=False,
            max_count=1000,
            force_like=True,
        )

        self.assertEqual(raw_query, params.raw_query)
        self.assertEqual(raw_query, spec["raw_query"])

    def test_candidate_fts_match_keeps_only_mandatory_terms(self) -> None:
        self.assertEqual("", to_fts_match("(a/b)+c"))
        self.assertEqual("", to_fts_match("a/-b"))
        self.assertEqual('"foo"', to_fts_match("foo+ab"))
        self.assertEqual("", to_fts_match("a+b"))

    def test_like_clause_uses_single_normalized_content_scan(self) -> None:
        params = SearchParams(
            raw_query="hello",
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
        )

        spec = _build_search_query_spec(
            params,
            from_sql="FROM messages m",
            fts_enabled=False,
            max_count=1000,
            force_like=True,
        )

        self.assertIn(
            "LOWER(COALESCE(NULLIF(m.content_norm, ''), m.content, '')) LIKE ?",
            spec["where_sql"],
        )
        self.assertNotIn("m.content LIKE ?", spec["where_sql"])

    def test_like_query_escapes_wildcard_characters(self) -> None:
        params = SearchParams(
            raw_query="100%_done",
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
        )

        spec = _build_search_query_spec(
            params,
            from_sql="FROM messages m",
            fts_enabled=False,
            max_count=1000,
            force_like=True,
        )

        self.assertIn("ESCAPE '\\'", spec["where_sql"])
        self.assertEqual(["%100\\%\\_done%"], spec["sql_params"])

    def test_and_query_like_params_match_term_count(self) -> None:
        params = SearchParams(
            raw_query="foo+bar",
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
        )

        spec = _build_search_query_spec(
            params,
            from_sql="FROM messages m",
            fts_enabled=False,
            max_count=1000,
            force_like=True,
        )

        self.assertEqual(["%foo%", "%bar%"], spec["sql_params"])

    def test_short_non_cjk_query_stays_on_like_path(self) -> None:
        params = SearchParams(
            raw_query="ab",
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
        )

        spec = _build_search_query_spec(
            params,
            from_sql="FROM messages m",
            fts_enabled=True,
            max_count=1000,
        )

        self.assertEqual("", spec["match_query"])
        self.assertFalse(spec["uses_text_index"])
        self.assertNotIn("messages_fts", spec["query_sql"])

    def test_boolean_query_uses_candidate_cte_when_all_terms_support_trigram(self) -> None:
        params = SearchParams(
            raw_query="onlyfans/magnet",
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
        )

        spec = _build_search_query_spec(
            params,
            from_sql="FROM messages m LEFT JOIN chats c ON c.chat_id = m.chat_id LEFT JOIN message_media mm ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id",
            fts_enabled=True,
            max_count=1000,
        )

        self.assertIn("WITH candidate_pks AS", spec["query_sql"])
        self.assertIn("JOIN candidate_pks cp ON cp.pk = m.pk", spec["query_sql"])
        self.assertNotIn("fts.messages_fts MATCH ?", spec["where_sql"])

    def test_not_query_uses_candidate_cte_when_term_supports_trigram(self) -> None:
        params = SearchParams(
            raw_query="onlyfans-magnet",
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
        )

        spec = _build_search_query_spec(
            params,
            from_sql="FROM messages m",
            fts_enabled=True,
            max_count=1000,
        )

        self.assertIn("EXCEPT", spec["query_sql"])
        self.assertIn("WITH candidate_pks AS", spec["query_sql"])

    def test_or_query_can_mix_cjk_bigram_and_trigram_candidates(self) -> None:
        params = SearchParams(
            raw_query="福利/onlyfans",
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
        )

        spec = _build_search_query_spec(
            params,
            from_sql="FROM messages m",
            fts_enabled=True,
            max_count=1000,
        )

        self.assertIn("WITH candidate_pks AS", spec["query_sql"])
        self.assertIn("FROM message_search_terms WHERE term = ?", spec["query_sql"])
        self.assertIn("FROM messages_fts WHERE messages_fts MATCH ?", spec["query_sql"])
        self.assertEqual("", spec["match_query"])

    def test_two_char_cjk_query_uses_auxiliary_term_index(self) -> None:
        params = SearchParams(
            raw_query="福利/会员",
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
        )

        spec = _build_search_query_spec(
            params,
            from_sql="FROM messages m",
            fts_enabled=True,
            max_count=1000,
        )

        self.assertIn("WITH candidate_pks AS", spec["query_sql"])
        self.assertIn("FROM message_search_terms WHERE term = ?", spec["query_sql"])
        self.assertTrue(spec["uses_text_index"])
        self.assertTrue(spec["uses_auxiliary_terms"])
        self.assertEqual(["福利", "会员", "%福利%", "%会员%"], spec["sql_params"])

    def test_one_char_cjk_query_uses_auxiliary_term_index(self) -> None:
        params = SearchParams(
            raw_query="福",
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
        )

        spec = _build_search_query_spec(
            params,
            from_sql="FROM messages m",
            fts_enabled=True,
            max_count=1000,
        )

        self.assertIn("WITH candidate_pks AS", spec["query_sql"])
        self.assertIn("FROM message_search_terms WHERE term = ?", spec["query_sql"])
        self.assertTrue(spec["uses_text_index"])
        self.assertTrue(spec["uses_auxiliary_terms"])
        self.assertEqual(["福", "%福%"], spec["sql_params"])

    def test_media_sort_count_sql_uses_same_media_join_as_rows_query(self) -> None:
        params = SearchParams(
            raw_query="",
            search_type="video",
            sort_by_req="size",
            order_req="desc",
            page=1,
            chat_id=None,
        )

        spec = _build_search_query_spec(
            params,
            from_sql=(
                "FROM messages m "
                "LEFT JOIN chats c ON c.chat_id = m.chat_id "
                "LEFT JOIN message_media mm ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id"
            ),
            fts_enabled=False,
            max_count=1000,
        )

        self.assertEqual("size", spec["effective_sort"])
        self.assertIn("JOIN message_media mm", spec["count_sql"])

    def test_all_type_rejects_media_sort_like_frontend_controls(self) -> None:
        params = SearchParams(
            raw_query="",
            search_type="all",
            sort_by_req="size",
            order_req="desc",
            page=1,
            chat_id=None,
        )

        spec = _build_search_query_spec(
            params,
            from_sql="FROM messages m",
            fts_enabled=False,
            max_count=1000,
        )

        self.assertEqual("time", spec["effective_sort"])

    def test_date_range_filters_use_message_timestamp_bounds(self) -> None:
        params = SearchParams(
            raw_query="hello",
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
            start_ts=100,
            end_ts_exclusive=200,
        )

        spec = _build_search_query_spec(
            params,
            from_sql="FROM messages m",
            fts_enabled=False,
            max_count=1000,
            force_like=True,
        )

        self.assertIn("m.msg_date_ts >= ?", spec["where_sql"])
        self.assertIn("m.msg_date_ts < ?", spec["where_sql"])
        self.assertEqual(["%hello%", 100, 200], spec["sql_params"])

    def test_chat_facet_sql_groups_by_chat_with_same_filters(self) -> None:
        params = SearchParams(
            raw_query="hello",
            search_type="all",
            sort_by_req="time",
            order_req="desc",
            page=1,
            chat_id=None,
            start_ts=100,
            end_ts_exclusive=200,
        )

        spec = _build_search_query_spec(
            params,
            from_sql="FROM messages m",
            fts_enabled=False,
            max_count=1000,
            force_like=True,
        )

        self.assertIn("GROUP BY m.chat_id", spec["chat_facet_sql"])
        self.assertIn("ORDER BY match_count DESC", spec["chat_facet_sql"])
        self.assertIn("m.msg_date_ts >= ?", spec["chat_facet_sql"])
        self.assertIn("m.msg_date_ts < ?", spec["chat_facet_sql"])


if __name__ == "__main__":
    unittest.main()
