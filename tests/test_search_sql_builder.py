import unittest

from tg_harvest.search.expression import expr_to_debug_dict, parse_query, to_fts_match
from tg_harvest.search.params import SearchParams
from tg_harvest.search.sql_builder import _build_search_query_spec


class SearchSqlBuilderTests(unittest.TestCase):
    def test_slash_groups_bind_before_required_terms(self) -> None:
        expr = parse_query("女同/百合/拉拉/女女+足交")
        self.assertEqual(
            {
                "kind": "AND",
                "value": "",
                "left": {
                    "kind": "OR",
                    "value": "",
                    "left": {
                        "kind": "OR",
                        "value": "",
                        "left": {
                            "kind": "OR",
                            "value": "",
                            "left": {
                                "kind": "TERM",
                                "value": "女同",
                                "left": None,
                                "right": None,
                            },
                            "right": {
                                "kind": "TERM",
                                "value": "百合",
                                "left": None,
                                "right": None,
                            },
                        },
                        "right": {
                            "kind": "TERM",
                            "value": "拉拉",
                            "left": None,
                            "right": None,
                        },
                    },
                    "right": {
                        "kind": "TERM",
                        "value": "女女",
                        "left": None,
                        "right": None,
                    },
                },
                "right": {
                    "kind": "TERM",
                    "value": "足交",
                    "left": None,
                    "right": None,
                },
            },
            expr_to_debug_dict(expr),
        )

    def test_slash_group_query_requires_terms_after_plus_in_sql(self) -> None:
        params = SearchParams(
            raw_query="女同/百合/拉拉/女女+足交",
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

        self.assertIn(" OR ", spec["where_sql"])
        self.assertIn(" AND ", spec["where_sql"])
        self.assertEqual(
            ["%女同%", "%百合%", "%拉拉%", "%女女%", "%足交%"],
            spec["sql_params"],
        )

    def test_full_width_boolean_symbols_are_supported(self) -> None:
        expr = parse_query("女同／百合＋足交－广告")

        self.assertEqual(
            parse_query("女同/百合+足交-广告"),
            expr,
        )

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

    def test_duration_token_is_extracted_into_media_filter(self) -> None:
        params = SearchParams(
            raw_query="女孩+【00:08:00】",
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

        self.assertIn("m.msg_type IN ('VIDEO', 'GIF', 'VIDEO_NOTE')", spec["where_sql"])
        self.assertIn("mm.duration_sec = ?", spec["where_sql"])
        self.assertEqual(["%女孩%", 480], spec["sql_params"])
        self.assertIn("LEFT JOIN message_media mm", spec["query_sql"])

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
            max_count=50000000,
            force_like=True,
        )

        self.assertIn(
            "LOWER(COALESCE(NULLIF(m.content_norm, ''), m.content, '')) LIKE ?",
            spec["where_sql"],
        )
        self.assertNotIn("m.content LIKE ?", spec["where_sql"])
        self.assertEqual(50000001, spec["count_limit"])
        self.assertEqual(50000001, spec["chat_facet_scan_limit"])

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

    def test_not_query_uses_only_positive_candidate_with_like(self) -> None:
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

        self.assertNotIn("EXCEPT", spec["query_sql"])
        self.assertIn("WITH candidate_pks AS", spec["query_sql"])
        self.assertIn("LIKE", spec["where_sql"])

    def test_negated_two_char_cjk_query_keeps_like_without_auxiliary_exclusion(
        self,
    ) -> None:
        params = SearchParams(
            raw_query="-福利",
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

        self.assertNotIn("message_search_terms", spec["query_sql"])
        self.assertIn("LIKE", spec["where_sql"])
        self.assertEqual(["%福利%"], spec["sql_params"])

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
        self.assertIn("LIKE", spec["where_sql"])
        self.assertEqual(
            ["福利", "会员", "%福利%", "%会员%"],
            spec["sql_params"],
        )

    def test_two_char_cjk_candidate_keeps_like_for_whitespace_semantics(self) -> None:
        params = SearchParams(
            raw_query="福利",
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

        self.assertIn("FROM message_search_terms WHERE term = ?", spec["query_sql"])
        self.assertNotIn("EXCEPT", spec["query_sql"])
        self.assertIn("LIKE", spec["where_sql"])
        self.assertEqual(["福利", "%福利%"], spec["sql_params"])

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
        self.assertNotIn("LIKE", spec["where_sql"])
        self.assertEqual(["福"], spec["sql_params"])
        self.assertIn("EXISTS (SELECT 1 FROM candidate_pks cp WHERE cp.pk = m.pk)", spec["query_sql_skip"])

    def test_mixed_cjk_and_trigram_query_keeps_like_verification(self) -> None:
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

        self.assertIn("LIKE", spec["where_sql"])
        self.assertEqual(["福利", '"onlyfans"', "%福利%", "%onlyfans%"], spec["sql_params"])

    def test_auxiliary_candidate_keeps_like_for_unsupported_and_term(self) -> None:
        params = SearchParams(
            raw_query="福+ab",
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

        self.assertIn("FROM message_search_terms WHERE term = ?", spec["query_sql"])
        self.assertIn("LIKE", spec["where_sql"])
        self.assertEqual(["福", "%福%", "%ab%"], spec["sql_params"])

    def test_auxiliary_candidate_keeps_like_for_partially_compiled_boolean_tree(
        self,
    ) -> None:
        params = SearchParams(
            raw_query="福+(-利/姬)",
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

        self.assertIn("FROM message_search_terms WHERE term = ?", spec["query_sql"])
        self.assertNotIn("EXCEPT", spec["query_sql"])
        self.assertIn("LIKE", spec["where_sql"])
        self.assertEqual(["福", "%福%", "%利%", "%姬%"], spec["sql_params"])

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
        self.assertIn(
            "ORDER BY mm.file_size DESC, mm.chat_id DESC, mm.message_id DESC",
            spec["query_sql"],
        )

    def test_time_sort_uses_index_aligned_stable_tiebreakers(self) -> None:
        params = SearchParams(
            raw_query="",
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
        )

        self.assertIn(
            "ORDER BY m.msg_date_ts DESC, m.message_id DESC, m.pk DESC",
            spec["query_sql"],
        )
        self.assertEqual(1001, spec["count_limit"])

    def test_text_search_count_limit_uses_max_count(self) -> None:
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

        self.assertEqual(1001, spec["count_limit"])

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

    def test_image_type_rejects_duration_sort_like_frontend_controls(self) -> None:
        params = SearchParams(
            raw_query="",
            search_type="image",
            sort_by_req="duration",
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
        self.assertIn(
            "ORDER BY m.msg_date_ts DESC, m.message_id DESC, m.pk DESC",
            spec["query_sql"],
        )

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
        self.assertIn("FROM (\n", spec["chat_facet_sql"])
        self.assertEqual(2, spec["chat_facet_sql"].count("LIMIT ?"))
        inner_sql = spec["chat_facet_sql"].split(") m", 1)[0]
        self.assertNotIn("ORDER BY", inner_sql)


if __name__ == "__main__":
    unittest.main()
