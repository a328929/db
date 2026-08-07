import unittest

from tg_harvest.search.params import (
    _cleanup_query_after_duration_removal,
    parse_bool_flag,
    split_query_media_duration,
)


class SearchParamsTests(unittest.TestCase):
    def test_boolean_flags_share_explicit_string_and_number_semantics(self) -> None:
        truthy_values = (True, 1, -1, "1", " true ", "YES", "on")
        falsey_values = (False, 0, None, "", "0", " false ", "NO", "off", "invalid")

        for value in truthy_values:
            with self.subTest(value=value):
                self.assertTrue(parse_bool_flag(value))
        for value in falsey_values:
            with self.subTest(value=value):
                self.assertFalse(parse_bool_flag(value))

    def test_cleanup_removes_orphaned_duration_operators(self) -> None:
        self.assertEqual(
            _cleanup_query_after_duration_removal(" + / - keyword - / + "),
            "keyword",
        )
        self.assertEqual(
            _cleanup_query_after_duration_removal("(  + / -   )"),
            "",
        )
        self.assertEqual(_cleanup_query_after_duration_removal("()"), "")

    def test_duration_token_is_removed_without_changing_remaining_query(self) -> None:
        self.assertEqual(
            split_query_media_duration("news + 【01:02】 - updates"),
            ("news updates", 62),
        )

    def test_cleanup_preserves_operators_between_query_terms(self) -> None:
        self.assertEqual(
            _cleanup_query_after_duration_removal("alpha++beta"),
            "alpha++beta",
        )


if __name__ == "__main__":
    unittest.main()
