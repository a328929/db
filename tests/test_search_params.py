import unittest

from tg_harvest.search.params import (
    _cleanup_query_after_duration_removal,
    split_query_media_duration,
)


class SearchParamsTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
