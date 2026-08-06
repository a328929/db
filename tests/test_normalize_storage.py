import unittest

from tg_harvest.domain.normalize import (
    _filter_and_denoise_tokens,
    normalize_text_light_for_storage,
)


class NormalizeStorageTests(unittest.TestCase):
    def test_equal_light_normalized_text_is_not_stored_twice(self) -> None:
        self.assertEqual(normalize_text_light_for_storage("福利姬"), "")

    def test_changed_light_normalized_text_is_kept_for_search(self) -> None:
        self.assertEqual(normalize_text_light_for_storage("ABC"), "abc")

    def test_filter_discards_symbol_only_tokens_and_keeps_word_characters(self) -> None:
        self.assertEqual(_filter_and_denoise_tokens("... hello !!!"), "hello")


if __name__ == "__main__":
    unittest.main()
