import unittest

from tg_harvest.domain.normalize import normalize_text_light_for_storage


class NormalizeStorageTests(unittest.TestCase):
    def test_equal_light_normalized_text_is_not_stored_twice(self) -> None:
        self.assertEqual("", normalize_text_light_for_storage("福利姬"))

    def test_changed_light_normalized_text_is_kept_for_search(self) -> None:
        self.assertEqual("abc", normalize_text_light_for_storage("ABC"))


if __name__ == "__main__":
    unittest.main()
