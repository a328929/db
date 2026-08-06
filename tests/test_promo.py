import unittest

from tg_harvest.domain.promo import _get_hit_stats


class PromoSignalTests(unittest.TestCase):
    def test_obfuscated_contact_pattern_accepts_separators(self) -> None:
        self.assertEqual(_get_hit_stats("t . m . e")["obf_raw"], 1)
        self.assertEqual(_get_hit_stats("v _ x")["obf_raw"], 1)

    def test_empty_text_has_all_zero_promo_hits(self) -> None:
        self.assertTrue(all(value == 0 for value in _get_hit_stats("").values()))


if __name__ == "__main__":
    unittest.main()
