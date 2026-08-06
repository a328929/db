import unittest
from types import SimpleNamespace

from tg_harvest.domain.promo import (
    _get_hit_stats,
    _media_group_dedupe_hash,
    build_group_promo_features,
)


class PromoSignalTests(unittest.TestCase):
    def test_obfuscated_contact_pattern_accepts_separators(self) -> None:
        self.assertEqual(_get_hit_stats("t . m . e")["obf_raw"], 1)
        self.assertEqual(_get_hit_stats("v _ x")["obf_raw"], 1)

    def test_empty_text_has_all_zero_promo_hits(self) -> None:
        self.assertTrue(all(value == 0 for value in _get_hit_stats("").values()))

    def test_media_group_dedupe_hash_prefers_text_then_media_signature(self) -> None:
        self.assertEqual(_media_group_dedupe_hash("text-hash", "media-hash"), "text-hash")
        self.assertEqual(_media_group_dedupe_hash("", "media-hash"), "gm:media-hash")
        self.assertEqual(_media_group_dedupe_hash("", ""), "")

    def test_group_features_reuse_the_media_group_dedupe_hash(self) -> None:
        cfg = SimpleNamespace(
            disable_promo_filter=0,
            promo_score_threshold=1000,
            media_caption_guard_len=58,
        )

        features = build_group_promo_features("ordinary caption", 1, "media-hash", cfg)

        self.assertEqual(features["dedupe_hash"], features["pure_hash"])


if __name__ == "__main__":
    unittest.main()
