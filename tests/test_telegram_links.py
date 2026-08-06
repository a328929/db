import unittest

from tg_harvest.domain.chat_ids import (
    candidate_chat_entity_ids,
    stored_chat_id_from_entity_id,
)
from tg_harvest.web.telegram_links import (
    build_telegram_app_link,
    build_telegram_chat_app_link,
    build_telegram_chat_link_bundle,
    build_telegram_chat_web_link,
    build_telegram_fallback_app_link,
    build_telegram_link_bundle,
    build_telegram_web_link,
)


class TelegramLinkBuilderTests(unittest.TestCase):
    def test_build_public_links_prefers_username(self) -> None:
        self.assertEqual(
            build_telegram_app_link(
                chat_id=123456, message_id=321, chat_username="@public_channel"
            ),
            "tg://resolve?domain=public_channel&post=321",
        )
        self.assertEqual(
            build_telegram_web_link(
                chat_id=123456, message_id=321, chat_username="@public_channel"
            ),
            "https://t.me/public_channel/321",
        )

    def test_public_channel_message_links_prefer_username_for_precise_post(self) -> None:
        self.assertEqual(
            build_telegram_app_link(
                chat_id=-1002202633364,
                message_id=321,
                chat_username="@possibly_stale",
                chat_type="Channel",
            ),
            "tg://resolve?domain=possibly_stale&post=321",
        )
        self.assertEqual(
            build_telegram_web_link(
                chat_id=-1002202633364,
                message_id=321,
                chat_username="@possibly_stale",
                chat_type="Channel",
            ),
            "https://t.me/possibly_stale/321",
        )

    def test_public_channel_message_links_expose_privatepost_fallback(self) -> None:
        self.assertEqual(
            build_telegram_fallback_app_link(
                chat_id=-1002202633364,
                message_id=321,
                chat_username="@public_channel",
                chat_type="Channel",
            ),
            "tg://privatepost?channel=2202633364&post=321",
        )

    def test_grouped_media_links_include_single_flag(self) -> None:
        self.assertEqual(
            build_telegram_app_link(
                chat_id=-1002202633364,
                message_id=321,
                chat_username="@public_channel",
                chat_type="Channel",
                single_message=True,
            ),
            "tg://resolve?domain=public_channel&post=321&single",
        )
        self.assertEqual(
            build_telegram_fallback_app_link(
                chat_id=-1002202633364,
                message_id=321,
                chat_username="@public_channel",
                chat_type="Channel",
                single_message=True,
            ),
            "tg://privatepost?channel=2202633364&post=321&single",
        )
        self.assertEqual(
            build_telegram_web_link(
                chat_id=-1002202633364,
                message_id=321,
                chat_username="@public_channel",
                chat_type="Channel",
                single_message=True,
            ),
            "https://t.me/public_channel/321?single",
        )

    def test_grouped_private_channel_links_include_single_flag(self) -> None:
        self.assertEqual(
            build_telegram_app_link(
                chat_id=-1002202633364,
                message_id=321,
                chat_type="Channel",
                single_message=True,
            ),
            "tg://privatepost?channel=2202633364&post=321&single",
        )
        self.assertEqual(
            build_telegram_web_link(
                chat_id=-1002202633364,
                message_id=321,
                chat_type="Channel",
                single_message=True,
            ),
            "https://t.me/c/2202633364/321?single",
        )

    def test_private_channel_message_links_have_no_duplicate_fallback(self) -> None:
        self.assertEqual(
            build_telegram_app_link(
                chat_id=-1002202633364,
                message_id=321,
                chat_type="Channel",
            ),
            "tg://privatepost?channel=2202633364&post=321",
        )
        self.assertEqual(
            build_telegram_fallback_app_link(
                chat_id=-1002202633364,
                message_id=321,
                chat_type="Channel",
            ),
            "",
        )

    def test_basic_chat_message_links_use_openmessage_not_channel_privatepost(self) -> None:
        self.assertEqual(
            build_telegram_app_link(
                chat_id=123456,
                message_id=321,
                chat_type="Chat",
            ),
            "tg://openmessage?chat_id=123456&message_id=321",
        )
        self.assertEqual(
            build_telegram_web_link(
                chat_id=123456,
                message_id=321,
                chat_type="Chat",
            ),
            "",
        )

    def test_build_private_links_use_chat_id(self) -> None:
        self.assertEqual(
            build_telegram_app_link(chat_id=2202633364, message_id=99),
            "tg://privatepost?channel=2202633364&post=99",
        )
        self.assertEqual(
            build_telegram_web_link(chat_id=2202633364, message_id=99),
            "https://t.me/c/2202633364/99",
        )

    def test_build_private_links_normalize_signed_channel_id(self) -> None:
        self.assertEqual(
            build_telegram_app_link(chat_id=-1002202633364, message_id=99),
            "tg://privatepost?channel=2202633364&post=99",
        )
        self.assertEqual(
            build_telegram_web_link(chat_id=-1002202633364, message_id=99),
            "https://t.me/c/2202633364/99",
        )

    def test_build_private_links_do_not_strip_positive_ids(self) -> None:
        self.assertEqual(
            build_telegram_app_link(chat_id=100123, message_id=99),
            "tg://privatepost?channel=100123&post=99",
        )
        self.assertEqual(
            build_telegram_web_link(chat_id=100123, message_id=99),
            "https://t.me/c/100123/99",
        )

    def test_build_link_bundle_exposes_station_open_link(self) -> None:
        bundle = build_telegram_link_bundle(chat_id=42, message_id=7)

        self.assertEqual(bundle.open_link, "/open/telegram?chat_id=42&message_id=7")
        self.assertEqual(bundle.fallback_app_link, "")

    def test_open_link_preserves_signed_chat_id_for_database_lookup(self) -> None:
        bundle = build_telegram_link_bundle(chat_id=-1002202633364, message_id=7)

        self.assertEqual(
            bundle.open_link,
            "/open/telegram?chat_id=-1002202633364&message_id=7",
        )

    def test_build_chat_home_links_prefer_username(self) -> None:
        self.assertEqual(
            build_telegram_chat_app_link(
                chat_id=123456,
                chat_username="@public_channel",
            ),
            "tg://resolve?domain=public_channel",
        )
        self.assertEqual(
            build_telegram_chat_web_link(chat_username="@public_channel"),
            "https://t.me/public_channel",
        )

    def test_build_private_chat_home_app_link_uses_chat_id(self) -> None:
        bundle = build_telegram_chat_link_bundle(chat_id=123456)

        self.assertEqual(bundle.app_link, "tg://openmessage?chat_id=123456")
        self.assertEqual(bundle.web_link, "")


class ChatIdCandidateTests(unittest.TestCase):
    def test_stored_chat_id_only_strips_negative_channel_marker(self) -> None:
        self.assertEqual(stored_chat_id_from_entity_id(-100123), 123)
        self.assertEqual(stored_chat_id_from_entity_id(100123), 100123)
        self.assertEqual(stored_chat_id_from_entity_id(-123), 123)

    def test_positive_chat_id_candidates_include_channel_peer_forms(self) -> None:
        self.assertEqual(candidate_chat_entity_ids(123), [123, -100123, -123])

    def test_signed_channel_chat_id_candidates_strip_telegram_prefix(self) -> None:
        self.assertEqual(
            candidate_chat_entity_ids(-100123),
            [-100123, 123, -123, 100123],
        )

    def test_negative_non_channel_chat_id_candidates_do_not_build_invalid_ids(self) -> None:
        candidates = candidate_chat_entity_ids(-123)

        self.assertEqual(candidates, [-123, -100123, 123])
        self.assertNotIn("--", [str(candidate) for candidate in candidates])


if __name__ == "__main__":
    unittest.main()
