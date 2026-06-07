import unittest

from tg_harvest.domain.chat_ids import candidate_chat_entity_ids
from tg_harvest.web.telegram_links import build_telegram_app_link
from tg_harvest.web.telegram_links import build_telegram_chat_app_link
from tg_harvest.web.telegram_links import build_telegram_chat_link_bundle
from tg_harvest.web.telegram_links import build_telegram_chat_web_link
from tg_harvest.web.telegram_links import build_telegram_fallback_app_link
from tg_harvest.web.telegram_links import build_telegram_link_bundle
from tg_harvest.web.telegram_links import build_telegram_web_link


class TelegramLinkBuilderTests(unittest.TestCase):
    def test_build_public_links_prefers_username(self) -> None:
        self.assertEqual(
            "tg://resolve?domain=public_channel&post=321",
            build_telegram_app_link(
                chat_id=123456, message_id=321, chat_username="@public_channel"
            ),
        )
        self.assertEqual(
            "https://t.me/public_channel/321",
            build_telegram_web_link(
                chat_id=123456, message_id=321, chat_username="@public_channel"
            ),
        )

    def test_public_channel_message_links_prefer_username_for_precise_post(self) -> None:
        self.assertEqual(
            "tg://resolve?domain=possibly_stale&post=321",
            build_telegram_app_link(
                chat_id=-1002202633364,
                message_id=321,
                chat_username="@possibly_stale",
                chat_type="Channel",
            ),
        )
        self.assertEqual(
            "https://t.me/possibly_stale/321",
            build_telegram_web_link(
                chat_id=-1002202633364,
                message_id=321,
                chat_username="@possibly_stale",
                chat_type="Channel",
            ),
        )

    def test_public_channel_message_links_expose_privatepost_fallback(self) -> None:
        self.assertEqual(
            "tg://privatepost?channel=2202633364&post=321",
            build_telegram_fallback_app_link(
                chat_id=-1002202633364,
                message_id=321,
                chat_username="@public_channel",
                chat_type="Channel",
            ),
        )

    def test_grouped_media_links_include_single_flag(self) -> None:
        self.assertEqual(
            "tg://resolve?domain=public_channel&post=321&single",
            build_telegram_app_link(
                chat_id=-1002202633364,
                message_id=321,
                chat_username="@public_channel",
                chat_type="Channel",
                single_message=True,
            ),
        )
        self.assertEqual(
            "tg://privatepost?channel=2202633364&post=321&single",
            build_telegram_fallback_app_link(
                chat_id=-1002202633364,
                message_id=321,
                chat_username="@public_channel",
                chat_type="Channel",
                single_message=True,
            ),
        )
        self.assertEqual(
            "https://t.me/public_channel/321?single",
            build_telegram_web_link(
                chat_id=-1002202633364,
                message_id=321,
                chat_username="@public_channel",
                chat_type="Channel",
                single_message=True,
            ),
        )

    def test_grouped_private_channel_links_include_single_flag(self) -> None:
        self.assertEqual(
            "tg://privatepost?channel=2202633364&post=321&single",
            build_telegram_app_link(
                chat_id=-1002202633364,
                message_id=321,
                chat_type="Channel",
                single_message=True,
            ),
        )
        self.assertEqual(
            "https://t.me/c/2202633364/321?single",
            build_telegram_web_link(
                chat_id=-1002202633364,
                message_id=321,
                chat_type="Channel",
                single_message=True,
            ),
        )

    def test_private_channel_message_links_have_no_duplicate_fallback(self) -> None:
        self.assertEqual(
            "tg://privatepost?channel=2202633364&post=321",
            build_telegram_app_link(
                chat_id=-1002202633364,
                message_id=321,
                chat_type="Channel",
            ),
        )
        self.assertEqual(
            "",
            build_telegram_fallback_app_link(
                chat_id=-1002202633364,
                message_id=321,
                chat_type="Channel",
            ),
        )

    def test_basic_chat_message_links_use_openmessage_not_channel_privatepost(self) -> None:
        self.assertEqual(
            "tg://openmessage?chat_id=123456&message_id=321",
            build_telegram_app_link(
                chat_id=123456,
                message_id=321,
                chat_type="Chat",
            ),
        )
        self.assertEqual(
            "",
            build_telegram_web_link(
                chat_id=123456,
                message_id=321,
                chat_type="Chat",
            ),
        )

    def test_build_private_links_use_chat_id(self) -> None:
        self.assertEqual(
            "tg://privatepost?channel=2202633364&post=99",
            build_telegram_app_link(chat_id=2202633364, message_id=99),
        )
        self.assertEqual(
            "https://t.me/c/2202633364/99",
            build_telegram_web_link(chat_id=2202633364, message_id=99),
        )

    def test_build_private_links_normalize_signed_channel_id(self) -> None:
        self.assertEqual(
            "tg://privatepost?channel=2202633364&post=99",
            build_telegram_app_link(chat_id=-1002202633364, message_id=99),
        )
        self.assertEqual(
            "https://t.me/c/2202633364/99",
            build_telegram_web_link(chat_id=-1002202633364, message_id=99),
        )

    def test_build_private_links_do_not_strip_positive_ids(self) -> None:
        self.assertEqual(
            "tg://privatepost?channel=100123&post=99",
            build_telegram_app_link(chat_id=100123, message_id=99),
        )
        self.assertEqual(
            "https://t.me/c/100123/99",
            build_telegram_web_link(chat_id=100123, message_id=99),
        )

    def test_build_link_bundle_exposes_station_open_link(self) -> None:
        bundle = build_telegram_link_bundle(chat_id=42, message_id=7)

        self.assertEqual("/open/telegram?chat_id=42&message_id=7", bundle.open_link)
        self.assertEqual("", bundle.fallback_app_link)

    def test_open_link_preserves_signed_chat_id_for_database_lookup(self) -> None:
        bundle = build_telegram_link_bundle(chat_id=-1002202633364, message_id=7)

        self.assertEqual(
            "/open/telegram?chat_id=-1002202633364&message_id=7",
            bundle.open_link,
        )

    def test_build_chat_home_links_prefer_username(self) -> None:
        self.assertEqual(
            "tg://resolve?domain=public_channel",
            build_telegram_chat_app_link(
                chat_id=123456,
                chat_username="@public_channel",
            ),
        )
        self.assertEqual(
            "https://t.me/public_channel",
            build_telegram_chat_web_link(chat_username="@public_channel"),
        )

    def test_build_private_chat_home_app_link_uses_chat_id(self) -> None:
        bundle = build_telegram_chat_link_bundle(chat_id=123456)

        self.assertEqual("tg://openmessage?chat_id=123456", bundle.app_link)
        self.assertEqual("", bundle.web_link)


class ChatIdCandidateTests(unittest.TestCase):
    def test_positive_chat_id_candidates_include_channel_peer_forms(self) -> None:
        self.assertEqual([123, -100123, -123], candidate_chat_entity_ids(123))

    def test_signed_channel_chat_id_candidates_strip_telegram_prefix(self) -> None:
        self.assertEqual(
            [-100123, 123, -123, 100123],
            candidate_chat_entity_ids(-100123),
        )

    def test_negative_non_channel_chat_id_candidates_do_not_build_invalid_ids(self) -> None:
        candidates = candidate_chat_entity_ids(-123)

        self.assertEqual([-123, -100123, 123], candidates)
        self.assertNotIn("--", [str(candidate) for candidate in candidates])


if __name__ == "__main__":
    unittest.main()
