import unittest

from tg_harvest.web.telegram_links import build_telegram_app_link
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

    def test_build_private_links_use_chat_id(self) -> None:
        self.assertEqual(
            "tg://privatepost?channel=2202633364&post=99",
            build_telegram_app_link(chat_id=2202633364, message_id=99),
        )
        self.assertEqual(
            "https://t.me/c/2202633364/99",
            build_telegram_web_link(chat_id=2202633364, message_id=99),
        )

    def test_build_link_bundle_exposes_station_open_link(self) -> None:
        bundle = build_telegram_link_bundle(chat_id=42, message_id=7)

        self.assertEqual("/open/telegram?chat_id=42&message_id=7", bundle.open_link)


if __name__ == "__main__":
    unittest.main()
