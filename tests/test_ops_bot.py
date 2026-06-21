import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tg_harvest.ops_bot.client import (
    is_notify_enabled,
    mask_bot_token,
    send_message_sync,
    trim_message,
)
from tg_harvest.ops_bot.notify import (
    maybe_notify_admin_job_log,
    notify_admin_job_status,
    should_notify_log_message,
)


class OpsBotClientTests(unittest.TestCase):
    def test_notification_requires_enabled_token_and_chat(self) -> None:
        cfg = SimpleNamespace(
            ops_bot_enabled=0,
            ops_bot_token="123:abc",
            ops_bot_notify_chat_id="42",
        )

        self.assertFalse(is_notify_enabled(cfg))
        cfg.ops_bot_enabled = 1
        self.assertTrue(is_notify_enabled(cfg))
        cfg.ops_bot_token = ""
        self.assertFalse(is_notify_enabled(cfg))

    def test_token_mask_keeps_only_edges(self) -> None:
        self.assertEqual("1234...wxyz", mask_bot_token("1234567890wxyz"))
        self.assertEqual("***", mask_bot_token("short"))

    def test_long_messages_are_trimmed_to_telegram_limit(self) -> None:
        message = trim_message("x" * 5000)

        self.assertLessEqual(len(message), 4096)
        self.assertTrue(message.endswith("[truncated]"))

    def test_send_message_uses_bot_api_without_logging_token(self) -> None:
        cfg = SimpleNamespace(
            ops_bot_token="123:secret-token",
            ops_bot_notify_chat_id="-1001",
            ops_bot_timeout_seconds=1,
        )

        with patch(
            "tg_harvest.ops_bot.client._post_bot_api",
            return_value={"ok": True},
        ) as post_mock:
            sent = send_message_sync(cfg, "hello")

        self.assertTrue(sent)
        kwargs = post_mock.call_args.kwargs
        self.assertEqual("123:secret-token", kwargs["token"])
        self.assertEqual("sendMessage", kwargs["method"])
        self.assertEqual("-1001", kwargs["payload"]["chat_id"])
        self.assertEqual("hello", kwargs["payload"]["text"])


class OpsBotNotificationTests(unittest.TestCase):
    def test_log_filter_only_keeps_operationally_important_lines(self) -> None:
        self.assertTrue(should_notify_log_message("主账号触发长等待 wait=70s"))
        self.assertTrue(should_notify_log_message("FloodWaitError retry later"))
        self.assertFalse(should_notify_log_message("正在抓取第 1000 条"))

    def test_status_notification_formats_snapshot(self) -> None:
        cfg = SimpleNamespace(
            ops_bot_enabled=1,
            ops_bot_token="123:abc",
            ops_bot_notify_chat_id="42",
        )
        snapshot = {
            "job_type": "update",
            "target_label": "all",
            "progress": {"current": 5, "total": 10, "stage": "updating"},
        }

        with patch(
            "tg_harvest.ops_bot.notify.enqueue_message",
            return_value=True,
        ) as enqueue_mock:
            sent = notify_admin_job_status("job-1", "done", snapshot, cfg=cfg)

        self.assertTrue(sent)
        text = enqueue_mock.call_args.args[1]
        self.assertIn("后台任务完成", text)
        self.assertIn("群组更新", text)
        self.assertIn("ID: job-1", text)
        self.assertIn("目标: all", text)

    def test_delete_empty_chats_status_uses_human_label(self) -> None:
        cfg = SimpleNamespace(
            ops_bot_enabled=1,
            ops_bot_token="123:abc",
            ops_bot_notify_chat_id="42",
        )
        snapshot = {
            "job_type": "delete_empty_chats",
            "target_label": "零消息群组",
            "progress": {"current": 1, "total": 1, "stage": "done"},
        }

        with patch(
            "tg_harvest.ops_bot.notify.enqueue_message",
            return_value=True,
        ) as enqueue_mock:
            sent = notify_admin_job_status("job-2", "done", snapshot, cfg=cfg)

        self.assertTrue(sent)
        text = enqueue_mock.call_args.args[1]
        self.assertIn("删除空群组", text)
        self.assertNotIn("delete_empty_chats", text)

    def test_clone_structure_status_uses_human_label(self) -> None:
        cfg = SimpleNamespace(
            ops_bot_enabled=1,
            ops_bot_token="123:abc",
            ops_bot_notify_chat_id="42",
        )
        snapshot = {
            "job_type": "clone_structure",
            "target_label": "Source 副本",
            "progress": {"current": 1, "total": 4, "stage": "creating"},
        }

        with patch(
            "tg_harvest.ops_bot.notify.enqueue_message",
            return_value=True,
        ) as enqueue_mock:
            sent = notify_admin_job_status("job-clone", "running", snapshot, cfg=cfg)

        self.assertTrue(sent)
        text = enqueue_mock.call_args.args[1]
        self.assertIn("结构克隆", text)
        self.assertNotIn("clone_structure", text)

    def test_clone_deep_preflight_status_uses_human_label(self) -> None:
        cfg = SimpleNamespace(
            ops_bot_enabled=1,
            ops_bot_token="123:abc",
            ops_bot_notify_chat_id="42",
        )
        snapshot = {
            "job_type": "clone_deep_preflight",
            "target_label": "Source -> Source 副本",
            "progress": {"current": 1, "total": 5, "stage": "checking_accounts"},
        }

        with patch(
            "tg_harvest.ops_bot.notify.enqueue_message",
            return_value=True,
        ) as enqueue_mock:
            sent = notify_admin_job_status("job-clone-deep", "running", snapshot, cfg=cfg)

        self.assertTrue(sent)
        text = enqueue_mock.call_args.args[1]
        self.assertIn("克隆深度预检", text)
        self.assertNotIn("clone_deep_preflight", text)

    def test_clone_timeline_migration_status_uses_human_label(self) -> None:
        cfg = SimpleNamespace(
            ops_bot_enabled=1,
            ops_bot_token="123:abc",
            ops_bot_notify_chat_id="42",
        )
        snapshot = {
            "job_type": "clone_timeline_migration",
            "target_label": "Source -> Source 副本",
            "progress": {"current": 10, "total": 100, "stage": "replaying_timeline"},
        }

        with patch(
            "tg_harvest.ops_bot.notify.enqueue_message",
            return_value=True,
        ) as enqueue_mock:
            sent = notify_admin_job_status(
                "job-clone-timeline",
                "running",
                snapshot,
                cfg=cfg,
            )

        self.assertTrue(sent)
        text = enqueue_mock.call_args.args[1]
        self.assertIn("克隆完整时间线迁移", text)
        self.assertNotIn("clone_timeline_migration", text)

    def test_unimportant_log_is_not_enqueued(self) -> None:
        cfg = SimpleNamespace(
            ops_bot_enabled=1,
            ops_bot_token="123:abc",
            ops_bot_notify_chat_id="42",
        )

        with patch("tg_harvest.ops_bot.notify.enqueue_message") as enqueue_mock:
            sent = maybe_notify_admin_job_log("job-1", "普通进度日志", cfg=cfg)

        self.assertFalse(sent)
        enqueue_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
