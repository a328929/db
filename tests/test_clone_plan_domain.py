import unittest

from tg_harvest.domain.clone_plan import clone_plan_timeline_readiness


class ClonePlanDomainTests(unittest.TestCase):
    def test_timeline_readiness_requires_plan(self) -> None:
        readiness = clone_plan_timeline_readiness(
            None,
            preview={
                "timeline_remaining": 2,
                "text_remaining": 1,
                "media_remaining": 1,
            },
        )

        self.assertFalse(readiness["can_migrate_timeline"])
        self.assertEqual(["plan_missing"], readiness["reason_codes"])

    def test_timeline_readiness_blocks_when_relay_is_incomplete(self) -> None:
        plan = {
            "status": "done",
            "target_access": "ok",
            "source_access": "ok",
            "text_strategy": "database_replay",
            "media_strategy": "relay_copy_without_attribution",
            "migration_account": "unavailable",
            "blocking_issues": [],
            "capabilities": {
                "target_write_account": "secondary",
                "source_snapshot": {"message_id": 100},
                "media_relay": {
                    "enabled": True,
                    "chat_id": 999,
                    "source_account": "primary",
                    "target_account": "",
                },
            },
            "plan": {
                "target_write_account": "secondary",
                "source_snapshot": {"message_id": 100},
                "media_relay": {
                    "enabled": True,
                    "chat_id": 999,
                    "source_account": "primary",
                    "target_account": "",
                },
            },
        }

        readiness = clone_plan_timeline_readiness(
            plan,
            preview={
                "timeline_remaining": 3,
                "text_remaining": 1,
                "media_remaining": 2,
            },
        )

        self.assertFalse(readiness["can_migrate_timeline"])
        self.assertIn("media_relay_not_ready", readiness["reason_codes"])

    def test_timeline_readiness_allows_ready_relay_plan(self) -> None:
        plan = {
            "status": "done",
            "target_access": "ok",
            "source_access": "ok",
            "text_strategy": "database_replay",
            "media_strategy": "relay_copy_without_attribution",
            "migration_account": "unavailable",
            "blocking_issues": [],
            "capabilities": {
                "target_write_account": "secondary",
                "source_snapshot": {"message_id": 100},
                "media_relay": {
                    "enabled": True,
                    "chat_id": 999,
                    "source_account": "primary",
                    "target_account": "secondary",
                },
            },
            "plan": {
                "target_write_account": "secondary",
                "source_snapshot": {"message_id": 100},
                "media_relay": {
                    "enabled": True,
                    "chat_id": 999,
                    "source_account": "primary",
                    "target_account": "secondary",
                },
            },
        }

        readiness = clone_plan_timeline_readiness(
            plan,
            preview={
                "timeline_remaining": 3,
                "text_remaining": 1,
                "media_remaining": 2,
            },
        )

        self.assertTrue(readiness["can_migrate_timeline"])
        self.assertEqual([], readiness["reason_codes"])
        self.assertEqual("secondary", readiness["text_account"])
        self.assertEqual(
            "primary->relay->secondary",
            readiness["media_execution_account"],
        )
        self.assertEqual("primary", readiness["media_source_account"])
        self.assertEqual("secondary", readiness["media_target_account"])


if __name__ == "__main__":
    unittest.main()
