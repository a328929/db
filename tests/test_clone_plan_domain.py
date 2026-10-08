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
        self.assertEqual(readiness["reason_codes"], ["plan_missing"])

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
        self.assertEqual(readiness["reason_codes"], [])
        self.assertEqual(readiness["text_account"], "secondary")
        self.assertEqual(
            readiness["media_execution_account"],
            "primary->relay->secondary",
        )
        self.assertEqual(readiness["media_source_account"], "primary")
        self.assertEqual(readiness["media_target_account"], "secondary")

    def test_timeline_readiness_blocks_missing_source_snapshot(self) -> None:
        plan = {
            "status": "done",
            "target_access": "ok",
            "source_access": "ok",
            "text_strategy": "database_replay",
            "media_strategy": "source_copy_without_attribution",
            "blocking_issues": [],
            "capabilities": {"target_write_account": "secondary"},
        }

        readiness = clone_plan_timeline_readiness(
            plan,
            preview={"timeline_remaining": 1, "text_remaining": 1},
        )

        self.assertFalse(readiness["can_migrate_timeline"])
        self.assertIn("source_snapshot_missing", readiness["reason_codes"])

if __name__ == "__main__":
    unittest.main()
