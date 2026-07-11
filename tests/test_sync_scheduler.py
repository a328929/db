import os
import sqlite3
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tg_harvest.ml.sync_predictor import train_sync_model
from tg_harvest.storage.connection import detect_sqlite_features
from tg_harvest.storage.schema import create_schema
from tg_harvest.storage.sync_scheduler import (
    MembershipScope,
    SyncObservation,
    SyncUpdateResult,
    build_scheduler_summary,
    build_update_preflight,
    claim_due_pending_updates,
    classify_membership_scope,
    complete_pending_update,
    deactivate_chat,
    enqueue_observation,
    fail_pending_update,
    list_scheduler_chats,
    recover_in_flight_pending_updates,
    refresh_chat_states,
    upsert_account_runtime_state,
)


def _cfg(**overrides):
    values = {
        "sync_min_delay_seconds": 15,
        "sync_max_active_delay_seconds": 600,
        "sync_max_cold_delay_seconds": 7200,
        "sync_ai_enabled": 0,
        "sync_ai_shadow": 1,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class SyncSchedulerStorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        create_schema(
            self.conn,
            detect_sqlite_features(self.conn),
            skip_fts_auto_heal=1,
        )
        self.conn.executemany(
            """
            INSERT INTO chats(chat_id, chat_title, chat_username, chat_type, message_count)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (1, "Both", "both", "Channel", 1),
                (2, "Secondary", None, "Channel", 0),
                (3, "Cached", None, "Channel", 0),
                (4, "Invisible", None, "Channel", 0),
            ],
        )
        self.conn.execute(
            """
            INSERT INTO messages(chat_id, message_id, msg_date_text, msg_date_ts, msg_type)
            VALUES (1, 10, '2026-07-03 00:00:00', 1783036800, 'TEXT')
            """
        )
        self.conn.commit()
        self.chat_rows = [
            {
                "chat_id": 1,
                "chat_title": "Both",
                "chat_username": "both",
                "last_message_id": 10,
            },
            {
                "chat_id": 2,
                "chat_title": "Secondary",
                "chat_username": "",
                "last_message_id": 0,
            },
            {
                "chat_id": 3,
                "chat_title": "Cached",
                "chat_username": "",
                "last_message_id": 0,
            },
            {
                "chat_id": 4,
                "chat_title": "Invisible",
                "chat_username": "",
                "last_message_id": 0,
            },
        ]

    def tearDown(self) -> None:
        self.conn.close()

    def _refresh_states(self) -> None:
        refresh_chat_states(
            self.conn,
            chat_rows=self.chat_rows,
            joined_by_account={"primary": {1}, "secondary": {1, 2}},
            cached_by_account={"primary": set(), "secondary": {3}},
            account_keys=["primary", "secondary"],
            now_text="2026-07-03 00:00:00",
        )

    def test_classify_membership_scope(self) -> None:
        self.assertEqual(
            MembershipScope.BOTH_JOINED,
            classify_membership_scope(
                chat_id=1,
                account_keys=["primary", "secondary"],
                joined_account_keys=["primary", "secondary"],
                cached_account_keys=[],
                chat_username="public",
            ),
        )
        self.assertEqual(
            MembershipScope.SINGLE_JOINED_SECONDARY,
            classify_membership_scope(
                chat_id=2,
                account_keys=["primary", "secondary"],
                joined_account_keys=["secondary"],
                cached_account_keys=[],
                chat_username="",
            ),
        )
        self.assertEqual(
            MembershipScope.NONE_JOINED,
            classify_membership_scope(
                chat_id=3,
                account_keys=["primary", "secondary"],
                joined_account_keys=[],
                cached_account_keys=["primary"],
                chat_username="",
            ),
        )
        self.assertEqual(
            MembershipScope.UNOBSERVABLE,
            classify_membership_scope(
                chat_id=4,
                account_keys=["primary", "secondary"],
                joined_account_keys=[],
                cached_account_keys=[],
                chat_username="",
            ),
        )

    def test_refresh_chat_states_persists_membership_scopes(self) -> None:
        self._refresh_states()

        rows = list_scheduler_chats(self.conn, limit=10)["items"]
        scopes = {item["chat_id"]: item["membership_scope"] for item in rows}

        self.assertEqual(MembershipScope.BOTH_JOINED, scopes[1])
        self.assertEqual(MembershipScope.SINGLE_JOINED_SECONDARY, scopes[2])
        self.assertEqual(MembershipScope.NONE_JOINED, scopes[3])
        self.assertEqual(MembershipScope.UNOBSERVABLE, scopes[4])

    def test_pending_merge_generation_and_dirty_generation_survives_in_flight(self) -> None:
        self._refresh_states()
        cfg = _cfg()

        enqueue_observation(
            self.conn,
            cfg=cfg,
            observation=SyncObservation(
                chat_id=1,
                chat_title="Both",
                reason="new_message",
                source_account="primary",
                observed_at="2026-07-03 00:00:00",
            ),
        )
        enqueue_observation(
            self.conn,
            cfg=cfg,
            observation=SyncObservation(
                chat_id=1,
                chat_title="Both",
                reason="message_edited",
                source_account="secondary",
                observed_at="2026-07-03 00:00:10",
            ),
        )

        task = claim_due_pending_updates(
            self.conn,
            now_text="2026-07-03 00:01:00",
            limit=1,
        )[0]
        self.assertEqual(2, task.event_count)
        self.assertEqual(2, task.generation)

        enqueue_observation(
            self.conn,
            cfg=cfg,
            observation=SyncObservation(
                chat_id=1,
                chat_title="Both",
                reason="new_message",
                source_account="primary",
                observed_at="2026-07-03 00:01:05",
            ),
        )
        row = self.conn.execute(
            "SELECT generation, dirty_generation, in_flight FROM sync_pending_updates WHERE chat_id = 1"
        ).fetchone()
        self.assertEqual(3, row["generation"])
        self.assertEqual(3, row["dirty_generation"])
        self.assertEqual(1, row["in_flight"])

        complete_pending_update(
            self.conn,
            task=task,
            result=SyncUpdateResult(
                chat_id=1,
                source_account="primary",
                added_message_count=2,
                scanned_message_count=5,
                local_last_id=12,
            ),
            now_text="2026-07-03 00:01:20",
        )

        row = self.conn.execute(
            "SELECT generation, dirty_generation, in_flight FROM sync_pending_updates WHERE chat_id = 1"
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(3, row["generation"])
        self.assertEqual(0, row["dirty_generation"])
        self.assertEqual(0, row["in_flight"])

    def test_complete_pending_update_without_dirty_generation_clears_pending(self) -> None:
        self._refresh_states()
        cfg = _cfg()
        enqueue_observation(
            self.conn,
            cfg=cfg,
            observation=SyncObservation(
                chat_id=1,
                chat_title="Both",
                reason="new_message",
                source_account="primary",
                observed_at="2026-07-03 00:00:00",
            ),
        )
        task = claim_due_pending_updates(
            self.conn,
            now_text="2026-07-03 00:01:00",
            limit=1,
        )[0]

        complete_pending_update(
            self.conn,
            task=task,
            result=SyncUpdateResult(
                chat_id=1,
                source_account="primary",
                added_message_count=1,
                scanned_message_count=3,
                local_last_id=11,
            ),
            now_text="2026-07-03 00:01:20",
        )

        self.assertIsNone(
            self.conn.execute(
                "SELECT 1 FROM sync_pending_updates WHERE chat_id = 1"
            ).fetchone()
        )
        state = self.conn.execute(
            "SELECT status, local_last_id, failure_count FROM sync_chat_state WHERE chat_id = 1"
        ).fetchone()
        self.assertEqual("idle", state["status"])
        self.assertEqual(11, state["local_last_id"])
        self.assertEqual(0, state["failure_count"])

    def test_failed_claim_rolls_back_lease_and_keeps_event_pending(self) -> None:
        self._refresh_states()
        enqueue_observation(
            self.conn,
            cfg=_cfg(),
            observation=SyncObservation(
                chat_id=1,
                chat_title="Both",
                reason="new_message",
                source_account="primary",
                observed_at="2026-07-03 00:00:00",
            ),
        )
        self.conn.execute(
            """
            CREATE TRIGGER abort_scheduler_claim
            BEFORE UPDATE OF status ON sync_chat_state
            WHEN NEW.status = 'updating'
            BEGIN
                SELECT RAISE(ABORT, 'claim status write failed');
            END
            """
        )

        with self.assertRaises(sqlite3.Error):
            claim_due_pending_updates(
                self.conn,
                now_text="2026-07-03 00:01:00",
                limit=1,
            )

        pending = self.conn.execute(
            "SELECT in_flight, generation FROM sync_pending_updates WHERE chat_id = 1"
        ).fetchone()
        state = self.conn.execute(
            "SELECT status FROM sync_chat_state WHERE chat_id = 1"
        ).fetchone()
        self.assertEqual(0, pending["in_flight"])
        self.assertEqual(1, pending["generation"])
        self.assertEqual("pending", state["status"])

    def test_failed_completion_does_not_confirm_newer_event_generation(self) -> None:
        self._refresh_states()
        cfg = _cfg()
        enqueue_observation(
            self.conn,
            cfg=cfg,
            observation=SyncObservation(
                chat_id=1,
                chat_title="Both",
                reason="new_message",
                source_account="primary",
                observed_at="2026-07-03 00:00:00",
            ),
        )
        task = claim_due_pending_updates(
            self.conn,
            now_text="2026-07-03 00:01:00",
            limit=1,
        )[0]
        enqueue_observation(
            self.conn,
            cfg=cfg,
            observation=SyncObservation(
                chat_id=1,
                chat_title="Both",
                reason="message_edited",
                source_account="secondary",
                observed_at="2026-07-03 00:01:05",
            ),
        )
        self.conn.execute(
            """
            CREATE TRIGGER abort_scheduler_complete
            BEFORE UPDATE OF status ON sync_chat_state
            WHEN NEW.status = 'pending'
            BEGIN
                SELECT RAISE(ABORT, 'completion state write failed');
            END
            """
        )

        with self.assertRaises(sqlite3.Error):
            complete_pending_update(
                self.conn,
                task=task,
                result=SyncUpdateResult(
                    chat_id=1,
                    source_account="primary",
                    local_last_id=12,
                ),
                now_text="2026-07-03 00:01:20",
            )

        pending = self.conn.execute(
            """
            SELECT generation, dirty_generation, in_flight
            FROM sync_pending_updates
            WHERE chat_id = 1
            """
        ).fetchone()
        state = self.conn.execute(
            "SELECT status, local_last_id FROM sync_chat_state WHERE chat_id = 1"
        ).fetchone()
        self.assertEqual(2, pending["generation"])
        self.assertEqual(2, pending["dirty_generation"])
        self.assertEqual(1, pending["in_flight"])
        self.assertEqual("updating", state["status"])
        self.assertEqual(10, state["local_last_id"])

    def test_recover_in_flight_task_preserves_new_generation_and_quiet_delay(self) -> None:
        self._refresh_states()
        cfg = _cfg()
        enqueue_observation(
            self.conn,
            cfg=cfg,
            observation=SyncObservation(
                chat_id=1,
                chat_title="Both",
                reason="new_message",
                source_account="primary",
                observed_at="2026-07-03 00:00:00",
            ),
        )
        claimed = claim_due_pending_updates(
            self.conn,
            now_text="2026-07-03 00:01:00",
            limit=1,
        )
        self.assertEqual(1, len(claimed))

        enqueue_observation(
            self.conn,
            cfg=cfg,
            observation=SyncObservation(
                chat_id=1,
                chat_title="Both",
                reason="message_edited",
                source_account="secondary",
                observed_at="2026-07-03 00:01:05",
            ),
        )
        before_recovery = self.conn.execute(
            """
            SELECT generation, dirty_generation, due_at
            FROM sync_pending_updates
            WHERE chat_id = 1
            """
        ).fetchone()

        self.assertEqual(
            1,
            recover_in_flight_pending_updates(
                self.conn,
                now_text="2026-07-03 00:01:10",
            ),
        )

        pending = self.conn.execute(
            """
            SELECT generation, dirty_generation, in_flight, in_flight_generation, due_at
            FROM sync_pending_updates
            WHERE chat_id = 1
            """
        ).fetchone()
        state = self.conn.execute(
            """
            SELECT status, next_update_at
            FROM sync_chat_state
            WHERE chat_id = 1
            """
        ).fetchone()
        self.assertEqual(before_recovery["generation"], pending["generation"])
        self.assertEqual(
            before_recovery["dirty_generation"], pending["dirty_generation"]
        )
        self.assertEqual(0, pending["in_flight"])
        self.assertEqual(0, pending["in_flight_generation"])
        self.assertEqual(before_recovery["due_at"], pending["due_at"])
        self.assertEqual("pending", state["status"])
        self.assertEqual(before_recovery["due_at"], state["next_update_at"])

        resumed = claim_due_pending_updates(
            self.conn,
            now_text=str(pending["due_at"]),
            limit=1,
        )
        self.assertEqual(1, len(resumed))
        self.assertEqual(pending["generation"], resumed[0].in_flight_generation)

    def test_recover_in_flight_task_requeues_elapsed_work_immediately(self) -> None:
        self._refresh_states()
        enqueue_observation(
            self.conn,
            cfg=_cfg(),
            observation=SyncObservation(
                chat_id=1,
                chat_title="Both",
                reason="new_message",
                source_account="primary",
                observed_at="2026-07-03 00:00:00",
            ),
        )
        claim_due_pending_updates(
            self.conn,
            now_text="2026-07-03 00:01:00",
            limit=1,
        )

        recovery_now = "2026-07-03 00:02:00"
        self.assertEqual(
            1,
            recover_in_flight_pending_updates(self.conn, now_text=recovery_now),
        )
        pending = self.conn.execute(
            """
            SELECT in_flight, due_at
            FROM sync_pending_updates
            WHERE chat_id = 1
            """
        ).fetchone()
        self.assertEqual(0, pending["in_flight"])
        self.assertEqual(recovery_now, pending["due_at"])
        self.assertEqual(
            1,
            len(
                claim_due_pending_updates(
                    self.conn,
                    now_text=recovery_now,
                    limit=1,
                )
            ),
        )

    def test_recovery_releases_account_slots_with_reclaimed_work(self) -> None:
        now = "2026-07-03 00:02:00"
        self._refresh_states()
        enqueue_observation(
            self.conn,
            cfg=_cfg(),
            observation=SyncObservation(
                chat_id=1,
                chat_title="Both",
                reason="new_message",
                source_account="primary",
                observed_at="2026-07-03 00:00:00",
            ),
        )
        claim_due_pending_updates(
            self.conn,
            now_text="2026-07-03 00:01:00",
            limit=1,
        )
        upsert_account_runtime_state(
            self.conn,
            account_key="primary",
            session_name="primary.session",
            label="Primary",
            in_flight_delta=2,
            now_text="2026-07-03 00:01:00",
        )

        self.assertEqual(
            1,
            recover_in_flight_pending_updates(self.conn, now_text=now),
        )
        row = self.conn.execute(
            """
            SELECT in_flight_count, updated_at
            FROM account_runtime_state
            WHERE account_key = 'primary'
            """
        ).fetchone()
        self.assertEqual(0, row["in_flight_count"])
        self.assertEqual(now, row["updated_at"])

    def test_recovery_preserves_live_or_unattributed_account_slots(self) -> None:
        now = "2026-07-03 00:02:00"
        upsert_account_runtime_state(
            self.conn,
            account_key="primary",
            session_name="primary.session",
            label="Primary",
            in_flight_delta=2,
            now_text="2026-07-03 00:01:00",
        )

        self.assertEqual(
            0,
            recover_in_flight_pending_updates(self.conn, now_text=now),
        )
        row = self.conn.execute(
            """
            SELECT in_flight_count, updated_at
            FROM account_runtime_state
            WHERE account_key = 'primary'
            """
        ).fetchone()
        self.assertEqual(2, row["in_flight_count"])
        self.assertEqual("2026-07-03 00:01:00", row["updated_at"])

    def test_recovery_keeps_work_owned_by_a_live_local_process(self) -> None:
        self._refresh_states()
        enqueue_observation(
            self.conn,
            cfg=_cfg(),
            observation=SyncObservation(
                chat_id=1,
                chat_title="Both",
                reason="new_message",
                source_account="primary",
                observed_at="2026-07-03 00:00:00",
            ),
        )
        claimed = claim_due_pending_updates(
            self.conn,
            now_text="2026-07-03 00:01:00",
            limit=1,
            owner_instance_id="live-worker",
            owner_pid=os.getpid(),
            owner_host="local-test-host",
        )
        self.assertEqual(1, len(claimed))

        self.assertEqual(
            0,
            recover_in_flight_pending_updates(
                self.conn,
                now_text="2026-07-03 00:02:00",
                local_host="local-test-host",
            ),
        )
        pending = self.conn.execute(
            """
            SELECT in_flight, in_flight_owner_instance_id
            FROM sync_pending_updates
            WHERE chat_id = 1
            """
        ).fetchone()
        self.assertEqual(1, pending["in_flight"])
        self.assertEqual("live-worker", pending["in_flight_owner_instance_id"])

    def test_recovery_reclaims_dead_local_owner_and_clears_lease(self) -> None:
        self._refresh_states()
        enqueue_observation(
            self.conn,
            cfg=_cfg(),
            observation=SyncObservation(
                chat_id=1,
                chat_title="Both",
                reason="new_message",
                source_account="primary",
                observed_at="2026-07-03 00:00:00",
            ),
        )
        claimed = claim_due_pending_updates(
            self.conn,
            now_text="2026-07-03 00:01:00",
            limit=1,
            owner_instance_id="dead-worker",
            owner_pid=12345,
            owner_host="local-test-host",
        )
        self.assertEqual(1, len(claimed))

        with patch(
            "tg_harvest.storage.sync_scheduler._process_is_alive",
            return_value=False,
        ):
            self.assertEqual(
                1,
                recover_in_flight_pending_updates(
                    self.conn,
                    now_text="2026-07-03 00:02:00",
                    local_host="local-test-host",
                ),
            )
        pending = self.conn.execute(
            """
            SELECT
                in_flight,
                in_flight_owner_instance_id,
                in_flight_owner_pid,
                in_flight_owner_host
            FROM sync_pending_updates
            WHERE chat_id = 1
            """
        ).fetchone()
        self.assertEqual(0, pending["in_flight"])
        self.assertEqual("", pending["in_flight_owner_instance_id"])
        self.assertEqual(0, pending["in_flight_owner_pid"])
        self.assertEqual("", pending["in_flight_owner_host"])

    def test_recovery_leaves_remote_owner_for_its_runtime(self) -> None:
        self._refresh_states()
        enqueue_observation(
            self.conn,
            cfg=_cfg(),
            observation=SyncObservation(
                chat_id=1,
                chat_title="Both",
                reason="new_message",
                source_account="primary",
                observed_at="2026-07-03 00:00:00",
            ),
        )
        claim_due_pending_updates(
            self.conn,
            now_text="2026-07-03 00:01:00",
            limit=1,
            owner_instance_id="remote-worker",
            owner_pid=12345,
            owner_host="other-host",
        )

        with patch(
            "tg_harvest.storage.sync_scheduler._process_is_alive",
            return_value=False,
        ):
            self.assertEqual(
                0,
                recover_in_flight_pending_updates(
                    self.conn,
                    now_text="2026-07-03 00:02:00",
                    local_host="local-test-host",
                ),
            )
        self.assertEqual(
            1,
            self.conn.execute(
                "SELECT in_flight FROM sync_pending_updates WHERE chat_id = 1"
            ).fetchone()[0],
        )

    def test_old_owner_cannot_complete_a_task_reclaimed_by_another_runtime(self) -> None:
        self._refresh_states()
        enqueue_observation(
            self.conn,
            cfg=_cfg(),
            observation=SyncObservation(
                chat_id=1,
                chat_title="Both",
                reason="new_message",
                source_account="primary",
                observed_at="2026-07-03 00:00:00",
            ),
        )
        old_task = claim_due_pending_updates(
            self.conn,
            now_text="2026-07-03 00:01:00",
            limit=1,
            owner_instance_id="old-worker",
            owner_pid=12345,
            owner_host="local-test-host",
        )[0]
        with patch(
            "tg_harvest.storage.sync_scheduler._process_is_alive",
            return_value=False,
        ):
            recover_in_flight_pending_updates(
                self.conn,
                now_text="2026-07-03 00:02:00",
                local_host="local-test-host",
            )
        new_task = claim_due_pending_updates(
            self.conn,
            now_text="2026-07-03 00:02:00",
            limit=1,
            owner_instance_id="new-worker",
            owner_pid=os.getpid(),
            owner_host="local-test-host",
        )[0]

        complete_pending_update(
            self.conn,
            task=old_task,
            result=SyncUpdateResult(chat_id=1, source_account="primary"),
            now_text="2026-07-03 00:02:01",
        )
        pending = self.conn.execute(
            """
            SELECT in_flight, in_flight_owner_instance_id
            FROM sync_pending_updates
            WHERE chat_id = 1
            """
        ).fetchone()
        self.assertEqual(1, pending["in_flight"])
        self.assertEqual(new_task.in_flight_owner_instance_id, pending["in_flight_owner_instance_id"])

    def test_fail_pending_update_keeps_task_with_backoff(self) -> None:
        self._refresh_states()
        cfg = _cfg()
        enqueue_observation(
            self.conn,
            cfg=cfg,
            observation=SyncObservation(
                chat_id=1,
                chat_title="Both",
                reason="new_message",
                source_account="primary",
                observed_at="2026-07-03 00:00:00",
            ),
        )
        task = claim_due_pending_updates(
            self.conn,
            now_text="2026-07-03 00:01:00",
            limit=1,
        )[0]

        fail_pending_update(
            self.conn,
            cfg=cfg,
            task=task,
            result=SyncUpdateResult(
                chat_id=1,
                source_account="primary",
                failure_type="flood_wait",
                failure_message="FloodWait 90s",
                retry_after_seconds=90,
            ),
            now_text="2026-07-03 00:01:20",
        )

        pending = self.conn.execute(
            "SELECT in_flight, due_at FROM sync_pending_updates WHERE chat_id = 1"
        ).fetchone()
        state = self.conn.execute(
            "SELECT status, failure_count, last_failure_message FROM sync_chat_state WHERE chat_id = 1"
        ).fetchone()
        self.assertEqual(0, pending["in_flight"])
        self.assertEqual("2026-07-03 00:02:50", pending["due_at"])
        self.assertEqual("backoff", state["status"])
        self.assertEqual(1, state["failure_count"])
        self.assertEqual("FloodWait 90s", state["last_failure_message"])

    def test_deactivate_chat_clears_pending_and_keeps_audit_state(self) -> None:
        self._refresh_states()
        enqueue_observation(
            self.conn,
            cfg=_cfg(),
            observation=SyncObservation(
                chat_id=1,
                chat_title="Both",
                reason="new_message",
                source_account="primary",
                observed_at="2026-07-03 00:00:00",
            ),
        )

        deactivate_chat(self.conn, 1, now_text="2026-07-03 00:02:00")

        self.assertIsNone(
            self.conn.execute(
                "SELECT 1 FROM sync_pending_updates WHERE chat_id = 1"
            ).fetchone()
        )
        state = self.conn.execute(
            "SELECT status, is_active, priority_score FROM sync_chat_state WHERE chat_id = 1"
        ).fetchone()
        self.assertEqual("deleted", state["status"])
        self.assertEqual(0, state["is_active"])
        self.assertEqual(0, state["priority_score"])

    def test_scheduler_summary_reports_pending_and_membership_counts(self) -> None:
        self._refresh_states()
        enqueue_observation(
            self.conn,
            cfg=_cfg(),
            observation=SyncObservation(
                chat_id=1,
                chat_title="Both",
                reason="new_message",
                source_account="primary",
                observed_at="2026-07-03 00:00:00",
            ),
        )

        summary = build_scheduler_summary(
            self.conn,
            health_snapshot={
                "scheduler_enabled": True,
                "ai_enabled": False,
                "ai_shadow": True,
                "accounts": [{"key": "primary", "cooldown_seconds": 0}],
            },
            now_text="2026-07-03 00:01:00",
        )

        self.assertTrue(summary["enabled"])
        self.assertEqual(1, summary["pending_count"])
        self.assertEqual(1, summary["due_count"])
        membership_counts = {
            item["scope"]: item["count"] for item in summary["membership_counts"]
        }
        self.assertEqual(1, membership_counts[MembershipScope.BOTH_JOINED])
        self.assertEqual("disabled", summary["model"]["backend"])

    def test_model_training_records_torch_unavailable_state(self) -> None:
        self._refresh_states()

        with patch(
            "tg_harvest.ml.sync_predictor._load_torch",
            return_value=(None, None, "missing torch"),
        ):
            result = train_sync_model(self.conn, _cfg(sync_ai_enabled=1))

        self.assertFalse(result["trained"])
        self.assertEqual("torch_unavailable", result["backend"])
        row = self.conn.execute(
            "SELECT backend, state_json FROM sync_model_state WHERE model_key = ?",
            ("temporal_batch_predictor",),
        ).fetchone()
        self.assertEqual("torch_unavailable", row["backend"])
        self.assertIn("torch_unavailable", row["state_json"])

    def test_model_shadow_prediction_does_not_override_heuristic_decision(self) -> None:
        self._refresh_states()

        class _Suggestion:
            available = True
            active = False
            quiet_delay_seconds = 300
            priority_score = 250.0

            def to_prediction_dict(self):
                return {
                    "available": True,
                    "active": False,
                    "quiet_delay_seconds": 300,
                    "priority_score": 250.0,
                }

        with patch(
            "tg_harvest.ml.sync_predictor.predict_sync_decision",
            return_value=_Suggestion(),
        ):
            decision = enqueue_observation(
                self.conn,
                cfg=_cfg(sync_ai_enabled=1),
                observation=SyncObservation(
                    chat_id=1,
                    chat_title="Both",
                    reason="new_message",
                    source_account="primary",
                    observed_at="2026-07-03 00:00:00",
                ),
            )

        self.assertEqual("heuristic_with_model_shadow", decision.source)
        self.assertEqual(30, decision.quiet_delay_seconds)
        self.assertEqual("2026-07-03 00:00:30", decision.due_at)

    def test_shadow_forces_observation_even_if_model_reports_active(self) -> None:
        self._refresh_states()

        class _Suggestion:
            available = True
            active = True
            quiet_delay_seconds = 300
            priority_score = 250.0

            def to_prediction_dict(self):
                return {
                    "available": True,
                    "active": True,
                    "quiet_delay_seconds": 300,
                    "priority_score": 250.0,
                }

        with patch(
            "tg_harvest.ml.sync_predictor.predict_sync_decision",
            return_value=_Suggestion(),
        ):
            decision = enqueue_observation(
                self.conn,
                cfg=_cfg(
                    sync_ai_enabled=1,
                    sync_ai_shadow=1,
                    sync_ai_auto_promote_enabled=1,
                ),
                observation=SyncObservation(
                    chat_id=1,
                    chat_title="Both",
                    reason="new_message",
                    source_account="primary",
                    observed_at="2026-07-03 00:00:00",
                ),
            )

        self.assertEqual("heuristic_with_model_shadow", decision.source)
        self.assertEqual(30, decision.quiet_delay_seconds)

    def test_active_model_prediction_overrides_due_at_with_bounds(self) -> None:
        self._refresh_states()

        class _Suggestion:
            available = True
            active = True
            quiet_delay_seconds = 300
            priority_score = 250.0

            def to_prediction_dict(self):
                return {
                    "available": True,
                    "active": True,
                    "quiet_delay_seconds": 300,
                    "priority_score": 250.0,
                }

        with patch(
            "tg_harvest.ml.sync_predictor.predict_sync_decision",
            return_value=_Suggestion(),
        ):
            decision = enqueue_observation(
                self.conn,
                cfg=_cfg(
                    sync_ai_enabled=1,
                    sync_ai_shadow=0,
                    sync_ai_auto_promote_enabled=1,
                ),
                observation=SyncObservation(
                    chat_id=1,
                    chat_title="Both",
                    reason="new_message",
                    source_account="primary",
                    observed_at="2026-07-03 00:00:00",
                ),
            )

        self.assertEqual("torch_model_active", decision.source)
        self.assertEqual(60, decision.quiet_delay_seconds)
        self.assertEqual("2026-07-03 00:01:00", decision.due_at)
        self.assertEqual(250.0, decision.priority_score)

    def test_complete_pending_update_backfills_remote_last_id_from_local_last_id(self) -> None:
        self._refresh_states()
        enqueue_observation(
            self.conn,
            cfg=_cfg(),
            observation=SyncObservation(
                chat_id=1,
                chat_title="Both",
                reason="new_message",
                source_account="primary",
                observed_at="2026-07-03 00:00:00",
            ),
        )
        task = claim_due_pending_updates(
            self.conn,
            now_text="2026-07-03 00:01:00",
            limit=1,
        )[0]

        complete_pending_update(
            self.conn,
            task=task,
            result=SyncUpdateResult(
                chat_id=1,
                source_account="primary",
                added_message_count=1,
                scanned_message_count=3,
                local_last_id=15,
                remote_last_id=0,
            ),
            now_text="2026-07-03 00:01:20",
        )

        state = self.conn.execute(
            "SELECT local_last_id, remote_last_id FROM sync_chat_state WHERE chat_id = 1"
        ).fetchone()
        self.assertEqual(15, state["local_last_id"])
        self.assertEqual(15, state["remote_last_id"])

    def test_build_update_preflight_reports_capacity_and_budget(self) -> None:
        payload = build_update_preflight(
            self.conn,
            _cfg(
                admin_update_concurrency=4,
                admin_update_secondary_public_resolve_limit=1,
                admin_update_max_cooldown_wait_seconds=45,
                sync_scheduler_concurrency=2,
                session_name="primary",
                secondary_session_name="secondary",
            ),
            chat_id="all",
            health_snapshot={
                "accounts": [
                    {"key": "primary", "label": "主账号", "cooldown_seconds": 0},
                    {"key": "secondary", "label": "第二账号", "cooldown_seconds": 90},
                ],
            },
        )
        self.assertTrue(payload["ok"])
        self.assertEqual(2, payload["account_capacity"]["configured"])
        self.assertEqual(1, payload["account_capacity"]["available"])
        self.assertEqual(1, payload["strategy"]["secondary_public_resolve_budget"])



if __name__ == "__main__":
    unittest.main()
