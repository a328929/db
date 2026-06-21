import datetime
import sqlite3
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from telethon.errors import FloodWaitError

import tg_harvest.ingest.runner as harvest_runner_module
from tg_harvest.ingest.flood_wait import AccountFloodWaitError
from tg_harvest.ingest.runner import (
    _build_iter_messages_kwargs,
    _harvest_messages_for_entity,
    _process_entity,
)


class _FakeCountResult:
    total = 3


class _FakeMessage:
    def __init__(self, message_id: int) -> None:
        self.id = message_id
        self.date = datetime.datetime(2024, 1, 1, 0, 0, message_id)
        self.sender_id = 1
        self.raw_text = f"msg-{message_id}"
        self.message = self.raw_text
        self.text = self.raw_text
        self.grouped_id = None
        self.sticker = None
        self.gif = None
        self.voice = None
        self.video_note = None
        self.audio = None
        self.video = None
        self.photo = None
        self.document = None
        self.poll = None
        self.contact = None
        self.geo = None
        self.file = None


class _BadMediaMessage(_FakeMessage):
    def __init__(self, message_id: int) -> None:
        super().__init__(message_id)
        self.raw_text = ""
        self.message = ""
        self.text = ""
        self.photo = object()
        self.file = SimpleNamespace(
            id="fid-1",
            name="demo.jpg",
            ext=".jpg",
            mime_type="image/jpeg",
            size="bad-size",
            width=100,
            height=100,
            duration=None,
            title=None,
            performer=None,
            emoji=None,
        )


class _FakeClient:
    def __init__(self, iter_plans):
        self._iter_plans = list(iter_plans)
        self.iter_calls = []

    def get_messages(self, _entity, **_kwargs):
        return _FakeCountResult()

    def iter_messages(self, _entity, **kwargs):
        self.iter_calls.append(dict(kwargs))
        plan = self._iter_plans.pop(0)
        if isinstance(plan, Exception):
            raise plan
        if callable(plan):
            return plan()
        return iter(plan)


def _message_stream(*events):
    def _iterator():
        for event in events:
            if isinstance(event, Exception):
                raise event
            yield event

    return _iterator


class HarvestRunnerIterMessagesKwargsTests(unittest.TestCase):
    def test_iter_messages_kwargs_keep_telethon_default_when_wait_time_is_auto(self) -> None:
        self.assertEqual({"reverse": True}, _build_iter_messages_kwargs(0))
        self.assertEqual(
            {"reverse": True, "min_id": 10}, _build_iter_messages_kwargs(10)
        )

    def test_iter_messages_kwargs_can_override_history_wait_time(self) -> None:
        self.assertEqual(
            {"reverse": True, "min_id": 10, "wait_time": 0.2},
            _build_iter_messages_kwargs(10, history_wait_time=0.2),
        )
        self.assertEqual(
            {"reverse": True, "wait_time": 0.0},
            _build_iter_messages_kwargs(0, history_wait_time=-1),
        )


class HarvestRunnerReliabilityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row

    def tearDown(self) -> None:
        self.conn.close()

    def test_incremental_sync_uses_ascending_resume_from_last_committed_message(self) -> None:
        client = _FakeClient(
            [
                _message_stream(
                    _FakeMessage(11),
                    _FakeMessage(12),
                    _FakeMessage(13),
                    ConnectionError("boom"),
                ),
                _message_stream(_FakeMessage(13)),
            ]
        )
        batch_calls = []

        with patch(
            "tg_harvest.ingest.runner.get_last_message_id", return_value=10
        ), patch(
            "tg_harvest.ingest.runner.batch_upsert",
            side_effect=lambda _conn, msg_rows, media_rows: batch_calls.append(
                ([row[1] for row in msg_rows], list(media_rows))
            ),
        ), patch("tg_harvest.ingest.runner.CFG.batch_size", 2), patch(
            "tg_harvest.ingest.runner.CFG.log_every", 1000
        ), patch(
            "tg_harvest.ingest.runner.CFG.history_wait_time", None
        ):
            counters, _touched_groups, first_sync = _harvest_messages_for_entity(
                self.conn, client, object(), 42
            )

        self.assertFalse(first_sync)
        self.assertEqual(
            [{"reverse": True, "min_id": 10}, {"reverse": True, "min_id": 12}],
            client.iter_calls,
        )
        self.assertEqual([[11, 12], [13]], [call[0] for call in batch_calls])
        self.assertEqual(3, counters.written)

    def test_retry_exhaustion_raises_instead_of_succeeding_silently(self) -> None:
        client = _FakeClient(
            [
                ConnectionError("a"),
                ConnectionError("b"),
                ConnectionError("c"),
            ]
        )

        with patch(
            "tg_harvest.ingest.runner.get_last_message_id", return_value=10
        ), patch("tg_harvest.ingest.runner.batch_upsert") as batch_upsert_mock, patch(
            "tg_harvest.ingest.runner.CFG.log_every", 1000
        ), patch(
            "tg_harvest.ingest.runner.CFG.history_wait_time", None
        ), self.assertRaises(RuntimeError):
            _harvest_messages_for_entity(self.conn, client, object(), 42)

        batch_upsert_mock.assert_not_called()
        self.assertEqual(
            [
                {"reverse": True, "min_id": 10},
                {"reverse": True, "min_id": 10},
                {"reverse": True, "min_id": 10},
            ],
            client.iter_calls,
        )

    def test_long_flood_wait_raises_switch_signal_without_sleeping(self) -> None:
        client = _FakeClient([FloodWaitError(request=None, capture=70)])

        with patch(
            "tg_harvest.ingest.runner.get_last_message_id", return_value=10
        ), patch("tg_harvest.ingest.runner.batch_upsert") as batch_upsert_mock, patch(
            "tg_harvest.ingest.runner.CFG.log_every", 1000
        ), patch(
            "tg_harvest.ingest.runner.CFG.history_wait_time", None
        ), patch(
            "tg_harvest.ingest.runner.CFG.flood_wait_switch_threshold", 30
        ), patch(
            "tg_harvest.ingest.runner.time.sleep"
        ) as sleep_mock, self.assertRaises(AccountFloodWaitError) as caught:
            _harvest_messages_for_entity(self.conn, client, object(), 42)

        self.assertEqual(70, caught.exception.seconds)
        self.assertEqual(30, caught.exception.threshold_seconds)
        sleep_mock.assert_not_called()
        batch_upsert_mock.assert_not_called()

    def test_harvest_messages_can_delegate_writes_to_external_writer(self) -> None:
        client = _FakeClient([_message_stream(_FakeMessage(11), _FakeMessage(12))])
        delegated_batches = []

        with patch(
            "tg_harvest.ingest.runner.get_last_message_id", return_value=10
        ), patch("tg_harvest.ingest.runner.batch_upsert") as batch_upsert_mock, patch(
            "tg_harvest.ingest.runner.CFG.batch_size", 2
        ), patch(
            "tg_harvest.ingest.runner.CFG.log_every", 1000
        ):
            counters, _touched_groups, first_sync = _harvest_messages_for_entity(
                self.conn,
                client,
                object(),
                42,
                write_batch_fn=lambda msg_rows, media_rows: delegated_batches.append(
                    ([row[1] for row in msg_rows], list(media_rows))
                ),
            )

        self.assertFalse(first_sync)
        self.assertEqual(2, counters.written)
        self.assertEqual([([11, 12], [])], delegated_batches)
        batch_upsert_mock.assert_not_called()

    def test_parse_failure_aborts_chat_harvest_instead_of_silently_skipping(self) -> None:
        client = _FakeClient([_message_stream(_BadMediaMessage(11))])

        with patch(
            "tg_harvest.ingest.runner.get_last_message_id", return_value=10
        ), patch("tg_harvest.ingest.runner.batch_upsert") as batch_upsert_mock, patch(
            "tg_harvest.ingest.runner.CFG.log_every", 1000
        ), self.assertLogs(level="ERROR") as captured, self.assertRaisesRegex(
            RuntimeError, "消息解析失败并已中止当前采集"
        ):
            _harvest_messages_for_entity(self.conn, client, object(), 42)

        batch_upsert_mock.assert_not_called()
        self.assertTrue(
            any("为避免静默丢数已中止当前采集" in line for line in captured.output)
        )


class HarvestRunnerPostprocessTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.entity = SimpleNamespace(id=42, title="Test Chat", username="test_chat")

    def tearDown(self) -> None:
        self.conn.close()

    def test_incremental_update_skips_postprocess_when_no_changes(self) -> None:
        counters = SimpleNamespace(seen=0, written=0, parse_failures=0)

        with patch.object(
            harvest_runner_module,
            "upsert_chat",
            return_value=None,
        ), patch.object(
            harvest_runner_module,
            "_harvest_messages_for_entity",
            return_value=(counters, set(), False),
        ), patch.object(
            harvest_runner_module,
            "refresh_media_groups_for_chat",
        ) as refresh_groups_mock, patch.object(
            harvest_runner_module,
            "refresh_chat_message_counts",
        ) as refresh_counts_mock, patch.object(
            harvest_runner_module,
            "dedupe_promotional_duplicates",
        ) as dedupe_mock, patch.object(
            harvest_runner_module,
            "log_parse_failure_summary",
            return_value=None,
        ):
            _process_entity(
                self.conn,
                object(),
                self.entity,
                idx=1,
                total=1,
                skip_postprocess_if_unchanged=True,
                enable_dedupe=False,
            )

        refresh_groups_mock.assert_not_called()
        refresh_counts_mock.assert_not_called()
        dedupe_mock.assert_not_called()

    def test_incremental_update_refreshes_counts_only_when_new_rows_without_groups(self) -> None:
        counters = SimpleNamespace(seen=3, written=3, parse_failures=0)

        with patch.object(
            harvest_runner_module,
            "upsert_chat",
            return_value=None,
        ), patch.object(
            harvest_runner_module,
            "_harvest_messages_for_entity",
            return_value=(counters, set(), False),
        ), patch.object(
            harvest_runner_module,
            "refresh_media_groups_for_chat",
        ) as refresh_groups_mock, patch.object(
            harvest_runner_module,
            "refresh_chat_message_counts",
            return_value=None,
        ) as refresh_counts_mock, patch.object(
            harvest_runner_module,
            "dedupe_promotional_duplicates",
        ) as dedupe_mock, patch.object(
            harvest_runner_module,
            "log_parse_failure_summary",
            return_value=None,
        ):
            _process_entity(
                self.conn,
                object(),
                self.entity,
                idx=1,
                total=1,
                skip_postprocess_if_unchanged=True,
                enable_dedupe=False,
            )

        refresh_groups_mock.assert_not_called()
        dedupe_mock.assert_not_called()
        refresh_counts_mock.assert_called_once_with(self.conn, [42])

    def test_incremental_update_refreshes_touched_media_groups_without_dedupe(self) -> None:
        counters = SimpleNamespace(seen=2, written=2, parse_failures=0)
        touched_groups = {1001, 1002}

        with patch.object(
            harvest_runner_module,
            "upsert_chat",
            return_value=None,
        ), patch.object(
            harvest_runner_module,
            "_harvest_messages_for_entity",
            return_value=(counters, touched_groups, False),
        ), patch.object(
            harvest_runner_module,
            "refresh_media_groups_for_chat",
            return_value=None,
        ) as refresh_groups_mock, patch.object(
            harvest_runner_module,
            "refresh_chat_message_counts",
            return_value=None,
        ) as refresh_counts_mock, patch.object(
            harvest_runner_module,
            "dedupe_promotional_duplicates",
        ) as dedupe_mock, patch.object(
            harvest_runner_module,
            "log_parse_failure_summary",
            return_value=None,
        ):
            _process_entity(
                self.conn,
                object(),
                self.entity,
                idx=1,
                total=1,
                skip_postprocess_if_unchanged=True,
                enable_dedupe=False,
            )

        refresh_groups_mock.assert_called_once_with(
            self.conn, 42, cfg=harvest_runner_module.CFG, grouped_ids=touched_groups
        )
        refresh_counts_mock.assert_called_once_with(self.conn, [42])
        dedupe_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
