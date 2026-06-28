import datetime
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from telethon.errors import FloodWaitError

from tg_harvest.ingest.range_harvest import (
    HistoryAccessProbeResult,
    MessageIdRange,
    harvest_message_id_range,
    probe_history_access,
    read_latest_message_id,
)


class _FakeMessage:
    def __init__(self, message_id: int) -> None:
        self.id = message_id
        self.date = datetime.datetime(2024, 1, 1, 0, 0, min(message_id, 59))
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


class _FakeClient:
    def __init__(self, iter_plans):
        self._iter_plans = list(iter_plans)
        self.iter_calls = []

    def iter_messages(self, _entity, **kwargs):
        self.iter_calls.append(dict(kwargs))
        plan = self._iter_plans.pop(0)
        if isinstance(plan, Exception):
            raise plan
        return iter(plan)

    def get_messages(self, _entity, **_kwargs):
        if not self._iter_plans:
            return [_FakeMessage(12)]
        plan = self._iter_plans.pop(0)
        if isinstance(plan, Exception):
            raise plan
        if isinstance(plan, list):
            return plan
        return [_FakeMessage(12)]


class RangeHarvestRetryTests(unittest.TestCase):
    def test_short_flood_wait_uses_bounded_backoff_sleep(self) -> None:
        client = _FakeClient(
            [
                FloodWaitError(request=None, capture=4),
                [_FakeMessage(12), _FakeMessage(11), _FakeMessage(10)],
            ]
        )
        written_batches = []

        with patch(
            "tg_harvest.ingest.range_harvest.CFG.batch_size", 100
        ), patch(
            "tg_harvest.ingest.range_harvest.CFG.history_wait_time", None
        ), patch(
            "tg_harvest.ingest.range_harvest.CFG.flood_wait_switch_threshold", 30
        ), patch(
            "tg_harvest.ingest.range_harvest.exponential_backoff_seconds",
            return_value=4.5,
        ) as backoff_mock, patch(
            "tg_harvest.ingest.range_harvest.time.sleep"
        ) as sleep_mock:
            counters, touched_groups = harvest_message_id_range(
                client=client,
                entity=SimpleNamespace(id=1),
                chat_id=42,
                message_range=MessageIdRange(start_id=10, end_id=12),
                write_batch_fn=lambda msg_rows, media_rows: written_batches.append(
                    ([row[1] for row in msg_rows], list(media_rows))
                ),
                account_label="secondary",
            )

        self.assertEqual(3, counters.written)
        self.assertEqual(set(), touched_groups)
        self.assertEqual([([12, 11, 10], [])], written_batches)
        backoff_mock.assert_called_once_with(1, required_wait_seconds=4)
        sleep_mock.assert_called_once_with(4.5)

    def test_read_latest_message_id_uses_bounded_retry_helper(self) -> None:
        client = _FakeClient([])

        with patch(
            "tg_harvest.ingest.range_harvest.call_with_bounded_retry",
            return_value=[_FakeMessage(25)],
        ) as retry_mock, patch(
            "tg_harvest.ingest.range_harvest.CFG.flood_wait_switch_threshold", 30
        ):
            latest_id = read_latest_message_id(client, SimpleNamespace(id=1))

        self.assertEqual(25, latest_id)
        retry_mock.assert_called_once()
        self.assertEqual("history-latest-id", retry_mock.call_args.kwargs["scope"])

    def test_probe_history_access_retries_latest_and_range_probes(self) -> None:
        client = _FakeClient([])

        def fake_retry(fn, *args, **kwargs):
            scope = kwargs.get("scope")
            if scope == "history-latest-probe":
                return [_FakeMessage(100)]
            if scope == "history-range-probe":
                return _FakeMessage(60)
            raise AssertionError(scope)

        with patch(
            "tg_harvest.ingest.range_harvest.call_with_bounded_retry",
            side_effect=fake_retry,
        ) as retry_mock, patch(
            "tg_harvest.ingest.range_harvest.CFG.flood_wait_switch_threshold", 30
        ):
            result = probe_history_access(
                client,
                SimpleNamespace(id=1),
                min_history_message_id=50,
                account_label="secondary",
            )

        self.assertEqual(HistoryAccessProbeResult(True, 100, ""), result)
        self.assertEqual(2, retry_mock.call_count)


if __name__ == "__main__":
    unittest.main()
