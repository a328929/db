from types import SimpleNamespace
from unittest.mock import patch

from telethon.tl.types import InputChannel

from tg_harvest.admin_jobs.clone_target_metrics import (
    load_clone_target_message_count,
)


def _cfg():
    return SimpleNamespace(
        session_name="primary",
        secondary_session_name="secondary",
        api_id=1,
        api_hash="hash",
        flood_wait_switch_threshold=30,
    )


def _clone_run():
    return {
        "target_chat_id": 777,
        "target_access_hash": "123",
    }


def test_clone_target_message_count_uses_second_account_and_total_only_history_request():
    class Client:
        def __init__(self):
            self.calls = []

        def get_messages(self, target, *, limit):
            self.calls.append((target, limit))
            return SimpleNamespace(total=4321)

    client = Client()
    with (
        patch(
            "tg_harvest.admin_jobs.clone_target_metrics._ensure_base_session_valid",
            return_value=True,
        ),
        patch(
            "tg_harvest.admin_jobs.clone_target_metrics._create_isolated_worker_client",
            return_value=client,
        ) as create_client,
        patch("tg_harvest.admin_jobs.clone_target_metrics._disconnect_worker_client"),
        patch("tg_harvest.admin_jobs.clone_target_metrics._cleanup_isolated_worker_session"),
    ):
        message_count = load_clone_target_message_count(_clone_run(), cfg=_cfg())

    assert message_count == 4321
    assert create_client.call_args.args[0].session_name == "secondary"
    assert len(client.calls) == 1
    target, limit = client.calls[0]
    assert isinstance(target, InputChannel)
    assert target.channel_id == 777
    assert target.access_hash == 123
    assert limit == 0


def test_clone_target_message_count_rejects_run_without_target():
    try:
        load_clone_target_message_count({"target_chat_id": None}, cfg=_cfg())
    except ValueError as exc:
        assert "尚未创建" in str(exc)
    else:
        raise AssertionError("expected invalid target to be rejected")
