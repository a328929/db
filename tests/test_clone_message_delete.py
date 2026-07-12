from types import SimpleNamespace
from unittest.mock import patch

from telethon.tl.types import InputChannel

from tg_harvest.admin_jobs.clone_message_delete import (
    _admin_clone_message_delete_job_runner,
)
from tg_harvest.domain.clone_message_delete import (
    CLONE_MESSAGE_DELETE_MAX_COUNT,
    parse_clone_message_delete_selection,
)


def _heartbeat_pair():
    return SimpleNamespace(set=lambda: None), SimpleNamespace(join=lambda timeout=None: None)


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
        "run_id": "run-message-delete",
        "target_chat_id": 777,
        "target_access_hash": "123",
        "target_title": "Clone Target",
    }


def _run_job(client, selection):
    statuses = []
    logs = []
    with (
        patch(
            "tg_harvest.admin_jobs.clone_message_delete.start_admin_job_heartbeat",
            return_value=_heartbeat_pair(),
        ),
        patch(
            "tg_harvest.admin_jobs.clone_message_delete.update_admin_job_progress"
        ) as update_progress,
        patch(
            "tg_harvest.admin_jobs.clone_message_delete._ensure_base_session_valid",
            return_value=True,
        ),
        patch(
            "tg_harvest.admin_jobs.clone_message_delete._create_isolated_worker_client",
            return_value=client,
        ) as create_client,
        patch("tg_harvest.admin_jobs.clone_message_delete._disconnect_worker_client"),
        patch("tg_harvest.admin_jobs.clone_message_delete._cleanup_isolated_worker_session"),
    ):
        _admin_clone_message_delete_job_runner(
            "job-message-delete",
            clone_run=_clone_run(),
            selection=selection,
            delete_delay_ms=0,
            cfg=_cfg(),
            admin_job_set_status_fn=lambda _job_id, status: statuses.append(status),
            admin_job_append_log_fn=lambda _job_id, message: logs.append(str(message)),
        )
    return statuses, logs, create_client, update_progress


def test_clone_message_delete_selection_parses_latest_and_forward_range():
    latest = parse_clone_message_delete_selection("1000")
    assert latest.mode == "latest"
    assert latest.requested_count == 1000
    assert latest.first_message_id is None

    message_range = parse_clone_message_delete_selection("200-1000")
    assert message_range.mode == "range"
    assert message_range.first_message_id == 200
    assert message_range.last_message_id == 1000
    assert message_range.requested_count == 801


def test_clone_message_delete_selection_rejects_invalid_or_excessive_ranges():
    for value in ("", "0", "1000-200", "1-100001"):
        try:
            parse_clone_message_delete_selection(value)
        except ValueError:
            pass
        else:
            raise AssertionError(f"expected ValueError for {value!r}")

    try:
        parse_clone_message_delete_selection(str(CLONE_MESSAGE_DELETE_MAX_COUNT + 1))
    except ValueError:
        pass
    else:
        raise AssertionError("expected count limit validation")


def test_latest_message_delete_uses_secondary_account_and_latest_message_ids():
    class Client:
        def __init__(self):
            self.iter_calls = []
            self.delete_calls = []

        def iter_messages(self, target, *, limit, wait_time):
            self.iter_calls.append((target, limit, wait_time))
            return iter(
                [
                    SimpleNamespace(id=905),
                    SimpleNamespace(id=901),
                    SimpleNamespace(id=899),
                ]
            )

        def delete_messages(self, target, message_ids, *, revoke):
            self.delete_calls.append((target, list(message_ids), revoke))
            return []

    client = Client()
    statuses, logs, create_client, update_progress = _run_job(
        client,
        parse_clone_message_delete_selection("5"),
    )

    assert statuses == ["running", "done"]
    assert create_client.call_args.args[0].session_name == "secondary"
    assert len(client.iter_calls) == 1
    target, limit, wait_time = client.iter_calls[0]
    assert isinstance(target, InputChannel)
    assert target.channel_id == 777
    assert target.access_hash == 123
    assert limit == 5
    assert wait_time == 0
    assert client.delete_calls == [(target, [905, 901, 899], True)]
    assert any("最新到最早顺序锁定 3 条" in message for message in logs)
    assert any("本地克隆映射保持不变" in message for message in logs)
    assert any(
        call.kwargs.get("total") == 3 for call in update_progress.call_args_list
    )


def test_range_message_delete_submits_ids_in_ascending_batches():
    class Client:
        def __init__(self):
            self.iter_called = False
            self.delete_calls = []

        def iter_messages(self, *_args, **_kwargs):
            self.iter_called = True
            return iter(())

        def delete_messages(self, target, message_ids, *, revoke):
            self.delete_calls.append((target, list(message_ids), revoke))
            return []

    client = Client()
    statuses, logs, _create_client, _update_progress = _run_job(
        client,
        parse_clone_message_delete_selection("200-399"),
    )

    assert statuses == ["running", "done"]
    assert not client.iter_called
    assert [call[1] for call in client.delete_calls] == [
        list(range(200, 300)),
        list(range(300, 400)),
    ]
    assert all(call[2] is True for call in client.delete_calls)
    assert any("消息 ID 从小到大" in message for message in logs)


def test_latest_message_delete_marks_empty_target_as_completed():
    class Client:
        def iter_messages(self, *_args, **_kwargs):
            return iter(())

        def delete_messages(self, *_args, **_kwargs):
            raise AssertionError("empty target must not issue a delete request")

    statuses, logs, _create_client, update_progress = _run_job(
        Client(),
        parse_clone_message_delete_selection("1000"),
    )

    assert statuses == ["running", "done"]
    assert any("没有可删除消息" in message for message in logs)
    assert update_progress.call_args_list[-1].kwargs == {
        "total": 0,
        "stage": "done",
    }
