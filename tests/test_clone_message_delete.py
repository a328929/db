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


def _rewind_result(*_args, **kwargs):
    return {
        "selected_target_message_count": len(kwargs["target_message_ids"]),
        "rewound_mapping_count": 0,
        "rewound_done_mapping_count": 0,
        "rewound_text_mapping_count": 0,
        "rewound_media_mapping_count": 0,
        "rewound_media_transfer_count": 0,
        "unmapped_target_message_count": len(kwargs["target_message_ids"]),
        "first_rewound_source_message_id": 0,
    }


def _tail_selection_result(target_message_ids=(905, 901, 899)):
    target_ids = list(target_message_ids)
    source_ids = list(range(6000, 6000 - len(target_ids), -1))
    return {
        "requested_source_message_count": 5,
        "selected_source_message_count": len(source_ids),
        "selected_target_message_count": len(target_ids),
        "first_source_message_id": min(source_ids, default=0),
        "last_source_message_id": max(source_ids, default=0),
        "source_message_ids": source_ids,
        "target_message_ids": target_ids,
    }


def _run_job(
    client,
    selection,
    *,
    clone_run=None,
    tail_selection_result=None,
    rewind_side_effect=None,
):
    statuses = []
    logs = []

    class Connection:
        def close(self):
            pass

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
        patch(
            "tg_harvest.admin_jobs.clone_message_delete."
            "load_clone_tail_delete_selection",
            return_value=(
                _tail_selection_result()
                if tail_selection_result is None
                else tail_selection_result
            ),
        ) as load_tail_selection,
        patch(
            "tg_harvest.admin_jobs.clone_message_delete."
            "rewind_clone_mappings_for_deleted_target_messages",
            side_effect=rewind_side_effect or _rewind_result,
        ) as rewind_mappings,
        patch("tg_harvest.admin_jobs.clone_message_delete._disconnect_worker_client"),
        patch("tg_harvest.admin_jobs.clone_message_delete._cleanup_isolated_worker_session"),
    ):
        _admin_clone_message_delete_job_runner(
            "job-message-delete",
            clone_run=clone_run or _clone_run(),
            selection=selection,
            delete_delay_ms=0,
            cfg=_cfg(),
            get_conn_fn=Connection,
            admin_job_set_status_fn=lambda _job_id, status: statuses.append(status),
            admin_job_append_log_fn=lambda _job_id, message: logs.append(str(message)),
        )
    return (
        statuses,
        logs,
        create_client,
        update_progress,
        load_tail_selection,
        rewind_mappings,
    )


def test_clone_message_delete_never_targets_source_identity():
    run = _clone_run()
    run["source_chat_id"] = -100777
    run["source_chat_type"] = "Megagroup"

    (
        statuses,
        logs,
        create_client,
        _update_progress,
        load_tail_selection,
        _rewind_mappings,
    ) = _run_job(
        client=None,
        selection=parse_clone_message_delete_selection("5"),
        clone_run=run,
    )

    assert statuses == ["running", "error"]
    assert not create_client.called
    assert not load_tail_selection.called
    assert any("与源群 ID 冲突" in message for message in logs)


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


def test_latest_message_delete_uses_mapped_clone_tail_and_ignores_remote_posts():
    class Client:
        def __init__(self):
            self.iter_calls = []
            self.delete_calls = []

        def iter_messages(self, *_args, **_kwargs):
            raise AssertionError("clone-tail rollback must not scan remote latest posts")

        def delete_messages(self, target, message_ids, *, revoke):
            self.delete_calls.append((target, list(message_ids), revoke))
            return []

    client = Client()
    (
        statuses,
        logs,
        create_client,
        update_progress,
        load_tail_selection,
        rewind_mappings,
    ) = _run_job(
        client,
        parse_clone_message_delete_selection("5"),
    )

    assert statuses == ["running", "done"]
    assert create_client.call_args.args[0].session_name == "secondary"
    target = client.delete_calls[0][0]
    assert isinstance(target, InputChannel)
    assert target.channel_id == 777
    assert target.access_hash == 123
    assert client.delete_calls == [(target, [905, 901, 899], True)]
    assert load_tail_selection.call_args.kwargs["source_message_limit"] == 5
    assert rewind_mappings.call_count == 1
    assert any("3/5 条已克隆源消息" in message for message in logs)
    assert any("目标群公告等未映射消息不会参与计数或删除" in message for message in logs)
    assert any("已回退 0 条本地映射" in message for message in logs)
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
    (
        statuses,
        logs,
        _create_client,
        _update_progress,
        load_tail_selection,
        rewind_mappings,
    ) = _run_job(
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
    assert load_tail_selection.call_count == 0
    assert rewind_mappings.call_count == 0
    assert any("区间清理不会修改克隆映射" in message for message in logs)
    assert any("克隆映射未修改" in message for message in logs)


def test_latest_message_delete_marks_empty_target_as_completed():
    class Client:
        def iter_messages(self, *_args, **_kwargs):
            return iter(())

        def delete_messages(self, *_args, **_kwargs):
            raise AssertionError("empty target must not issue a delete request")

    empty_selection = _tail_selection_result(())
    statuses, logs, create_client, update_progress, _load_tail, _rewind = _run_job(
        Client(),
        parse_clone_message_delete_selection("1000"),
        tail_selection_result=empty_selection,
    )

    assert statuses == ["running", "done"]
    assert create_client.call_count == 0
    assert any("没有可回滚的已完成消息" in message for message in logs)
    assert update_progress.call_args_list[-1].kwargs == {
        "total": 0,
        "stage": "done",
    }


def test_mapping_rewind_failure_stops_after_the_confirmed_remote_batch():
    class Client:
        def __init__(self):
            self.delete_calls = []

        def delete_messages(self, target, message_ids, *, revoke):
            self.delete_calls.append((target, list(message_ids), revoke))
            return []

    def fail_rewind(*_args, **_kwargs):
        raise RuntimeError("database unavailable")

    client = Client()
    tail_selection = _tail_selection_result(range(1200, 1000, -1))
    statuses, logs, _create_client, _update_progress, _load_tail, _rewind = _run_job(
        client,
        parse_clone_message_delete_selection("200"),
        tail_selection_result=tail_selection,
        rewind_side_effect=fail_rewind,
    )

    assert statuses == ["running", "error"]
    assert [call[1] for call in client.delete_calls] == [list(range(1200, 1100, -1))]
    assert any("本地续克隆状态回退失败" in message for message in logs)
