import sqlite3
from dataclasses import replace
from types import SimpleNamespace

import pytest

from tg_harvest.admin_jobs.clone_media_copy import (
    CloneMediaTransferContext,
    copy_clone_media_direct_without_source,
    copy_clone_media_via_relay_without_source,
    validate_clone_relay_execution,
)
from tg_harvest.storage.clone import (
    create_clone_migration,
    create_clone_plan,
    create_clone_run,
    ensure_clone_text_delivery,
    record_clone_message_mapping,
)
from tg_harvest.storage.connection import detect_sqlite_features
from tg_harvest.storage.schema import create_schema


def _connect(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _create_context(path, *, relay_chat_id=0):
    conn = _connect(path)
    try:
        create_schema(conn, detect_sqlite_features(conn), skip_fts_auto_heal=1)
        conn.execute(
            """
            INSERT INTO chats(
                chat_id, chat_title, chat_type, message_count, first_seen_at, last_seen_at
            ) VALUES (100, 'Source', 'Megagroup', 1, '2026-01-01', '2026-01-01')
            """
        )
        create_clone_run(
            conn,
            run_id="run-1",
            job_id="run-1",
            source_chat={
                "chat_id": 100,
                "chat_title": "Source",
                "chat_type": "Megagroup",
                "message_count": 1,
            },
            target_title="Target",
            target_kind="megagroup",
            target_owner_session="secondary",
        )
        create_clone_plan(
            conn,
            plan_id="plan-1",
            run_id="run-1",
            job_id="plan-1",
            status="done",
        )
        create_clone_migration(
            conn,
            migration_id="migration-1",
            run_id="run-1",
            plan_id="plan-1",
            job_id="migration-1",
            mode="timeline_replay",
            target_chat_id=777,
        )
    finally:
        conn.close()

    return CloneMediaTransferContext(
        get_conn_fn=lambda: _connect(path),
        migration_id="migration-1",
        run_id="run-1",
        plan_id="plan-1",
        source_chat_id=100,
        target_chat_id=777,
        source_account="primary",
        target_account="secondary",
        relay_chat_id=relay_chat_id,
    )


class _ForwardClient:
    def __init__(
        self,
        *,
        first_id=7000,
        fail=False,
        failure_message="temporary target failure",
    ):
        self.first_id = int(first_id)
        self.fail = bool(fail)
        self.failure_message = str(failure_message)
        self.forward_calls = []
        self.delete_calls = []

    def forward_messages(self, target, messages, **kwargs):
        self.forward_calls.append((target, messages, kwargs))
        if self.fail:
            raise RuntimeError(self.failure_message)
        items = messages if isinstance(messages, list) else [messages]
        return [
            SimpleNamespace(id=self.first_id + index) for index, _ in enumerate(items)
        ]

    def delete_messages(self, target, message_ids, **kwargs):
        self.delete_calls.append((target, message_ids, kwargs))
        return True


def test_direct_media_delivery_reuses_checkpointed_target_after_restart(tmp_path):
    context = _create_context(tmp_path / "direct-transfer.db")
    first_client = _ForwardClient(first_id=8100)

    first_result = copy_clone_media_direct_without_source(
        client=first_client,
        target_entity="target",
        message_ids=11,
        source_entity="source",
        transfer_context=context,
    )

    assert first_result.id == 8100
    assert len(first_client.forward_calls) == 1

    resumed_client = _ForwardClient(first_id=9000)
    resumed_result = copy_clone_media_direct_without_source(
        client=resumed_client,
        target_entity="target",
        message_ids=11,
        source_entity="source",
        transfer_context=context,
    )

    assert resumed_result.id == 8100
    assert resumed_client.forward_calls == []


def test_direct_media_delivery_recovers_valid_partial_response_without_duplication(
    tmp_path,
):
    class _PartialForwardClient(_ForwardClient):
        def forward_messages(self, target, messages, **kwargs):
            self.forward_calls.append((target, messages, kwargs))
            return [SimpleNamespace(id=8100), SimpleNamespace(id=None)]

    context = _create_context(tmp_path / "direct-partial-transfer.db")
    partial_client = _PartialForwardClient()

    with pytest.raises(RuntimeError, match="未完整返回有效消息 ID"):
        copy_clone_media_direct_without_source(
            client=partial_client,
            target_entity="target",
            message_ids=[11, 12],
            source_entity="source",
            transfer_context=context,
        )

    resumed_client = _ForwardClient(first_id=9000)
    resumed_result = copy_clone_media_direct_without_source(
        client=resumed_client,
        target_entity="target",
        message_ids=[11, 12],
        source_entity="source",
        transfer_context=context,
    )

    assert [message.id for message in resumed_result] == [8100, 9000]
    assert resumed_client.forward_calls == [
        (
            "target",
            12,
            {
                "from_peer": "source",
                "drop_author": True,
                "silent": True,
            },
        )
    ]


def test_media_delivery_refuses_cross_account_resume(tmp_path):
    context = _create_context(tmp_path / "direct-account-lock.db")
    copy_clone_media_direct_without_source(
        client=_ForwardClient(first_id=8100),
        target_entity="target",
        message_ids=11,
        source_entity="source",
        transfer_context=context,
    )

    with pytest.raises(RuntimeError, match="拒绝跨账号恢复"):
        copy_clone_media_direct_without_source(
            client=_ForwardClient(first_id=9000),
            target_entity="target",
            message_ids=11,
            source_entity="source",
            transfer_context=replace(context, target_account="primary"),
        )


def test_direct_media_delivery_replans_a_failed_target_hop_for_new_account(tmp_path):
    context = _create_context(tmp_path / "direct-replan-target-account.db")

    with pytest.raises(RuntimeError, match="ChatAdminRequiredError"):
        copy_clone_media_direct_without_source(
            client=_ForwardClient(
                fail=True,
                failure_message="ChatAdminRequiredError: target post permission denied",
            ),
            target_entity="target",
            message_ids=11,
            source_entity="source",
            transfer_context=replace(
                context,
                source_account="primary",
                target_account="primary",
            ),
        )

    resumed_client = _ForwardClient(first_id=8300)
    result = copy_clone_media_direct_without_source(
        client=resumed_client,
        target_entity="target",
        message_ids=11,
        source_entity="source",
        transfer_context=replace(
            context,
            source_account="secondary",
            target_account="secondary",
        ),
    )

    assert result.id == 8300
    assert len(resumed_client.forward_calls) == 1


def test_direct_media_delivery_replans_real_telethon_permission_failure_to_relay(
    tmp_path,
):
    db_path = tmp_path / "direct-to-relay-replan.db"
    context = _create_context(db_path, relay_chat_id=999)
    permission_error = (
        "Chat admin privileges are required to do that in the specified chat "
        "(for example, in a channel which is not yours), or invalid permissions "
        "used for the channel or group"
    )

    with pytest.raises(RuntimeError, match="Chat admin privileges are required"):
        copy_clone_media_direct_without_source(
            client=_ForwardClient(fail=True, failure_message=permission_error),
            target_entity="target",
            message_ids=11,
            source_entity="source",
            transfer_context=replace(
                context,
                source_account="primary",
                target_account="primary",
            ),
        )

    result = copy_clone_media_via_relay_without_source(
        source_client=_ForwardClient(first_id=7200),
        target_client=_ForwardClient(first_id=8300),
        relay_entity_for_source="relay",
        relay_entity_for_target="relay",
        target_entity="target",
        message_ids=11,
        source_entity="source",
        transfer_context=replace(
            context,
            source_account="primary",
            target_account="secondary",
        ),
    )

    assert result.id == 8300
    conn = _connect(db_path)
    try:
        transfer = conn.execute(
            """
            SELECT transfer_strategy, source_account, target_account,
                   source_hop_status, target_hop_status, target_message_id
            FROM admin_clone_media_transfers
            WHERE run_id = 'run-1' AND source_message_id = 11
            """
        ).fetchone()
    finally:
        conn.close()

    assert dict(transfer) == {
        "transfer_strategy": "relay",
        "source_account": "primary",
        "target_account": "secondary",
        "source_hop_status": "sent",
        "target_hop_status": "sent",
        "target_message_id": 8300,
    }


def test_text_delivery_refuses_cross_account_resume(tmp_path):
    db_path = tmp_path / "text-account-lock.db"
    context = _create_context(db_path)
    conn = _connect(db_path)
    try:
        first = ensure_clone_text_delivery(
            conn,
            migration_id=context.migration_id,
            run_id=context.run_id,
            plan_id=context.plan_id,
            source_chat_id=context.source_chat_id,
            source_message_id=11,
            target_chat_id=context.target_chat_id,
            target_account="primary",
            chunk_index=0,
            chunk_count=1,
        )
        with pytest.raises(RuntimeError, match="拒绝跨账号恢复"):
            ensure_clone_text_delivery(
                conn,
                migration_id=context.migration_id,
                run_id=context.run_id,
                plan_id=context.plan_id,
                source_chat_id=context.source_chat_id,
                source_message_id=11,
                target_chat_id=context.target_chat_id,
                target_account="secondary",
                chunk_index=0,
                chunk_count=1,
            )
    finally:
        conn.close()

    assert first["delivery_account"] == "primary"


def test_text_delivery_replans_after_a_confirmed_target_permission_rejection(tmp_path):
    db_path = tmp_path / "text-replan-target-account.db"
    context = _create_context(db_path)
    conn = _connect(db_path)
    try:
        first = ensure_clone_text_delivery(
            conn,
            migration_id=context.migration_id,
            run_id=context.run_id,
            plan_id=context.plan_id,
            source_chat_id=context.source_chat_id,
            source_message_id=11,
            target_chat_id=context.target_chat_id,
            target_account="primary",
            chunk_index=0,
            chunk_count=1,
        )
        record_clone_message_mapping(
            conn,
            migration_id=context.migration_id,
            run_id=context.run_id,
            plan_id=context.plan_id,
            source_chat_id=context.source_chat_id,
            source_message_id=11,
            target_chat_id=context.target_chat_id,
            target_message_id=None,
            chunk_index=0,
            chunk_count=1,
            mode="text_replay",
            status="error",
            error_message="ChatAdminRequiredError: target post permission denied",
            delivery_random_id=first["delivery_random_id"],
            delivery_account="primary",
        )

        replanned = ensure_clone_text_delivery(
            conn,
            migration_id=context.migration_id,
            run_id=context.run_id,
            plan_id=context.plan_id,
            source_chat_id=context.source_chat_id,
            source_message_id=11,
            target_chat_id=context.target_chat_id,
            target_account="secondary",
            chunk_index=0,
            chunk_count=1,
        )
    finally:
        conn.close()

    assert replanned["status"] == "pending"
    assert replanned["delivery_account"] == "secondary"
    assert replanned["target_message_id"] is None
    assert replanned["delivery_random_id"] != first["delivery_random_id"]


def test_relay_delivery_resumes_second_hop_without_repeating_first_hop(tmp_path):
    context = _create_context(tmp_path / "relay-transfer.db", relay_chat_id=999)
    source_client = _ForwardClient(first_id=7100)
    failing_target_client = _ForwardClient(first_id=8100, fail=True)

    with pytest.raises(RuntimeError, match="temporary target failure"):
        copy_clone_media_via_relay_without_source(
            source_client=source_client,
            target_client=failing_target_client,
            relay_entity_for_source="relay",
            relay_entity_for_target="relay",
            target_entity="target",
            message_ids=[21, 22],
            source_entity="source",
            transfer_context=context,
        )

    assert len(source_client.forward_calls) == 1
    assert source_client.delete_calls == []

    resumed_source_client = _ForwardClient(first_id=7200)
    resumed_target_client = _ForwardClient(first_id=8200)
    result = copy_clone_media_via_relay_without_source(
        source_client=resumed_source_client,
        target_client=resumed_target_client,
        relay_entity_for_source="relay",
        relay_entity_for_target="relay",
        target_entity="target",
        message_ids=[21, 22],
        source_entity="source",
        transfer_context=context,
    )

    assert [message.id for message in result] == [8200, 8201]
    assert resumed_source_client.forward_calls == []
    assert len(resumed_target_client.forward_calls) == 1
    assert resumed_source_client.delete_calls == [
        ("relay", [7100, 7101], {"revoke": True})
    ]


def test_relay_delivery_stops_when_second_account_cannot_read_target_message(
    tmp_path,
    monkeypatch,
):
    class _InvisibleTargetClient(_ForwardClient):
        def get_messages(self, _target, *, ids):
            items = ids if isinstance(ids, list) else [ids]
            return [None for _ in items]

    class _VisibleTargetClient(_ForwardClient):
        def get_messages(self, _target, *, ids):
            items = ids if isinstance(ids, list) else [ids]
            return [SimpleNamespace(id=int(message_id)) for message_id in items]

    monkeypatch.setattr(
        "tg_harvest.admin_jobs.clone_media_copy.time.sleep",
        lambda *_args: None,
    )
    db_path = tmp_path / "relay-target-unconfirmed.db"
    context = _create_context(db_path, relay_chat_id=999)
    source_client = _ForwardClient(first_id=7100)
    invisible_target = _InvisibleTargetClient(first_id=8100)

    with pytest.raises(RuntimeError, match="第二账号无法从克隆群回读"):
        copy_clone_media_via_relay_without_source(
            source_client=source_client,
            target_client=invisible_target,
            relay_entity_for_source="relay",
            relay_entity_for_target="relay",
            target_entity="target",
            message_ids=21,
            source_entity="source",
            transfer_context=context,
        )

    conn = _connect(db_path)
    try:
        transfer = conn.execute(
            """
            SELECT target_hop_status, target_message_id, cleanup_status
            FROM admin_clone_media_transfers
            WHERE run_id = 'run-1' AND source_message_id = 21
            """
        ).fetchone()
    finally:
        conn.close()

    assert dict(transfer) == {
        "target_hop_status": "unconfirmed",
        "target_message_id": 8100,
        "cleanup_status": "pending",
    }
    assert source_client.delete_calls == []

    resumed_source = _ForwardClient(first_id=7200)
    visible_target = _VisibleTargetClient(first_id=9000)
    result = copy_clone_media_via_relay_without_source(
        source_client=resumed_source,
        target_client=visible_target,
        relay_entity_for_source="relay",
        relay_entity_for_target="relay",
        target_entity="target",
        message_ids=21,
        source_entity="source",
        transfer_context=context,
    )

    assert result.id == 8100
    assert resumed_source.forward_calls == []
    assert visible_target.forward_calls == []
    assert resumed_source.delete_calls == [("relay", [7100], {"revoke": True})]


def test_relay_cleanup_failure_is_fatal_and_retried_without_reforwarding(tmp_path):
    class _CleanupFailClient(_ForwardClient):
        def delete_messages(self, target, message_ids, **kwargs):
            self.delete_calls.append((target, message_ids, kwargs))
            raise RuntimeError("delete permission denied")

    context = _create_context(tmp_path / "relay-cleanup-retry.db", relay_chat_id=999)
    failing_source = _CleanupFailClient(first_id=7100)
    target_client = _ForwardClient(first_id=8100)

    with pytest.raises(RuntimeError, match="中转临时消息清理失败"):
        copy_clone_media_via_relay_without_source(
            source_client=failing_source,
            target_client=target_client,
            relay_entity_for_source="relay",
            relay_entity_for_target="relay",
            target_entity="target",
            message_ids=21,
            source_entity="source",
            transfer_context=context,
        )

    resumed_source = _ForwardClient(first_id=7200)
    resumed_target = _ForwardClient(first_id=9000)
    result = copy_clone_media_via_relay_without_source(
        source_client=resumed_source,
        target_client=resumed_target,
        relay_entity_for_source="relay",
        relay_entity_for_target="relay",
        target_entity="target",
        message_ids=21,
        source_entity="source",
        transfer_context=context,
    )

    assert result.id == 8100
    assert resumed_source.forward_calls == []
    assert resumed_target.forward_calls == []
    assert resumed_source.delete_calls == [("relay", [7100], {"revoke": True})]


def test_relay_execution_rejects_source_identity_and_extra_participants():
    relay = SimpleNamespace(
        id=999,
        username="",
        broadcast=True,
        megagroup=False,
        participants_count=2,
        creator=True,
    )

    with pytest.raises(RuntimeError, match="不能与源群或克隆目标相同"):
        validate_clone_relay_execution(
            relay_entity_for_source=relay,
            relay_entity_for_target=relay,
            relay_chat_id=999,
            source_chat_id=999,
            target_chat_id=777,
        )

    relay.participants_count = 3
    with pytest.raises(RuntimeError, match="存在额外成员"):
        validate_clone_relay_execution(
            relay_entity_for_source=relay,
            relay_entity_for_target=relay,
            relay_chat_id=999,
            source_chat_id=100,
            target_chat_id=777,
        )


def test_relay_delivery_replans_unsent_target_hop_for_new_target_account(tmp_path):
    context = _create_context(tmp_path / "relay-replan-target-account.db", relay_chat_id=999)
    source_client = _ForwardClient(first_id=7100)

    with pytest.raises(RuntimeError, match="ChatAdminRequiredError"):
        copy_clone_media_via_relay_without_source(
            source_client=source_client,
            target_client=_ForwardClient(
                fail=True,
                failure_message="ChatAdminRequiredError: target post permission denied",
            ),
            relay_entity_for_source="relay",
            relay_entity_for_target="relay",
            target_entity="target",
            message_ids=21,
            source_entity="source",
            transfer_context=replace(context, target_account="primary"),
        )

    resumed_source_client = _ForwardClient(first_id=7200)
    resumed_target_client = _ForwardClient(first_id=8300)
    result = copy_clone_media_via_relay_without_source(
        source_client=resumed_source_client,
        target_client=resumed_target_client,
        relay_entity_for_source="relay",
        relay_entity_for_target="relay",
        target_entity="target",
        message_ids=21,
        source_entity="source",
        transfer_context=replace(context, target_account="secondary"),
    )

    assert result.id == 8300
    assert resumed_source_client.forward_calls == []
    assert len(resumed_target_client.forward_calls) == 1
