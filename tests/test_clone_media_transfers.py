import sqlite3
from dataclasses import replace
from types import SimpleNamespace

import pytest

from tg_harvest.admin_jobs.clone_media_copy import (
    CloneMediaTransferContext,
    copy_clone_media_direct_without_source,
    copy_clone_media_via_relay_without_source,
)
from tg_harvest.storage.clone import (
    create_clone_migration,
    create_clone_plan,
    create_clone_run,
    ensure_clone_text_delivery,
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
    def __init__(self, *, first_id=7000, fail=False):
        self.first_id = int(first_id)
        self.fail = bool(fail)
        self.forward_calls = []
        self.delete_calls = []

    def forward_messages(self, target, messages, **kwargs):
        self.forward_calls.append((target, messages, kwargs))
        if self.fail:
            raise RuntimeError("temporary target failure")
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
