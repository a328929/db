import sqlite3

from tg_harvest.domain.clone_plan import CLONE_TEXT_REPLAY_CHUNK_MAX_LEN
from tg_harvest.storage.clone import (
    CLONE_MEDIA_TRANSFER_DIRECT,
    CLONE_MEDIA_TRANSFER_RELAY,
    create_clone_migration,
    create_clone_plan,
    create_clone_run,
    ensure_clone_media_transfers,
    list_clone_timeline_replay_batch,
    load_clone_tail_delete_selection,
    mark_clone_media_transfer_source_hop_sent,
    mark_clone_media_transfer_target_hop_sent,
    mark_clone_run_message_reset_required,
    record_clone_message_mapping,
    reset_clone_run_timeline,
    rewind_clone_mappings_for_deleted_target_messages,
    update_clone_run,
)
from tg_harvest.storage.connection import detect_sqlite_features
from tg_harvest.storage.schema import create_schema


def _connect(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _create_clone_context(path):
    conn = _connect(path)
    create_schema(conn, detect_sqlite_features(conn))
    conn.execute(
        """
        INSERT INTO chats(
            chat_id, chat_title, chat_type, message_count, first_seen_at, last_seen_at
        ) VALUES (100, 'Source Group', 'Megagroup', 0, '2026-01-01', '2026-01-01')
        """
    )
    source_chat = {
        "chat_id": 100,
        "chat_title": "Source Group",
        "chat_type": "Megagroup",
        "message_count": 0,
        "last_message_at": "2026-01-01",
        "last_message_ts": 0,
    }
    create_clone_run(
        conn,
        run_id="run-rewind",
        job_id="job-run-rewind",
        source_chat=source_chat,
        target_title="Clone Target",
        target_kind="megagroup",
        target_owner_session="secondary",
        plan={},
    )
    update_clone_run(
        conn,
        run_id="run-rewind",
        status="done",
        phase="done",
        target_chat_id=777,
        target_access_hash="123",
        completed_at="2026-01-01T00:00:00+00:00",
    )
    create_clone_plan(
        conn,
        plan_id="plan-rewind",
        run_id="run-rewind",
        job_id="job-plan-rewind",
        status="done",
        plan={},
    )
    create_clone_migration(
        conn,
        migration_id="migration-rewind",
        run_id="run-rewind",
        plan_id="plan-rewind",
        job_id="job-migration-rewind",
        mode="timeline_replay",
        target_chat_id=777,
        target_title="Clone Target",
    )
    return conn


def _insert_message(
    conn,
    *,
    message_id: int,
    content: str = "message",
    has_media: int = 0,
    grouped_id: int | None = None,
):
    conn.execute(
        """
        INSERT INTO messages(
            chat_id, message_id, msg_date_text, msg_date_ts, content,
            content_norm, msg_type, grouped_id, has_media
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            100,
            message_id,
            f"2026-01-01 00:00:{message_id:02d}",
            message_id,
            content,
            content,
            "PHOTO" if has_media else "TEXT",
            grouped_id,
            has_media,
        ),
    )


def _record_mapping(
    conn,
    *,
    source_message_id: int,
    target_message_id: int,
    mode: str = "text_replay",
    chunk_index: int = 0,
    chunk_count: int = 1,
):
    record_clone_message_mapping(
        conn,
        migration_id="migration-rewind",
        run_id="run-rewind",
        plan_id="plan-rewind",
        source_chat_id=100,
        source_message_id=source_message_id,
        source_msg_date_ts=source_message_id,
        source_msg_date_text=f"2026-01-01 00:00:{source_message_id:02d}",
        target_chat_id=777,
        target_message_id=target_message_id,
        chunk_index=chunk_index,
        chunk_count=chunk_count,
        mode=mode,
        status="done",
    )


def test_rewind_deleted_target_messages_replays_from_the_first_deleted_source_item(
    tmp_path,
):
    conn = _create_clone_context(tmp_path / "rewind-text.db")
    try:
        for source_message_id in range(1, 7):
            _insert_message(conn, message_id=source_message_id)
            _record_mapping(
                conn,
                source_message_id=source_message_id,
                target_message_id=9000 + source_message_id,
            )

        rewind = rewind_clone_mappings_for_deleted_target_messages(
            conn,
            run_id="run-rewind",
            target_chat_id=777,
            target_message_ids=[9005, 9006],
        )
        remaining = list_clone_timeline_replay_batch(
            conn,
            run_id="run-rewind",
            chat_id=100,
            limit=10,
        )
    finally:
        conn.close()

    assert rewind == {
        "selected_target_message_count": 2,
        "rewound_mapping_count": 2,
        "rewound_done_mapping_count": 2,
        "rewound_text_mapping_count": 2,
        "rewound_media_mapping_count": 0,
        "rewound_media_transfer_count": 0,
        "unmapped_target_message_count": 0,
        "first_rewound_source_message_id": 5,
    }
    assert [item["source_message_id"] for item in remaining] == [5, 6]


def test_clone_tail_selection_counts_source_mappings_not_target_message_ids(tmp_path):
    conn = _create_clone_context(tmp_path / "clone-tail-selection.db")
    try:
        target_ids = (101, 250, 900, 1200, 9000, 15000)
        for source_message_id, target_message_id in enumerate(target_ids, start=1):
            _insert_message(conn, message_id=source_message_id)
            _record_mapping(
                conn,
                source_message_id=source_message_id,
                target_message_id=target_message_id,
            )

        selection = load_clone_tail_delete_selection(
            conn,
            run_id="run-rewind",
            target_chat_id=777,
            source_message_limit=2,
        )
        rewind_clone_mappings_for_deleted_target_messages(
            conn,
            run_id="run-rewind",
            target_chat_id=777,
            target_message_ids=selection["target_message_ids"],
        )
        remaining = list_clone_timeline_replay_batch(
            conn,
            run_id="run-rewind",
            chat_id=100,
            limit=10,
        )
    finally:
        conn.close()

    assert selection == {
        "requested_source_message_count": 2,
        "selected_source_message_count": 2,
        "selected_target_message_count": 2,
        "first_source_message_id": 5,
        "last_source_message_id": 6,
        "source_message_ids": [6, 5],
        "target_message_ids": [15000, 9000],
    }
    assert [item["source_message_id"] for item in remaining] == [5, 6]


def test_clone_tail_selection_counts_long_text_once_and_deletes_all_chunks(tmp_path):
    conn = _create_clone_context(tmp_path / "clone-tail-long-text.db")
    try:
        _insert_message(conn, message_id=1)
        _insert_message(
            conn,
            message_id=2,
            content="x" * (CLONE_TEXT_REPLAY_CHUNK_MAX_LEN + 1),
        )
        _record_mapping(
            conn,
            source_message_id=1,
            target_message_id=101,
        )
        _record_mapping(
            conn,
            source_message_id=2,
            target_message_id=201,
            chunk_index=0,
            chunk_count=2,
        )
        _record_mapping(
            conn,
            source_message_id=2,
            target_message_id=202,
            chunk_index=1,
            chunk_count=2,
        )

        selection = load_clone_tail_delete_selection(
            conn,
            run_id="run-rewind",
            target_chat_id=777,
            source_message_limit=1,
        )
    finally:
        conn.close()

    assert selection["selected_source_message_count"] == 1
    assert selection["selected_target_message_count"] == 2
    assert selection["source_message_ids"] == [2]
    assert selection["target_message_ids"] == [202, 201]


def test_clone_tail_selection_rolls_6000_messages_back_to_source_4001(tmp_path):
    conn = _create_clone_context(tmp_path / "clone-tail-6000.db")
    try:
        conn.executemany(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, content,
                content_norm, msg_type, grouped_id, has_media
            ) VALUES (100, ?, '', ?, ?, ?, 'TEXT', NULL, 0)
            """,
            (
                (
                    message_id,
                    message_id,
                    f"message-{message_id}",
                    f"message-{message_id}",
                )
                for message_id in range(1, 6001)
            ),
        )
        conn.executemany(
            """
            INSERT INTO admin_clone_message_map(
                migration_id, run_id, plan_id,
                source_chat_id, source_message_id, source_msg_date_ts,
                target_chat_id, target_message_id,
                chunk_index, chunk_count, mode, status,
                created_at, updated_at
            ) VALUES (
                'migration-rewind', 'run-rewind', 'plan-rewind',
                100, ?, ?, 777, ?, 0, 1, 'text_replay', 'done',
                '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00'
            )
            """,
            ((message_id, message_id, message_id * 2) for message_id in range(1, 6001)),
        )
        conn.commit()

        selection = load_clone_tail_delete_selection(
            conn,
            run_id="run-rewind",
            target_chat_id=777,
            source_message_limit=2000,
        )
        rewind_clone_mappings_for_deleted_target_messages(
            conn,
            run_id="run-rewind",
            target_chat_id=777,
            target_message_ids=selection["target_message_ids"],
        )
        first_remaining = list_clone_timeline_replay_batch(
            conn,
            run_id="run-rewind",
            chat_id=100,
            limit=1,
        )
    finally:
        conn.close()

    assert selection["selected_source_message_count"] == 2000
    assert selection["selected_target_message_count"] == 2000
    assert selection["first_source_message_id"] == 4001
    assert selection["last_source_message_id"] == 6000
    assert selection["source_message_ids"][:2] == [6000, 5999]
    assert selection["target_message_ids"][:2] == [12000, 11998]
    assert [item["source_message_id"] for item in first_remaining] == [4001]


def test_rewind_one_text_chunk_replays_only_the_source_message_with_the_gap(tmp_path):
    conn = _create_clone_context(tmp_path / "rewind-text-chunk.db")
    try:
        _insert_message(
            conn,
            message_id=1,
            content="x" * (CLONE_TEXT_REPLAY_CHUNK_MAX_LEN + 1),
        )
        _record_mapping(
            conn,
            source_message_id=1,
            target_message_id=9101,
            chunk_index=0,
            chunk_count=2,
        )
        _record_mapping(
            conn,
            source_message_id=1,
            target_message_id=9102,
            chunk_index=1,
            chunk_count=2,
        )

        rewind = rewind_clone_mappings_for_deleted_target_messages(
            conn,
            run_id="run-rewind",
            target_chat_id=777,
            target_message_ids=[9102],
        )
        remaining = list_clone_timeline_replay_batch(
            conn,
            run_id="run-rewind",
            chat_id=100,
            limit=10,
        )
        remaining_chunk_indexes = conn.execute(
            """
            SELECT chunk_index
            FROM admin_clone_message_map
            WHERE run_id = 'run-rewind' AND source_message_id = 1
            ORDER BY chunk_index
            """
        ).fetchall()
    finally:
        conn.close()

    assert rewind["rewound_mapping_count"] == 1
    assert rewind["rewound_text_mapping_count"] == 1
    assert [item["source_message_id"] for item in remaining] == [1]
    assert [row["chunk_index"] for row in remaining_chunk_indexes] == [0]


def test_rewind_media_group_member_clears_only_its_delivery_intent(tmp_path):
    conn = _create_clone_context(tmp_path / "rewind-media-group.db")
    try:
        for source_message_id in (1, 2):
            _insert_message(
                conn,
                message_id=source_message_id,
                content="",
                has_media=1,
                grouped_id=123,
            )
            _record_mapping(
                conn,
                source_message_id=source_message_id,
                target_message_id=9200 + source_message_id,
                mode="media_group_copy",
            )
        transfers = ensure_clone_media_transfers(
            conn,
            migration_id="migration-rewind",
            run_id="run-rewind",
            plan_id="plan-rewind",
            source_chat_id=100,
            source_message_ids=[1, 2],
            target_chat_id=777,
            transfer_strategy=CLONE_MEDIA_TRANSFER_DIRECT,
        )
        old_target_random_id = int(transfers[1]["target_random_id"])
        mark_clone_media_transfer_target_hop_sent(
            conn,
            run_id="run-rewind",
            source_chat_id=100,
            target_message_ids_by_source={1: 9201, 2: 9202},
        )

        rewind = rewind_clone_mappings_for_deleted_target_messages(
            conn,
            run_id="run-rewind",
            target_chat_id=777,
            target_message_ids=[9202, 9999],
        )
        remaining = list_clone_timeline_replay_batch(
            conn,
            run_id="run-rewind",
            chat_id=100,
            limit=10,
        )
        existing_transfer_sources = conn.execute(
            """
            SELECT source_message_id
            FROM admin_clone_media_transfers
            WHERE run_id = 'run-rewind'
            ORDER BY source_message_id
            """
        ).fetchall()
        recreated_transfer = ensure_clone_media_transfers(
            conn,
            migration_id="migration-rewind",
            run_id="run-rewind",
            plan_id="plan-rewind",
            source_chat_id=100,
            source_message_ids=[2],
            target_chat_id=777,
            transfer_strategy=CLONE_MEDIA_TRANSFER_DIRECT,
        )[0]
    finally:
        conn.close()

    assert rewind["rewound_mapping_count"] == 1
    assert rewind["rewound_media_mapping_count"] == 1
    assert rewind["rewound_media_transfer_count"] == 1
    assert rewind["unmapped_target_message_count"] == 1
    assert [item["item_type"] for item in remaining] == ["media_group"]
    assert [row["source_message_id"] for row in existing_transfer_sources] == [1]
    assert recreated_transfer["target_message_id"] is None
    assert recreated_transfer["target_hop_status"] == "pending"
    assert int(recreated_transfer["target_random_id"]) != old_target_random_id


def test_rewind_reuses_uncleaned_relay_message_with_a_fresh_target_random_id(tmp_path):
    conn = _create_clone_context(tmp_path / "rewind-relay.db")
    try:
        _insert_message(conn, message_id=1, content="", has_media=1)
        _record_mapping(
            conn,
            source_message_id=1,
            target_message_id=9301,
            mode="media_copy",
        )
        transfer = ensure_clone_media_transfers(
            conn,
            migration_id="migration-rewind",
            run_id="run-rewind",
            plan_id="plan-rewind",
            source_chat_id=100,
            source_message_ids=[1],
            target_chat_id=777,
            transfer_strategy=CLONE_MEDIA_TRANSFER_RELAY,
            relay_chat_id=999,
            source_account="primary",
            target_account="secondary",
        )[0]
        old_target_random_id = int(transfer["target_random_id"])
        mark_clone_media_transfer_source_hop_sent(
            conn,
            run_id="run-rewind",
            source_chat_id=100,
            relay_message_ids_by_source={1: 8101},
        )
        mark_clone_media_transfer_target_hop_sent(
            conn,
            run_id="run-rewind",
            source_chat_id=100,
            target_message_ids_by_source={1: 9301},
        )

        rewind = rewind_clone_mappings_for_deleted_target_messages(
            conn,
            run_id="run-rewind",
            target_chat_id=777,
            target_message_ids=[9301],
        )
        retained_transfer = conn.execute(
            """
            SELECT *
            FROM admin_clone_media_transfers
            WHERE run_id = 'run-rewind' AND source_message_id = 1
            """
        ).fetchone()
    finally:
        conn.close()

    assert rewind["rewound_media_transfer_count"] == 1
    assert retained_transfer is not None
    assert retained_transfer["source_hop_status"] == "sent"
    assert retained_transfer["relay_message_id"] == 8101
    assert retained_transfer["target_hop_status"] == "pending"
    assert retained_transfer["target_message_id"] is None
    assert retained_transfer["cleanup_status"] == "pending"
    assert int(retained_transfer["target_random_id"]) != old_target_random_id


def test_reset_clone_timeline_preserves_target_and_plan_but_removes_all_delivery_state(
    tmp_path,
):
    conn = _create_clone_context(tmp_path / "reset-clone-timeline.db")
    try:
        _insert_message(conn, message_id=1, content="", has_media=1)
        _record_mapping(
            conn,
            source_message_id=1,
            target_message_id=9301,
            mode="media_copy",
        )
        ensure_clone_media_transfers(
            conn,
            migration_id="migration-rewind",
            run_id="run-rewind",
            plan_id="plan-rewind",
            source_chat_id=100,
            source_message_ids=[1],
            target_chat_id=777,
            transfer_strategy=CLONE_MEDIA_TRANSFER_DIRECT,
        )
        conn.execute(
            """
            INSERT INTO admin_jobs(
                job_id, job_type, status, created_at, updated_at
            ) VALUES (
                'job-migration-rewind', 'clone_timeline_migration', 'done',
                '2026-01-01', '2026-01-01'
            )
            """
        )
        conn.execute(
            """
            INSERT INTO admin_job_logs(job_id, seq, ts, message)
            VALUES ('job-migration-rewind', 1, '2026-01-01', 'done')
            """
        )
        conn.commit()

        reset = reset_clone_run_timeline(
            conn,
            run_id="run-rewind",
            target_chat_id=777,
        )
        run = conn.execute(
            "SELECT * FROM admin_clone_runs WHERE run_id = 'run-rewind'"
        ).fetchone()
        plan_count = conn.execute(
            "SELECT COUNT(*) FROM admin_clone_plans WHERE run_id = 'run-rewind'"
        ).fetchone()[0]
        migration_count = conn.execute(
            "SELECT COUNT(*) FROM admin_clone_migrations WHERE run_id = 'run-rewind'"
        ).fetchone()[0]
        mapping_count = conn.execute(
            "SELECT COUNT(*) FROM admin_clone_message_map WHERE run_id = 'run-rewind'"
        ).fetchone()[0]
        transfer_count = conn.execute(
            "SELECT COUNT(*) FROM admin_clone_media_transfers WHERE run_id = 'run-rewind'"
        ).fetchone()[0]
        migration_job_count = conn.execute(
            "SELECT COUNT(*) FROM admin_jobs WHERE job_id = 'job-migration-rewind'"
        ).fetchone()[0]
        migration_log_count = conn.execute(
            "SELECT COUNT(*) FROM admin_job_logs WHERE job_id = 'job-migration-rewind'"
        ).fetchone()[0]
    finally:
        conn.close()

    assert reset == {
        "media_transfer_count": 1,
        "mapping_count": 1,
        "migration_count": 1,
        "migration_job_count": 1,
    }
    assert run is not None
    assert run["target_chat_id"] == 777
    assert run["status"] == "done"
    assert run["phase"] == "done"
    assert plan_count == 1
    assert migration_count == 0
    assert mapping_count == 0
    assert transfer_count == 0
    assert migration_job_count == 0
    assert migration_log_count == 0


def test_complete_reset_barrier_persists_until_timeline_reset(tmp_path):
    conn = _create_clone_context(tmp_path / "reset-barrier.db")
    try:
        mark_clone_run_message_reset_required(
            conn,
            run_id="run-rewind",
            target_chat_id=777,
        )
        marked = conn.execute(
            "SELECT status, phase, error_message FROM admin_clone_runs "
            "WHERE run_id = 'run-rewind'"
        ).fetchone()

        reset_clone_run_timeline(
            conn,
            run_id="run-rewind",
            target_chat_id=777,
        )
        reset = conn.execute(
            "SELECT status, phase, error_message FROM admin_clone_runs "
            "WHERE run_id = 'run-rewind'"
        ).fetchone()
    finally:
        conn.close()

    assert marked["status"] == "done"
    assert marked["phase"] == "message_reset_required"
    assert "必须重试完整清空" in marked["error_message"]
    assert reset["status"] == "done"
    assert reset["phase"] == "done"
    assert reset["error_message"] == ""


def test_rewind_deleted_target_resets_media_transfer_without_mapping(tmp_path):
    conn = _create_clone_context(tmp_path / "rewind-unmapped-transfer.db")
    try:
        _insert_message(conn, message_id=1, content="", has_media=1)
        ensure_clone_media_transfers(
            conn,
            migration_id="migration-rewind",
            run_id="run-rewind",
            plan_id="plan-rewind",
            source_chat_id=100,
            source_message_ids=[1],
            target_chat_id=777,
            transfer_strategy=CLONE_MEDIA_TRANSFER_DIRECT,
        )
        mark_clone_media_transfer_target_hop_sent(
            conn,
            run_id="run-rewind",
            source_chat_id=100,
            target_message_ids_by_source={1: 9301},
        )

        rewind = rewind_clone_mappings_for_deleted_target_messages(
            conn,
            run_id="run-rewind",
            target_chat_id=777,
            target_message_ids=[9301],
        )
        transfer_count = conn.execute(
            "SELECT COUNT(*) FROM admin_clone_media_transfers "
            "WHERE run_id = 'run-rewind' AND source_message_id = 1"
        ).fetchone()[0]
    finally:
        conn.close()

    assert rewind["rewound_mapping_count"] == 0
    assert rewind["rewound_media_transfer_count"] == 1
    assert rewind["unmapped_target_message_count"] == 1
    assert transfer_count == 0
