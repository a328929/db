import sqlite3
from types import SimpleNamespace

from tg_harvest.storage.clone import (
    build_clone_media_copy_preview,
    build_clone_preflight_report,
    build_clone_text_replay_preview,
    count_clone_media_replay_skips,
    count_clone_runs,
    count_clone_text_replay_candidates,
    create_clone_migration,
    create_clone_plan,
    create_clone_run,
    delete_clone_run,
    list_clone_message_mappings,
    list_clone_plans,
    list_clone_runs,
    list_clone_source_chats,
    list_clone_text_replay_batch,
    load_clone_message_mapping,
    load_clone_run,
    load_clone_run_progress,
    load_latest_clone_migration,
    load_latest_clone_plan,
    record_clone_message_mapping,
    update_clone_migration,
    update_clone_plan,
    update_clone_run,
)


def _create_clone_schema(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE chats (
            chat_id INTEGER PRIMARY KEY,
            chat_title TEXT NOT NULL,
            chat_username TEXT,
            is_public INTEGER NOT NULL DEFAULT 0,
            chat_type TEXT,
            message_count INTEGER NOT NULL DEFAULT 0,
            first_seen_at TEXT NOT NULL DEFAULT '',
            last_seen_at TEXT NOT NULL DEFAULT ''
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE messages (
            chat_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            msg_date_text TEXT NOT NULL,
            msg_date_ts INTEGER,
            content TEXT,
            content_norm TEXT,
            msg_type TEXT NOT NULL,
            grouped_id INTEGER,
            has_media INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(chat_id, message_id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE message_media (
            chat_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            file_name TEXT,
            media_fingerprint TEXT,
            grouped_id INTEGER,
            PRIMARY KEY(chat_id, message_id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE media_groups (
            chat_id INTEGER NOT NULL,
            grouped_id INTEGER NOT NULL,
            item_count INTEGER NOT NULL DEFAULT 0,
            active_items INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(chat_id, grouped_id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE admin_clone_runs (
            run_id TEXT PRIMARY KEY,
            job_id TEXT NOT NULL UNIQUE,
            source_chat_id INTEGER NOT NULL,
            source_title TEXT NOT NULL,
            source_chat_username TEXT,
            source_chat_type TEXT,
            source_message_count INTEGER NOT NULL DEFAULT 0,
            source_last_message_at TEXT,
            source_last_message_ts INTEGER,
            target_chat_id INTEGER,
            target_access_hash TEXT,
            target_title TEXT NOT NULL,
            target_kind TEXT NOT NULL,
            target_username TEXT,
            target_owner_session TEXT,
            phase TEXT NOT NULL DEFAULT 'queued',
            status TEXT NOT NULL DEFAULT 'queued',
            plan_json TEXT,
            error_message TEXT,
            target_created_at TEXT,
            completed_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE admin_clone_plans (
            plan_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            job_id TEXT,
            status TEXT NOT NULL DEFAULT 'queued',
            source_access TEXT NOT NULL DEFAULT 'unknown',
            target_access TEXT NOT NULL DEFAULT 'unknown',
            primary_session_status TEXT NOT NULL DEFAULT 'unknown',
            secondary_session_status TEXT NOT NULL DEFAULT 'unknown',
            migration_account TEXT NOT NULL DEFAULT '',
            text_strategy TEXT NOT NULL DEFAULT '',
            media_strategy TEXT NOT NULL DEFAULT '',
            media_group_strategy TEXT NOT NULL DEFAULT '',
            avatar_strategy TEXT NOT NULL DEFAULT '',
            blocking_issues_json TEXT NOT NULL DEFAULT '[]',
            warnings_json TEXT NOT NULL DEFAULT '[]',
            capabilities_json TEXT NOT NULL DEFAULT '{}',
            plan_json TEXT NOT NULL DEFAULT '{}',
            error_message TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            completed_at TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE admin_clone_migrations (
            migration_id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL,
            plan_id TEXT,
            job_id TEXT,
            mode TEXT NOT NULL DEFAULT 'text_replay',
            status TEXT NOT NULL DEFAULT 'queued',
            phase TEXT NOT NULL DEFAULT 'queued',
            target_chat_id INTEGER,
            target_title TEXT,
            target_write_account TEXT NOT NULL DEFAULT '',
            requested_limit INTEGER NOT NULL DEFAULT 0,
            send_delay_ms INTEGER NOT NULL DEFAULT 0,
            text_total INTEGER NOT NULL DEFAULT 0,
            text_sent INTEGER NOT NULL DEFAULT 0,
            text_skipped INTEGER NOT NULL DEFAULT 0,
            text_failed INTEGER NOT NULL DEFAULT 0,
            media_total INTEGER NOT NULL DEFAULT 0,
            media_sent INTEGER NOT NULL DEFAULT 0,
            media_skipped INTEGER NOT NULL DEFAULT 0,
            media_failed INTEGER NOT NULL DEFAULT 0,
            media_group_total INTEGER NOT NULL DEFAULT 0,
            media_group_sent INTEGER NOT NULL DEFAULT 0,
            media_group_skipped INTEGER NOT NULL DEFAULT 0,
            media_group_failed INTEGER NOT NULL DEFAULT 0,
            plan_json TEXT NOT NULL DEFAULT '{}',
            error_message TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            completed_at TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE admin_clone_message_map (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            migration_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            plan_id TEXT,
            source_chat_id INTEGER NOT NULL,
            source_message_id INTEGER NOT NULL,
            source_msg_date_ts INTEGER,
            source_msg_date_text TEXT,
            target_chat_id INTEGER NOT NULL,
            target_message_id INTEGER,
            delivery_random_id INTEGER,
            delivery_account TEXT NOT NULL DEFAULT '',
            chunk_index INTEGER NOT NULL DEFAULT 0,
            chunk_count INTEGER NOT NULL DEFAULT 1,
            mode TEXT NOT NULL DEFAULT 'text_replay',
            status TEXT NOT NULL DEFAULT 'done',
            error_message TEXT,
            sent_at TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(
                run_id,
                source_chat_id,
                source_message_id,
                chunk_index,
                mode
            )
        )
        """
    )
    conn.commit()


def _insert_clone_fixture(conn):
    conn.execute(
        """
        INSERT INTO chats(
            chat_id, chat_title, chat_username, chat_type, message_count,
            first_seen_at, last_seen_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            100,
            "Source Group",
            "",
            "Megagroup",
            5,
            "2026-01-01 00:00:00",
            "2026-01-05 00:00:00",
        ),
    )
    conn.executemany(
        """
        INSERT INTO messages(
            chat_id, message_id, msg_date_text, msg_date_ts, content,
            content_norm, msg_type, grouped_id, has_media
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (100, 1, "2026-01-01 00:00:01", 1, "hello", "hello", "TEXT", None, 0),
            (100, 2, "2026-01-01 00:00:02", 2, "", "", "MEDIA", 111, 1),
            (100, 3, "2026-01-01 00:00:03", 3, "", "", "MEDIA", 111, 1),
            (100, 4, "2026-01-01 00:00:04", 4, "", "", "MEDIA", 222, 1),
            (100, 5, "2026-01-01 00:00:05", 5, "", "", "MEDIA", None, 1),
        ],
    )
    conn.executemany(
        """
        INSERT INTO message_media(
            chat_id, message_id, file_name, media_fingerprint, grouped_id
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            (100, 2, "photo.jpg", "fp-photo", 111),
            (100, 5, "", "fp-video", None),
        ],
    )
    conn.executemany(
        """
        INSERT INTO media_groups(chat_id, grouped_id, item_count, active_items)
        VALUES (?, ?, ?, ?)
        """,
        [
            (100, 111, 3, 3),
            (100, 222, 1, 1),
        ],
    )
    conn.commit()


def _new_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _create_clone_schema(conn)
    _insert_clone_fixture(conn)
    return conn


def test_clone_media_copy_preview_separates_incomplete_and_suspected_groups():
    conn = _new_conn()
    try:
        conn.executemany(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, content,
                content_norm, msg_type, grouped_id, has_media
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (100, 10, "2026-01-01 00:00:10", 10, "", "", "MEDIA", 333, 1),
                (100, 12, "2026-01-01 00:00:12", 12, "", "", "MEDIA", 333, 1),
            ],
        )
        conn.execute(
            """
            INSERT INTO media_groups(chat_id, grouped_id, item_count, active_items)
            VALUES (?, ?, ?, ?)
            """,
            (100, 333, 2, 2),
        )
        conn.commit()
        preview = build_clone_media_copy_preview(
            conn,
            run_id="run-media-preview",
            source_chat_id=100,
        )
    finally:
        conn.close()

    assert preview["media_total"] == 6
    assert preview["media_candidate_total"] == 6
    assert preview["media_executable_total"] == 6
    assert preview["media_candidate_remaining"] == 6
    assert preview["media_remaining"] == 6
    assert preview["solo_media_total"] == 1
    assert preview["complete_group_total"] == 0
    assert preview["complete_group_items"] == 0
    assert preview["media_group_candidate_total"] == 3
    assert preview["media_group_candidate_items"] == 5
    assert preview["incomplete_group_total"] == 1
    assert preview["incomplete_group_items"] == 2
    assert preview["suspected_incomplete_group_total"] == 2
    assert preview["suspected_incomplete_group_items"] == 3
    assert preview["missing_group_meta_total"] == 0
    assert preview["missing_group_meta_items"] == 0
    assert preview["db_self_check_risk_group_total"] == 3
    assert preview["db_self_check_risk_group_items"] == 5


def test_clone_preflight_flags_media_group_gaps_and_second_account_blocker():
    conn = _new_conn()
    try:
        report = build_clone_preflight_report(
            conn,
            chat_id=100,
            cfg=SimpleNamespace(session_name="main", secondary_session_name=""),
        )
    finally:
        conn.close()

    assert report["source"]["chat_title"] == "Source Group"
    assert report["confirm"] == "CLONE:STRUCTURE:100"
    assert report["account"]["secondary_session_distinct"] is False
    assert report["recommendation"]["mode"] == "blocked"

    metrics = report["metrics"]
    assert metrics["total_messages"] == 5
    assert metrics["text_messages"] == 1
    assert metrics["media_messages"] == 4
    assert metrics["grouped_messages"] == 3
    assert metrics["media_metadata_rows"] == 2
    assert metrics["media_metadata_coverage_percent"] == 50.0
    assert metrics["media_group_count"] == 2
    assert metrics["suspect_media_group_count"] == 2
    assert metrics["single_item_media_group_count"] == 1
    assert metrics["metadata_incomplete_media_group_count"] == 2
    assert metrics["recorded_larger_media_group_count"] == 1

    warnings_text = "\n".join(report["warnings"])
    assert "未配置独立第二账号" in warnings_text
    assert "疑似残缺媒体组" in warnings_text
    assert "第一版不复制历史消息" in warnings_text


def test_clone_preflight_allows_structure_clone_with_distinct_second_account():
    conn = _new_conn()
    try:
        report = build_clone_preflight_report(
            conn,
            chat_id=100,
            cfg=SimpleNamespace(
                session_name="main",
                secondary_session_name="clone-target",
            ),
        )
    finally:
        conn.close()

    structure = {item["key"]: item for item in report["capabilities"]}[
        "structure_clone"
    ]
    assert structure["status"] == "ready"
    assert report["account"]["target_owner_account"] == "secondary"
    assert report["target"]["default_title"] == "Source Group 副本"
    assert report["recommendation"]["level"] == "C"
    assert report["recommendation"]["mode"] == "structure_then_text"


def test_list_clone_source_chats_avoids_media_table_aggregation():
    conn = _new_conn()
    try:
        items = list_clone_source_chats(conn, sort="updated_desc")
    finally:
        conn.close()

    assert len(items) == 1
    item = items[0]
    assert item["chat_id"] == 100
    assert item["chat_title"] == "Source Group"
    assert "media_rows" not in item
    assert item["last_message_at"] == "2026-01-01 00:00:05"
    assert item["last_message_ts"] == 5


def test_list_clone_source_chats_defaults_to_display_name_order():
    conn = _new_conn()
    try:
        conn.executemany(
            """
            INSERT INTO chats(
                chat_id, chat_title, chat_username, chat_type, message_count,
                first_seen_at, last_seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (200, "Beta", "", "Megagroup", 2, "", ""),
                (201, "阿尔法", "", "Megagroup", 2, "", ""),
                (202, " 中华 ", "", "Megagroup", 2, "", ""),
                (203, "Alpha", "", "Megagroup", 2, "", ""),
                (204, "  ", "", "Megagroup", 2, "", ""),
                (205, "[Only]足控", "", "Megagroup", 2, "", ""),
            ],
        )
        items = list_clone_source_chats(conn)
    finally:
        conn.close()

    assert [item["chat_title"] for item in items] == [
        "阿尔法",
        "Alpha",
        "Beta",
        "Chat 204",
        "[Only]足控",
        "Source Group",
        "中华",
    ]


def test_list_clone_source_chats_sorts_message_count_descending():
    conn = _new_conn()
    try:
        conn.executemany(
            """
            INSERT INTO chats(
                chat_id, chat_title, chat_username, chat_type, message_count,
                first_seen_at, last_seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (200, "Small", "", "Megagroup", 2, "", ""),
                (201, "Large", "", "Megagroup", 20, "", ""),
                (202, "Medium", "", "Megagroup", 10, "", ""),
            ],
        )
        items = list_clone_source_chats(conn, sort="message_count_desc")
    finally:
        conn.close()

    assert [item["message_count"] for item in items] == [20, 10, 5, 2]


def test_clone_run_create_update_and_list_roundtrip():
    conn = _new_conn()
    try:
        source = build_clone_preflight_report(
            conn,
            chat_id=100,
            cfg=SimpleNamespace(session_name="main", secondary_session_name="clone"),
        )["source"]
        created = create_clone_run(
            conn,
            run_id="run-1",
            job_id="job-1",
            source_chat=source,
            target_title="Source Backup",
            target_kind="megagroup",
            target_owner_session="clone",
            plan={"confirm": "CLONE:STRUCTURE:100"},
        )

        assert created["run_id"] == "run-1"
        assert created["status"] == "queued"
        assert created["source_chat_id"] == 100
        assert created["target_title"] == "Source Backup"
        assert '"confirm":"CLONE:STRUCTURE:100"' in created["plan_json"]

        updated = update_clone_run(
            conn,
            run_id="run-1",
            status="done",
            phase="done",
            target_chat_id=777,
            target_access_hash="123456",
            target_title="Source Backup",
            target_kind="megagroup",
            target_username="",
            target_owner_session="clone",
            target_created_at="2026-06-18T00:00:00+00:00",
            completed_at="2026-06-18T00:00:00+00:00",
        )
        assert updated is not None
        assert updated["status"] == "done"
        assert updated["target_chat_id"] == 777

        loaded = load_clone_run(conn, "run-1")
        assert loaded is not None
        assert loaded["target_access_hash"] == "123456"

        runs = list_clone_runs(conn, source_chat_id=100)
        assert [run["run_id"] for run in runs] == ["run-1"]
    finally:
        conn.close()


def test_clone_run_management_filters_count_and_paginates():
    conn = _new_conn()
    try:
        source = build_clone_preflight_report(
            conn,
            chat_id=100,
            cfg=SimpleNamespace(session_name="main", secondary_session_name="clone"),
        )["source"]
        for index, status in enumerate(("done", "error", "done"), start=1):
            create_clone_run(
                conn,
                run_id=f"run-{index}",
                job_id=f"job-{index}",
                source_chat=source,
                target_title=f"Backup {index}",
                target_kind="megagroup",
                target_owner_session="clone",
                plan={},
            )
            update_clone_run(
                conn,
                run_id=f"run-{index}",
                status=status,
                phase="done" if status == "done" else "error",
                target_chat_id=700 + index if status == "done" else None,
                completed_at=f"2026-06-18T00:0{index}:00+00:00",
            )

        assert count_clone_runs(conn, status="done") == 2
        page = list_clone_runs(conn, status="done", q="Backup", limit=1, offset=1)
        assert len(page) == 1
        assert page[0]["status"] == "done"
    finally:
        conn.close()


def test_delete_clone_run_removes_local_children_without_touching_target():
    conn = _new_conn()
    try:
        source = build_clone_preflight_report(
            conn,
            chat_id=100,
            cfg=SimpleNamespace(session_name="main", secondary_session_name="clone"),
        )["source"]
        create_clone_run(
            conn,
            run_id="run-delete",
            job_id="job-delete",
            source_chat=source,
            target_title="Source Backup",
            target_kind="megagroup",
            target_owner_session="clone",
            plan={},
        )
        create_clone_plan(
            conn,
            plan_id="plan-delete",
            run_id="run-delete",
            job_id="job-plan-delete",
            status="done",
            plan={},
        )
        create_clone_migration(
            conn,
            migration_id="migration-delete",
            run_id="run-delete",
            plan_id="plan-delete",
            job_id="job-migration-delete",
            mode="timeline_replay",
            target_chat_id=777,
            target_title="Source Backup",
        )
        record_clone_message_mapping(
            conn,
            migration_id="migration-delete",
            run_id="run-delete",
            plan_id="plan-delete",
            source_chat_id=100,
            source_message_id=1,
            target_chat_id=777,
            target_message_id=9001,
            mode="text_replay",
        )

        assert delete_clone_run(conn, run_id="run-delete") is True
        assert load_clone_run(conn, "run-delete") is None
        assert load_latest_clone_plan(conn, "run-delete") is None
        assert load_latest_clone_migration(conn, "run-delete") is None
        assert list_clone_message_mappings(conn, run_id="run-delete") == []
        assert delete_clone_run(conn, run_id="run-delete") is False
    finally:
        conn.close()


def test_clone_run_error_status_does_not_create_target_identity():
    conn = _new_conn()
    try:
        source = build_clone_preflight_report(
            conn,
            chat_id=100,
            cfg=SimpleNamespace(session_name="main", secondary_session_name="clone"),
        )["source"]
        create_clone_run(
            conn,
            run_id="run-error",
            job_id="job-error",
            source_chat=source,
            target_title="Source Failed Copy",
            target_kind="channel",
            target_owner_session="clone",
            plan=None,
        )

        failed = update_clone_run(
            conn,
            run_id="run-error",
            status="error",
            phase="error",
            error_message="boom",
            completed_at="2026-06-18T00:01:00+00:00",
        )
        assert failed is not None
        assert failed["status"] == "error"
        assert failed["target_chat_id"] is None
        assert failed["error_message"] == "boom"
    finally:
        conn.close()


def test_clone_plan_create_update_latest_and_list_roundtrip():
    conn = _new_conn()
    try:
        source = build_clone_preflight_report(
            conn,
            chat_id=100,
            cfg=SimpleNamespace(session_name="main", secondary_session_name="clone"),
        )["source"]
        create_clone_run(
            conn,
            run_id="run-plan",
            job_id="job-plan-structure",
            source_chat=source,
            target_title="Source Backup",
            target_kind="megagroup",
            target_owner_session="clone",
            plan={},
        )

        first = create_clone_plan(
            conn,
            plan_id="plan-1",
            run_id="run-plan",
            job_id="job-plan-1",
            status="queued",
            plan={"step": "queued"},
        )
        assert first["plan_id"] == "plan-1"
        assert first["status"] == "queued"
        assert first["plan"]["step"] == "queued"

        updated = update_clone_plan(
            conn,
            plan_id="plan-1",
            status="done",
            source_access="ok",
            target_access="ok",
            primary_session_status="ok",
            secondary_session_status="ok",
            migration_account="primary",
            text_strategy="database_replay",
            media_strategy="source_copy_without_attribution",
            media_group_strategy="strict_skip_incomplete",
            avatar_strategy="skip_not_implemented",
            blocking_issues=[],
            warnings=["目标副本写入权限未执行试发验证"],
            capabilities={"target_write_account": "primary"},
            plan={"version": 1, "run_id": "run-plan"},
            completed_at="2026-06-18T00:00:00+00:00",
        )
        assert updated is not None
        assert updated["status"] == "done"
        assert updated["source_access"] == "ok"
        assert updated["warnings"] == ["目标副本写入权限未执行试发验证"]
        assert updated["capabilities"]["target_write_account"] == "primary"
        assert updated["plan"]["version"] == 1

        second = create_clone_plan(
            conn,
            plan_id="plan-2",
            run_id="run-plan",
            job_id="job-plan-2",
            status="queued",
            plan={"step": "newer"},
        )
        assert second["plan"]["step"] == "newer"

        latest = load_latest_clone_plan(conn, "run-plan")
        assert latest is not None
        assert latest["plan_id"] == "plan-2"

        plans = list_clone_plans(conn, run_id="run-plan")
        assert [plan["plan_id"] for plan in plans] == ["plan-2", "plan-1"]
    finally:
        conn.close()


def test_clone_migration_mapping_and_text_candidate_roundtrip():
    conn = _new_conn()
    try:
        source = build_clone_preflight_report(
            conn,
            chat_id=100,
            cfg=SimpleNamespace(session_name="main", secondary_session_name="clone"),
        )["source"]
        create_clone_run(
            conn,
            run_id="run-migration",
            job_id="job-migration-structure",
            source_chat=source,
            target_title="Source Backup",
            target_kind="megagroup",
            target_owner_session="clone",
            plan={},
        )
        create_clone_plan(
            conn,
            plan_id="plan-migration",
            run_id="run-migration",
            job_id="job-plan",
            status="done",
            target_access="ok",
            text_strategy="database_replay",
            capabilities={"target_write_account": "primary"},
            plan={"target_write_account": "primary"},
        )

        assert count_clone_text_replay_candidates(conn, 100) == 1
        assert count_clone_media_replay_skips(conn, 100) == 4
        preview = build_clone_text_replay_preview(
            conn,
            run_id="run-migration",
            source_chat_id=100,
        )
        assert preview["text_total"] == 1
        assert preview["text_remaining"] == 1
        assert preview["media_skipped"] == 4
        assert preview["grouped_skipped"] == 3
        assert preview["empty_text_skipped"] == 0

        batch = list_clone_text_replay_batch(conn, chat_id=100)
        assert [item["message_id"] for item in batch] == [1]
        assert batch[0]["text"] == "hello"

        created = create_clone_migration(
            conn,
            migration_id="migration-1",
            run_id="run-migration",
            plan_id="plan-migration",
            job_id="job-migration",
            target_chat_id=777,
            target_title="Source Backup",
            target_write_account="primary",
            requested_limit=10,
            send_delay_ms=500,
            plan={"plan_id": "plan-migration"},
        )
        assert created["status"] == "queued"
        assert created["target_chat_id"] == 777
        assert created["requested_limit"] == 10
        assert created["send_delay_ms"] == 500

        updated = update_clone_migration(
            conn,
            migration_id="migration-1",
            status="running",
            phase="sending_text",
            text_total=1,
            text_sent=1,
            media_skipped=4,
        )
        assert updated is not None
        assert updated["phase"] == "sending_text"
        assert updated["text_sent"] == 1

        mapping = record_clone_message_mapping(
            conn,
            migration_id="migration-1",
            run_id="run-migration",
            plan_id="plan-migration",
            source_chat_id=100,
            source_message_id=1,
            source_msg_date_ts=1,
            source_msg_date_text="2026-01-01 00:00:01",
            target_chat_id=777,
            target_message_id=9001,
            chunk_index=0,
            chunk_count=1,
            status="done",
        )
        assert mapping["target_message_id"] == 9001

        loaded_mapping = load_clone_message_mapping(
            conn,
            run_id="run-migration",
            source_chat_id=100,
            source_message_id=1,
            chunk_index=0,
        )
        assert loaded_mapping is not None
        assert loaded_mapping["status"] == "done"
        preview_after_mapping = build_clone_text_replay_preview(
            conn,
            run_id="run-migration",
            source_chat_id=100,
        )
        assert preview_after_mapping["text_completed"] == 1
        assert preview_after_mapping["text_remaining"] == 0
        assert preview_after_mapping["text_chunks_done"] == 1

        latest = load_latest_clone_migration(conn, "run-migration")
        assert latest is not None
        assert latest["migration_id"] == "migration-1"
    finally:
        conn.close()


def test_clone_run_progress_separates_verified_group_totals_from_mappings():
    conn = _new_conn()
    try:
        create_clone_migration(
            conn,
            migration_id="migration-progress",
            run_id="run-progress",
            mode="timeline_replay",
            status="done",
            text_total=2,
            media_total=4,
            media_group_total=1,
        )

        def record(
            source_message_id: int,
            *,
            mode: str,
            status: str,
            chunk_index: int = 0,
            chunk_count: int = 1,
        ) -> None:
            record_clone_message_mapping(
                conn,
                migration_id="migration-progress",
                run_id="run-progress",
                source_chat_id=100,
                source_message_id=source_message_id,
                target_chat_id=777,
                mode=mode,
                status=status,
                chunk_index=chunk_index,
                chunk_count=chunk_count,
            )

        # A long text can have several mapping rows but is one group message.
        record(1, mode="text_replay", status="done", chunk_index=0, chunk_count=2)
        record(1, mode="text_replay", status="done", chunk_index=1, chunk_count=2)
        record(2, mode="text_replay", status="error")
        record(3, mode="media_copy", status="done")
        record(4, mode="media_group_copy", status="done")
        record(5, mode="media_group_copy", status="done")
        record(6, mode="media_copy", status="error")

        progress = load_clone_run_progress(conn, "run-progress")

        assert progress["assessment_state"] == "verified"
        assert progress["snapshot_migration_id"] == "migration-progress"
        assert progress["messages_total"] == 6
        assert progress["messages_done"] == 4
        assert progress["messages_error"] == 2
        assert progress["messages_remaining"] == 2
        assert progress["text_done"] == 1
        assert progress["text_error"] == 1
        assert progress["media_done"] == 3
        assert progress["media_error"] == 1
    finally:
        conn.close()


def test_clone_text_replay_batch_paginates_null_timestamp_messages():
    conn = _new_conn()
    try:
        conn.executemany(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, content,
                content_norm, msg_type, grouped_id, has_media
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (100, 6, "", None, "legacy a", "legacy a", "TEXT", None, 0),
                (100, 7, "", None, "legacy b", "legacy b", "TEXT", None, 0),
            ],
        )
        first_batch = list_clone_text_replay_batch(conn, chat_id=100, limit=1)
        assert [item["message_id"] for item in first_batch] == [6]
        second_batch = list_clone_text_replay_batch(
            conn,
            chat_id=100,
            after_ts=first_batch[-1]["sort_ts"],
            after_message_id=first_batch[-1]["message_id"],
            limit=1,
        )
        assert [item["message_id"] for item in second_batch] == [7]
    finally:
        conn.close()
