import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from telethon.tl.functions.channels import DeleteChannelRequest

from tg_harvest.admin_jobs.clone_target_access import (
    clone_run_target_conflicts_with_source,
)
from tg_harvest.admin_jobs.clone_target_delete import (
    _admin_clone_target_delete_job_runner,
)
from tg_harvest.storage.clone import (
    CLONE_MEDIA_TRANSFER_DIRECT,
    claim_clone_run_for_deletion,
    create_clone_migration,
    create_clone_plan,
    create_clone_run,
    delete_clone_run,
    ensure_clone_media_transfers,
    load_clone_run,
    record_clone_message_mapping,
)
from tg_harvest.storage.connection import detect_sqlite_features
from tg_harvest.storage.schema import create_schema


def _connect(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _heartbeat_pair():
    return SimpleNamespace(set=lambda: None), SimpleNamespace(join=lambda timeout=None: None)


def test_clone_target_source_collision_respects_telegram_peer_namespace():
    assert clone_run_target_conflicts_with_source(
        {
            "source_chat_id": -100777,
            "source_chat_type": "Megagroup",
            "target_chat_id": 777,
        }
    )
    assert not clone_run_target_conflicts_with_source(
        {
            "source_chat_id": 777,
            "source_chat_type": "Chat",
            "target_chat_id": 777,
        }
    )
    assert not clone_run_target_conflicts_with_source(
        {
            "source_chat_id": 100777,
            "source_chat_type": "Channel",
            "target_chat_id": 777,
        }
    )
    assert clone_run_target_conflicts_with_source(
        {
            "source_chat_id": -100100777,
            "source_chat_type": "Channel",
            "target_chat_id": 100777,
        }
    )


def _cfg():
    return SimpleNamespace(
        session_name="primary",
        secondary_session_name="secondary",
        api_id=1,
        api_hash="hash",
    )


def _insert_admin_job(conn, job_id):
    conn.execute(
        """
        INSERT INTO admin_jobs(
            job_id, job_type, status, target_chat_id, target_label,
            created_at, updated_at, heartbeat_at
        ) VALUES (?, 'clone_structure', 'done', NULL, '', '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')
        """,
        (job_id,),
    )
    conn.execute(
        """
        INSERT INTO admin_job_logs(job_id, seq, ts, message)
        VALUES (?, 1, '2026-01-01T00:00:00+00:00', 'clone history')
        """,
        (job_id,),
    )


def _create_clone_fixture(path, *, target_chat_id=777, target_access_hash="123"):
    conn = _connect(path)
    try:
        create_schema(conn, detect_sqlite_features(conn))
        conn.execute(
            """
            INSERT INTO chats(
                chat_id, chat_title, chat_type, message_count, first_seen_at, last_seen_at
            ) VALUES (100, 'Source Group', 'Megagroup', 1, '2026-01-01', '2026-01-01')
            """
        )
        conn.execute(
            """
            INSERT INTO messages(chat_id, message_id, msg_date_text, msg_date_ts, msg_type)
            VALUES (100, 1, '2026-01-01', 1, 'TEXT')
            """
        )
        source = {
            "chat_id": 100,
            "chat_title": "Source Group",
            "chat_username": "source",
            "chat_type": "Megagroup",
            "message_count": 1,
            "last_message_at": "2026-01-01",
            "last_message_ts": 1,
        }
        clone_run = create_clone_run(
            conn,
            run_id="run-delete",
            job_id="job-run-delete",
            source_chat=source,
            target_title="Clone Target",
            target_kind="megagroup",
            target_owner_session="secondary",
            plan={},
        )
        conn.execute(
            """
            UPDATE admin_clone_runs
            SET status = 'done', phase = 'done', target_chat_id = ?, target_access_hash = ?
            WHERE run_id = ?
            """,
            (target_chat_id, target_access_hash, "run-delete"),
        )
        related_job_ids = ["job-run-delete"]
        if target_chat_id is not None:
            plan = create_clone_plan(
                conn,
                plan_id="plan-delete",
                run_id="run-delete",
                job_id="job-plan-delete",
                status="done",
                plan={},
            )
            migration = create_clone_migration(
                conn,
                migration_id="migration-delete",
                run_id="run-delete",
                plan_id="plan-delete",
                job_id="job-migration-delete",
                mode="timeline_replay",
                target_chat_id=target_chat_id,
                target_title="Clone Target",
            )
            record_clone_message_mapping(
                conn,
                migration_id=migration["migration_id"],
                run_id="run-delete",
                plan_id=plan["plan_id"],
                source_chat_id=100,
                source_message_id=1,
                target_chat_id=target_chat_id,
                target_message_id=9001,
                mode="text_replay",
            )
            ensure_clone_media_transfers(
                conn,
                migration_id=migration["migration_id"],
                run_id="run-delete",
                plan_id=plan["plan_id"],
                source_chat_id=100,
                source_message_ids=[1],
                target_chat_id=target_chat_id,
                transfer_strategy=CLONE_MEDIA_TRANSFER_DIRECT,
                source_account="secondary",
                target_account="secondary",
            )
            related_job_ids.extend(["job-plan-delete", "job-migration-delete"])
        for job_id in related_job_ids:
            _insert_admin_job(conn, job_id)
        conn.commit()
        return dict(clone_run) | {
            "target_chat_id": target_chat_id,
            "target_access_hash": target_access_hash,
        }
    finally:
        conn.close()


def _run_delete_job(
    path,
    clone_run,
    *,
    client=None,
    append_log_fn=None,
    status_fn=None,
):
    statuses = []
    logs = []
    append_log = append_log_fn or (lambda _job_id, message: logs.append(str(message)))
    set_status = status_fn or (
        lambda _job_id, status: statuses.append(status) or True
    )
    with (
        patch(
            "tg_harvest.admin_jobs.clone_target_delete.start_admin_job_heartbeat",
            return_value=_heartbeat_pair(),
        ),
        patch(
            "tg_harvest.admin_jobs.clone_target_delete._ensure_base_session_valid",
            return_value=True,
        ),
        patch(
            "tg_harvest.admin_jobs.clone_target_delete._create_isolated_worker_client",
            return_value=client,
        ) as create_client,
        patch("tg_harvest.admin_jobs.clone_target_delete._disconnect_worker_client"),
        patch("tg_harvest.admin_jobs.clone_target_delete._cleanup_isolated_worker_session"),
    ):
        _admin_clone_target_delete_job_runner(
            "job-delete-target",
            clone_run=clone_run,
            cfg=_cfg(),
            get_conn_fn=lambda: _connect(path),
            admin_job_set_status_fn=set_status,
            admin_job_append_log_fn=append_log,
        )
    return statuses, logs, create_client


def test_clone_target_delete_removes_remote_target_and_only_clone_local_chain(tmp_path):
    db_path = tmp_path / "clone-target-delete.db"
    clone_run = _create_clone_fixture(db_path)
    requests = []

    class Client:
        def __call__(self, request):
            requests.append(request)
            return None

    statuses, logs, _create_client = _run_delete_job(
        db_path,
        clone_run,
        client=Client(),
    )

    assert statuses == ["running", "done"]
    assert len(requests) == 1
    assert isinstance(requests[0], DeleteChannelRequest)
    assert requests[0].channel.channel_id == 777
    assert requests[0].channel.access_hash == 123
    assert any("不会读取或删除源群" in message for message in logs)

    conn = _connect(db_path)
    try:
        assert load_clone_run(conn, "run-delete") is None
        assert conn.execute("SELECT COUNT(*) FROM admin_clone_plans").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM admin_clone_migrations").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM admin_clone_message_map").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM admin_clone_media_transfers").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM admin_jobs").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM admin_job_logs").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM chats WHERE chat_id = 100").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM messages WHERE chat_id = 100").fetchone()[0] == 1
    finally:
        conn.close()


def test_delete_clone_run_rejects_active_clone_job(tmp_path):
    db_path = tmp_path / "clone-target-delete-active.db"
    _create_clone_fixture(db_path)
    conn = _connect(db_path)
    try:
        conn.execute(
            "UPDATE admin_clone_runs SET status = 'running' WHERE run_id = 'run-delete'"
        )
        conn.execute(
            "UPDATE admin_jobs SET status = 'running' WHERE job_id = 'job-run-delete'"
        )
        conn.commit()
        try:
            delete_clone_run(conn, run_id="run-delete")
        except RuntimeError as exc:
            assert "仍在运行" in str(exc)
        else:
            raise AssertionError("active clone deletion should be rejected")
        assert conn.execute(
            "SELECT 1 FROM admin_clone_runs WHERE run_id = 'run-delete'"
        ).fetchone() is not None
    finally:
        conn.close()


def test_claim_clone_run_for_deletion_rejects_another_active_delete_job(tmp_path):
    db_path = tmp_path / "clone-target-delete-claim-race.db"
    _create_clone_fixture(db_path)
    conn = _connect(db_path)
    try:
        conn.execute(
            "UPDATE admin_clone_runs SET status = 'deleting', phase = 'deleting' "
            "WHERE run_id = 'run-delete'"
        )
        conn.execute(
            """
            INSERT INTO admin_jobs(
                job_id, job_type, status, target_chat_id, target_label,
                created_at, updated_at, heartbeat_at
            ) VALUES (
                'job-target-active', 'clone_target_delete', 'running', NULL, '',
                '2026-01-01T00:00:00+00:00',
                '2026-01-01T00:00:00+00:00',
                '2026-01-01T00:00:00+00:00'
            )
            """
        )
        conn.commit()

        try:
            claim_clone_run_for_deletion(
                conn,
                run_id="run-delete",
                job_id="job-target-second",
            )
        except RuntimeError as exc:
            assert "重复删除" in str(exc)
        else:
            raise AssertionError("an active target-delete job should block a second claim")
    finally:
        conn.close()


def test_claim_clone_run_for_deletion_scopes_active_owner_to_same_run(tmp_path):
    db_path = tmp_path / "clone-target-delete-owner-scope.db"
    _create_clone_fixture(db_path)
    conn = _connect(db_path)
    try:
        conn.execute(
            """
            UPDATE admin_clone_runs
            SET status = 'deleting', phase = 'deleting', deletion_job_id = ?
            WHERE run_id = 'run-delete'
            """,
            ("job-delete-owner",),
        )
        _insert_admin_job(conn, "job-delete-owner")
        conn.execute(
            """
            UPDATE admin_jobs
            SET job_type = 'clone_target_delete', status = 'running'
            WHERE job_id = 'job-delete-owner'
            """
        )
        other_run = create_clone_run(
            conn,
            run_id="run-other",
            job_id="job-other-create",
            source_chat={
                "chat_id": 100,
                "chat_title": "Source Group",
                "chat_type": "Megagroup",
                "message_count": 1,
            },
            target_title="Other Target",
            target_kind="megagroup",
            target_owner_session="secondary",
        )
        assert other_run["run_id"] == "run-other"
        _insert_admin_job(conn, "job-other-create")
        conn.commit()

        assert claim_clone_run_for_deletion(
            conn,
            run_id="run-other",
            job_id="job-other-delete",
        ) is True
        assert conn.execute(
            "SELECT deletion_job_id FROM admin_clone_runs WHERE run_id = 'run-other'"
        ).fetchone()[0] == "job-other-delete"
    finally:
        conn.close()


def test_claim_clone_run_for_deletion_rejects_same_run_reentry_by_token(tmp_path):
    db_path = tmp_path / "clone-target-delete-owner-reentry.db"
    _create_clone_fixture(db_path)
    conn = _connect(db_path)
    try:
        conn.execute(
            """
            UPDATE admin_clone_runs
            SET status = 'deleting', phase = 'deleting', deletion_job_id = ?
            WHERE run_id = 'run-delete'
            """,
            ("job-delete-owner",),
        )
        _insert_admin_job(conn, "job-delete-owner")
        conn.execute(
            """
            UPDATE admin_jobs
            SET job_type = 'clone_target_delete', status = 'running'
            WHERE job_id = 'job-delete-owner'
            """
        )
        conn.commit()

        try:
            claim_clone_run_for_deletion(
                conn,
                run_id="run-delete",
                job_id="job-delete-second",
            )
        except RuntimeError as exc:
            assert "重复删除" in str(exc)
        else:
            raise AssertionError("same-run deletion reentry should be rejected")

        conn.execute(
            "UPDATE admin_clone_runs SET status = 'error' WHERE run_id = 'run-delete'"
        )
        conn.commit()
        try:
            claim_clone_run_for_deletion(
                conn,
                run_id="run-delete",
                job_id="job-delete-second",
            )
        except RuntimeError as exc:
            assert "重复删除" in str(exc)
        else:
            raise AssertionError("active deletion token should survive status healing")

        assert claim_clone_run_for_deletion(
            conn,
            run_id="run-delete",
            job_id="job-delete-owner",
        ) is True
    finally:
        conn.close()


def test_delete_clone_run_preserves_current_deletion_job_and_logs(tmp_path):
    db_path = tmp_path / "clone-target-delete-current-job.db"
    _create_clone_fixture(db_path)
    conn = _connect(db_path)
    try:
        _insert_admin_job(conn, "job-delete-current")
        conn.execute(
            """
            UPDATE admin_jobs
            SET job_type = 'clone_target_delete', status = 'running'
            WHERE job_id = 'job-delete-current'
            """
        )
        conn.commit()

        assert claim_clone_run_for_deletion(
            conn,
            run_id="run-delete",
            job_id="job-delete-current",
        ) is True
        assert delete_clone_run(
            conn,
            run_id="run-delete",
            job_id="job-delete-current",
        ) is True

        assert conn.execute(
            "SELECT status FROM admin_jobs WHERE job_id = 'job-delete-current'"
        ).fetchone()[0] == "running"
        assert conn.execute(
            "SELECT COUNT(*) FROM admin_job_logs WHERE job_id = 'job-delete-current'"
        ).fetchone()[0] == 1
    finally:
        conn.close()


def test_clone_target_delete_purges_local_chain_when_target_is_already_dissolved(tmp_path):
    db_path = tmp_path / "clone-target-dissolved.db"
    clone_run = _create_clone_fixture(db_path)

    class Client:
        def __call__(self, _request):
            raise RuntimeError("ChannelInvalidError")

    statuses, logs, _create_client = _run_delete_job(
        db_path,
        clone_run,
        client=Client(),
    )

    assert statuses == ["running", "done"]
    assert any("已解散或不存在" in message for message in logs)
    conn = _connect(db_path)
    try:
        assert load_clone_run(conn, "run-delete") is None
        assert conn.execute("SELECT COUNT(*) FROM chats WHERE chat_id = 100").fetchone()[0] == 1
    finally:
        conn.close()


def test_clone_target_delete_purges_failed_record_without_target_api_call(tmp_path):
    db_path = tmp_path / "clone-target-missing.db"
    clone_run = _create_clone_fixture(db_path, target_chat_id=None, target_access_hash="")
    clone_run["target_chat_id"] = None
    clone_run["target_access_hash"] = ""

    statuses, logs, create_client = _run_delete_job(db_path, clone_run, client=None)

    assert statuses == ["running", "done"]
    assert not create_client.called
    assert any("没有已创建的目标副本" in message for message in logs)
    conn = _connect(db_path)
    try:
        assert load_clone_run(conn, "run-delete") is None
        assert conn.execute("SELECT COUNT(*) FROM chats WHERE chat_id = 100").fetchone()[0] == 1
    finally:
        conn.close()


def test_clone_target_delete_never_sends_delete_for_source_identity(tmp_path):
    db_path = tmp_path / "clone-target-source-collision.db"
    clone_run = _create_clone_fixture(
        db_path,
        target_chat_id=100,
        target_access_hash="123",
    )
    # Cover Telegram's marked channel-id form as well as the positive entity id.
    clone_run["source_chat_id"] = -100100

    statuses, logs, create_client = _run_delete_job(
        db_path,
        clone_run,
        client=None,
    )

    assert statuses == ["running", "done"]
    assert not create_client.called
    assert any("与源群 ID 冲突" in message for message in logs)
    conn = _connect(db_path)
    try:
        assert load_clone_run(conn, "run-delete") is None
        assert conn.execute("SELECT COUNT(*) FROM chats WHERE chat_id = 100").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM messages WHERE chat_id = 100").fetchone()[0] == 1
    finally:
        conn.close()


def test_clone_target_delete_purges_local_chain_when_owner_session_is_unavailable(tmp_path):
    db_path = tmp_path / "clone-target-owner-unavailable.db"
    clone_run = _create_clone_fixture(db_path)
    clone_run["target_owner_session"] = "missing-owner-session"

    statuses, logs, create_client = _run_delete_job(db_path, clone_run, client=None)

    assert statuses == ["running", "done"]
    assert not create_client.called
    assert any("未找到目标副本对应的已配置创建账号" in message for message in logs)
    conn = _connect(db_path)
    try:
        assert load_clone_run(conn, "run-delete") is None
        assert conn.execute("SELECT COUNT(*) FROM chats WHERE chat_id = 100").fetchone()[0] == 1
    finally:
        conn.close()


def test_clone_target_delete_keeps_done_state_when_final_telemetry_fails(tmp_path):
    db_path = tmp_path / "clone-target-delete-telemetry.db"
    clone_run = _create_clone_fixture(db_path)
    statuses = []

    def fail_final_log(_job_id, message):
        if "已清除克隆记录" in str(message):
            raise RuntimeError("log backend unavailable")

    _run_delete_job(
        db_path,
        clone_run,
        client=None,
        append_log_fn=fail_final_log,
        status_fn=lambda _job_id, status: statuses.append(status) or True,
    )

    assert statuses[-1] == "done"
    conn = _connect(db_path)
    try:
        assert load_clone_run(conn, "run-delete") is None
    finally:
        conn.close()


def test_clone_target_delete_failure_callbacks_cannot_leave_job_running(tmp_path):
    db_path = tmp_path / "clone-target-delete-error-telemetry.db"
    clone_run = _create_clone_fixture(db_path)
    statuses = []

    def fail_log(_job_id, _message):
        raise RuntimeError("log backend unavailable")

    _run_delete_job(
        db_path,
        clone_run,
        client=None,
        append_log_fn=fail_log,
        status_fn=lambda _job_id, status: statuses.append(status) or True,
    )

    assert statuses[-1] == "error"


def test_clone_target_delete_marks_claimed_run_error_when_purge_does_not_start(
    tmp_path,
):
    db_path = tmp_path / "clone-target-delete-claimed-error.db"
    clone_run = _create_clone_fixture(db_path)

    class Client:
        def __call__(self, _request):
            return None

    with patch(
        "tg_harvest.admin_jobs.clone_target_delete.delete_clone_run",
        side_effect=RuntimeError("local purge unavailable"),
    ):
        statuses, _logs, _client = _run_delete_job(
            db_path,
            clone_run,
            client=Client(),
        )

    assert statuses[-1] == "error"
    conn = _connect(db_path)
    try:
        run = load_clone_run(conn, "run-delete")
        assert run is not None
        assert run["status"] == "error"
        assert run["phase"] == "delete_error"
        assert run["deletion_job_id"] == "job-delete-target"
        assert "local purge unavailable" in run["error_message"]
    finally:
        conn.close()
