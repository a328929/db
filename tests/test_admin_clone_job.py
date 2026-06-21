import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from tg_harvest.admin_jobs.clone import (
    CLONE_TARGET_TITLE_MAX_LEN,
    _admin_clone_structure_job_runner,
    normalize_clone_target_kind,
    normalize_clone_target_title,
)
from tg_harvest.admin_jobs.clone_preflight import _admin_clone_deep_preflight_job_runner
from tg_harvest.admin_jobs.clone_timeline_migration import (
    _admin_clone_timeline_migration_job_runner,
)
from tg_harvest.storage.clone import (
    create_clone_migration,
    create_clone_plan,
    create_clone_run,
    list_clone_timeline_replay_batch,
    load_clone_message_mapping,
    load_clone_run,
    load_latest_clone_migration,
    load_latest_clone_plan,
    record_clone_message_mapping,
    update_clone_run,
)
from tg_harvest.storage.connection import detect_sqlite_features
from tg_harvest.storage.schema import create_schema


def test_clone_target_kind_uses_explicit_valid_kind_first():
    assert normalize_clone_target_kind("channel", source_chat_type="Megagroup") == "channel"
    assert normalize_clone_target_kind("broadcast", source_chat_type="Megagroup") == "channel"
    assert normalize_clone_target_kind("group", source_chat_type="Channel") == "megagroup"


def test_clone_target_kind_infers_from_source_when_missing():
    assert normalize_clone_target_kind("", source_chat_type="Megagroup") == "megagroup"
    assert normalize_clone_target_kind(None, source_chat_type="chat") == "megagroup"
    assert normalize_clone_target_kind("", source_chat_type="Channel") == "channel"


def test_clone_target_title_uses_fallback_and_truncates():
    assert normalize_clone_target_title("", fallback_title="Source 副本") == "Source 副本"
    assert normalize_clone_target_title("  Target  ", fallback_title="Source") == "Target"

    long_title = "x" * (CLONE_TARGET_TITLE_MAX_LEN + 10)
    normalized = normalize_clone_target_title(long_title, fallback_title="Source")
    assert len(normalized) == CLONE_TARGET_TITLE_MAX_LEN


def _connect(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _create_job_db(path):
    conn = _connect(path)
    try:
        create_schema(conn, detect_sqlite_features(conn), skip_fts_auto_heal=1)
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
                1,
                "2026-01-01 00:00:00",
                "2026-01-01 00:00:00",
            ),
        )
        conn.execute(
            """
            INSERT INTO messages(chat_id, message_id, msg_date_text, msg_date_ts, msg_type)
            VALUES (?, ?, ?, ?, ?)
            """,
            (100, 1, "2026-01-01 00:00:00", 1, "TEXT"),
        )
        conn.commit()
        create_clone_run(
            conn,
            run_id="job-clone",
            job_id="job-clone",
            source_chat={
                "chat_id": 100,
                "chat_title": "Source Group",
                "chat_username": "",
                "chat_type": "Megagroup",
                "message_count": 1,
                "last_message_at": "2026-01-01 00:00:00",
                "last_message_ts": 1,
            },
            target_title="Source Backup",
            target_kind="megagroup",
            target_owner_session="secondary",
            plan={},
        )
    finally:
        conn.close()


def _heartbeat_pair():
    stop = SimpleNamespace(set=lambda: None)
    thread = SimpleNamespace(join=lambda timeout=None: None)
    return stop, thread


def _runner_cfg():
    return SimpleNamespace(
        session_name="main",
        secondary_session_name="secondary",
        clone_relay_chat_id=0,
        clone_relay_chat_username="",
        api_id=1,
        api_hash="hash",
    )


def test_clone_structure_job_persists_created_target(tmp_path):
    db_path = tmp_path / "clone-job.db"
    _create_job_db(db_path)
    status_updates = []
    logs = []

    class Client:
        def __call__(self, request):
            return SimpleNamespace(
                chats=[
                    SimpleNamespace(
                        id=777,
                        access_hash=123456,
                        title=request.title,
                        username="",
                    )
                ]
            )

    with (
        patch("tg_harvest.admin_jobs.clone._start_job_heartbeat", return_value=_heartbeat_pair()),
        patch("tg_harvest.admin_jobs.clone.finish_job_heartbeat"),
        patch("tg_harvest.admin_jobs.clone._admin_job_update_progress"),
        patch("tg_harvest.admin_jobs.clone._ensure_base_session_valid", return_value=True),
        patch("tg_harvest.admin_jobs.clone._create_isolated_worker_client", return_value=Client()),
        patch("tg_harvest.admin_jobs.clone._disconnect_worker_client"),
        patch("tg_harvest.admin_jobs.clone._cleanup_isolated_worker_session"),
    ):
        _admin_clone_structure_job_runner(
            "job-clone",
            source_chat_id=100,
            target_title="Source Backup",
            target_kind="megagroup",
            clone_run_id="job-clone",
            cfg=_runner_cfg(),
            get_conn_fn=lambda: _connect(db_path),
            admin_job_set_status_fn=lambda _job_id, status: status_updates.append(status) or True,
            admin_job_append_log_fn=lambda _job_id, message: logs.append(str(message)),
        )

    conn = _connect(db_path)
    try:
        run = load_clone_run(conn, "job-clone")
    finally:
        conn.close()

    assert run is not None
    assert run["status"] == "done"
    assert run["phase"] == "done"
    assert run["target_chat_id"] == 777
    assert run["target_access_hash"] == "123456"
    assert run["target_title"] == "Source Backup"
    assert run["target_owner_session"] == "secondary"
    assert status_updates[-1] == "done"
    assert any("目标结构创建完成" in message for message in logs)


def test_clone_structure_job_failure_does_not_persist_target_identity(tmp_path):
    db_path = tmp_path / "clone-job-error.db"
    _create_job_db(db_path)

    class FailingClient:
        def __call__(self, _request):
            raise RuntimeError("boom")

    with (
        patch("tg_harvest.admin_jobs.clone._start_job_heartbeat", return_value=_heartbeat_pair()),
        patch("tg_harvest.admin_jobs.clone.finish_job_heartbeat"),
        patch("tg_harvest.admin_jobs.clone._admin_job_update_progress"),
        patch("tg_harvest.admin_jobs.clone._ensure_base_session_valid", return_value=True),
        patch("tg_harvest.admin_jobs.clone._create_isolated_worker_client", return_value=FailingClient()),
        patch("tg_harvest.admin_jobs.clone._disconnect_worker_client"),
        patch("tg_harvest.admin_jobs.clone._cleanup_isolated_worker_session"),
    ):
        _admin_clone_structure_job_runner(
            "job-clone",
            source_chat_id=100,
            target_title="Source Backup",
            target_kind="megagroup",
            clone_run_id="job-clone",
            cfg=_runner_cfg(),
            get_conn_fn=lambda: _connect(db_path),
            admin_job_set_status_fn=lambda *_args: True,
            admin_job_append_log_fn=lambda *_args: None,
        )

    conn = _connect(db_path)
    try:
        run = load_clone_run(conn, "job-clone")
    finally:
        conn.close()

    assert run is not None
    assert run["status"] == "error"
    assert run["phase"] == "error"
    assert run["target_chat_id"] is None
    assert "boom" in run["error_message"]


def _create_deep_plan(path, *, with_target=True):
    conn = _connect(path)
    try:
        create_clone_plan(
            conn,
            plan_id="job-deep",
            run_id="job-clone",
            job_id="job-deep",
            status="queued",
            plan={},
        )
        if with_target:
            update_clone_run(
                conn,
                run_id="job-clone",
                status="done",
                phase="done",
                target_chat_id=777,
                target_access_hash="123456",
                target_title="Source Backup",
                target_kind="megagroup",
                target_owner_session="secondary",
                target_created_at="2026-06-18T00:00:00+00:00",
                completed_at="2026-06-18T00:00:00+00:00",
            )
    finally:
        conn.close()


class _DeepPreflightClient:
    def __init__(self, *, source_ok=True, target_ok=True, relay_ok=True):
        self.source_ok = source_ok
        self.target_ok = target_ok
        self.relay_ok = relay_ok

    def get_entity(self, value):
        normalized = int(value) if isinstance(value, int) or str(value).lstrip("-").isdigit() else value
        if normalized in {100, -100100, -100}:
            if not self.source_ok:
                raise ValueError("Could not find the input entity")
            return SimpleNamespace(id=100, title="Source Group")
        if normalized in {777, -100777, -777}:
            if not self.target_ok:
                raise ValueError("Could not find the input entity")
            return SimpleNamespace(
                id=777,
                title="Source Backup",
                creator=True,
                default_banned_rights=SimpleNamespace(send_messages=False),
            )
        if normalized in {999, -100999, -999}:
            if not self.relay_ok:
                raise ValueError("Could not find the input entity")
            return SimpleNamespace(
                id=999,
                title="Relay Channel",
                creator=True,
                default_banned_rights=SimpleNamespace(send_messages=False),
            )
        raise ValueError("Could not find the input entity")

    def get_messages(self, _entity, **_kwargs):
        return [SimpleNamespace(id=1, date="2026-01-01 00:00:00+00:00")]


def test_clone_deep_preflight_job_persists_online_plan(tmp_path):
    db_path = tmp_path / "clone-deep.db"
    _create_job_db(db_path)
    _create_deep_plan(db_path, with_target=True)
    status_updates = []
    logs = []

    with (
        patch("tg_harvest.admin_jobs.clone_preflight._start_job_heartbeat", return_value=_heartbeat_pair()),
        patch("tg_harvest.admin_jobs.clone_preflight.finish_job_heartbeat"),
        patch("tg_harvest.admin_jobs.clone_preflight._admin_job_update_progress"),
        patch("tg_harvest.admin_jobs.clone_preflight._ensure_base_session_valid", return_value=True),
        patch(
            "tg_harvest.admin_jobs.clone_preflight._create_isolated_worker_client",
            side_effect=[
                _DeepPreflightClient(source_ok=True, target_ok=True),
                _DeepPreflightClient(source_ok=False, target_ok=True),
            ],
        ),
        patch("tg_harvest.admin_jobs.clone_preflight._disconnect_worker_client"),
        patch("tg_harvest.admin_jobs.clone_preflight._cleanup_isolated_worker_session"),
    ):
        _admin_clone_deep_preflight_job_runner(
            "job-deep",
            run_id="job-clone",
            plan_id="job-deep",
            cfg=_runner_cfg(),
            get_conn_fn=lambda: _connect(db_path),
            admin_job_set_status_fn=lambda _job_id, status: status_updates.append(status) or True,
            admin_job_append_log_fn=lambda _job_id, message: logs.append(str(message)),
        )

    conn = _connect(db_path)
    try:
        plan = load_latest_clone_plan(conn, "job-clone")
    finally:
        conn.close()

    assert plan is not None
    assert plan["status"] == "done"
    assert plan["source_access"] == "ok"
    assert plan["target_access"] == "ok"
    assert plan["primary_session_status"] == "ok"
    assert plan["secondary_session_status"] == "ok"
    assert plan["migration_account"] == "primary"
    assert plan["text_strategy"] == "database_replay"
    assert plan["media_strategy"] == "source_copy_without_attribution"
    assert plan["capabilities"]["forward_privacy"] == "without_source_attribution"
    assert plan["capabilities"]["forward_requires_drop_author"] is True
    assert plan["capabilities"]["forward_keeps_source_link"] is False
    assert plan["media_group_strategy"] == "strict_skip_incomplete"
    assert plan["blocking_issues"] == []
    assert plan["capabilities"]["target_write_account"] == "primary"
    assert plan["plan"]["network_access_checked"] is True
    assert status_updates[-1] == "done"
    assert any("深度预检完成" in message for message in logs)


def test_clone_deep_preflight_job_blocks_when_target_missing(tmp_path):
    db_path = tmp_path / "clone-deep-missing-target.db"
    _create_job_db(db_path)
    _create_deep_plan(db_path, with_target=False)

    with (
        patch("tg_harvest.admin_jobs.clone_preflight._start_job_heartbeat", return_value=_heartbeat_pair()),
        patch("tg_harvest.admin_jobs.clone_preflight.finish_job_heartbeat"),
        patch("tg_harvest.admin_jobs.clone_preflight._admin_job_update_progress"),
        patch("tg_harvest.admin_jobs.clone_preflight._ensure_base_session_valid") as session_mock,
        patch("tg_harvest.admin_jobs.clone_preflight._create_isolated_worker_client") as client_mock,
    ):
        _admin_clone_deep_preflight_job_runner(
            "job-deep",
            run_id="job-clone",
            plan_id="job-deep",
            cfg=_runner_cfg(),
            get_conn_fn=lambda: _connect(db_path),
            admin_job_set_status_fn=lambda *_args: True,
            admin_job_append_log_fn=lambda *_args: None,
        )

    session_mock.assert_not_called()
    client_mock.assert_not_called()
    conn = _connect(db_path)
    try:
        plan = load_latest_clone_plan(conn, "job-clone")
    finally:
        conn.close()

    assert plan is not None
    assert plan["status"] == "done"
    assert plan["target_access"] == "missing"
    assert any("目标副本尚未创建" in item for item in plan["blocking_issues"])
    assert plan["text_strategy"] == "blocked"


def test_clone_deep_preflight_job_allows_text_when_source_is_missing(tmp_path):
    db_path = tmp_path / "clone-deep-source-missing.db"
    _create_job_db(db_path)
    _create_deep_plan(db_path, with_target=True)

    with (
        patch("tg_harvest.admin_jobs.clone_preflight._start_job_heartbeat", return_value=_heartbeat_pair()),
        patch("tg_harvest.admin_jobs.clone_preflight.finish_job_heartbeat"),
        patch("tg_harvest.admin_jobs.clone_preflight._admin_job_update_progress"),
        patch("tg_harvest.admin_jobs.clone_preflight._ensure_base_session_valid", return_value=True),
        patch(
            "tg_harvest.admin_jobs.clone_preflight._create_isolated_worker_client",
            side_effect=[
                _DeepPreflightClient(source_ok=False, target_ok=True),
                _DeepPreflightClient(source_ok=False, target_ok=True),
            ],
        ),
        patch("tg_harvest.admin_jobs.clone_preflight._disconnect_worker_client"),
        patch("tg_harvest.admin_jobs.clone_preflight._cleanup_isolated_worker_session"),
    ):
        _admin_clone_deep_preflight_job_runner(
            "job-deep",
            run_id="job-clone",
            plan_id="job-deep",
            cfg=_runner_cfg(),
            get_conn_fn=lambda: _connect(db_path),
            admin_job_set_status_fn=lambda *_args: True,
            admin_job_append_log_fn=lambda *_args: None,
        )

    conn = _connect(db_path)
    try:
        plan = load_latest_clone_plan(conn, "job-clone")
    finally:
        conn.close()

    assert plan is not None
    assert plan["status"] == "done"
    assert plan["source_access"] == "missing"
    assert plan["target_access"] == "ok"
    assert plan["migration_account"] == "unavailable"
    assert plan["text_strategy"] == "database_replay"
    assert plan["media_strategy"] == "impossible_without_local_vault"
    assert any("源群无法" in item for item in plan["warnings"])


def test_clone_deep_preflight_job_enables_relay_when_accounts_split_access(tmp_path):
    db_path = tmp_path / "clone-deep-relay.db"
    _create_job_db(db_path)
    _create_deep_plan(db_path, with_target=True)
    cfg = _runner_cfg()
    cfg.clone_relay_chat_id = 999
    cfg.clone_relay_chat_username = ""

    with (
        patch("tg_harvest.admin_jobs.clone_preflight._start_job_heartbeat", return_value=_heartbeat_pair()),
        patch("tg_harvest.admin_jobs.clone_preflight.finish_job_heartbeat"),
        patch("tg_harvest.admin_jobs.clone_preflight._admin_job_update_progress"),
        patch("tg_harvest.admin_jobs.clone_preflight._ensure_base_session_valid", return_value=True),
        patch(
            "tg_harvest.admin_jobs.clone_preflight._create_isolated_worker_client",
            side_effect=[
                _DeepPreflightClient(source_ok=True, target_ok=False, relay_ok=True),
                _DeepPreflightClient(source_ok=False, target_ok=True, relay_ok=True),
            ],
        ),
        patch("tg_harvest.admin_jobs.clone_preflight._disconnect_worker_client"),
        patch("tg_harvest.admin_jobs.clone_preflight._cleanup_isolated_worker_session"),
    ):
        _admin_clone_deep_preflight_job_runner(
            "job-deep",
            run_id="job-clone",
            plan_id="job-deep",
            cfg=cfg,
            get_conn_fn=lambda: _connect(db_path),
            admin_job_set_status_fn=lambda *_args: True,
            admin_job_append_log_fn=lambda *_args: None,
        )

    conn = _connect(db_path)
    try:
        plan = load_latest_clone_plan(conn, "job-clone")
    finally:
        conn.close()

    assert plan is not None
    assert plan["status"] == "done"
    assert plan["source_access"] == "ok"
    assert plan["target_access"] == "ok"
    assert plan["migration_account"] == "unavailable"
    assert plan["text_strategy"] == "database_replay"
    assert plan["media_strategy"] == "relay_copy_without_attribution"
    assert plan["media_group_strategy"] == "relay_api_rebuild"
    assert plan["avatar_strategy"] == "skip_not_implemented"
    assert plan["capabilities"]["media_relay"]["enabled"] is True
    assert plan["capabilities"]["media_relay"]["chat_id"] == 999
    assert plan["capabilities"]["media_relay"]["source_account"] == "primary"
    assert plan["capabilities"]["media_relay"]["target_account"] == "secondary"
    assert plan["capabilities"]["media_relay"]["keeps_source_link"] is False
    assert plan["capabilities"]["media_relay"]["keeps_relay_link"] is False
    assert any("固定中转频道桥接" in item for item in plan["warnings"])


def test_clone_deep_preflight_explains_incomplete_relay_access(tmp_path):
    db_path = tmp_path / "clone-deep-relay-incomplete.db"
    _create_job_db(db_path)
    _create_deep_plan(db_path, with_target=True)
    cfg = _runner_cfg()
    cfg.clone_relay_chat_id = 999
    cfg.clone_relay_chat_username = ""

    with (
        patch("tg_harvest.admin_jobs.clone_preflight._start_job_heartbeat", return_value=_heartbeat_pair()),
        patch("tg_harvest.admin_jobs.clone_preflight.finish_job_heartbeat"),
        patch("tg_harvest.admin_jobs.clone_preflight._admin_job_update_progress"),
        patch("tg_harvest.admin_jobs.clone_preflight._ensure_base_session_valid", return_value=True),
        patch(
            "tg_harvest.admin_jobs.clone_preflight._create_isolated_worker_client",
            side_effect=[
                _DeepPreflightClient(source_ok=True, target_ok=False, relay_ok=True),
                _DeepPreflightClient(source_ok=False, target_ok=True, relay_ok=False),
            ],
        ),
        patch("tg_harvest.admin_jobs.clone_preflight._disconnect_worker_client"),
        patch("tg_harvest.admin_jobs.clone_preflight._cleanup_isolated_worker_session"),
    ):
        _admin_clone_deep_preflight_job_runner(
            "job-deep",
            run_id="job-clone",
            plan_id="job-deep",
            cfg=cfg,
            get_conn_fn=lambda: _connect(db_path),
            admin_job_set_status_fn=lambda *_args: True,
            admin_job_append_log_fn=lambda *_args: None,
        )

    conn = _connect(db_path)
    try:
        plan = load_latest_clone_plan(conn, "job-clone")
    finally:
        conn.close()

    assert plan is not None
    assert plan["media_strategy"] == "impossible_without_local_vault"
    warnings_text = "\n".join(plan["warnings"])
    assert "固定中转频道访问不完整" in warnings_text
    assert "secondary: access=missing" in warnings_text
    assert "该群组/频道已解散或不存在" in warnings_text
    assert plan["capabilities"]["media_relay"]["enabled"] is False
    assert plan["capabilities"]["media_relay"]["chat_id"] == 999


def _create_ready_clone_state(path):
    conn = _connect(path)
    try:
        conn.execute(
            """
            UPDATE messages
            SET content = ?, content_norm = ?, has_media = 0, grouped_id = NULL
            WHERE chat_id = ? AND message_id = ?
            """,
            ("hello clone", "hello clone", 100, 1),
        )
        update_clone_run(
            conn,
            run_id="job-clone",
            status="done",
            phase="done",
            target_chat_id=777,
            target_access_hash="123456",
            target_title="Source Backup",
            target_kind="megagroup",
            target_owner_session="secondary",
            target_created_at="2026-06-18T00:00:00+00:00",
            completed_at="2026-06-18T00:00:00+00:00",
        )
        create_clone_plan(
            conn,
            plan_id="plan-text",
            run_id="job-clone",
            job_id="job-deep",
            status="done",
            source_access="ok",
            target_access="ok",
            primary_session_status="ok",
            secondary_session_status="ok",
            migration_account="primary",
            text_strategy="database_replay",
            media_strategy="source_copy_without_attribution",
            media_group_strategy="strict_skip_incomplete",
            avatar_strategy="copy_if_accessible",
            blocking_issues=[],
            warnings=[],
            capabilities={"target_write_account": "primary"},
            plan={"target_write_account": "primary"},
            completed_at="2026-06-18T00:00:00+00:00",
        )
        conn.commit()
    finally:
        conn.close()


class _MediaMigrationClient:
    def __init__(self):
        self.forward_calls = []
        self.source_messages = {}

    def _build_media_stub(self, media_kind: str):
        normalized = str(media_kind or "").strip().lower()
        if normalized == "photo":
            return SimpleNamespace(photo=object(), media=object())
        if normalized == "video":
            return SimpleNamespace(
                video=object(),
                media=object(),
                document=SimpleNamespace(
                    mime_type="video/mp4",
                    attributes=[],
                ),
            )
        if normalized == "audio":
            return SimpleNamespace(
                audio=object(),
                media=object(),
                document=SimpleNamespace(
                    mime_type="audio/mpeg",
                    attributes=[],
                ),
            )
        if normalized == "voice":
            return SimpleNamespace(
                voice=object(),
                media=object(),
                document=SimpleNamespace(
                    mime_type="audio/ogg",
                    attributes=[],
                ),
            )
        if normalized == "document":
            return SimpleNamespace(
                document=SimpleNamespace(mime_type="application/pdf", attributes=[]),
                media=object(),
            )
        return SimpleNamespace(media=object())

    def add_source_message(self, message_id, *, grouped_id=None, media=True, media_kind="photo"):
        message = SimpleNamespace(
            id=int(message_id),
            grouped_id=grouped_id,
        )
        if media:
            message.media_kind = str(media_kind or "").strip().lower()
            media_stub = self._build_media_stub(media_kind)
            for name, value in vars(media_stub).items():
                setattr(message, name, value)
        else:
            message.media = None
        self.source_messages[int(message_id)] = message
        return message

    def get_entity(self, value):
        normalized = int(value) if isinstance(value, int) or str(value).lstrip("-").isdigit() else value
        if normalized in {100, -100100, -100}:
            return SimpleNamespace(id=100, title="Source Group")
        if normalized in {777, -100777, -777}:
            return SimpleNamespace(id=777, title="Source Backup")
        raise ValueError("Could not find the input entity")

    def get_messages(self, _entity, **kwargs):
        ids = kwargs.get("ids")
        if ids is not None:
            if isinstance(ids, list):
                return [
                    self.source_messages.get(int(message_id))
                    or self.add_source_message(int(message_id))
                    for message_id in ids
                ]
            return self.source_messages.get(int(ids)) or self.add_source_message(int(ids))

        min_id = int(kwargs.get("min_id") or 0)
        max_id = int(kwargs.get("max_id") or 0)
        return [
            message
            for message_id, message in sorted(self.source_messages.items())
            if message_id > min_id and (max_id <= 0 or message_id < max_id)
        ]

    def forward_messages(self, *args, **kwargs):
        self.forward_calls.append((args, kwargs))
        messages = args[1] if len(args) > 1 else None
        if isinstance(messages, list):
            return [
                SimpleNamespace(id=9100 + len(self.forward_calls) * 10 + index)
                for index, _message_id in enumerate(messages, start=1)
            ]
        return [SimpleNamespace(id=9100 + len(self.forward_calls))]


class _TimelineMigrationClient(_MediaMigrationClient):
    def __init__(self):
        super().__init__()
        self.timeline_events = []
        self.sent_messages = []

    def send_message(self, _entity, text):
        self.sent_messages.append(str(text))
        self.timeline_events.append(("text", str(text)))
        return SimpleNamespace(id=9300 + len(self.sent_messages))

    def forward_messages(self, *args, **kwargs):
        result = super().forward_messages(*args, **kwargs)
        messages = args[1] if len(args) > 1 else None
        self.timeline_events.append(("media", messages, kwargs))
        return result


def test_clone_timeline_batch_filters_completed_text_media_and_groups(tmp_path):
    db_path = tmp_path / "clone-timeline-batch-filter.db"
    _create_job_db(db_path)
    _create_ready_clone_state(db_path)
    conn = _connect(db_path)
    try:
        conn.execute(
            """
            UPDATE messages
            SET content = ?, content_norm = ?, msg_type = ?, has_media = 0, grouped_id = NULL
            WHERE chat_id = ? AND message_id = ?
            """,
            ("done text", "done text", "TEXT", 100, 1),
        )
        rows = [
            (2, "remaining text", "TEXT", None, 0),
            (3, "done media", "PHOTO", None, 1),
            (4, "", "PHOTO", 444, 1),
            (5, "", "PHOTO", 444, 1),
        ]
        for message_id, text, msg_type, grouped_id, has_media in rows:
            conn.execute(
                """
                INSERT INTO messages(
                    chat_id, message_id, msg_date_text, msg_date_ts, content,
                    content_norm, msg_type, grouped_id, has_media
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    100,
                    message_id,
                    f"2026-01-01 00:00:0{message_id}",
                    message_id,
                    text,
                    text,
                    msg_type,
                    grouped_id,
                    has_media,
                ),
            )
        create_clone_migration(
            conn,
            migration_id="job-timeline",
            run_id="job-clone",
            plan_id="plan-text",
            job_id="job-timeline",
            mode="timeline_replay",
            target_chat_id=777,
            target_title="Source Backup",
            target_write_account="text:primary; media:primary",
            plan={"plan_id": "plan-text"},
        )
        for source_message_id, mode in (
            (1, "text_replay"),
            (3, "media_copy"),
            (4, "media_group_copy"),
            (5, "media_group_copy"),
        ):
            record_clone_message_mapping(
                conn,
                migration_id="job-timeline",
                run_id="job-clone",
                plan_id="plan-text",
                source_chat_id=100,
                source_message_id=source_message_id,
                source_msg_date_ts=source_message_id,
                source_msg_date_text=f"2026-01-01 00:00:0{source_message_id}",
                target_chat_id=777,
                target_message_id=9000 + source_message_id,
                chunk_index=0,
                chunk_count=1,
                mode=mode,
                status="done",
            )

        all_items = list_clone_timeline_replay_batch(
            conn,
            chat_id=100,
            limit=10,
        )
        remaining_items = list_clone_timeline_replay_batch(
            conn,
            run_id="job-clone",
            chat_id=100,
            limit=10,
        )
    finally:
        conn.close()

    assert [
        (item["item_type"], item["source_message_id"], item["grouped_id"])
        for item in all_items
    ] == [
        ("text", 1, None),
        ("text", 2, None),
        ("solo_media", 3, None),
        ("media_group", 4, 444),
    ]
    assert [
        (item["item_type"], item["source_message_id"], item["grouped_id"])
        for item in remaining_items
    ] == [("text", 2, None)]


def test_clone_timeline_migration_limit_applies_to_remaining_items(tmp_path):
    db_path = tmp_path / "clone-timeline-resume-limit.db"
    _create_job_db(db_path)
    _create_ready_clone_state(db_path)
    conn = _connect(db_path)
    try:
        conn.execute(
            """
            UPDATE messages
            SET content = ?, content_norm = ?, msg_type = ?, has_media = 0, grouped_id = NULL
            WHERE chat_id = ? AND message_id = ?
            """,
            ("already copied", "already copied", "TEXT", 100, 1),
        )
        for message_id, text in ((2, "next text"), (3, "later text")):
            conn.execute(
                """
                INSERT INTO messages(
                    chat_id, message_id, msg_date_text, msg_date_ts, content,
                    content_norm, msg_type, grouped_id, has_media
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    100,
                    message_id,
                    f"2026-01-01 00:00:0{message_id}",
                    message_id,
                    text,
                    text,
                    "TEXT",
                    None,
                    0,
                ),
            )
        create_clone_migration(
            conn,
            migration_id="job-timeline",
            run_id="job-clone",
            plan_id="plan-text",
            job_id="job-timeline",
            mode="timeline_replay",
            target_chat_id=777,
            target_title="Source Backup",
            target_write_account="text:primary",
            plan={"plan_id": "plan-text"},
        )
        record_clone_message_mapping(
            conn,
            migration_id="job-timeline",
            run_id="job-clone",
            plan_id="plan-text",
            source_chat_id=100,
            source_message_id=1,
            source_msg_date_ts=1,
            source_msg_date_text="2026-01-01 00:00:00",
            target_chat_id=777,
            target_message_id=9001,
            chunk_index=0,
            chunk_count=1,
            mode="text_replay",
            status="done",
        )
        conn.commit()
    finally:
        conn.close()

    client = _TimelineMigrationClient()
    with (
        patch("tg_harvest.admin_jobs.clone_timeline_migration._start_job_heartbeat", return_value=_heartbeat_pair()),
        patch("tg_harvest.admin_jobs.clone_timeline_migration.finish_job_heartbeat"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._admin_job_update_progress"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._admin_job_stop_requested", return_value=False),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._ensure_base_session_valid", return_value=True),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._create_isolated_worker_client", return_value=client),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._disconnect_worker_client"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._cleanup_isolated_worker_session"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration.time.sleep"),
    ):
        _admin_clone_timeline_migration_job_runner(
            "job-timeline",
            run_id="job-clone",
            plan_id="plan-text",
            migration_id="job-timeline",
            cfg=_runner_cfg(),
            get_conn_fn=lambda: _connect(db_path),
            admin_job_set_status_fn=lambda *_args: True,
            admin_job_append_log_fn=lambda *_args: None,
            message_limit=1,
        )

    conn = _connect(db_path)
    try:
        migration = load_latest_clone_migration(conn, "job-clone")
        existing_mapping = load_clone_message_mapping(
            conn,
            run_id="job-clone",
            source_chat_id=100,
            source_message_id=1,
            chunk_index=0,
            mode="text_replay",
        )
        next_mapping = load_clone_message_mapping(
            conn,
            run_id="job-clone",
            source_chat_id=100,
            source_message_id=2,
            chunk_index=0,
            mode="text_replay",
        )
        later_mapping = load_clone_message_mapping(
            conn,
            run_id="job-clone",
            source_chat_id=100,
            source_message_id=3,
            chunk_index=0,
            mode="text_replay",
        )
    finally:
        conn.close()

    assert client.timeline_events == [("text", "next text")]
    assert migration is not None
    assert migration["status"] == "done"
    assert migration["phase"] == "limited_done"
    assert migration["text_sent"] == 1
    assert migration["text_skipped"] == 0
    assert existing_mapping is not None and existing_mapping["target_message_id"] == 9001
    assert next_mapping is not None and next_mapping["target_message_id"] == 9301
    assert later_mapping is None


def test_clone_timeline_migration_job_preserves_mixed_text_media_order(tmp_path):
    db_path = tmp_path / "clone-timeline-migration.db"
    _create_job_db(db_path)
    _create_ready_clone_state(db_path)
    conn = _connect(db_path)
    try:
        conn.execute(
            """
            UPDATE messages
            SET content = ?, content_norm = ?, msg_type = ?, has_media = 0, grouped_id = NULL
            WHERE chat_id = ? AND message_id = ?
            """,
            ("first text", "first text", "TEXT", 100, 1),
        )
        rows = [
            (2, "2026-01-01 00:00:02", 2, "photo caption", "photo caption", "PHOTO", None, 1),
            (3, "2026-01-01 00:00:03", 3, "middle text", "middle text", "TEXT", None, 0),
            (4, "2026-01-01 00:00:04", 4, "", "", "PHOTO", 444, 1),
            (5, "2026-01-01 00:00:05", 5, "", "", "PHOTO", 444, 1),
            (6, "2026-01-01 00:00:06", 6, "last text", "last text", "TEXT", None, 0),
        ]
        for row in rows:
            conn.execute(
                """
                INSERT INTO messages(
                    chat_id, message_id, msg_date_text, msg_date_ts, content,
                    content_norm, msg_type, grouped_id, has_media
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (100, *row),
            )
        for message_id, grouped_id in ((2, None), (4, 444), (5, 444)):
            conn.execute(
                """
                INSERT INTO message_media(
                    chat_id, message_id, media_kind, file_name, grouped_id, media_fingerprint
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    100,
                    message_id,
                    "PHOTO",
                    f"photo-{message_id}.jpg",
                    grouped_id,
                    f"fp-{message_id}",
                ),
            )
        conn.execute(
            """
            INSERT INTO media_groups(
                chat_id, grouped_id, first_message_id, first_msg_date_ts,
                item_count, active_items
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (100, 444, 4, 4, 2, 2),
        )
        create_clone_migration(
            conn,
            migration_id="job-timeline",
            run_id="job-clone",
            plan_id="plan-text",
            job_id="job-timeline",
            mode="timeline_replay",
            target_chat_id=777,
            target_title="Source Backup",
            target_write_account="text:primary; media:primary",
            plan={"plan_id": "plan-text"},
        )
        conn.commit()
    finally:
        conn.close()

    client = _TimelineMigrationClient()
    client.add_source_message(2)
    client.add_source_message(4, grouped_id=444)
    client.add_source_message(5, grouped_id=444)
    with (
        patch("tg_harvest.admin_jobs.clone_timeline_migration._start_job_heartbeat", return_value=_heartbeat_pair()),
        patch("tg_harvest.admin_jobs.clone_timeline_migration.finish_job_heartbeat"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._admin_job_update_progress"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._admin_job_stop_requested", return_value=False),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._ensure_base_session_valid", return_value=True),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._create_isolated_worker_client", return_value=client),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._disconnect_worker_client"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._cleanup_isolated_worker_session"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration.time.sleep"),
    ):
        _admin_clone_timeline_migration_job_runner(
            "job-timeline",
            run_id="job-clone",
            plan_id="plan-text",
            migration_id="job-timeline",
            cfg=_runner_cfg(),
            get_conn_fn=lambda: _connect(db_path),
            admin_job_set_status_fn=lambda *_args: True,
            admin_job_append_log_fn=lambda *_args: None,
        )

    conn = _connect(db_path)
    try:
        migration = load_latest_clone_migration(conn, "job-clone")
        text_1 = load_clone_message_mapping(
            conn,
            run_id="job-clone",
            source_chat_id=100,
            source_message_id=1,
            chunk_index=0,
            mode="text_replay",
        )
        media_2 = load_clone_message_mapping(
            conn,
            run_id="job-clone",
            source_chat_id=100,
            source_message_id=2,
            chunk_index=0,
            mode="media_copy",
        )
        text_3 = load_clone_message_mapping(
            conn,
            run_id="job-clone",
            source_chat_id=100,
            source_message_id=3,
            chunk_index=0,
            mode="text_replay",
        )
        group_4 = load_clone_message_mapping(
            conn,
            run_id="job-clone",
            source_chat_id=100,
            source_message_id=4,
            chunk_index=0,
            mode="media_group_copy",
        )
        text_6 = load_clone_message_mapping(
            conn,
            run_id="job-clone",
            source_chat_id=100,
            source_message_id=6,
            chunk_index=0,
            mode="text_replay",
        )
    finally:
        conn.close()

    assert [event[0:2] for event in client.timeline_events] == [
        ("text", "first text"),
        ("media", 2),
        ("text", "middle text"),
        ("media", [4, 5]),
        ("text", "last text"),
    ]
    assert client.timeline_events[1][2]["drop_author"] is True
    assert client.timeline_events[3][2]["drop_author"] is True
    assert "as_album" not in client.timeline_events[3][2]
    assert migration is not None
    assert migration["status"] == "done"
    assert migration["phase"] == "done"
    assert migration["text_sent"] == 3
    assert migration["media_sent"] == 3
    assert migration["media_group_sent"] == 1
    assert text_1 is not None and text_1["target_message_id"] == 9301
    assert media_2 is not None and media_2["target_message_id"] == 9101
    assert text_3 is not None and text_3["target_message_id"] == 9302
    assert group_4 is not None and group_4["target_message_id"] == 9121
    assert text_6 is not None and text_6["target_message_id"] == 9303


def test_clone_timeline_migration_job_api_expands_incomplete_media_group_in_place(tmp_path):
    db_path = tmp_path / "clone-timeline-media-group-gap.db"
    _create_job_db(db_path)
    _create_ready_clone_state(db_path)
    conn = _connect(db_path)
    try:
        conn.execute(
            """
            UPDATE messages
            SET content = ?, content_norm = ?, msg_type = ?, has_media = 0, grouped_id = NULL
            WHERE chat_id = ? AND message_id = ?
            """,
            ("before album", "before album", "TEXT", 100, 1),
        )
        for message_id in (11, 13):
            conn.execute(
                """
                INSERT INTO messages(
                    chat_id, message_id, msg_date_text, msg_date_ts, content,
                    content_norm, msg_type, grouped_id, has_media
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    100,
                    message_id,
                    f"2026-01-01 00:00:{message_id:02d}",
                    message_id,
                    "",
                    "",
                    "PHOTO",
                    999,
                    1,
                ),
            )
            conn.execute(
                """
                INSERT INTO message_media(
                    chat_id, message_id, media_kind, file_name, grouped_id, media_fingerprint
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    100,
                    message_id,
                    "PHOTO",
                    f"gap-{message_id}.jpg",
                    999,
                    f"fp-gap-{message_id}",
                ),
            )
        conn.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, content,
                content_norm, msg_type, grouped_id, has_media
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                100,
                20,
                "2026-01-01 00:00:20",
                20,
                "after album",
                "after album",
                "TEXT",
                None,
                0,
            ),
        )
        conn.execute(
            """
            INSERT INTO media_groups(
                chat_id, grouped_id, first_message_id, first_msg_date_ts,
                item_count, active_items
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (100, 999, 11, 11, 2, 2),
        )
        create_clone_migration(
            conn,
            migration_id="job-timeline",
            run_id="job-clone",
            plan_id="plan-text",
            job_id="job-timeline",
            mode="timeline_replay",
            target_chat_id=777,
            target_title="Source Backup",
            target_write_account="text:primary; media:primary",
            plan={"plan_id": "plan-text"},
        )
        conn.commit()
    finally:
        conn.close()

    client = _TimelineMigrationClient()
    client.add_source_message(11, grouped_id=999)
    client.add_source_message(12, grouped_id=999)
    client.add_source_message(13, grouped_id=999)
    with (
        patch("tg_harvest.admin_jobs.clone_timeline_migration._start_job_heartbeat", return_value=_heartbeat_pair()),
        patch("tg_harvest.admin_jobs.clone_timeline_migration.finish_job_heartbeat"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._admin_job_update_progress"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._admin_job_stop_requested", return_value=False),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._ensure_base_session_valid", return_value=True),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._create_isolated_worker_client", return_value=client),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._disconnect_worker_client"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._cleanup_isolated_worker_session"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration.time.sleep"),
    ):
        _admin_clone_timeline_migration_job_runner(
            "job-timeline",
            run_id="job-clone",
            plan_id="plan-text",
            migration_id="job-timeline",
            cfg=_runner_cfg(),
            get_conn_fn=lambda: _connect(db_path),
            admin_job_set_status_fn=lambda *_args: True,
            admin_job_append_log_fn=lambda *_args: None,
        )

    conn = _connect(db_path)
    try:
        migration = load_latest_clone_migration(conn, "job-clone")
        mapping_11 = load_clone_message_mapping(
            conn,
            run_id="job-clone",
            source_chat_id=100,
            source_message_id=11,
            chunk_index=0,
            mode="media_group_copy",
        )
        mapping_12 = load_clone_message_mapping(
            conn,
            run_id="job-clone",
            source_chat_id=100,
            source_message_id=12,
            chunk_index=0,
            mode="media_group_copy",
        )
        mapping_13 = load_clone_message_mapping(
            conn,
            run_id="job-clone",
            source_chat_id=100,
            source_message_id=13,
            chunk_index=0,
            mode="media_group_copy",
        )
    finally:
        conn.close()

    assert [event[0:2] for event in client.timeline_events] == [
        ("text", "before album"),
        ("media", [11, 12, 13]),
        ("text", "after album"),
    ]
    assert client.timeline_events[1][2]["drop_author"] is True
    assert "as_album" not in client.timeline_events[1][2]
    assert migration is not None
    assert migration["status"] == "done"
    assert migration["phase"] == "done"
    assert migration["text_sent"] == 2
    assert migration["media_sent"] == 3
    assert migration["media_group_sent"] == 1
    assert mapping_11 is not None and mapping_11["target_message_id"] == 9111
    assert mapping_12 is not None and mapping_12["target_message_id"] == 9112
    assert mapping_13 is not None and mapping_13["target_message_id"] == 9113


def test_clone_timeline_migration_splits_mixed_media_group_in_order(tmp_path):
    db_path = tmp_path / "clone-timeline-mixed-media-group.db"
    _create_job_db(db_path)
    _create_ready_clone_state(db_path)
    conn = _connect(db_path)
    try:
        conn.execute(
            """
            UPDATE messages
            SET content = '', content_norm = '', msg_type = 'TEXT',
                has_media = 0, grouped_id = NULL
            WHERE chat_id = ? AND message_id = ?
            """,
            (100, 1),
        )
        rows = [
            (21, "photo caption", "PHOTO"),
            (22, "", "VIDEO"),
            (23, "", "AUDIO"),
            (24, "", "PHOTO"),
        ]
        for message_id, text, msg_type in rows:
            conn.execute(
                """
                INSERT INTO messages(
                    chat_id, message_id, msg_date_text, msg_date_ts, content,
                    content_norm, msg_type, grouped_id, has_media
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    100,
                    message_id,
                    f"2026-01-01 00:00:{message_id}",
                    message_id,
                    text,
                    text,
                    msg_type,
                    2121,
                    1,
                ),
            )
        conn.execute(
            """
            INSERT INTO media_groups(
                chat_id, grouped_id, first_message_id, first_msg_date_ts,
                item_count, active_items
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (100, 2121, 21, 21, 4, 4),
        )
        create_clone_migration(
            conn,
            migration_id="job-timeline",
            run_id="job-clone",
            plan_id="plan-text",
            job_id="job-timeline",
            mode="timeline_replay",
            target_chat_id=777,
            target_title="Source Backup",
            target_write_account="text:primary; media:primary",
            plan={"plan_id": "plan-text"},
        )
        conn.commit()
    finally:
        conn.close()

    client = _TimelineMigrationClient()
    client.add_source_message(21, grouped_id=2121, media_kind="photo")
    client.add_source_message(22, grouped_id=2121, media_kind="video")
    client.add_source_message(23, grouped_id=2121, media_kind="audio")
    client.add_source_message(24, grouped_id=2121, media_kind="photo")

    with (
        patch("tg_harvest.admin_jobs.clone_timeline_migration._start_job_heartbeat", return_value=_heartbeat_pair()),
        patch("tg_harvest.admin_jobs.clone_timeline_migration.finish_job_heartbeat"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._admin_job_update_progress"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._admin_job_stop_requested", return_value=False),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._ensure_base_session_valid", return_value=True),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._create_isolated_worker_client", return_value=client),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._disconnect_worker_client"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._cleanup_isolated_worker_session"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration.time.sleep"),
    ):
        _admin_clone_timeline_migration_job_runner(
            "job-timeline",
            run_id="job-clone",
            plan_id="plan-text",
            migration_id="job-timeline",
            cfg=_runner_cfg(),
            get_conn_fn=lambda: _connect(db_path),
            admin_job_set_status_fn=lambda *_args: True,
            admin_job_append_log_fn=lambda *_args: None,
        )

    conn = _connect(db_path)
    try:
        migration = load_latest_clone_migration(conn, "job-clone")
        mappings = [
            load_clone_message_mapping(
                conn,
                run_id="job-clone",
                source_chat_id=100,
                source_message_id=message_id,
                chunk_index=0,
                mode="media_group_copy",
            )
            for message_id in (21, 22, 23, 24)
        ]
    finally:
        conn.close()

    assert [event[0:2] for event in client.timeline_events] == [
        ("media", 21),
        ("media", 22),
        ("media", 23),
        ("media", 24),
    ]
    assert all(event[2]["drop_author"] is True for event in client.timeline_events)
    assert all("as_album" not in event[2] for event in client.timeline_events)
    assert migration is not None
    assert migration["status"] == "done"
    assert migration["phase"] == "done"
    assert migration["media_sent"] == 4
    assert migration["media_group_sent"] == 1
    assert [mapping["target_message_id"] for mapping in mappings] == [
        9101,
        9102,
        9103,
        9104,
    ]


def test_clone_timeline_migration_chunks_album_compatible_group_at_limit(tmp_path):
    db_path = tmp_path / "clone-timeline-album-chunks.db"
    _create_job_db(db_path)
    _create_ready_clone_state(db_path)
    conn = _connect(db_path)
    try:
        conn.execute(
            """
            UPDATE messages
            SET content = '', content_norm = '', msg_type = 'TEXT',
                has_media = 0, grouped_id = NULL
            WHERE chat_id = ? AND message_id = ?
            """,
            (100, 1),
        )
        for message_id in range(31, 43):
            conn.execute(
                """
                INSERT INTO messages(
                    chat_id, message_id, msg_date_text, msg_date_ts, content,
                    content_norm, msg_type, grouped_id, has_media
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    100,
                    message_id,
                    f"2026-01-01 00:00:{message_id:02d}",
                    message_id,
                    "",
                    "",
                    "PHOTO",
                    3030,
                    1,
                ),
            )
        conn.execute(
            """
            INSERT INTO media_groups(
                chat_id, grouped_id, first_message_id, first_msg_date_ts,
                item_count, active_items
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (100, 3030, 31, 31, 12, 12),
        )
        create_clone_migration(
            conn,
            migration_id="job-timeline",
            run_id="job-clone",
            plan_id="plan-text",
            job_id="job-timeline",
            mode="timeline_replay",
            target_chat_id=777,
            target_title="Source Backup",
            target_write_account="text:primary; media:primary",
            plan={"plan_id": "plan-text"},
        )
        conn.commit()
    finally:
        conn.close()

    client = _TimelineMigrationClient()
    for message_id in range(31, 43):
        client.add_source_message(message_id, grouped_id=3030, media_kind="photo")

    with (
        patch("tg_harvest.admin_jobs.clone_timeline_migration._start_job_heartbeat", return_value=_heartbeat_pair()),
        patch("tg_harvest.admin_jobs.clone_timeline_migration.finish_job_heartbeat"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._admin_job_update_progress"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._admin_job_stop_requested", return_value=False),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._ensure_base_session_valid", return_value=True),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._create_isolated_worker_client", return_value=client),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._disconnect_worker_client"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._cleanup_isolated_worker_session"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration.time.sleep"),
    ):
        _admin_clone_timeline_migration_job_runner(
            "job-timeline",
            run_id="job-clone",
            plan_id="plan-text",
            migration_id="job-timeline",
            cfg=_runner_cfg(),
            get_conn_fn=lambda: _connect(db_path),
            admin_job_set_status_fn=lambda *_args: True,
            admin_job_append_log_fn=lambda *_args: None,
        )

    conn = _connect(db_path)
    try:
        migration = load_latest_clone_migration(conn, "job-clone")
        mappings = [
            load_clone_message_mapping(
                conn,
                run_id="job-clone",
                source_chat_id=100,
                source_message_id=message_id,
                chunk_index=0,
                mode="media_group_copy",
            )
            for message_id in range(31, 43)
        ]
    finally:
        conn.close()

    assert [event[0:2] for event in client.timeline_events] == [
        ("media", list(range(31, 41))),
        ("media", [41, 42]),
    ]
    assert all(event[2]["drop_author"] is True for event in client.timeline_events)
    assert all("as_album" not in event[2] for event in client.timeline_events)
    assert migration is not None
    assert migration["status"] == "done"
    assert migration["phase"] == "done"
    assert migration["media_sent"] == 12
    assert migration["media_group_sent"] == 1
    assert [mapping["target_message_id"] for mapping in mappings] == [
        9111,
        9112,
        9113,
        9114,
        9115,
        9116,
        9117,
        9118,
        9119,
        9120,
        9121,
        9122,
    ]


class _RelayMediaClient:
    def __init__(self, *, source_ok=True, target_ok=True, relay_ok=True):
        self.forward_calls = []
        self.delete_calls = []
        self.source_messages = {}
        self.source_ok = source_ok
        self.target_ok = target_ok
        self.relay_ok = relay_ok

    def add_source_message(self, message_id, *, grouped_id=None, media=True):
        message = SimpleNamespace(
            id=int(message_id),
            grouped_id=grouped_id,
            media=object() if media else None,
        )
        self.source_messages[int(message_id)] = message
        return message

    def get_entity(self, value):
        normalized = int(value) if isinstance(value, int) or str(value).lstrip("-").isdigit() else value
        if normalized in {100, -100100, -100}:
            if not self.source_ok:
                raise ValueError("Could not find the input entity")
            return SimpleNamespace(id=100, title="Source Group")
        if normalized in {777, -100777, -777}:
            if not self.target_ok:
                raise ValueError("Could not find the input entity")
            return SimpleNamespace(id=777, title="Source Backup")
        if normalized in {999, -100999, -999}:
            if not self.relay_ok:
                raise ValueError("Could not find the input entity")
            return SimpleNamespace(id=999, title="Relay Channel")
        raise ValueError("Could not find the input entity")

    def get_messages(self, _entity, **kwargs):
        ids = kwargs.get("ids")
        if ids is not None:
            if isinstance(ids, list):
                return [
                    self.source_messages.get(int(message_id))
                    or self.add_source_message(int(message_id))
                    for message_id in ids
                ]
            return self.source_messages.get(int(ids)) or self.add_source_message(int(ids))
        return list(self.source_messages.values())

    def forward_messages(self, *args, **kwargs):
        self.forward_calls.append((args, kwargs))
        messages = args[1] if len(args) > 1 else None
        if isinstance(messages, list):
            return [
                SimpleNamespace(id=9200 + len(self.forward_calls) * 10 + index)
                for index, _message_id in enumerate(messages, start=1)
            ]
        return [SimpleNamespace(id=9200 + len(self.forward_calls))]

    def delete_messages(self, *args, **kwargs):
        self.delete_calls.append((args, kwargs))
        return True



def test_clone_timeline_migration_job_copies_media_via_relay_without_attribution(tmp_path):
    db_path = tmp_path / "clone-timeline-relay.db"
    _create_job_db(db_path)
    _create_ready_clone_state(db_path)
    conn = _connect(db_path)
    try:
        conn.execute(
            """
            UPDATE messages
            SET content = ?, content_norm = ?, msg_type = ?, has_media = 1, grouped_id = NULL
            WHERE chat_id = ? AND message_id = ?
            """,
            ("photo caption", "photo caption", "PHOTO", 100, 1),
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO message_media(
                chat_id, message_id, media_kind, file_name, grouped_id, media_fingerprint
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (100, 1, "PHOTO", "photo.jpg", None, "fp-photo"),
        )
        conn.execute(
            """
            UPDATE admin_clone_plans
            SET
                migration_account = ?,
                media_strategy = ?,
                media_group_strategy = ?,
                avatar_strategy = ?,
                capabilities_json = ?,
                plan_json = ?
            WHERE plan_id = ?
            """,
            (
                "unavailable",
                "relay_copy_without_attribution",
                "relay_api_rebuild",
                "skip_not_implemented",
                (
                    '{"target_write_account":"secondary","media_relay":'
                    '{"enabled":true,"chat_id":999,"source_account":"primary",'
                    '"target_account":"secondary","keeps_source_link":false,'
                    '"keeps_relay_link":false}}'
                ),
                (
                    '{"target_write_account":"secondary","media_relay":'
                    '{"enabled":true,"chat_id":999,"source_account":"primary",'
                    '"target_account":"secondary","keeps_source_link":false,'
                    '"keeps_relay_link":false}}'
                ),
                "plan-text",
            ),
        )
        create_clone_migration(
            conn,
            migration_id="job-timeline",
            run_id="job-clone",
            plan_id="plan-text",
            job_id="job-timeline",
            mode="timeline_replay",
            target_chat_id=777,
            target_title="Source Backup",
            target_write_account="text:secondary; media:primary->relay->secondary",
            plan={"plan_id": "plan-text"},
        )
        conn.commit()
    finally:
        conn.close()

    source_client = _RelayMediaClient(source_ok=True, target_ok=False, relay_ok=True)
    target_client = _RelayMediaClient(source_ok=False, target_ok=True, relay_ok=True)
    source_client.add_source_message(1)
    with (
        patch("tg_harvest.admin_jobs.clone_timeline_migration._start_job_heartbeat", return_value=_heartbeat_pair()),
        patch("tg_harvest.admin_jobs.clone_timeline_migration.finish_job_heartbeat"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._admin_job_update_progress"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._admin_job_stop_requested", return_value=False),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._ensure_base_session_valid", return_value=True),
        patch(
            "tg_harvest.admin_jobs.clone_timeline_migration._create_isolated_worker_client",
            side_effect=[source_client, target_client],
        ),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._disconnect_worker_client"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration._cleanup_isolated_worker_session"),
        patch("tg_harvest.admin_jobs.clone_timeline_migration.time.sleep"),
    ):
        _admin_clone_timeline_migration_job_runner(
            "job-timeline",
            run_id="job-clone",
            plan_id="plan-text",
            migration_id="job-timeline",
            cfg=_runner_cfg(),
            get_conn_fn=lambda: _connect(db_path),
            admin_job_set_status_fn=lambda *_args: True,
            admin_job_append_log_fn=lambda *_args: None,
        )

    conn = _connect(db_path)
    try:
        migration = load_latest_clone_migration(conn, "job-clone", mode="timeline_replay")
        mapping = load_clone_message_mapping(
            conn,
            run_id="job-clone",
            source_chat_id=100,
            source_message_id=1,
            chunk_index=0,
            mode="media_copy",
        )
    finally:
        conn.close()

    assert source_client.forward_calls == [
        (
            (SimpleNamespace(id=999, title="Relay Channel"), 1),
            {
                "from_peer": SimpleNamespace(id=100, title="Source Group"),
                "drop_author": True,
            },
        )
    ]
    assert target_client.forward_calls == [
        (
            (SimpleNamespace(id=777, title="Source Backup"), 9201),
            {
                "from_peer": SimpleNamespace(id=999, title="Relay Channel"),
                "drop_author": True,
            },
        )
    ]
    assert source_client.delete_calls == [
        (
            (SimpleNamespace(id=999, title="Relay Channel"), [9201]),
            {"revoke": True},
        )
    ]
    assert target_client.delete_calls == []
    assert migration is not None
    assert migration["status"] == "done"
    assert migration["phase"] == "done"
    assert migration["media_sent"] == 1
    assert mapping is not None
    assert mapping["target_message_id"] == 9201
    assert mapping["status"] == "done"
