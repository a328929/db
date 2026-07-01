import sqlite3
from types import SimpleNamespace

from tg_harvest.admin_jobs.recovery import _filter_recovery_chat_scan_rows
from tg_harvest.domain.chat_inventory import (
    SessionChatRecoveryRow,
    discover_session_files,
    scan_session_chat_recovery_rows,
)
from tg_harvest.storage.recovery import (
    build_recovery_overview,
    list_recovery_chat_candidates,
    recover_chats_from_candidates,
    replace_recovery_chat_scan_results,
)


def _create_session_db(path):
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE entities (
                id integer primary key,
                hash integer not null,
                username text,
                phone integer,
                name text,
                date integer
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO entities(id, hash, username, phone, name, date)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (-1001234567890, 111, "public_chat", None, "Public Chat", 1775037600),
                (-42, 222, "", None, "Small Group", 1775037500),
                (99, 333, "user", None, "User", 1775037400),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def _create_recovery_schema(conn):
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
            first_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
            last_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
            last_message_created_at TEXT NOT NULL DEFAULT ''
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE admin_recovery_chats (
            chat_id INTEGER PRIMARY KEY,
            chat_title TEXT NOT NULL,
            chat_username TEXT,
            chat_type TEXT,
            is_public INTEGER NOT NULL DEFAULT 0,
            source_session TEXT,
            source_entity_id INTEGER,
            source_access_hash INTEGER,
            availability_reason TEXT,
            session_entity_date TEXT,
            session_entity_ts INTEGER,
            recovered_at TEXT,
            recovered_job_id TEXT,
            scan_job_id TEXT,
            scanned_at TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE messages (
            chat_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            msg_date_text TEXT NOT NULL,
            msg_date_ts INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT '',
            PRIMARY KEY(chat_id, message_id)
        )
        """
    )
    conn.commit()


def test_scan_session_chat_recovery_rows_reads_negative_entities(tmp_path):
    session_path = tmp_path / "my_session.session"
    _create_session_db(session_path)

    rows, errors = scan_session_chat_recovery_rows([session_path])

    assert errors == []
    assert [row.chat_id for row in rows] == [1234567890, 42]
    public = {row.chat_id: row for row in rows}[1234567890]
    assert public.chat_title == "Public Chat"
    assert public.chat_username == "public_chat"
    assert public.is_public == 1
    assert public.source_session == "my_session.session"
    assert public.source_entity_id == -1001234567890
    assert public.source_access_hash == 111
    assert public.session_entity_date == "2026-04-01 10:00:00"


def test_discover_session_files_includes_worker_sessions(tmp_path):
    main_session = tmp_path / "my_session.session"
    worker_session = tmp_path / "my_session_worker_job.session"
    main_session.write_bytes(b"")
    worker_session.write_bytes(b"")

    files = discover_session_files(tmp_path / "my_session")

    assert [path.name for path in files] == [
        "my_session.session",
        "my_session_worker_job.session",
    ]


def test_recovery_storage_saves_candidates_and_recovers_chats():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    _create_recovery_schema(conn)
    try:
        saved = replace_recovery_chat_scan_results(
            conn,
            [
                SessionChatRecoveryRow(
                    chat_id=1,
                    chat_title="Recovered One",
                    chat_username="one",
                    chat_type="SessionEntity",
                    is_public=1,
                    source_session="main.session",
                    source_entity_id=-1001,
                    source_access_hash=9001,
                    session_entity_date="2026-04-01 10:00:00",
                    session_entity_ts=1775037600,
                ),
                SessionChatRecoveryRow(
                    chat_id=2,
                    chat_title="Recovered Two",
                    source_session="main.session",
                    source_entity_id=-1002,
                    source_access_hash=9002,
                    availability_reason="Telegram 返回该会话不可访问",
                    session_entity_ts=1775037500,
                ),
            ],
            scan_job_id="scan-1",
            scanned_at="2026-04-01T00:00:00+00:00",
        )

        assert saved == 2
        overview = build_recovery_overview(conn)
        assert overview["total_count"] == 2
        assert overview["pending_count"] == 2

        result = recover_chats_from_candidates(
            conn,
            chat_ids=[1],
            job_id="restore-1",
            recovered_at="2026-04-02T00:00:00+00:00",
        )

        assert result == {
            "candidate_count": 1,
            "recovered_count": 1,
            "skipped_count": 0,
        }
        rows = list_recovery_chat_candidates(conn)
        rows_by_id = {row["chat_id"]: row for row in rows}
        assert rows_by_id[1]["in_database"] == 1
        assert rows_by_id[1]["recovered_job_id"] == "restore-1"
        assert rows_by_id[1]["source_access_hash"] == 9001
        assert rows_by_id[2]["in_database"] == 0
        assert rows_by_id[2]["source_access_hash"] == 9002
        assert rows_by_id[2]["availability_reason"] == "Telegram 返回该会话不可访问"

        cur = conn.cursor()
        cur.execute("SELECT chat_title, chat_username, is_public FROM chats WHERE chat_id = 1")
        chat_row = cur.fetchone()
        assert dict(chat_row) == {
            "chat_title": "Recovered One",
            "chat_username": "one",
            "is_public": 1,
        }
        conn.execute(
            """
            INSERT INTO messages(chat_id, message_id, msg_date_text, msg_date_ts)
            VALUES (1, 10, '2026-04-02 12:00:00', 1775131200)
            """
        )
        conn.execute("UPDATE chats SET message_count = 1 WHERE chat_id = 1")
        conn.commit()
        refreshed_rows = list_recovery_chat_candidates(conn)
        refreshed_by_id = {row["chat_id"]: row for row in refreshed_rows}
        assert refreshed_by_id[1]["message_count"] == 1
        assert refreshed_by_id[1]["last_message_at"] == "2026-04-02 12:00:00"
        assert refreshed_by_id[1]["last_message_ts"] == 1775131200

        saved = replace_recovery_chat_scan_results(
            conn,
            [
                SessionChatRecoveryRow(
                    chat_id=1,
                    chat_title="Recovered One Renamed",
                    chat_username="one",
                    chat_type="SessionEntity",
                    is_public=1,
                    source_session="main.session",
                    source_entity_id=-1001,
                    source_access_hash=9001,
                    session_entity_ts=1775037700,
                ),
            ],
            scan_job_id="scan-2",
            scanned_at="2026-04-03T00:00:00+00:00",
        )

        assert saved == 1
        replaced_rows = list_recovery_chat_candidates(conn)
        assert [row["chat_id"] for row in replaced_rows] == [1]
        assert replaced_rows[0]["chat_title"] == "Recovered One Renamed"
        assert replaced_rows[0]["scan_job_id"] == "scan-2"
        assert replaced_rows[0]["recovered_job_id"] == "restore-1"
        assert replaced_rows[0]["source_access_hash"] == 9001
        replaced_overview = build_recovery_overview(conn)
        assert replaced_overview["total_count"] == 1
    finally:
        conn.close()


class _RestrictionReason:
    def __init__(self, *, platform="", reason="", text=""):
        self.platform = platform
        self.reason = reason
        self.text = text


class _RecoveryValidationClient:
    def get_entity(self, value):
        value_id = int(getattr(value, "channel_id", value))
        if value_id == 1001:
            return SimpleNamespace(id=1001, title="Accessible")
        if value_id == 1002:
            return SimpleNamespace(
                id=1002,
                title="Terms",
                restriction_reason=[
                    _RestrictionReason(platform="all", reason="terms"),
                ],
            )
        if value_id == 1003:
            return SimpleNamespace(
                id=1003,
                title="Porn Restricted",
                restriction_reason=[
                    _RestrictionReason(platform="all", reason="porn"),
                ],
            )
        if value_id == 1004:
            raise ValueError("Could not find the input entity")
        raise RuntimeError(f"unexpected entity lookup {value!r}")


class _BatchedRecoveryValidationClient(_RecoveryValidationClient):
    def __init__(self, *, fail_batch=False):
        self.fail_batch = fail_batch
        self.batch_call_count = 0
        self.get_entity_call_count = 0

    def __call__(self, request):
        self.batch_call_count += 1
        if self.fail_batch:
            raise RuntimeError("batch unavailable")
        entities = []
        for value in request.id:
            value_id = int(value.channel_id)
            if value_id == 1001:
                entities.append(SimpleNamespace(id=1001, title="Accessible"))
            elif value_id == 1002:
                entities.append(
                    SimpleNamespace(
                        id=1002,
                        title="Terms",
                        restriction_reason=[
                            _RestrictionReason(platform="all", reason="terms"),
                        ],
                    )
                )
            elif value_id == 1003:
                entities.append(
                    SimpleNamespace(
                        id=1003,
                        title="Porn Restricted",
                        restriction_reason=[
                            _RestrictionReason(platform="all", reason="porn"),
                        ],
                    )
                )
        return SimpleNamespace(chats=entities)

    def get_entity(self, value):
        self.get_entity_call_count += 1
        return super().get_entity(value)


def test_recovery_validation_marks_unavailable_rows_but_keeps_them():
    rows = [
        SessionChatRecoveryRow(
            chat_id=1001,
            chat_title="Accessible",
            source_entity_id=-1001001,
            source_access_hash=11,
        ),
        SessionChatRecoveryRow(
            chat_id=1002,
            chat_title="Terms",
            source_entity_id=-1001002,
            source_access_hash=22,
        ),
        SessionChatRecoveryRow(
            chat_id=1003,
            chat_title="Porn Restricted",
            source_entity_id=-1001003,
            source_access_hash=33,
        ),
        SessionChatRecoveryRow(
            chat_id=1004,
            chat_title="Dissolved",
            source_entity_id=-1001004,
            source_access_hash=44,
        ),
    ]

    filtered_rows, stats = _filter_recovery_chat_scan_rows(
        _RecoveryValidationClient(),
        rows,
    )

    assert [row.chat_id for row in filtered_rows] == [1001, 1002, 1003]
    filtered_by_id = {row.chat_id: row for row in filtered_rows}
    assert (
        filtered_by_id[1002].availability_reason
        == "Telegram 返回全部平台/违反条款，该会话不可访问"
    )
    assert stats["dissolved_count"] == 1
    assert stats["unavailable_count"] == 1
    assert stats["warning_count"] == 0


def test_recovery_validation_uses_batched_channel_lookup_when_possible():
    rows = [
        SessionChatRecoveryRow(
            chat_id=1001,
            chat_title="Accessible",
            source_entity_id=-1001001,
            source_access_hash=11,
        ),
        SessionChatRecoveryRow(
            chat_id=1002,
            chat_title="Terms",
            source_entity_id=-1001002,
            source_access_hash=22,
        ),
        SessionChatRecoveryRow(
            chat_id=1003,
            chat_title="Porn Restricted",
            source_entity_id=-1001003,
            source_access_hash=33,
        ),
    ]
    progress_calls = []
    client = _BatchedRecoveryValidationClient()

    filtered_rows, stats = _filter_recovery_chat_scan_rows(
        client,
        rows,
        progress_callback=lambda current, total: progress_calls.append(
            (current, total)
        ),
    )

    assert [row.chat_id for row in filtered_rows] == [1001, 1002, 1003]
    filtered_by_id = {row.chat_id: row for row in filtered_rows}
    assert (
        filtered_by_id[1002].availability_reason
        == "Telegram 返回全部平台/违反条款，该会话不可访问"
    )
    assert stats["dissolved_count"] == 0
    assert stats["unavailable_count"] == 1
    assert stats["warning_count"] == 0
    assert client.batch_call_count == 1
    assert client.get_entity_call_count == 0
    assert progress_calls == [(1, 3), (2, 3), (3, 3)]


def test_recovery_validation_falls_back_when_batch_lookup_fails():
    rows = [
        SessionChatRecoveryRow(
            chat_id=1001,
            chat_title="Accessible",
            source_entity_id=-1001001,
            source_access_hash=11,
        ),
        SessionChatRecoveryRow(
            chat_id=1002,
            chat_title="Terms",
            source_entity_id=-1001002,
            source_access_hash=22,
        ),
    ]
    client = _BatchedRecoveryValidationClient(fail_batch=True)

    filtered_rows, stats = _filter_recovery_chat_scan_rows(client, rows)

    assert [row.chat_id for row in filtered_rows] == [1001, 1002]
    filtered_by_id = {row.chat_id: row for row in filtered_rows}
    assert (
        filtered_by_id[1002].availability_reason
        == "Telegram 返回全部平台/违反条款，该会话不可访问"
    )
    assert stats["dissolved_count"] == 0
    assert stats["unavailable_count"] == 1
    assert stats["warning_count"] == 0
    assert client.batch_call_count == 1
    assert client.get_entity_call_count == 2
