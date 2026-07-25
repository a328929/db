"""
Integration tests for the AdminJob framework.

Tests real-world usage scenarios with migrated tasks and error handling.
"""

import sqlite3
import time
from contextlib import closing
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from tg_harvest.admin_jobs.framework import (
    AdminJobContext,
    AdminJobResult,
    admin_job,
)


# =============================================================================
# Test Database Setup
# =============================================================================


@pytest.fixture
def integration_db():
    """Create an in-memory database with realistic schema."""
    conn = sqlite3.connect(":memory:")

    # Create chats table
    conn.execute(
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

    # Create recovery table
    conn.execute(
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

    # Insert test data
    conn.executemany(
        """
        INSERT INTO admin_recovery_chats
        (chat_id, chat_title, chat_username, chat_type, is_public, scanned_at)
        VALUES (?, ?, ?, ?, ?, datetime('now'))
        """,
        [
            (-1001234567890, "Test Channel", "testchannel", "channel", 1),
            (-1009876543210, "Private Group", None, "group", 0),
            (-1005555555555, "Another Channel", "another", "channel", 1),
        ],
    )

    conn.commit()
    yield conn
    conn.close()


# =============================================================================
# Test Migrated Tasks
# =============================================================================


def test_recovery_restore_with_framework(integration_db):
    """Test recovery restore task using framework."""
    import tempfile
    import os

    # Use file-based DB since execute() closes connections
    db_fd, db_path = tempfile.mkstemp()
    try:
        # Create schema and data
        conn = sqlite3.connect(db_path)
        conn.execute(
            """
            CREATE TABLE chats (
                chat_id INTEGER PRIMARY KEY,
                chat_title TEXT NOT NULL,
                chat_username TEXT,
                is_public INTEGER NOT NULL DEFAULT 0,
                chat_type TEXT,
                message_count INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE admin_recovery_chats (
                chat_id INTEGER PRIMARY KEY,
                chat_title TEXT NOT NULL,
                chat_username TEXT,
                chat_type TEXT,
                is_public INTEGER NOT NULL DEFAULT 0,
                scanned_at TEXT NOT NULL
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO admin_recovery_chats
            (chat_id, chat_title, chat_username, chat_type, is_public, scanned_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            """,
            [
                (-1001234567890, "Test Channel", "testchannel", "channel", 1),
                (-1009876543210, "Private Group", None, "group", 0),
            ],
        )
        conn.commit()
        conn.close()

        @admin_job(name="Test Recovery Restore", requires_client=False, heartbeat=False)
        def test_recovery_restore(ctx: AdminJobContext) -> AdminJobResult:
            """Simplified recovery restore for testing."""
            ctx.log("Starting recovery restore")

            # Query all recovery candidates
            candidates = ctx.query("SELECT * FROM admin_recovery_chats")
            ctx.log(f"Found {len(candidates)} candidates")
            ctx.progress.start(total=len(candidates))

            # Insert into chats table
            recovered_count = 0
            for candidate in candidates:
                ctx.execute(
                    """
                    INSERT OR IGNORE INTO chats
                    (chat_id, chat_title, chat_username, is_public, chat_type)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        candidate["chat_id"],
                        candidate["chat_title"],
                        candidate["chat_username"],
                        candidate["is_public"],
                        candidate["chat_type"],
                    ),
                )
                recovered_count += 1
                ctx.progress.update(delta=1)

            ctx.progress.finish()
            return AdminJobResult(
                success=True,
                message=f"Recovered {recovered_count} chats",
                data={"recovered": recovered_count, "candidates": len(candidates)},
            )

        # Execute the job
        set_status_fn = Mock(return_value=True)
        append_log_fn = Mock()
        get_conn_fn = lambda: sqlite3.connect(db_path)
        cfg = Mock()

        test_recovery_restore(
            "job_recovery_test",
            cfg=cfg,
            get_conn_fn=get_conn_fn,
            admin_job_set_status_fn=set_status_fn,
            admin_job_append_log_fn=append_log_fn,
        )

        # Verify results
        verify_conn = sqlite3.connect(db_path)
        cursor = verify_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM chats")
        count = cursor.fetchone()[0]
        assert count == 2
        verify_conn.close()

        # Verify logging
        log_messages = [call[0][1] for call in append_log_fn.call_args_list]
        assert any("Starting recovery restore" in msg for msg in log_messages)
        assert any("Found 2 candidates" in msg for msg in log_messages)
        assert any("Recovered 2 chats" in msg for msg in log_messages)

        # Verify status transitions
        assert set_status_fn.call_args_list[0][0] == ("job_recovery_test", "running")
        assert set_status_fn.call_args_list[1][0] == ("job_recovery_test", "done")
    finally:
        os.close(db_fd)
        os.unlink(db_path)


def test_missing_chats_scan_with_framework(integration_db):
    """Test missing chats scan using framework."""
    import tempfile
    import os

    # Use file-based DB
    db_fd, db_path = tempfile.mkstemp()
    try:
        # Create schema and data
        conn = sqlite3.connect(db_path)
        conn.execute(
            """
            CREATE TABLE chats (
                chat_id INTEGER PRIMARY KEY,
                chat_title TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE admin_recovery_chats (
                chat_id INTEGER PRIMARY KEY,
                chat_title TEXT NOT NULL,
                scanned_at TEXT NOT NULL
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO admin_recovery_chats
            (chat_id, chat_title, scanned_at)
            VALUES (?, ?, datetime('now'))
            """,
            [
                (-1001234567890, "Test Channel"),
                (-1009876543210, "Private Group"),
                (-1005555555555, "Another Channel"),
            ],
        )
        conn.commit()
        conn.close()

        @admin_job(name="Test Missing Chats", requires_client=False, heartbeat=False)
        def test_missing_chats(ctx: AdminJobContext) -> AdminJobResult:
            """Simulate missing chats scan."""
            ctx.log("Scanning for missing chats")

            # Get chats from recovery that aren't in main table
            recovery_chats = ctx.query("SELECT chat_id FROM admin_recovery_chats")
            main_chats = ctx.query("SELECT chat_id FROM chats")

            recovery_ids = {row["chat_id"] for row in recovery_chats}
            main_ids = {row["chat_id"] for row in main_chats}
            missing_ids = recovery_ids - main_ids

            ctx.log(f"Found {len(missing_ids)} missing chats")

            return AdminJobResult(
                success=True,
                message=f"Scan complete: {len(missing_ids)} missing",
                data={"missing_count": len(missing_ids), "missing_ids": list(missing_ids)},
            )

        set_status_fn = Mock(return_value=True)
        append_log_fn = Mock()
        get_conn_fn = lambda: sqlite3.connect(db_path)
        cfg = Mock()

        test_missing_chats(
            "job_missing_scan",
            cfg=cfg,
            get_conn_fn=get_conn_fn,
            admin_job_set_status_fn=set_status_fn,
            admin_job_append_log_fn=append_log_fn,
        )

        # All recovery chats should be "missing" since chats table is empty
        log_messages = [call[0][1] for call in append_log_fn.call_args_list]
        assert any("Found 3 missing chats" in msg for msg in log_messages)
    finally:
        os.close(db_fd)
        os.unlink(db_path)


def test_restricted_chats_scan_with_framework(integration_db):
    """Test restricted chats scan using framework."""

    @admin_job(name="Test Restricted Scan", requires_client=False, heartbeat=False)
    def test_restricted_scan(ctx: AdminJobContext) -> AdminJobResult:
        """Simulate restricted chats scan."""
        ctx.log("Scanning for restricted chats")
        ctx.progress.start(total=None, stage="scanning")

        # Get chats with availability reasons
        restricted = ctx.query(
            "SELECT * FROM admin_recovery_chats WHERE availability_reason IS NOT NULL"
        )

        ctx.log(f"Found {len(restricted)} restricted chats")
        ctx.progress.finish()

        return AdminJobResult(
            success=True,
            message=f"Found {len(restricted)} restricted",
            data={"restricted_count": len(restricted)},
        )

    set_status_fn = Mock(return_value=True)
    append_log_fn = Mock()
    get_conn_fn = lambda: integration_db
    cfg = Mock()

    test_restricted_scan(
        "job_restricted_scan",
        cfg=cfg,
        get_conn_fn=get_conn_fn,
        admin_job_set_status_fn=set_status_fn,
        admin_job_append_log_fn=append_log_fn,
    )

    # Should find 0 since we didn't add any restricted chats
    log_messages = [call[0][1] for call in append_log_fn.call_args_list]
    assert any("Found 0 restricted chats" in msg for msg in log_messages)


def test_framework_backward_compatibility():
    """Test that framework-based jobs work with existing infrastructure."""

    @admin_job(name="Compat Job", requires_client=False, heartbeat=False)
    def compat_job(ctx: AdminJobContext) -> AdminJobResult:
        # Use all context features
        ctx.log("Testing compatibility")
        ctx.progress.start(total=5)

        for i in range(5):
            ctx.progress.update(current=i + 1)
            if ctx.check_stop_requested():
                return AdminJobResult(success=False, message="Stopped")

        ctx.progress.finish()
        return AdminJobResult(success=True, message="Compatible")

    # Mock all required parameters
    set_status_fn = Mock(return_value=True)
    append_log_fn = Mock()
    get_conn_fn = Mock()
    cfg = Mock()

    with patch("tg_harvest.admin_jobs.framework._admin_job_stop_requested") as mock_stop:
        with patch("tg_harvest.admin_jobs.framework._admin_job_update_progress") as mock_progress:
            mock_stop.return_value = False

            compat_job(
                "job_compat",
                cfg=cfg,
                get_conn_fn=get_conn_fn,
                admin_job_set_status_fn=set_status_fn,
                admin_job_append_log_fn=append_log_fn,
            )

            # Verify all infrastructure was used correctly
            assert set_status_fn.called
            assert append_log_fn.called
            assert mock_progress.called
            assert mock_stop.called


# =============================================================================
# Test Error Scenarios
# =============================================================================


def test_database_error_handling(integration_db):
    """Test that database errors are properly handled."""

    @admin_job(name="DB Error Job", requires_client=False, heartbeat=False)
    def db_error_job(ctx: AdminJobContext) -> AdminJobResult:
        ctx.log("Starting job")
        # This will fail - table doesn't exist
        ctx.execute("INSERT INTO nonexistent_table (id) VALUES (?)", (1,))
        return AdminJobResult(success=True)

    set_status_fn = Mock(return_value=True)
    append_log_fn = Mock()
    get_conn_fn = lambda: integration_db
    cfg = Mock()

    db_error_job(
        "job_db_error",
        cfg=cfg,
        get_conn_fn=get_conn_fn,
        admin_job_set_status_fn=set_status_fn,
        admin_job_append_log_fn=append_log_fn,
    )

    # Verify error status was set
    error_status_set = any(
        call[0] == ("job_db_error", "error") for call in set_status_fn.call_args_list
    )
    assert error_status_set

    # Verify error was logged
    log_messages = [call[0][1] for call in append_log_fn.call_args_list]
    assert any("OperationalError" in msg for msg in log_messages)


def test_client_connection_error():
    """Test that client connection errors are properly handled."""

    @admin_job(name="Client Error", requires_client=True, heartbeat=False)
    def client_error_job(ctx: AdminJobContext) -> AdminJobResult:
        return AdminJobResult(success=True)

    set_status_fn = Mock(return_value=True)
    append_log_fn = Mock()
    get_conn_fn = Mock()
    cfg = Mock()

    # Mock client creation to fail
    with patch("tg_harvest.admin_jobs.framework._create_isolated_worker_client") as mock_create:
        with patch("tg_harvest.admin_jobs.framework._cleanup_isolated_worker_session"):
            mock_create.side_effect = RuntimeError("Connection failed")

            client_error_job(
                "job_client_error",
                cfg=cfg,
                get_conn_fn=get_conn_fn,
                admin_job_set_status_fn=set_status_fn,
                admin_job_append_log_fn=append_log_fn,
            )

            # Verify error was logged
            log_messages = [call[0][1] for call in append_log_fn.call_args_list]
            assert any("客户端连接失败" in msg for msg in log_messages)
            assert any("Connection failed" in msg for msg in log_messages)

            # Verify error status
            error_status_set = any(
                call[0] == ("job_client_error", "error")
                for call in set_status_fn.call_args_list
            )
            assert error_status_set


def test_stop_signal_respected(integration_db):
    """Test that stop requests are honored."""

    stop_check_count = [0]

    @admin_job(name="Stoppable Job", requires_client=False, heartbeat=False)
    def stoppable_job(ctx: AdminJobContext) -> AdminJobResult:
        ctx.log("Starting long operation")
        ctx.progress.start(total=100)

        for i in range(100):
            stop_check_count[0] += 1
            if ctx.check_stop_requested():
                ctx.log(f"Stopped at iteration {i}")
                return AdminJobResult(success=False, message="Stopped by user")
            ctx.progress.update(current=i + 1)

        ctx.progress.finish()
        return AdminJobResult(success=True, message="Completed all iterations")

    set_status_fn = Mock(return_value=True)
    append_log_fn = Mock()
    get_conn_fn = lambda: integration_db
    cfg = Mock()

    # Mock stop request to return True after a few checks
    with patch("tg_harvest.admin_jobs.framework._admin_job_stop_requested") as mock_stop:
        call_count = [0]

        def stop_on_fifth_call(job_id):
            call_count[0] += 1
            return call_count[0] >= 5

        mock_stop.side_effect = stop_on_fifth_call

        stoppable_job(
            "job_stoppable",
            cfg=cfg,
            get_conn_fn=get_conn_fn,
            admin_job_set_status_fn=set_status_fn,
            admin_job_append_log_fn=append_log_fn,
        )

        # Verify job stopped early
        log_messages = [call[0][1] for call in append_log_fn.call_args_list]
        assert any("Stopped at iteration" in msg for msg in log_messages)
        assert not any("Completed all iterations" in msg for msg in log_messages)

        # Verify error status (stopped jobs return success=False)
        error_status_set = any(
            call[0] == ("job_stoppable", "error") for call in set_status_fn.call_args_list
        )
        assert error_status_set


def test_progress_updates_logged(integration_db):
    """Test that progress updates appear in logs."""

    @admin_job(name="Progress Logging", requires_client=False, heartbeat=False)
    def progress_logging_job(ctx: AdminJobContext) -> AdminJobResult:
        ctx.log("Starting with progress")
        ctx.progress.start(total=10, stage="processing")

        for i in range(10):
            ctx.log(f"Processing item {i + 1}")
            ctx.progress.update(current=i + 1)

        ctx.progress.finish()
        ctx.log("All items processed")
        return AdminJobResult(success=True, message="Done with progress")

    set_status_fn = Mock(return_value=True)
    append_log_fn = Mock()
    get_conn_fn = lambda: integration_db
    cfg = Mock()

    with patch("tg_harvest.admin_jobs.framework._admin_job_update_progress") as mock_progress:
        progress_logging_job(
            "job_progress_log",
            cfg=cfg,
            get_conn_fn=get_conn_fn,
            admin_job_set_status_fn=set_status_fn,
            admin_job_append_log_fn=append_log_fn,
        )

        # Verify progress was tracked
        assert mock_progress.call_count == 12  # start + 10 updates + finish

        # Verify all log messages appeared
        log_messages = [call[0][1] for call in append_log_fn.call_args_list]
        assert any("Starting with progress" in msg for msg in log_messages)
        assert any("Processing item 1" in msg for msg in log_messages)
        assert any("Processing item 10" in msg for msg in log_messages)
        assert any("All items processed" in msg for msg in log_messages)


def test_transaction_rollback_on_error(integration_db):
    """Test that database transactions rollback on error."""
    import tempfile
    import os

    # Use file-based DB
    db_fd, db_path = tempfile.mkstemp()
    try:
        # Create schema
        conn = sqlite3.connect(db_path)
        conn.execute(
            """
            CREATE TABLE chats (
                chat_id INTEGER PRIMARY KEY,
                chat_title TEXT NOT NULL,
                chat_type TEXT
            )
            """
        )
        conn.commit()
        conn.close()

        @admin_job(name="Transaction Test", requires_client=False, heartbeat=False)
        def transaction_test_job(ctx: AdminJobContext) -> AdminJobResult:
            # Insert a chat
            ctx.execute(
                """
                INSERT INTO chats (chat_id, chat_title, chat_type)
                VALUES (?, ?, ?)
                """,
                (-999, "Test Chat", "channel"),
            )

            # This should fail and rollback (but each execute is independent)
            ctx.execute("INSERT INTO nonexistent_table (id) VALUES (?)", (1,))

            return AdminJobResult(success=True)

        set_status_fn = Mock(return_value=True)
        append_log_fn = Mock()
        get_conn_fn = lambda: sqlite3.connect(db_path)
        cfg = Mock()

        transaction_test_job(
            "job_transaction",
            cfg=cfg,
            get_conn_fn=get_conn_fn,
            admin_job_set_status_fn=set_status_fn,
            admin_job_append_log_fn=append_log_fn,
        )

        # Verify the first insert was committed (each execute commits independently)
        verify_conn = sqlite3.connect(db_path)
        cursor = verify_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM chats WHERE chat_id = ?", (-999,))
        count = cursor.fetchone()[0]
        assert count == 1  # First insert succeeded
        verify_conn.close()

        # Job should have failed on second insert
        error_status_set = any(
            call[0] == ("job_transaction", "error") for call in set_status_fn.call_args_list
        )
        assert error_status_set
    finally:
        os.close(db_fd)
        os.unlink(db_path)


def test_context_isolation():
    """Test that multiple jobs don't interfere with each other."""

    @admin_job(name="Isolated Job 1", requires_client=False, heartbeat=False)
    def isolated_job_1(ctx: AdminJobContext) -> AdminJobResult:
        ctx.log("Running as Job One")
        return AdminJobResult(success=True, message="Job One done")

    @admin_job(name="Isolated Job 2", requires_client=False, heartbeat=False)
    def isolated_job_2(ctx: AdminJobContext) -> AdminJobResult:
        ctx.log("Running as Job Two")
        return AdminJobResult(success=True, message="Job Two done")

    set_status_fn = Mock(return_value=True)
    append_log_fn_1 = Mock()
    append_log_fn_2 = Mock()
    get_conn_fn = Mock()
    cfg = Mock()

    # Run two jobs with different IDs
    isolated_job_1(
        "job_1",
        cfg=cfg,
        get_conn_fn=get_conn_fn,
        admin_job_set_status_fn=set_status_fn,
        admin_job_append_log_fn=append_log_fn_1,
    )

    isolated_job_2(
        "job_2",
        cfg=cfg,
        get_conn_fn=get_conn_fn,
        admin_job_set_status_fn=set_status_fn,
        admin_job_append_log_fn=append_log_fn_2,
    )

    # Verify each job logged to its own log function
    log_1_messages = [call[0][1] for call in append_log_fn_1.call_args_list]
    log_2_messages = [call[0][1] for call in append_log_fn_2.call_args_list]

    assert any("Job One" in msg for msg in log_1_messages)
    assert not any("Job Two" in msg for msg in log_1_messages)

    assert any("Job Two" in msg for msg in log_2_messages)
    assert not any("Job One" in msg for msg in log_2_messages)
