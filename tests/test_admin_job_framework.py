"""
Unit tests for the AdminJob framework components.

Tests AdminJobContext, ProgressTracker, AdminJobResult, and @admin_job decorator.
"""

import sqlite3
from unittest.mock import Mock, call, patch

import pytest

from tg_harvest.admin_jobs.framework import (
    AdminJobContext,
    AdminJobResult,
    ProgressTracker,
    _format_error,
    admin_job,
)

# =============================================================================
# Test AdminJobResult
# =============================================================================


def test_result_success():
    """Test creating a successful result."""
    result = AdminJobResult(success=True, message="Task completed")
    assert result.success is True
    assert result.message == "Task completed"
    assert result.data is None


def test_result_failure():
    """Test creating a failure result."""
    result = AdminJobResult(success=False, message="Task failed")
    assert result.success is False
    assert result.message == "Task failed"
    assert result.data is None


def test_result_with_data():
    """Test result with optional data."""
    result = AdminJobResult(
        success=True,
        message="Processed items",
        data={"count": 100, "errors": 0},
    )
    assert result.success is True
    assert result.message == "Processed items"
    assert result.data == {"count": 100, "errors": 0}


def test_result_empty_message():
    """Test result with empty message."""
    result = AdminJobResult(success=True)
    assert result.success is True
    assert result.message == ""
    assert result.data is None


# =============================================================================
# Test ProgressTracker
# =============================================================================


def test_progress_start():
    """Test initializing progress tracking."""
    update_fn = Mock()
    tracker = ProgressTracker("job_123", update_fn)

    tracker.start(total=100, stage="processing")

    update_fn.assert_called_once_with(
        "job_123",
        0,
        total=100,
        stage="processing",
        log_step=0,
        auto_log=False,
    )


def test_progress_start_indeterminate():
    """Test starting progress with no total."""
    update_fn = Mock()
    tracker = ProgressTracker("job_456", update_fn)

    tracker.start()

    update_fn.assert_called_once_with(
        "job_456",
        0,
        total=None,
        stage="running",
        log_step=0,
        auto_log=False,
    )


def test_progress_update_absolute():
    """Test updating progress with absolute value."""
    update_fn = Mock()
    tracker = ProgressTracker("job_789", update_fn)
    tracker.start(total=100)
    update_fn.reset_mock()

    tracker.update(current=50)

    update_fn.assert_called_once_with(
        "job_789",
        50,
        total=100,
        stage="running",
        log_step=0,
        auto_log=False,
    )


def test_progress_update_delta():
    """Test updating progress with delta increment."""
    update_fn = Mock()
    tracker = ProgressTracker("job_delta", update_fn)
    tracker.start(total=100)
    update_fn.reset_mock()

    tracker.update(delta=10)
    tracker.update(delta=5)

    assert update_fn.call_count == 2
    # Check the current values in calls
    assert update_fn.call_args_list[0][0][1] == 10  # First update: current=10
    assert update_fn.call_args_list[1][0][1] == 15  # Second update: current=15


def test_progress_update_negative_clamped():
    """Test that negative progress values are clamped to 0."""
    update_fn = Mock()
    tracker = ProgressTracker("job_clamp", update_fn)
    tracker.start(total=100)
    update_fn.reset_mock()

    tracker.update(current=-10)

    # Should be clamped to 0
    assert update_fn.call_args[0][1] == 0


def test_progress_finish():
    """Test marking progress as complete."""
    update_fn = Mock()
    tracker = ProgressTracker("job_finish", update_fn)
    tracker.start(total=100)
    update_fn.reset_mock()

    tracker.finish()

    update_fn.assert_called_once_with(
        "job_finish",
        100,  # current set to total
        total=100,
        stage="done",
        log_step=0,
        auto_log=False,
    )


def test_progress_finish_indeterminate():
    """Test finishing progress with no total."""
    update_fn = Mock()
    tracker = ProgressTracker("job_finish_ind", update_fn)
    tracker.start(total=None)
    tracker.update(delta=50)
    update_fn.reset_mock()

    tracker.finish()

    # When total is None, current stays at its last value
    assert update_fn.call_args[0][1] == 50
    assert update_fn.call_args[1]["stage"] == "done"


# =============================================================================
# Test AdminJobContext - Database Operations
# =============================================================================


@pytest.fixture
def test_db():
    """Create an in-memory test database."""
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
        [(1, "Alice", 100), (2, "Bob", 200), (3, "Charlie", 300)],
    )
    conn.commit()
    yield conn
    conn.close()


def test_query_returns_dict_rows(test_db):
    """Test that query() returns list of dicts."""
    def get_conn_fn():
        return test_db
    log_fn = Mock()
    set_status_fn = Mock()
    update_fn = Mock()

    ctx = AdminJobContext(
        job_id="test_query",
        config=None,
        client=None,
        progress=ProgressTracker("test_query", update_fn),
        _get_conn_fn=get_conn_fn,
        _log_fn=log_fn,
        _set_status_fn=set_status_fn,
    )

    rows = ctx.query("SELECT * FROM test_table WHERE value > ?", (150,))

    assert len(rows) == 2
    assert all(isinstance(row, dict) for row in rows)
    assert rows[0]["name"] == "Bob"
    assert rows[1]["name"] == "Charlie"


def test_query_empty_result(test_db):
    """Test query with no results."""
    def get_conn_fn():
        return test_db
    log_fn = Mock()
    set_status_fn = Mock()
    update_fn = Mock()

    ctx = AdminJobContext(
        job_id="test_empty",
        config=None,
        client=None,
        progress=ProgressTracker("test_empty", update_fn),
        _get_conn_fn=get_conn_fn,
        _log_fn=log_fn,
        _set_status_fn=set_status_fn,
    )

    rows = ctx.query("SELECT * FROM test_table WHERE value > ?", (1000,))

    assert rows == []


def test_execute_with_commit():
    """Test that execute() commits on success."""
    # Use a file-based database so we can verify commits across connections
    import os
    import tempfile

    db_fd, db_path = tempfile.mkstemp()
    try:
        # Create initial schema and data
        conn = sqlite3.connect(db_path)
        conn.execute(
            """
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value INTEGER
            )
            """
        )
        conn.executemany(
            "INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)",
            [(1, "Alice", 100), (2, "Bob", 200)],
        )
        conn.commit()
        conn.close()

        def get_conn_fn():
            return sqlite3.connect(db_path)

        log_fn = Mock()
        set_status_fn = Mock()
        update_fn = Mock()

        ctx = AdminJobContext(
            job_id="test_execute",
            config=None,
            client=None,
            progress=ProgressTracker("test_execute", update_fn),
            _get_conn_fn=get_conn_fn,
            _log_fn=log_fn,
            _set_status_fn=set_status_fn,
        )

        affected = ctx.execute("UPDATE test_table SET value = ? WHERE name = ?", (999, "Alice"))

        assert affected == 1

        # Verify the change was committed by opening a new connection
        verify_conn = sqlite3.connect(db_path)
        cur = verify_conn.cursor()
        cur.execute("SELECT value FROM test_table WHERE name = ?", ("Alice",))
        row = cur.fetchone()
        assert row[0] == 999
        verify_conn.close()
    finally:
        os.close(db_fd)
        os.unlink(db_path)


def test_execute_with_rollback(test_db):
    """Test that execute() rolls back on error."""
    # Create a connection that will fail on purpose
    def get_failing_conn():
        conn = sqlite3.connect(":memory:")
        # Create table but make it fail on update
        conn.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY)")
        return conn

    log_fn = Mock()
    set_status_fn = Mock()
    update_fn = Mock()

    ctx = AdminJobContext(
        job_id="test_rollback",
        config=None,
        client=None,
        progress=ProgressTracker("test_rollback", update_fn),
        _get_conn_fn=get_failing_conn,
        _log_fn=log_fn,
        _set_status_fn=set_status_fn,
    )

    # This should raise an error and rollback
    with pytest.raises(sqlite3.OperationalError):
        ctx.execute("UPDATE nonexistent_table SET value = ?", (1,))


def test_get_connection(test_db):
    """Test get_connection() returns raw connection."""
    def get_conn_fn():
        return test_db
    log_fn = Mock()
    set_status_fn = Mock()
    update_fn = Mock()

    ctx = AdminJobContext(
        job_id="test_conn",
        config=None,
        client=None,
        progress=ProgressTracker("test_conn", update_fn),
        _get_conn_fn=get_conn_fn,
        _log_fn=log_fn,
        _set_status_fn=set_status_fn,
    )

    conn = ctx.get_connection()

    assert isinstance(conn, sqlite3.Connection)
    assert conn is test_db


def test_log_appends_message():
    """Test that log() appends messages."""
    log_fn = Mock()
    set_status_fn = Mock()
    update_fn = Mock()

    ctx = AdminJobContext(
        job_id="test_log",
        config=None,
        client=None,
        progress=ProgressTracker("test_log", update_fn),
        _get_conn_fn=lambda: None,
        _log_fn=log_fn,
        _set_status_fn=set_status_fn,
    )

    ctx.log("First message")
    ctx.log("Second message")

    assert log_fn.call_count == 2
    log_fn.assert_any_call("test_log", "First message")
    log_fn.assert_any_call("test_log", "Second message")


def test_check_stop_requested():
    """Test check_stop_requested() integration."""
    log_fn = Mock()
    set_status_fn = Mock()
    update_fn = Mock()

    ctx = AdminJobContext(
        job_id="test_stop",
        config=None,
        client=None,
        progress=ProgressTracker("test_stop", update_fn),
        _get_conn_fn=lambda: None,
        _log_fn=log_fn,
        _set_status_fn=set_status_fn,
    )

    # Mock the global stop check function
    with patch("tg_harvest.admin_jobs.framework._admin_job_stop_requested") as mock_stop:
        mock_stop.return_value = False
        assert ctx.check_stop_requested() is False

        mock_stop.return_value = True
        assert ctx.check_stop_requested() is True

        mock_stop.assert_called_with("test_stop")


# =============================================================================
# Test @admin_job Decorator
# =============================================================================


def test_decorator_basic_job():
    """Test decorator with simple job without client."""
    @admin_job(name="Test Job", requires_client=False, heartbeat=False)
    def simple_job(ctx: AdminJobContext) -> AdminJobResult:
        ctx.log("Job executed")
        return AdminJobResult(success=True, message="Done")

    # Mock dependencies
    set_status_fn = Mock(return_value=True)
    append_log_fn = Mock()
    get_conn_fn = Mock()
    cfg = Mock()

    # Execute the wrapped job
    simple_job(
        "job_123",
        cfg=cfg,
        get_conn_fn=get_conn_fn,
        admin_job_set_status_fn=set_status_fn,
        admin_job_append_log_fn=append_log_fn,
    )

    # Verify status transitions
    assert set_status_fn.call_args_list[0] == call("job_123", "running")
    assert set_status_fn.call_args_list[1] == call("job_123", "done")

    # Verify logging
    append_log_fn.assert_any_call("job_123", "Test Job：任务开始执行")
    append_log_fn.assert_any_call("job_123", "Job executed")
    append_log_fn.assert_any_call("job_123", "Done")


def test_decorator_forwards_business_arguments():
    received = {}

    @admin_job(name="Delete Chat", requires_client=False, heartbeat=False)
    def delete_chat_job(
        ctx: AdminJobContext,
        operation: str,
        *,
        chat_id: int,
        chat_title: str,
    ) -> AdminJobResult:
        received.update(
            operation=operation,
            chat_id=chat_id,
            chat_title=chat_title,
        )
        return AdminJobResult(success=True, message="Deleted")

    set_status_fn = Mock(return_value=True)

    delete_chat_job(
        "job_delete",
        "delete",
        chat_id=7,
        chat_title="Chat 7",
        cfg=Mock(),
        get_conn_fn=Mock(),
        admin_job_set_status_fn=set_status_fn,
        admin_job_append_log_fn=Mock(),
    )

    assert received == {
        "operation": "delete",
        "chat_id": 7,
        "chat_title": "Chat 7",
    }
    assert set_status_fn.call_args_list[-1] == call("job_delete", "done")


def test_decorator_with_client():
    """Test decorator with job requiring Telegram client."""
    @admin_job(name="Client Job", requires_client=True, heartbeat=False)
    def client_job(ctx: AdminJobContext) -> AdminJobResult:
        assert ctx.client is not None
        ctx.log("Using client")
        return AdminJobResult(success=True, message="Client job done")

    # Mock dependencies
    set_status_fn = Mock(return_value=True)
    append_log_fn = Mock()
    get_conn_fn = Mock()
    cfg = Mock()
    mock_client = Mock()

    with (
        patch("tg_harvest.admin_jobs.framework._create_isolated_worker_client") as mock_create,
        patch("tg_harvest.admin_jobs.framework._disconnect_worker_client") as mock_disconnect,
        patch("tg_harvest.admin_jobs.framework._cleanup_isolated_worker_session") as mock_cleanup,
    ):
        mock_create.return_value = mock_client

        client_job(
            "job_client",
            cfg=cfg,
            get_conn_fn=get_conn_fn,
            admin_job_set_status_fn=set_status_fn,
            admin_job_append_log_fn=append_log_fn,
        )

        # Verify client lifecycle
        mock_create.assert_called_once()
        mock_disconnect.assert_called_once_with(mock_client)
        mock_cleanup.assert_called_once()

        # Verify success logging
        append_log_fn.assert_any_call("job_client", "Telegram 客户端连接成功")


def test_decorator_heartbeat_lifecycle():
    """Test heartbeat start/stop."""
    @admin_job(name="Heartbeat Job", requires_client=False, heartbeat=True)
    def heartbeat_job(ctx: AdminJobContext) -> AdminJobResult:
        return AdminJobResult(success=True)

    set_status_fn = Mock(return_value=True)
    append_log_fn = Mock()
    get_conn_fn = Mock()
    cfg = Mock()

    mock_stop_event = Mock()
    mock_thread = Mock()

    with (
        patch("tg_harvest.admin_jobs.framework.start_admin_job_heartbeat") as mock_start,
        patch("tg_harvest.admin_jobs.framework.finish_job_heartbeat") as mock_finish,
    ):
        mock_start.return_value = (mock_stop_event, mock_thread)

        heartbeat_job(
            "job_hb",
            cfg=cfg,
            get_conn_fn=get_conn_fn,
            admin_job_set_status_fn=set_status_fn,
            admin_job_append_log_fn=append_log_fn,
        )

        # Verify heartbeat lifecycle
        mock_start.assert_called_once_with("job_hb")
        mock_finish.assert_called_once_with(mock_stop_event, mock_thread)


def test_decorator_status_transitions():
    """Test status transitions from running to done/error."""
    @admin_job(name="Status Job", requires_client=False, heartbeat=False)
    def status_job_success(ctx: AdminJobContext) -> AdminJobResult:
        return AdminJobResult(success=True, message="Success")

    @admin_job(name="Status Job", requires_client=False, heartbeat=False)
    def status_job_failure(ctx: AdminJobContext) -> AdminJobResult:
        return AdminJobResult(success=False, message="Failed")

    set_status_fn = Mock(return_value=True)
    append_log_fn = Mock()
    get_conn_fn = Mock()
    cfg = Mock()

    # Test success path
    status_job_success(
        "job_success",
        cfg=cfg,
        get_conn_fn=get_conn_fn,
        admin_job_set_status_fn=set_status_fn,
        admin_job_append_log_fn=append_log_fn,
    )

    assert set_status_fn.call_args_list[-1] == call("job_success", "done")

    set_status_fn.reset_mock()

    # Test failure path
    status_job_failure(
        "job_fail",
        cfg=cfg,
        get_conn_fn=get_conn_fn,
        admin_job_set_status_fn=set_status_fn,
        admin_job_append_log_fn=append_log_fn,
    )

    assert set_status_fn.call_args_list[-1] == call("job_fail", "error")


def test_decorator_error_handling():
    """Test exception catching and error status."""
    @admin_job(name="Error Job", requires_client=False, heartbeat=False)
    def error_job(ctx: AdminJobContext) -> AdminJobResult:
        raise ValueError("Something went wrong")

    set_status_fn = Mock(return_value=True)
    append_log_fn = Mock()
    get_conn_fn = Mock()
    cfg = Mock()

    # Should not raise - decorator catches exceptions
    error_job(
        "job_error",
        cfg=cfg,
        get_conn_fn=get_conn_fn,
        admin_job_set_status_fn=set_status_fn,
        admin_job_append_log_fn=append_log_fn,
    )

    # Verify error status set
    assert any(
        call_args == call("job_error", "error")
        for call_args in set_status_fn.call_args_list
    )

    # Verify error logged
    error_logged = any(
        "ValueError" in str(call_args) and "Something went wrong" in str(call_args)
        for call_args in append_log_fn.call_args_list
    )
    assert error_logged


def test_decorator_resource_cleanup():
    """Test that finally block executes for cleanup."""
    @admin_job(name="Cleanup Job Success", requires_client=False, heartbeat=False)
    def cleanup_job_success(ctx: AdminJobContext) -> AdminJobResult:
        return AdminJobResult(success=True)

    @admin_job(name="Cleanup Job Fail", requires_client=False, heartbeat=False)
    def cleanup_job_fail(ctx: AdminJobContext) -> AdminJobResult:
        raise RuntimeError("Intentional failure")

    set_status_fn = Mock(return_value=True)
    append_log_fn = Mock()
    get_conn_fn = Mock()
    cfg = Mock()

    with patch("tg_harvest.admin_jobs.framework.finish_job_heartbeat") as mock_finish:
        # Test cleanup on success (heartbeat=False so won't call finish_job_heartbeat)
        cleanup_job_success(
            "job_cleanup_ok",
            cfg=cfg,
            get_conn_fn=get_conn_fn,
            admin_job_set_status_fn=set_status_fn,
            admin_job_append_log_fn=append_log_fn,
        )

        # Test cleanup on failure (heartbeat=False so won't call finish_job_heartbeat)
        cleanup_job_fail(
            "job_cleanup_fail",
            cfg=cfg,
            get_conn_fn=get_conn_fn,
            admin_job_set_status_fn=set_status_fn,
            admin_job_append_log_fn=append_log_fn,
        )

        # Verify cleanup doesn't call finish when heartbeat=False
        assert mock_finish.call_count == 0

    # Now test with heartbeat=True
    @admin_job(name="Cleanup With Heartbeat", requires_client=False, heartbeat=True)
    def cleanup_job_with_hb(ctx: AdminJobContext) -> AdminJobResult:
        return AdminJobResult(success=True)

    mock_stop_event = Mock()
    mock_thread = Mock()

    with (
        patch("tg_harvest.admin_jobs.framework.start_admin_job_heartbeat") as mock_start,
        patch("tg_harvest.admin_jobs.framework.finish_job_heartbeat") as mock_finish,
    ):
        mock_start.return_value = (mock_stop_event, mock_thread)

        cleanup_job_with_hb(
            "job_with_hb",
            cfg=cfg,
            get_conn_fn=get_conn_fn,
            admin_job_set_status_fn=set_status_fn,
            admin_job_append_log_fn=append_log_fn,
        )

        # Verify cleanup happens with heartbeat
        mock_finish.assert_called_once_with(mock_stop_event, mock_thread)


def test_decorator_client_cleanup_on_error():
    """Test client cleanup when job fails."""
    @admin_job(name="Client Error Job", requires_client=True, heartbeat=False)
    def client_error_job(ctx: AdminJobContext) -> AdminJobResult:
        raise RuntimeError("Job failed after client setup")

    set_status_fn = Mock(return_value=True)
    append_log_fn = Mock()
    get_conn_fn = Mock()
    cfg = Mock()
    mock_client = Mock()

    with (
        patch("tg_harvest.admin_jobs.framework._create_isolated_worker_client") as mock_create,
        patch("tg_harvest.admin_jobs.framework._disconnect_worker_client") as mock_disconnect,
        patch("tg_harvest.admin_jobs.framework._cleanup_isolated_worker_session") as mock_cleanup,
    ):
        mock_create.return_value = mock_client

        client_error_job(
            "job_client_error",
            cfg=cfg,
            get_conn_fn=get_conn_fn,
            admin_job_set_status_fn=set_status_fn,
            admin_job_append_log_fn=append_log_fn,
        )

        # Verify client was cleaned up even though job failed
        mock_disconnect.assert_called_once_with(mock_client)
        mock_cleanup.assert_called_once()


def test_decorator_cleans_isolated_session_when_client_creation_fails():
    @admin_job(name="Client Setup Error", requires_client=True, heartbeat=False)
    def client_setup_error_job(ctx: AdminJobContext) -> AdminJobResult:
        return AdminJobResult(success=True)

    with (
        patch(
            "tg_harvest.admin_jobs.framework._create_isolated_worker_client",
            side_effect=RuntimeError("connection failed"),
        ),
        patch(
            "tg_harvest.admin_jobs.framework._cleanup_isolated_worker_session"
        ) as cleanup,
    ):
        client_setup_error_job(
            "job_client_setup_error",
            cfg=Mock(),
            get_conn_fn=Mock(),
            admin_job_set_status_fn=Mock(return_value=True),
            admin_job_append_log_fn=Mock(),
        )

    cleanup.assert_called_once()


def test_decorator_progress_tracking():
    """Test progress tracking integration."""
    @admin_job(name="Progress Job", requires_client=False, heartbeat=False)
    def progress_job(ctx: AdminJobContext) -> AdminJobResult:
        ctx.progress.start(total=10)
        ctx.progress.update(current=5)
        ctx.progress.finish()
        return AdminJobResult(success=True, message="Progress tracked")

    set_status_fn = Mock(return_value=True)
    append_log_fn = Mock()
    get_conn_fn = Mock()
    cfg = Mock()

    with patch("tg_harvest.admin_jobs.framework._admin_job_update_progress") as mock_progress:
        progress_job(
            "job_progress",
            cfg=cfg,
            get_conn_fn=get_conn_fn,
            admin_job_set_status_fn=set_status_fn,
            admin_job_append_log_fn=append_log_fn,
        )

        # Verify progress calls
        assert mock_progress.call_count == 3  # start, update, finish

        # Check start call
        assert mock_progress.call_args_list[0][0] == ("job_progress", 0)
        assert mock_progress.call_args_list[0][1]["total"] == 10

        # Check update call
        assert mock_progress.call_args_list[1][0] == ("job_progress", 5)

        # Check finish call
        assert mock_progress.call_args_list[2][0] == ("job_progress", 10)
        assert mock_progress.call_args_list[2][1]["stage"] == "done"


# =============================================================================
# Test Error Formatting
# =============================================================================


def test_format_error_simple():
    """Test formatting simple exception."""
    exc = ValueError("Invalid value")
    formatted = _format_error(exc)
    assert formatted == "ValueError: Invalid value"


def test_format_error_no_message():
    """Test formatting exception with no message."""
    exc = ValueError()
    formatted = _format_error(exc)
    assert formatted == "ValueError"


def test_format_error_long_message():
    """Test formatting exception with long message."""
    long_msg = "x" * 300
    exc = RuntimeError(long_msg)
    formatted = _format_error(exc)
    assert len(formatted) <= 220  # Type name + 200 chars + "..."
    assert formatted.endswith("...")


def test_format_error_preserves_type():
    """Test that exception type is preserved."""
    exc = KeyError("missing_key")
    formatted = _format_error(exc)
    assert "KeyError" in formatted
