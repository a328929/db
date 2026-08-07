"""
AdminJob Framework - Core infrastructure for admin job implementations.

This module provides a decorator-based framework that handles all common admin job
infrastructure: heartbeat management, client lifecycle, progress tracking, error
handling, and status management.

Usage:
    @admin_job(name="my_job", requires_client=True)
    def my_job_impl(ctx: AdminJobContext) -> AdminJobResult:
        ctx.log("Starting work...")
        rows = ctx.query("SELECT * FROM table")
        ctx.progress.start(total=len(rows))
        for i, row in enumerate(rows):
            # Do work
            ctx.progress.update(current=i+1)
            if ctx.check_stop_requested():
                return AdminJobResult(success=False, message="Stopped by user")
        ctx.progress.finish()
        return AdminJobResult(success=True, message="Completed")
"""

import logging
import sqlite3
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from functools import wraps
from typing import Any

from tg_harvest.admin_jobs.common import (
    finish_job_heartbeat,
    start_admin_job_heartbeat,
)
from tg_harvest.admin_jobs.core import (
    _admin_job_stop_requested,
    _admin_job_update_progress,
    job_context,
)
from tg_harvest.admin_jobs.sessions import (
    _cleanup_isolated_worker_session,
    _create_isolated_worker_client,
    _disconnect_worker_client,
)


@dataclass
class AdminJobResult:
    """Result returned by admin job business logic.

    Attributes:
        success: Whether the job completed successfully
        message: Human-readable result message
        data: Optional structured data to include in logs or response

    Examples:
        >>> AdminJobResult(success=True, message="Processed 100 items")
        >>> AdminJobResult(success=False, message="Failed to connect", data={"error_code": 500})
    """
    success: bool
    message: str = ""
    data: dict[str, Any] | None = None


class ProgressTracker:
    """Tracks and reports job progress to the database.

    This class provides a simple API for reporting progress during job execution.
    Progress updates are persisted to the database and visible in the admin UI.

    Usage:
        progress = ProgressTracker(job_id, update_fn)
        progress.start(total=100, stage="processing")
        for i in range(100):
            progress.update(current=i+1)
        progress.finish()
    """

    def __init__(self, job_id: str, update_fn: Callable) -> None:
        """Initialize progress tracker.

        Args:
            job_id: The job ID to track progress for
            update_fn: Function to call to persist progress updates
        """
        self._job_id = str(job_id)
        self._update_fn = update_fn
        self._total: int | None = None
        self._current: int = 0
        self._stage: str = "running"

    def start(self, total: int | None = None, stage: str = "running") -> None:
        """Mark the start of progress tracking.

        Args:
            total: Total number of items to process (None for indeterminate)
            stage: Stage name for display (default: "running")

        Examples:
            >>> progress.start(total=1000, stage="importing")
            >>> progress.start()  # indeterminate progress
        """
        self._total = int(total) if total is not None else None
        self._current = 0
        self._stage = str(stage)
        self._update_fn(
            self._job_id,
            self._current,
            total=self._total,
            stage=self._stage,
            log_step=0,
            auto_log=False,
        )

    def update(self, current: int | None = None, delta: int = 0) -> None:
        """Update progress counter.

        Args:
            current: Set progress to this absolute value
            delta: Increment progress by this amount (ignored if current is set)

        Examples:
            >>> progress.update(current=50)  # set to 50
            >>> progress.update(delta=1)     # increment by 1
        """
        if current is not None:
            self._current = max(0, int(current))
        else:
            self._current = max(0, self._current + int(delta))

        self._update_fn(
            self._job_id,
            self._current,
            total=self._total,
            stage=self._stage,
            log_step=0,
            auto_log=False,
        )

    def finish(self) -> None:
        """Mark progress as complete.

        Sets current to total (if known) and updates the stage to "done".
        """
        if self._total is not None:
            self._current = self._total
        self._stage = "done"
        self._update_fn(
            self._job_id,
            self._current,
            total=self._total,
            stage=self._stage,
            log_step=0,
            auto_log=False,
        )


@dataclass
class AdminJobContext:
    """Context object passed to admin job implementations.

    Provides a clean API for job implementations to interact with the database,
    log messages, track progress, and check for stop requests.

    Attributes:
        job_id: Unique identifier for this job
        config: Configuration object (CFG)
        client: Telegram client (None if requires_client=False)
        progress: Progress tracker instance

    Examples:
        def my_job(ctx: AdminJobContext) -> AdminJobResult:
            ctx.log("Starting...")
            rows = ctx.query("SELECT * FROM table")
            ctx.execute("UPDATE table SET status = ?", ("done",))
            return AdminJobResult(success=True)
    """

    job_id: str
    config: Any
    client: Any | None
    progress: ProgressTracker

    # Private attributes for internal use
    _get_conn_fn: Callable[[], sqlite3.Connection]
    _log_fn: Callable[[str, str], Any]
    _set_status_fn: Callable[[str, str], bool]

    def query(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]:
        """Execute a SELECT query and return results as dictionaries.

        Args:
            sql: SQL query string
            params: Query parameters (use ? placeholders)

        Returns:
            List of row dictionaries

        Examples:
            >>> rows = ctx.query("SELECT * FROM chats WHERE chat_id = ?", (12345,))
            >>> for row in rows:
            ...     print(row["chat_title"])
        """
        with closing(self._get_conn_fn()) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            try:
                cur.execute(sql, params)
                return [dict(row) for row in cur.fetchall()]
            finally:
                cur.close()

    def execute(self, sql: str, params: tuple = ()) -> int:
        """Execute an INSERT, UPDATE, or DELETE statement.

        Args:
            sql: SQL statement string
            params: Statement parameters (use ? placeholders)

        Returns:
            Number of rows affected

        Examples:
            >>> affected = ctx.execute("UPDATE chats SET status = ?", ("active",))
            >>> ctx.log(f"Updated {affected} rows")
        """
        with closing(self._get_conn_fn()) as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, params)
                conn.commit()
                return int(cur.rowcount or 0)
            except Exception:
                conn.rollback()
                raise
            finally:
                cur.close()

    def get_connection(self) -> sqlite3.Connection:
        """Get a raw database connection.

        Use this for advanced operations. Remember to close the connection when done.

        Returns:
            SQLite connection object

        Examples:
            >>> conn = ctx.get_connection()
            >>> try:
            ...     cur = conn.cursor()
            ...     # complex operation
            ...     conn.commit()
            >>> finally:
            ...     conn.close()
        """
        return self._get_conn_fn()

    def log(self, message: str) -> None:
        """Append a log message to the job log.

        Args:
            message: Log message to append

        Examples:
            >>> ctx.log("Processing batch 1 of 10")
            >>> ctx.log(f"Completed in {elapsed:.2f}s")
        """
        self._log_fn(self.job_id, str(message))

    def check_stop_requested(self) -> bool:
        """Check if the user has requested this job to stop.

        Returns:
            True if stop was requested, False otherwise

        Examples:
            >>> if ctx.check_stop_requested():
            ...     ctx.log("Stopping early per user request")
            ...     return AdminJobResult(success=False, message="Stopped")
        """
        return _admin_job_stop_requested(self.job_id)


def legacy_admin_job_log_adapter(
    ctx: AdminJobContext,
) -> Callable[[str, str], Any]:
    """Adapt a context logger for helpers that still require a job-id callback."""

    def append_log(_job_id: str, message: str) -> Any:
        return ctx.log(message)

    return append_log


def admin_job(
    *,
    name: str,
    requires_client: bool = False,
    heartbeat: bool = True,
) -> Callable:
    """Decorator that handles all admin job infrastructure.

    This decorator wraps a business logic function and provides:
    - Automatic heartbeat management
    - Telegram client creation/cleanup (if required)
    - Context object with database and logging helpers
    - Progress tracking
    - Status management (running -> done/error)
    - Error handling and logging

    Args:
        name: Human-readable job name for logging
        requires_client: Whether to create a Telegram client
        heartbeat: Whether to run automatic heartbeat updates

    Returns:
        Decorator function

    Examples:
        @admin_job(name="Import Data", requires_client=False)
        def import_data(ctx: AdminJobContext) -> AdminJobResult:
            ctx.log("Importing...")
            # business logic
            return AdminJobResult(success=True)

        @admin_job(name="Sync Messages", requires_client=True)
        def sync_messages(ctx: AdminJobContext) -> AdminJobResult:
            entity = ctx.client.get_entity(chat_id)
            # use Telegram API
            return AdminJobResult(success=True)
    """

    def decorator(func: Callable[[AdminJobContext], AdminJobResult]) -> Callable:
        @wraps(func)
        def wrapper(
            job_id: str,
            *,
            cfg: Any,
            get_conn_fn: Callable[[], sqlite3.Connection],
            admin_job_set_status_fn: Callable[[str, str], bool],
            admin_job_append_log_fn: Callable[[str, str], Any],
            **kwargs: Any,
        ) -> None:
            """Wrapper that handles all infrastructure concerns."""

            # Set job context for log passthrough
            job_context.set(str(job_id))

            # Initialize state
            heartbeat_stop = None
            heartbeat_thread = None
            client = None
            worker_id = f"{job_id}_worker"

            try:
                # 1. Setup heartbeat
                if heartbeat:
                    heartbeat_stop, heartbeat_thread = start_admin_job_heartbeat(job_id)

                # 2. Mark as running
                if not admin_job_set_status_fn(job_id, "running"):
                    raise RuntimeError(f"Failed to set job status to running: job_id={job_id}")

                admin_job_append_log_fn(job_id, f"{name}：任务开始执行")

                # 3. Setup client (if required)
                if requires_client:
                    try:
                        client = _create_isolated_worker_client(cfg, worker_id)
                        admin_job_append_log_fn(job_id, "Telegram 客户端连接成功")
                    except Exception as client_exc:
                        admin_job_append_log_fn(
                            job_id,
                            f"Telegram 客户端连接失败：{_format_error(client_exc)}",
                        )
                        raise

                # 4. Create context
                progress = ProgressTracker(job_id, _admin_job_update_progress)
                context = AdminJobContext(
                    job_id=job_id,
                    config=cfg,
                    client=client,
                    progress=progress,
                    _get_conn_fn=get_conn_fn,
                    _log_fn=admin_job_append_log_fn,
                    _set_status_fn=admin_job_set_status_fn,
                )

                # 5. Call business logic function
                result = func(context)

                # 6. Handle result
                if result.success:
                    status = "done"
                    log_message = result.message or f"{name}：任务成功完成"
                    if result.data:
                        log_message += f"（数据：{result.data}）"
                else:
                    status = "error"
                    log_message = result.message or f"{name}：任务失败"
                    if result.data:
                        log_message += f"（数据：{result.data}）"

                admin_job_append_log_fn(job_id, log_message)

                if not admin_job_set_status_fn(job_id, status):
                    logging.error("Failed to set final job status: job_id=%s status=%s", job_id, status)

            except Exception as exc:
                # Handle any unhandled exceptions
                logging.exception("Admin job failed: job_id=%s name=%s", job_id, name)
                error_message = _format_error(exc)
                admin_job_append_log_fn(job_id, f"{name}：执行失败 - {error_message}")

                # Try to mark as error (best effort)
                try:
                    admin_job_set_status_fn(job_id, "error")
                except Exception:
                    logging.exception("Failed to set error status: job_id=%s", job_id)

            finally:
                # 7. Cleanup (always executed)
                if client is not None:
                    try:
                        _disconnect_worker_client(client)
                    except Exception:
                        logging.exception("Failed to disconnect client: job_id=%s", job_id)

                    try:
                        _cleanup_isolated_worker_session(cfg, worker_id)
                    except Exception:
                        logging.exception("Failed to cleanup session: job_id=%s", job_id)

                if heartbeat and heartbeat_stop is not None and heartbeat_thread is not None:
                    finish_job_heartbeat(heartbeat_stop, heartbeat_thread)

        return wrapper

    return decorator


def _format_error(exc: Exception) -> str:
    """Format an exception for user-friendly display.

    Args:
        exc: Exception to format

    Returns:
        Formatted error message
    """
    exc_type = type(exc).__name__
    exc_message = str(exc).strip()

    if not exc_message:
        return exc_type

    # Keep it concise for common errors
    if len(exc_message) > 200:
        return f"{exc_type}: {exc_message[:200]}..."

    return f"{exc_type}: {exc_message}"
