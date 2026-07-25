# AdminJob Framework Implementation

## Overview

Successfully implemented the AdminJob core infrastructure framework as specified in Phase 3, Task 12.

**File**: `/root/db/tg_harvest/admin_jobs/framework.py`  
**Total Lines**: 452 lines (exceeds target of ~250 lines due to comprehensive docstrings)

## Components Implemented

### 1. AdminJobResult Class (~20 lines)
```python
@dataclass
class AdminJobResult:
    success: bool
    message: str = ""
    data: dict[str, Any] | None = None
```

**Purpose**: Return value for job business logic functions.

**Features**:
- Simple success/failure indicator
- Human-readable message
- Optional structured data payload
- Comprehensive docstrings with examples

### 2. ProgressTracker Class (~90 lines)
```python
class ProgressTracker:
    def start(self, total: int | None = None, stage: str = "running") -> None
    def update(self, current: int | None = None, delta: int = 0) -> None
    def finish(self) -> None
```

**Purpose**: Track and report job progress to database.

**Features**:
- Start with total (determinate) or without (indeterminate)
- Update by absolute value or delta increment
- Automatic stage management
- Zero-overhead when not used
- Full integration with existing `_admin_job_update_progress`

### 3. AdminJobContext Class (~120 lines)
```python
@dataclass
class AdminJobContext:
    job_id: str
    config: Any
    client: Any | None
    progress: ProgressTracker
    
    # Public API methods:
    def query(self, sql: str, params: tuple = ()) -> list[dict[str, Any]]
    def execute(self, sql: str, params: tuple = ()) -> int
    def get_connection(self) -> sqlite3.Connection
    def log(self, message: str) -> None
    def check_stop_requested(self) -> bool
```

**Purpose**: Clean API for job implementations.

**Features**:
- **query()**: SELECT queries returning dict rows
- **execute()**: INSERT/UPDATE/DELETE with automatic commit/rollback
- **get_connection()**: Raw connection for advanced operations
- **log()**: Append messages to job log
- **check_stop_requested()**: Poll for user stop request
- Private attributes (`_get_conn_fn`, `_log_fn`, `_set_status_fn`) for internal use
- Comprehensive docstrings and usage examples

### 4. @admin_job Decorator (~150 lines)
```python
def admin_job(
    *,
    name: str,
    requires_client: bool = False,
    heartbeat: bool = True,
) -> Callable:
```

**Purpose**: Declarative infrastructure management.

**Features**:
1. **Heartbeat management**: Automatic background thread (optional)
2. **Client lifecycle**: Create isolated worker client (if required)
3. **Context creation**: Build AdminJobContext with all dependencies
4. **Status management**: Set running → done/error
5. **Error handling**: Catch exceptions, format messages, log failures
6. **Cleanup**: Always disconnect client and stop heartbeat (finally block)
7. **Job context binding**: Set contextvars for log passthrough

**Infrastructure handled automatically**:
- Threading and heartbeat
- Telegram client connection/disconnection
- Session file cleanup
- Status persistence
- Error logging
- Resource cleanup (always executed)

## API Design Highlights

### Type Safety
- Full type annotations on all functions
- Proper use of `Callable`, `Any`, `dict`, `list`, `tuple`
- Dataclass for structured types
- mypy compatible (no mypy available in environment, but syntax checked)

### Documentation
- Module-level docstring with usage examples
- Class docstrings with attribute descriptions
- Method docstrings with:
  - Parameter documentation
  - Return value documentation  
  - Practical usage examples
  - Edge case handling

### Integration
- Uses existing `_admin_job_update_progress` function
- Uses existing `start_admin_job_heartbeat` and `finish_job_heartbeat`
- Uses existing `_create_isolated_worker_client` and cleanup functions
- Uses existing `_admin_job_stop_requested` function
- Sets `job_context` contextvar for log passthrough
- Compatible with existing admin job patterns

### Error Handling
- Try-except-finally structure ensures cleanup
- Best-effort status updates (never fail on cleanup)
- Detailed error logging with exception traces
- User-friendly error message formatting
- Proper rollback on database errors

## Testing

Created validation test (`test_framework_api.py`) demonstrating:

1. ✓ Simple job with progress tracking
2. ✓ Database operations (query/execute)
3. ✓ Stop request checking
4. ✓ Exception handling and error status

All tests passed successfully.

## Usage Example

```python
from tg_harvest.admin_jobs.framework import admin_job, AdminJobContext, AdminJobResult

@admin_job(name="Cleanup Old Messages", requires_client=False)
def cleanup_old_messages(ctx: AdminJobContext) -> AdminJobResult:
    ctx.log("Starting cleanup...")
    
    # Query database
    rows = ctx.query(
        "SELECT message_id FROM messages WHERE created_at < ?",
        (cutoff_date,)
    )
    
    # Track progress
    ctx.progress.start(total=len(rows), stage="cleaning")
    
    deleted = 0
    for i, row in enumerate(rows):
        if ctx.check_stop_requested():
            return AdminJobResult(success=False, message="Stopped by user")
        
        # Delete message
        ctx.execute("DELETE FROM messages WHERE message_id = ?", (row["message_id"],))
        deleted += 1
        ctx.progress.update(current=i+1)
    
    ctx.progress.finish()
    ctx.log(f"Cleanup complete: deleted {deleted} messages")
    
    return AdminJobResult(
        success=True,
        message=f"Deleted {deleted} old messages",
        data={"deleted_count": deleted}
    )
```

## Implementation Statistics

- **Total Lines**: 452 (including comprehensive docstrings)
- **Code Lines**: ~300 (estimated, excluding docstrings/comments)
- **AdminJobResult**: ~20 lines
- **ProgressTracker**: ~90 lines
- **AdminJobContext**: ~120 lines
- **@admin_job decorator**: ~150 lines
- **Helper functions**: ~20 lines

## Next Steps

The framework is ready for use. Next phase tasks can now:

1. Use `@admin_job` decorator instead of manual infrastructure
2. Use `AdminJobContext` for clean database/logging access
3. Use `ProgressTracker` for user-visible progress
4. Return `AdminJobResult` instead of manual status management

This eliminates ~100 lines of boilerplate per job implementation.
