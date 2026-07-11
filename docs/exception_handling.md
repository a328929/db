# Exception Handling Convention

This document defines the exception contract for `tg_harvest`'s runtime paths.
The goal is not to remove every broad boundary. It is to make retry, rollback,
user-facing responses, and terminal state changes intentional.

## Categories

| Category | Examples | Log level | Retry | User/job result | Transaction rule |
| --- | --- | --- | --- | --- | --- |
| External Telegram, retryable | `FloodWaitError` below the configured switch threshold, `RPCError`, connection timeout, `OSError` during an API call | `WARNING` with account, chat, attempt, and wait context | Keep the existing bounded retry and FloodWait policy | Do not report success until the later write succeeds | No database transaction is confirmed from the failed API attempt |
| External Telegram, not retryable | access/hash/cache miss, forbidden/private chat, long `FloodWait`, unavailable history | `WARNING` for a recoverable account switch or `ERROR` when the unit of work fails | No new retry policy; long FloodWait keeps its existing account-switch behavior | Per-item failure/backoff or job error, depending on the existing workflow | Persist the failure/backoff before releasing a claimed task |
| Database consistency | `sqlite3.Error` while writing messages, pending state, job state, or clone mappings | `ERROR` with stack trace and operation identifiers | Never retried by Telegram retry loops | Propagate to the job/listener boundary and mark the job/migration/task failed or leave it in-flight for recovery | Roll back the whole transaction; never acknowledge a successful task or mapping after a failed write |
| Best-effort cleanup | disconnect, removing an already-obsolete worker Session file, notification delivery | `DEBUG` or `WARNING` with the resource identity; stack trace for unexpected cleanup failures | No retry required | Does not replace the primary failure | Cleanup may be ignored only after a contextual log, or when an explicit `FileNotFoundError` race is expected |
| Web boundary | malformed JSON/query values, `sqlite3.Error` in an HTTP handler | `INFO`/no stack for input errors; `ERROR` stack for server errors | No | Invalid input is a 400/409/429 response; storage/runtime errors are a sanitized 500 | Request handlers do not commit partial work themselves |
| Unknown bug | invariant failure, unexpected object shape, unclassified library failure | `ERROR` with `exc_info=True` / `logging.exception` | No automatic retry | Convert at the outer job/listener/web boundary to `error`/500; re-raise inside storage | If a transaction is open, roll it back before re-raising |

## Non-negotiable Invariants

- A message parser failure stops the current chat/range. It must not be treated
  as an empty message or silently advance a cursor.
- A message batch, scheduler claim/finish, and clone mapping write are atomic.
  If their database work fails, their success state is not persisted.
- A claimed scheduler task remains in-flight when its completion write fails;
  recovery can safely reclaim it. A stale owner may not complete a newer lease.
- A Telegram send whose clone mapping cannot be persisted stops the migration.
  Continuing would make a later resume unable to distinguish a sent item from
  an unsent item.
- A worker Session cleanup failure is observable. Removing stale artifacts
  before creating a worker is required; cleanup after a worker exits is best
  effort and must include the artifact path in the log.

## Audit Baseline And Current Classification

The baseline was counted from the requested files before this change. It had
205 `except` clauses, including 129 broad `except Exception` clauses. The
current audited set has 236 `except` clauses, including 126 broad captures:
the increase in total handlers is intentional because dangerous transaction
paths now distinguish `sqlite3.Error` from unknown bugs. The tables below
classify the broad captures rather than treating all concrete input/SQLite
handlers as a problem.

| Area | Broad before | Broad after | Current treatment | Classification |
| --- | ---: | ---: | --- | --- |
| `ingest/runner.py`, `range_harvest.py`, `store.py` | 13 | 11 | Parser/write paths now split database consistency from outer unknown bugs; entity fallbacks remain Telegram resolution boundaries | Telegram retryable/non-retryable, database consistency, unknown bug |
| `storage/sync_scheduler.py` | 17 | 17 | enqueue, claim, complete, and fail roll back and re-raise with operation context; model JSON/model prediction fallbacks remain isolated best-effort features | database consistency, best effort, unknown bug |
| `runtime/db_listener.py` | 29 | 29 | worker and scheduler-loop outer boundaries retain stack logging; a failed result write leaves the lease unacknowledged | Telegram/runtime boundary, database consistency, best effort |
| `admin_jobs/runners.py`, `core.py` | 27 | 29 | job runners added two explicit best-effort error-status fallbacks while terminal success persistence became strict; notification hooks remain best effort | job-state database consistency, cleanup, unknown bug |
| `admin_jobs/sessions.py` | 6 | 3 | Session SQLite/configuration is concrete; artifact cleanup is contextual and stale-artifact removal is mandatory before reuse | Session SQLite/OSError, best effort cleanup, unknown bug |
| `storage/search_terms.py` | 4 | 4 | transactional rebuild guards re-raise after rollback; optional maintenance remains best effort | database consistency, best effort |
| `web/routes/*.py` | 33 | 33 | input handlers use concrete conversion errors; existing final route boundaries deliberately return sanitized 500 responses through `logged_json_error` with a stack trace | web boundary, unknown bug |

The remaining broad boundaries are deliberate: they either (1) roll back and
re-raise an unknown error at a transaction guard, (2) form the top-level
job/listener/web crash boundary and record a stack trace, or (3) isolate an
explicitly non-authoritative best-effort operation such as notification,
cleanup, or optional model prediction. They must not be used around message
writes, pending-task acknowledgement, task terminal-state persistence, or
clone mapping success writes without first preserving the corresponding
consistency invariant.
