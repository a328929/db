import logging
import sqlite3
from contextlib import suppress

from tg_harvest.domain.clone_message_delete import (
    CLONE_MESSAGE_RESET_REQUIRED_PHASE,
)
from tg_harvest.storage.clone_common import _clean_text
from tg_harvest.storage.connection import synchronized_write

_JOB_ID_BATCH_SIZE = 400
_ACTIVE_JOB_STATUSES = frozenset(
    {"pending", "queued", "running", "stopping", "deleting"}
)


def _table_exists(cur: sqlite3.Cursor, table_name: str) -> bool:
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (str(table_name),),
    )
    return cur.fetchone() is not None


def _table_has_column(
    cur: sqlite3.Cursor,
    table_name: str,
    column_name: str,
) -> bool:
    """Return whether a legacy clone table exposes a required column.

    Clone state tables predate the current schema in some installations.  The
    deletion path is also used while healing those databases, so checking only
    for the table itself is not enough before selecting a newer column.
    """
    cur.execute(f"PRAGMA table_info({table_name})")
    return any(str(row[1] or "") == column_name for row in cur.fetchall())


def _clone_related_job_ids(cur: sqlite3.Cursor, run_id: str) -> list[str]:
    job_ids: list[str] = []
    for table_name in (
        "admin_clone_runs",
        "admin_clone_plans",
        "admin_clone_migrations",
    ):
        if (
            not _table_exists(cur, table_name)
            or not _table_has_column(cur, table_name, "run_id")
            or not _table_has_column(cur, table_name, "job_id")
        ):
            continue
        cur.execute(
            f"SELECT job_id FROM {table_name} WHERE run_id = ?",
            (run_id,),
        )
        job_ids.extend(str(row[0] or "").strip() for row in cur.fetchall())
    return sorted({job_id for job_id in job_ids if job_id})


def _iter_batches(values: list[str], size: int = _JOB_ID_BATCH_SIZE):
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _active_related_job_ids(cur: sqlite3.Cursor, job_ids: list[str]) -> list[str]:
    if (
        not job_ids
        or not _table_exists(cur, "admin_jobs")
        or not _table_has_column(cur, "admin_jobs", "job_id")
        or not _table_has_column(cur, "admin_jobs", "status")
    ):
        return []
    active_ids: set[str] = set()
    for job_id_part in _iter_batches(job_ids):
        placeholders = ", ".join("?" for _ in job_id_part)
        cur.execute(
            f"""
            SELECT job_id
            FROM admin_jobs
            WHERE job_id IN ({placeholders})
              AND lower(trim(status)) IN (
                  'pending', 'queued', 'running', 'stopping', 'deleting'
              )
            ORDER BY job_id
            """,
            job_id_part,
        )
        active_ids.update(
            str(row[0] or "") for row in cur.fetchall() if str(row[0] or "")
        )
    return sorted(active_ids)


def _active_target_delete_job_ids(
    cur: sqlite3.Cursor, *, exclude_job_id: str = ""
) -> list[str]:
    if (
        not _table_exists(cur, "admin_jobs")
        or not _table_has_column(cur, "admin_jobs", "job_id")
        or not _table_has_column(cur, "admin_jobs", "job_type")
        or not _table_has_column(cur, "admin_jobs", "status")
    ):
        return []
    params: list[str] = []
    exclusion_sql = ""
    if exclude_job_id:
        exclusion_sql = "AND job_id <> ?"
        params.append(exclude_job_id)
    cur.execute(
        f"""
        SELECT job_id
        FROM admin_jobs
        WHERE lower(trim(job_type)) = 'clone_target_delete'
          AND lower(trim(status)) IN (
              'pending', 'queued', 'running', 'stopping', 'deleting'
          )
          {exclusion_sql}
        ORDER BY job_id
        """,
        params,
    )
    return [str(row[0] or "") for row in cur.fetchall() if str(row[0] or "")]


def _job_status(cur: sqlite3.Cursor, job_id: str) -> str | None:
    if (
        not job_id
        or not _table_exists(cur, "admin_jobs")
        or not _table_has_column(cur, "admin_jobs", "job_id")
        or not _table_has_column(cur, "admin_jobs", "status")
    ):
        return None
    cur.execute(
        "SELECT status FROM admin_jobs WHERE job_id = ? LIMIT 1",
        (job_id,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return str(row[0] or "").strip().lower()


def _clone_run_deletion_job_id(
    cur: sqlite3.Cursor,
    run_id: str,
) -> str | None:
    """Read the deletion token, returning None when an old table lacks it."""
    if (
        not _table_exists(cur, "admin_clone_runs")
        or not _table_has_column(cur, "admin_clone_runs", "run_id")
        or not _table_has_column(cur, "admin_clone_runs", "deletion_job_id")
    ):
        return None
    cur.execute(
        """
        SELECT deletion_job_id
        FROM admin_clone_runs
        WHERE run_id = ?
        LIMIT 1
        """,
        (run_id,),
    )
    row = cur.fetchone()
    if row is None:
        return ""
    return str(row[0] or "").strip()


def _deletion_conflict_job_ids(
    cur: sqlite3.Cursor,
    *,
    run_status: str | None,
    stored_job_id: str | None,
    current_job_id: str,
) -> list[str]:
    """Return active deletion owners, scoped by the durable token when present.

    Rows created before the token migration have no way to identify their
    deletion owner.  Keep the former global check for those rows only; new rows
    can run deletions for different clone runs concurrently without blocking.
    """
    if stored_job_id:
        if current_job_id and stored_job_id == current_job_id:
            return []
        owner_status = _job_status(cur, stored_job_id)
        return [stored_job_id] if owner_status in _ACTIVE_JOB_STATUSES else []
    if run_status != "deleting":
        return []
    return _active_target_delete_job_ids(cur, exclude_job_id=current_job_id)


def _clone_run_status(cur: sqlite3.Cursor, run_id: str) -> str | None:
    if (
        not _table_exists(cur, "admin_clone_runs")
        or not _table_has_column(cur, "admin_clone_runs", "run_id")
        or not _table_has_column(cur, "admin_clone_runs", "status")
    ):
        return None
    cur.execute(
        """
        SELECT status
        FROM admin_clone_runs
        WHERE run_id = ?
        LIMIT 1
        """,
        (run_id,),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return str(row[0] or "").strip().lower()


@synchronized_write
def claim_clone_run_for_deletion(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    job_id: str = "",
) -> bool:
    """Atomically reserve a clone run before touching its remote target."""
    normalized_run_id = _clean_text(run_id)
    if not normalized_run_id:
        return False
    normalized_job_id = _clean_text(job_id)
    cur = conn.cursor()
    owns_transaction = not conn.in_transaction
    savepoint_name = f"claim_clone_run_delete_{id(cur)}"
    try:
        if owns_transaction:
            cur.execute("BEGIN IMMEDIATE")
        else:
            cur.execute(f"SAVEPOINT {savepoint_name}")

        if (
            not _table_exists(cur, "admin_clone_runs")
            or not _table_has_column(cur, "admin_clone_runs", "run_id")
            or not _table_has_column(cur, "admin_clone_runs", "status")
        ):
            claimed = False
        else:
            related_job_ids = _clone_related_job_ids(cur, normalized_run_id)
            run_status = _clone_run_status(cur, normalized_run_id)
            stored_deletion_job_id = _clone_run_deletion_job_id(
                cur,
                normalized_run_id,
            )
            if run_status in {"running", "stopping"}:
                raise RuntimeError("克隆任务仍在运行，拒绝删除克隆记录")
            active_delete_job_ids = _deletion_conflict_job_ids(
                cur,
                run_status=run_status,
                stored_job_id=stored_deletion_job_id,
                current_job_id=normalized_job_id,
            )
            if active_delete_job_ids:
                raise RuntimeError(
                    "克隆删除任务仍在运行，拒绝重复删除："
                    + ", ".join(active_delete_job_ids)
                )
            active_job_ids = _active_related_job_ids(cur, related_job_ids)
            if active_job_ids:
                raise RuntimeError(
                    "克隆任务仍在运行，拒绝删除克隆记录："
                    + ", ".join(active_job_ids)
                )
            update_parts = ["status = ?"]
            update_params: list[object] = ["deleting"]
            for column_name, value in (
                ("phase", "deleting"),
                ("error_message", ""),
            ):
                if _table_has_column(cur, "admin_clone_runs", column_name):
                    update_parts.append(f"{column_name} = ?")
                    update_params.append(value)
            if _table_has_column(cur, "admin_clone_runs", "deletion_job_id"):
                update_parts.append("deletion_job_id = ?")
                update_params.append(normalized_job_id)
            if _table_has_column(cur, "admin_clone_runs", "updated_at"):
                update_parts.append("updated_at = datetime('now')")
            update_params.append(normalized_run_id)
            cur.execute(
                f"""
                UPDATE admin_clone_runs
                SET {", ".join(update_parts)}
                WHERE run_id = ?
                """,
                update_params,
            )
            claimed = cur.rowcount > 0

        if owns_transaction:
            conn.commit()
        else:
            cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        return claimed
    except Exception:
        if owns_transaction:
            try:
                conn.rollback()
            except Exception:
                logging.exception("抢占克隆删除状态后的事务回滚也失败")
        else:
            try:
                cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
            except Exception:
                logging.exception("抢占克隆删除状态后的 SAVEPOINT 回滚失败")
            try:
                cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            except Exception:
                logging.exception("抢占克隆删除状态后的 SAVEPOINT 释放失败")
        raise
    finally:
        cur.close()


@synchronized_write
def delete_clone_run(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    job_id: str = "",
) -> bool:
    """Delete one clone chain without committing a caller-owned transaction."""
    normalized_run_id = _clean_text(run_id)
    if not normalized_run_id:
        return False
    normalized_job_id = _clean_text(job_id)
    cur = conn.cursor()
    owns_transaction = not conn.in_transaction
    savepoint_name = f"delete_clone_run_{id(cur)}"
    try:
        if owns_transaction:
            cur.execute("BEGIN IMMEDIATE")
        else:
            # A caller may have unrelated uncommitted work on this connection.
            # A savepoint keeps clone deletion atomic without rolling that work
            # back or committing it behind the caller's back.
            cur.execute(f"SAVEPOINT {savepoint_name}")
        related_job_ids = _clone_related_job_ids(cur, normalized_run_id)
        run_status = _clone_run_status(cur, normalized_run_id)
        stored_deletion_job_id = _clone_run_deletion_job_id(cur, normalized_run_id)
        if run_status in {"running", "stopping"}:
            raise RuntimeError("克隆任务仍在运行，拒绝删除克隆记录")
        active_delete_job_ids = _deletion_conflict_job_ids(
            cur,
            run_status=run_status,
            stored_job_id=stored_deletion_job_id,
            current_job_id=normalized_job_id,
        )
        if active_delete_job_ids:
            raise RuntimeError(
                "克隆删除任务仍在运行，拒绝删除克隆记录："
                + ", ".join(active_delete_job_ids)
            )
        active_job_ids = _active_related_job_ids(cur, related_job_ids)
        if active_job_ids:
            raise RuntimeError(
                "克隆任务仍在运行，拒绝删除克隆记录："
                + ", ".join(active_job_ids)
            )
        if _table_exists(cur, "admin_clone_media_transfers") and _table_has_column(
            cur, "admin_clone_media_transfers", "run_id"
        ):
            cur.execute(
                "DELETE FROM admin_clone_media_transfers WHERE run_id = ?",
                (normalized_run_id,),
            )
        for table_name in (
            "admin_clone_message_map",
            "admin_clone_migrations",
            "admin_clone_plans",
        ):
            if _table_exists(cur, table_name) and _table_has_column(
                cur, table_name, "run_id"
            ):
                cur.execute(
                    f"DELETE FROM {table_name} WHERE run_id = ?",
                    (normalized_run_id,),
                )
        deleted = False
        if _table_exists(cur, "admin_clone_runs") and _table_has_column(
            cur, "admin_clone_runs", "run_id"
        ):
            cur.execute(
                "DELETE FROM admin_clone_runs WHERE run_id = ?",
                (normalized_run_id,),
            )
            deleted = cur.rowcount > 0
        # Old clone jobs no longer describe a live record after this purge.
        purge_job_ids = [
            related_job_id
            for related_job_id in related_job_ids
            if related_job_id != normalized_job_id
        ]
        has_admin_jobs = _table_exists(cur, "admin_jobs") and _table_has_column(
            cur,
            "admin_jobs",
            "job_id",
        )
        if deleted and purge_job_ids and has_admin_jobs:
            has_job_logs = _table_exists(
                cur,
                "admin_job_logs",
            ) and _table_has_column(cur, "admin_job_logs", "job_id")
            for job_id_part in _iter_batches(purge_job_ids):
                placeholders = ", ".join("?" for _ in job_id_part)
                if has_job_logs:
                    cur.execute(
                        f"DELETE FROM admin_job_logs WHERE job_id IN ({placeholders})",
                        job_id_part,
                    )
                cur.execute(
                    f"DELETE FROM admin_jobs WHERE job_id IN ({placeholders})",
                    job_id_part,
                )
        if owns_transaction:
            conn.commit()
        else:
            cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        return deleted
    except Exception:
        if owns_transaction:
            try:
                conn.rollback()
            except Exception:
                logging.exception("删除克隆记录失败后的事务回滚也失败")
        else:
            try:
                cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
            except Exception:
                logging.exception("删除克隆记录失败后的 SAVEPOINT 回滚失败")
            try:
                cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            except Exception:
                logging.exception("删除克隆记录失败后的 SAVEPOINT 释放失败")
        raise
    finally:
        cur.close()


@synchronized_write
def mark_clone_run_message_reset_required(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    target_chat_id: int,
) -> None:
    """Persist a migration barrier before deleting any target messages."""
    normalized_run_id = _clean_text(run_id)
    normalized_target_chat_id = int(target_chat_id)
    if not normalized_run_id:
        raise ValueError("run_id 不能为空")
    if normalized_target_chat_id <= 0:
        raise ValueError("target_chat_id 参数非法")

    cur = conn.cursor()
    owns_transaction = not conn.in_transaction
    savepoint_name = f"mark_clone_message_reset_{id(cur)}"
    try:
        if owns_transaction:
            cur.execute("BEGIN IMMEDIATE")
        else:
            cur.execute(f"SAVEPOINT {savepoint_name}")
        cur.execute(
            """
            SELECT status, target_chat_id
            FROM admin_clone_runs
            WHERE run_id = ?
            LIMIT 1
            """,
            (normalized_run_id,),
        )
        run = cur.fetchone()
        if run is None:
            raise RuntimeError("克隆运行记录不存在")
        if int(run["target_chat_id"] or 0) != normalized_target_chat_id:
            raise RuntimeError("克隆目标与运行记录不一致，拒绝开始回退")
        if str(run["status"] or "").strip().lower() in {
            "queued",
            "running",
            "stopping",
            "deleting",
        }:
            raise RuntimeError("克隆运行仍有其他操作，拒绝开始回退")
        cur.execute(
            """
            UPDATE admin_clone_runs
            SET status = 'done', phase = ?,
                error_message = '完整清空尚未完成，必须重试完整清空后才能继续迁移',
                updated_at = datetime('now')
            WHERE run_id = ?
            """,
            (CLONE_MESSAGE_RESET_REQUIRED_PHASE, normalized_run_id),
        )
        if cur.rowcount != 1:
            raise RuntimeError("克隆回退安全状态写入失败")
        if owns_transaction:
            conn.commit()
        else:
            cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
    except Exception:
        if owns_transaction:
            conn.rollback()
        else:
            with suppress(sqlite3.Error):
                cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
            with suppress(sqlite3.Error):
                cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        raise
    finally:
        cur.close()


@synchronized_write
def reset_clone_run_timeline(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    target_chat_id: int,
) -> dict[str, int]:
    """Return a created target to its pre-migration local state.

    The clone run and its preflight plans remain usable. Delivery checkpoints,
    timeline migrations, and the jobs owned by those migrations are removed in
    one transaction after the caller has independently cleared the remote
    target and relay messages.
    """
    normalized_run_id = _clean_text(run_id)
    normalized_target_chat_id = int(target_chat_id)
    if not normalized_run_id:
        raise ValueError("run_id 不能为空")
    if normalized_target_chat_id <= 0:
        raise ValueError("target_chat_id 参数非法")

    cur = conn.cursor()
    owns_transaction = not conn.in_transaction
    savepoint_name = f"reset_clone_timeline_{id(cur)}"
    try:
        if owns_transaction:
            cur.execute("BEGIN IMMEDIATE")
        else:
            cur.execute(f"SAVEPOINT {savepoint_name}")

        cur.execute(
            """
            SELECT status, target_chat_id
            FROM admin_clone_runs
            WHERE run_id = ?
            LIMIT 1
            """,
            (normalized_run_id,),
        )
        run = cur.fetchone()
        if run is None:
            raise RuntimeError("克隆运行记录不存在")
        if int(run["target_chat_id"] or 0) != normalized_target_chat_id:
            raise RuntimeError("克隆目标与运行记录不一致，拒绝重置")
        if str(run["status"] or "").strip().lower() in {
            "queued",
            "running",
            "stopping",
            "deleting",
        }:
            raise RuntimeError("克隆任务仍在执行，拒绝重置迁移状态")

        cur.execute(
            """
            SELECT migration_id, job_id
            FROM admin_clone_migrations
            WHERE run_id = ?
            """,
            (normalized_run_id,),
        )
        migration_rows = cur.fetchall()
        migration_job_ids = sorted(
            {
                str(row["job_id"] or "").strip()
                for row in migration_rows
                if str(row["job_id"] or "").strip()
            }
        )
        active_job_ids = _active_related_job_ids(cur, migration_job_ids)
        if active_job_ids:
            raise RuntimeError(
                "克隆迁移任务仍在运行，拒绝重置：" + ", ".join(active_job_ids)
            )

        counts: dict[str, int] = {}
        for result_key, table_name in (
            ("media_transfer_count", "admin_clone_media_transfers"),
            ("mapping_count", "admin_clone_message_map"),
        ):
            cur.execute(
                f"DELETE FROM {table_name} WHERE run_id = ?",
                (normalized_run_id,),
            )
            counts[result_key] = max(0, int(cur.rowcount or 0))

        cur.execute(
            "DELETE FROM admin_clone_migrations WHERE run_id = ?",
            (normalized_run_id,),
        )
        counts["migration_count"] = max(0, int(cur.rowcount or 0))

        purged_job_count = 0
        if migration_job_ids and _table_exists(cur, "admin_jobs"):
            has_job_logs = _table_exists(
                cur,
                "admin_job_logs",
            ) and _table_has_column(cur, "admin_job_logs", "job_id")
            for job_id_part in _iter_batches(migration_job_ids):
                placeholders = ", ".join("?" for _ in job_id_part)
                if has_job_logs:
                    cur.execute(
                        f"DELETE FROM admin_job_logs WHERE job_id IN ({placeholders})",
                        job_id_part,
                    )
                cur.execute(
                    f"DELETE FROM admin_jobs WHERE job_id IN ({placeholders})",
                    job_id_part,
                )
                purged_job_count += max(0, int(cur.rowcount or 0))
        counts["migration_job_count"] = purged_job_count

        deletion_owner_reset = (
            ", deletion_job_id = ''"
            if _table_has_column(cur, "admin_clone_runs", "deletion_job_id")
            else ""
        )
        cur.execute(
            f"""
            UPDATE admin_clone_runs
            SET status = 'done', phase = 'done', error_message = '',
                updated_at = datetime('now'){deletion_owner_reset}
            WHERE run_id = ?
            """,
            (normalized_run_id,),
        )
        if cur.rowcount != 1:
            raise RuntimeError("克隆运行记录重置失败")

        if owns_transaction:
            conn.commit()
        else:
            cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        return counts
    except Exception:
        if owns_transaction:
            conn.rollback()
        else:
            with suppress(sqlite3.Error):
                cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
            with suppress(sqlite3.Error):
                cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        raise
    finally:
        cur.close()
