import sqlite3
from contextlib import closing
from typing import Any

from tg_harvest.config import CFG
from tg_harvest.storage.connection import connect_configured_db


def _admin_connect() -> sqlite3.Connection:
    conn, _ = connect_configured_db(cfg=CFG)
    return conn


def _admin_fetch_job_snapshot_row(job_id: str) -> sqlite3.Row | None:
    with closing(_admin_connect()) as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT
                    j.job_id,
                    j.job_type,
                    j.status,
                    j.target_chat_id,
                    j.target_label,
                    j.created_at,
                    j.updated_at,
                    j.progress_current,
                    j.progress_total,
                    j.progress_stage,
                    j.last_logged_current,
                    j.stop_requested,
                    (
                        SELECT COUNT(*)
                        FROM admin_job_logs l
                        WHERE l.job_id = j.job_id
                    ) AS log_count,
                    (
                        SELECT COALESCE(MAX(l.seq), 0)
                        FROM admin_job_logs l
                        WHERE l.job_id = j.job_id
                    ) AS last_seq
                FROM admin_jobs j
                WHERE j.job_id = ?
                LIMIT 1
                """,
                (job_id,),
            )
            return cur.fetchone()
        finally:
            cur.close()


def _admin_fetch_last_seq(job_id: str) -> int:
    with closing(_admin_connect()) as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT COALESCE(MAX(seq), 0) AS last_seq FROM admin_job_logs WHERE job_id = ?",
                (job_id,),
            )
            row = cur.fetchone()
            return int(row["last_seq"] or 0) if row is not None else 0
        finally:
            cur.close()


def _admin_snapshot_from_row(row: sqlite3.Row) -> dict[str, Any]:
    progress_total = row["progress_total"]
    return {
        "job_id": str(row["job_id"] or ""),
        "job_type": str(row["job_type"] or "unknown"),
        "status": str(row["status"] or "queued"),
        "target_chat_id": row["target_chat_id"],
        "target_label": row["target_label"],
        "created_at": str(row["created_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
        "progress": {
            "current": int(row["progress_current"] or 0),
            "total": int(progress_total) if isinstance(progress_total, int) else None,
            "stage": str(row["progress_stage"] or "queued"),
        },
        "stop_requested": int(row["stop_requested"] or 0) == 1,
        "log_count": int(row["log_count"] or 0),
        "last_seq": int(row["last_seq"] or 0),
    }


def _admin_active_job_summary_from_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "job_id": str(row["job_id"] or ""),
        "job_type": str(row["job_type"] or "").lower(),
        "status": str(row["status"] or "").lower(),
        "target_chat_id": row["target_chat_id"],
        "target_label": row["target_label"],
        "stop_requested": int(row["stop_requested"] or 0) == 1,
    }


def _admin_insert_job_row(
    cur: sqlite3.Cursor,
    *,
    job_id: str,
    job_type: str,
    target_chat_id: int | None,
    target_label: str | None,
    created_at: str,
    owner_instance_id: str,
    owner_pid: int,
) -> None:
    cur.execute(
        """
        INSERT INTO admin_jobs(
            job_id,
            job_type,
            status,
            target_chat_id,
            target_label,
            created_at,
            updated_at,
            owner_instance_id,
            owner_pid,
            heartbeat_at,
            progress_current,
            progress_total,
            progress_stage,
            last_logged_current,
            stop_requested
        )
        VALUES (?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, 0, NULL, 'queued', 0, 0)
        """,
        (
            job_id,
            str(job_type or "unknown"),
            target_chat_id,
            (target_label or "").strip() or None,
            created_at,
            created_at,
            owner_instance_id,
            int(owner_pid),
            created_at,
        ),
    )


def _admin_persist_job_create(
    *,
    job_id: str,
    job_type: str,
    target_chat_id: int | None,
    target_label: str | None,
    created_at: str,
    owner_instance_id: str,
    owner_pid: int,
) -> None:
    with closing(_admin_connect()) as conn:
        cur = conn.cursor()
        try:
            _admin_insert_job_row(
                cur,
                job_id=job_id,
                job_type=job_type,
                target_chat_id=target_chat_id,
                target_label=target_label,
                created_at=created_at,
                owner_instance_id=owner_instance_id,
                owner_pid=owner_pid,
            )
            conn.commit()
        finally:
            cur.close()


def _admin_persist_log_locked(
    job_id: str,
    log_item: dict[str, Any],
    *,
    owner_instance_id: str,
    owner_pid: int,
    trim_logs: bool = True,
) -> None:
    with closing(_admin_connect()) as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO admin_job_logs(job_id, seq, ts, message)
                VALUES (?, ?, ?, ?)
                """,
                (
                    job_id,
                    int(log_item["seq"]),
                    str(log_item["ts"]),
                    str(log_item["message"]),
                ),
            )
            cur.execute(
                """
                UPDATE admin_jobs
                SET updated_at = ?,
                    owner_instance_id = ?,
                    owner_pid = ?,
                    heartbeat_at = ?
                WHERE job_id = ?
                """,
                (
                    str(log_item["ts"]),
                    owner_instance_id,
                    int(owner_pid),
                    str(log_item["ts"]),
                    job_id,
                ),
            )
            if trim_logs:
                cur.execute(
                    """
                    DELETE FROM admin_job_logs
                    WHERE job_id = ?
                      AND seq <= (
                          SELECT COALESCE(MAX(seq), 0) - ?
                          FROM admin_job_logs
                          WHERE job_id = ?
                      )
                    """,
                    (job_id, int(CFG.admin_job_log_max_lines), job_id),
                )
            conn.commit()
        finally:
            cur.close()
