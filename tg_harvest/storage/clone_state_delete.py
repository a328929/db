import sqlite3

from tg_harvest.storage.clone_common import _clean_text


def _table_exists(cur: sqlite3.Cursor, table_name: str) -> bool:
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (str(table_name),),
    )
    return cur.fetchone() is not None


def _clone_related_job_ids(cur: sqlite3.Cursor, run_id: str) -> list[str]:
    job_ids: list[str] = []
    for table_name in (
        "admin_clone_runs",
        "admin_clone_plans",
        "admin_clone_migrations",
    ):
        cur.execute(
            f"SELECT job_id FROM {table_name} WHERE run_id = ?",
            (run_id,),
        )
        job_ids.extend(str(row[0] or "").strip() for row in cur.fetchall())
    return sorted({job_id for job_id in job_ids if job_id})


def delete_clone_run(
    conn: sqlite3.Connection,
    *,
    run_id: str,
) -> bool:
    normalized_run_id = _clean_text(run_id)
    if not normalized_run_id:
        return False
    cur = conn.cursor()
    try:
        related_job_ids = _clone_related_job_ids(cur, normalized_run_id)
        cur.execute(
            "DELETE FROM admin_clone_message_map WHERE run_id = ?",
            (normalized_run_id,),
        )
        cur.execute(
            "DELETE FROM admin_clone_migrations WHERE run_id = ?",
            (normalized_run_id,),
        )
        cur.execute(
            "DELETE FROM admin_clone_plans WHERE run_id = ?",
            (normalized_run_id,),
        )
        cur.execute(
            "DELETE FROM admin_clone_runs WHERE run_id = ?",
            (normalized_run_id,),
        )
        deleted = cur.rowcount > 0
        # Old clone jobs no longer describe a live record after this purge.
        if deleted and related_job_ids and _table_exists(cur, "admin_jobs"):
            placeholders = ", ".join("?" for _ in related_job_ids)
            if _table_exists(cur, "admin_job_logs"):
                cur.execute(
                    f"DELETE FROM admin_job_logs WHERE job_id IN ({placeholders})",
                    related_job_ids,
                )
            cur.execute(
                f"DELETE FROM admin_jobs WHERE job_id IN ({placeholders})",
                related_job_ids,
            )
        conn.commit()
        return deleted
    finally:
        cur.close()
