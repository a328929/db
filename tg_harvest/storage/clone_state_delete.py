import sqlite3

from tg_harvest.storage.clone_common import _clean_text


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
        conn.commit()
        return deleted
    finally:
        cur.close()
