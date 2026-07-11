from __future__ import annotations

import shutil
import sqlite3
import threading
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from tg_harvest.storage import fts as _fts
from tg_harvest.storage import search_terms as _search_terms

_COMPACTION_HEADROOM_BYTES = 64 * 1024 * 1024
_WAL_GROWTH_MIN_BYTES = 4 * 1024 * 1024
_WAL_OBSERVATIONS: dict[str, int] = {}
_WAL_OBSERVATIONS_LOCK = threading.Lock()

_COUNTED_TABLES = frozenset(
    {
        "media_groups",
        "message_search_terms",
    }
)
_CJK_META_KEYS = (
    "cjk_terms_rebuild_state",
    "cjk_terms_last_maintenance_at",
    "cjk_terms_last_maintenance_result",
    "cjk_terms_last_rebuild_at",
    "cjk_terms_queue_length",
)
_MAINTENANCE_JOB_TYPES = (
    "cleanup",
    "cleanup_empty",
    "delete",
    "delete_empty_chats",
)


def _table_names(cur: sqlite3.Cursor) -> set[str]:
    cur.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    return {str(row[0] or "") for row in cur.fetchall()}


def _fetch_pragma_int(cur: sqlite3.Cursor, name: str) -> int:
    cur.execute(f"PRAGMA {name}")
    row = cur.fetchone()
    return max(0, int((row[0] if row else 0) or 0))


def _fetch_journal_mode(cur: sqlite3.Cursor) -> str:
    cur.execute("PRAGMA journal_mode")
    row = cur.fetchone()
    return str((row[0] if row else "") or "").lower()


def _path_size(path: Path | None) -> int:
    if path is None:
        return 0
    try:
        return max(0, int(path.stat().st_size))
    except OSError:
        return 0


def _resolve_db_path(
    conn: sqlite3.Connection,
    *,
    cfg: Any | None,
    db_path: str | Path | None,
) -> Path | None:
    raw_path = str(
        db_path if db_path is not None else getattr(cfg, "db_name", "")
    ).strip()
    if raw_path and raw_path != ":memory:":
        return Path(raw_path).expanduser().resolve()

    cur = conn.cursor()
    try:
        cur.execute("PRAGMA database_list")
        for row in cur.fetchall():
            if str(row[1] or "") != "main":
                continue
            candidate = str(row[2] or "").strip()
            if candidate and candidate != ":memory:":
                return Path(candidate).expanduser().resolve()
    finally:
        cur.close()
    return None


def _disk_free_bytes(path: Path | None) -> int | None:
    if path is None:
        return None
    try:
        return max(0, int(shutil.disk_usage(path.parent).free))
    except OSError:
        return None


def _sqlite_stat_row_estimate(cur: sqlite3.Cursor, table_name: str) -> int | None:
    try:
        cur.execute(
            """
            SELECT stat
            FROM sqlite_stat1
            WHERE tbl = ?
            ORDER BY CASE WHEN idx IS NULL THEN 0 ELSE 1 END, idx ASC
            LIMIT 1
            """,
            (table_name,),
        )
    except sqlite3.Error:
        return None
    row = cur.fetchone()
    if row is None:
        return None
    try:
        return max(0, int(str(row[0] or "").split()[0]))
    except (IndexError, TypeError, ValueError):
        return None


def _metadata_table_count(
    cur: sqlite3.Cursor,
    *,
    table_name: str,
) -> tuple[int | None, str]:
    if table_name not in _COUNTED_TABLES:
        return None, "unavailable"

    estimate = _sqlite_stat_row_estimate(cur, table_name)
    if estimate is not None:
        return estimate, "sqlite_stat1"
    return None, "unavailable"


def _message_count(cur: sqlite3.Cursor, table_names: set[str]) -> int | None:
    if "chats" not in table_names:
        return 0
    cur.execute("SELECT COALESCE(SUM(message_count), 0) FROM chats")
    row = cur.fetchone()
    return max(0, int((row[0] if row else 0) or 0))


def _read_cjk_maintenance(
    cur: sqlite3.Cursor, table_names: set[str]
) -> dict[str, str]:
    if "message_search_terms_meta" not in table_names:
        return {
            "rebuild_state": "",
            "last_maintenance_at": "",
            "last_maintenance_result": "",
            "last_rebuild_at": "",
            "queue_length": "",
        }
    placeholders = ", ".join("?" for _ in _CJK_META_KEYS)
    cur.execute(
        f"SELECT key, value FROM message_search_terms_meta WHERE key IN ({placeholders})",
        _CJK_META_KEYS,
    )
    values = {str(row[0] or ""): str(row[1] or "") for row in cur.fetchall()}
    return {
        "rebuild_state": values.get("cjk_terms_rebuild_state", ""),
        "last_maintenance_at": values.get("cjk_terms_last_maintenance_at", ""),
        "last_maintenance_result": values.get(
            "cjk_terms_last_maintenance_result", ""
        ),
        "last_rebuild_at": values.get("cjk_terms_last_rebuild_at", ""),
        "queue_length": values.get("cjk_terms_queue_length", ""),
    }


def _read_last_maintenance_job(
    cur: sqlite3.Cursor, table_names: set[str]
) -> dict[str, str] | None:
    if "admin_jobs" not in table_names:
        return None
    placeholders = ", ".join("?" for _ in _MAINTENANCE_JOB_TYPES)
    cur.execute(
        f"""
        SELECT job_type, status, updated_at
        FROM admin_jobs
        WHERE job_type IN ({placeholders})
        ORDER BY updated_at DESC, created_at DESC
        LIMIT 1
        """,
        _MAINTENANCE_JOB_TYPES,
    )
    row = cur.fetchone()
    if row is None:
        return None
    return {
        "job_type": str(row[0] or ""),
        "status": str(row[1] or ""),
        "updated_at": str(row[2] or ""),
    }


def _configured_threshold(cfg: Any | None, name: str, default: int) -> int:
    try:
        return int(getattr(cfg, name, default))
    except (TypeError, ValueError):
        return int(default)


def _metadata_nonnegative_int(value: Any) -> int | None:
    try:
        return max(0, int(str(value or "").strip()))
    except (TypeError, ValueError):
        return None


def _health_thresholds(cfg: Any | None) -> dict[str, int]:
    size_warning = _configured_threshold(
        cfg, "db_health_size_warning_bytes", 20 * 1024 * 1024 * 1024
    )
    wal_warning = _configured_threshold(
        cfg, "db_health_wal_warning_bytes", 512 * 1024 * 1024
    )
    disk_warning = _configured_threshold(
        cfg, "db_health_disk_free_warning_bytes", 10 * 1024 * 1024 * 1024
    )
    cjk_warning = _configured_threshold(cfg, "db_health_cjk_queue_warning", 10000)
    return {
        "size_warning_bytes": size_warning,
        "size_critical_bytes": _configured_threshold(
            cfg, "db_health_size_critical_bytes", 50 * 1024 * 1024 * 1024
        ),
        "wal_warning_bytes": wal_warning,
        "wal_critical_bytes": _configured_threshold(
            cfg, "db_health_wal_critical_bytes", 2 * 1024 * 1024 * 1024
        ),
        "disk_free_warning_bytes": disk_warning,
        "disk_free_critical_bytes": _configured_threshold(
            cfg, "db_health_disk_free_critical_bytes", 3 * 1024 * 1024 * 1024
        ),
        "cjk_queue_warning": cjk_warning,
        "cjk_queue_critical": _configured_threshold(
            cfg, "db_health_cjk_queue_critical", 100000
        ),
    }


def _last_wal_size(path: Path | None, current_size: int) -> int | None:
    if path is None:
        return None
    key = str(path)
    with _WAL_OBSERVATIONS_LOCK:
        previous = _WAL_OBSERVATIONS.get(key)
        _WAL_OBSERVATIONS[key] = max(0, int(current_size))
    return previous


def _append_reason(
    reasons: list[dict[str, str]],
    actions: list[str],
    *,
    severity: str,
    code: str,
    message: str,
    action: str,
) -> None:
    reasons.append({"severity": severity, "code": code, "message": message})
    if action and action not in actions:
        actions.append(action)


def _overall_status(reasons: list[dict[str, str]]) -> str:
    if any(item["severity"] == "critical" for item in reasons):
        return "critical"
    if any(item["severity"] == "warning" for item in reasons):
        return "warning"
    return "healthy"


def build_database_health_payload(
    conn: sqlite3.Connection,
    *,
    cfg: Any | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    """Build a read-only capacity snapshot without dbstat or integrity scans."""
    resolved_path = _resolve_db_path(conn, cfg=cfg, db_path=db_path)
    path_exists = bool(resolved_path and resolved_path.exists())
    wal_path = Path(f"{resolved_path}-wal") if resolved_path else None
    shm_path = Path(f"{resolved_path}-shm") if resolved_path else None
    main_bytes = _path_size(resolved_path)
    wal_bytes = _path_size(wal_path)
    shm_bytes = _path_size(shm_path)
    disk_free_bytes = _disk_free_bytes(resolved_path)
    previous_wal_bytes = _last_wal_size(resolved_path, wal_bytes)
    thresholds = _health_thresholds(cfg)

    cur = conn.cursor()
    try:
        page_size = _fetch_pragma_int(cur, "page_size")
        page_count = _fetch_pragma_int(cur, "page_count")
        freelist_count = _fetch_pragma_int(cur, "freelist_count")
        journal_mode = _fetch_journal_mode(cur)
        table_names = _table_names(cur)
        message_count = _message_count(cur, table_names)
        media_group_count, media_group_count_source = (
            _metadata_table_count(
                cur,
                table_name="media_groups",
            )
            if "media_groups" in table_names
            else (0, "table_missing")
        )
        cjk_term_count, cjk_term_count_source = (
            _metadata_table_count(
                cur,
                table_name="message_search_terms",
            )
            if "message_search_terms" in table_names
            else (0, "table_missing")
        )
        cjk_maintenance = _read_cjk_maintenance(cur, table_names)
        cjk_queue_length = _metadata_nonnegative_int(
            cjk_maintenance["queue_length"]
        )
        cjk_queue_count_source = "maintenance_meta"
        if cjk_queue_length is None:
            cjk_queue_length = _sqlite_stat_row_estimate(
                cur, "message_search_terms_rebuild_queue"
            )
            cjk_queue_count_source = (
                "sqlite_stat1" if cjk_queue_length is not None else "unavailable"
            )
        if "message_search_terms_rebuild_queue" not in table_names:
            cjk_queue_length = 0
            cjk_queue_count_source = "table_missing"
        last_maintenance_job = _read_last_maintenance_job(cur, table_names)
    finally:
        cur.close()

    fts_ready = _fts.fts_index_is_marked_ready(conn)
    cjk_terms_current = _search_terms.message_search_terms_are_current(conn)
    compaction_required_bytes = (
        (main_bytes + wal_bytes + shm_bytes) * 2 + _COMPACTION_HEADROOM_BYTES
        if path_exists
        else None
    )
    can_compact_safely = (
        None
        if compaction_required_bytes is None or disk_free_bytes is None
        else disk_free_bytes >= compaction_required_bytes
    )

    reasons: list[dict[str, str]] = []
    actions: list[str] = []
    if resolved_path is None:
        _append_reason(
            reasons,
            actions,
            severity="warning",
            code="database_path_unconfigured",
            message="未配置可检查的主数据库路径，无法读取主库和磁盘容量。",
            action="检查 TG_DB_NAME，确保管理进程能访问数据库所在目录。",
        )
    elif not path_exists:
        _append_reason(
            reasons,
            actions,
            severity="warning",
            code="database_path_missing",
            message="配置的主数据库文件不存在，容量数据只反映当前连接的元数据。",
            action="检查 TG_DB_NAME 指向的运行目录和数据库文件权限。",
        )

    if main_bytes >= thresholds["size_critical_bytes"]:
        _append_reason(
            reasons,
            actions,
            severity="critical",
            code="database_size_critical",
            message="主库容量已达到严重阈值，继续增长可能耗尽维护和恢复空间。",
            action="尽快扩容磁盘，并在维护窗口规划紧凑库构建与替换。",
        )
    elif main_bytes >= thresholds["size_warning_bytes"]:
        _append_reason(
            reasons,
            actions,
            severity="warning",
            code="database_size_warning",
            message="主库容量已达到预警阈值，应安排容量规划和维护窗口。",
            action="评估保留策略和可用磁盘空间，提前安排紧凑库维护。",
        )

    if wal_bytes >= thresholds["wal_critical_bytes"]:
        _append_reason(
            reasons,
            actions,
            severity="critical",
            code="wal_size_critical",
            message="WAL 文件已达到严重阈值，写入和磁盘风险显著上升。",
            action="检查长期读取事务，并在低峰期安排安全的 WAL checkpoint。",
        )
    elif wal_bytes >= thresholds["wal_warning_bytes"]:
        _append_reason(
            reasons,
            actions,
            severity="warning",
            code="wal_size_warning",
            message="WAL 文件已达到预警阈值，应关注 checkpoint 是否被读取连接阻塞。",
            action="检查长期读取事务和后台任务，确认 WAL 能在低峰期回收。",
        )

    wal_growing = (
        previous_wal_bytes is not None
        and wal_bytes >= thresholds["wal_warning_bytes"]
        and wal_bytes > previous_wal_bytes + _WAL_GROWTH_MIN_BYTES
        and wal_bytes > int(previous_wal_bytes * 1.1)
    )
    if wal_growing:
        _append_reason(
            reasons,
            actions,
            severity="warning",
            code="wal_growing",
            message="WAL 持续增长，可能存在阻塞 checkpoint 的长时间读取连接。",
            action="排查长时间读取事务，并在低峰期观察 checkpoint 后 WAL 是否回落。",
        )

    if disk_free_bytes is not None:
        if disk_free_bytes <= thresholds["disk_free_critical_bytes"]:
            _append_reason(
                reasons,
                actions,
                severity="critical",
                code="disk_free_critical",
                message="数据库所在磁盘余量已低于严重阈值。",
                action="磁盘余量不足以安全压缩；先扩容或清理非数据库文件。",
            )
        elif disk_free_bytes <= thresholds["disk_free_warning_bytes"]:
            _append_reason(
                reasons,
                actions,
                severity="warning",
                code="disk_free_warning",
                message="数据库所在磁盘余量偏低，维护操作的可用空间有限。",
                action="在维护窗口前预留额外磁盘空间，避免紧凑构建失败。",
            )

    if can_compact_safely is False:
        _append_reason(
            reasons,
            actions,
            severity="warning",
            code="compaction_space_insufficient",
            message="当前磁盘余量不足以同时保留一致性快照和紧凑构建库。",
            action="磁盘余量不足以安全压缩；先扩容或将目标库放到容量充足的磁盘。",
        )

    if cjk_queue_length is not None:
        if cjk_queue_length >= thresholds["cjk_queue_critical"]:
            _append_reason(
                reasons,
                actions,
                severity="critical",
                code="cjk_queue_critical",
                message="中文短词队列严重积压，短词检索可能明显滞后。",
                action="检查搜索维护线程和后台任务占用，待队列清空后复查索引状态。",
            )
        elif cjk_queue_length >= thresholds["cjk_queue_warning"]:
            _append_reason(
                reasons,
                actions,
                severity="warning",
                code="cjk_queue_warning",
                message="中文短词队列积压，新增短词检索结果可能暂时不完整。",
                action="观察后台短词维护是否持续排空队列，避免长时间并行大任务。",
            )

    if not fts_ready:
        _append_reason(
            reasons,
            actions,
            severity="critical",
            code="fts_not_ready",
            message="FTS 未就绪，全文检索可能退化或返回不完整结果。",
            action="先确认磁盘余量，再恢复 FTS 索引维护；不要把在线状态视为完整性校验。",
        )
    elif not cjk_terms_current or cjk_maintenance["rebuild_state"]:
        _append_reason(
            reasons,
            actions,
            severity="warning",
            code="cjk_index_rebuilding",
            message="中文短词索引尚未完成当前维护，短词搜索可能暂时不完整。",
            action="等待中文短词维护完成；若持续不恢复，检查维护日志和队列状态。",
        )

    if not actions:
        actions.append("当前未发现容量或索引维护风险，可继续观察增长趋势。")

    return {
        "ok": True,
        "status": _overall_status(reasons),
        "checked_at": datetime.now(UTC).replace(microsecond=0).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        "database": {
            "path": str(resolved_path or ""),
            "path_configured": resolved_path is not None,
            "path_exists": path_exists,
            "main_bytes": main_bytes,
            "wal_bytes": wal_bytes,
            "shm_bytes": shm_bytes,
            "page_size": page_size,
            "page_count": page_count,
            "freelist_count": freelist_count,
            "freelist_bytes": page_size * freelist_count,
            "journal_mode": journal_mode,
            "disk_free_bytes": disk_free_bytes,
            "compaction_required_bytes": compaction_required_bytes,
            "can_compact_safely": can_compact_safely,
        },
        "counts": {
            "message_count": message_count,
            "media_group_count": media_group_count,
            "cjk_term_count": cjk_term_count,
            "cjk_queue_length": cjk_queue_length,
            "sources": {
                "message_count": "chat_summary",
                "media_group_count": media_group_count_source,
                "cjk_term_count": cjk_term_count_source,
                "cjk_queue_length": cjk_queue_count_source,
            },
        },
        "indexes": {
            "fts_ready": fts_ready,
            "cjk_terms_current": cjk_terms_current,
        },
        "maintenance": {
            "cjk": cjk_maintenance,
            "last_recorded_job": last_maintenance_job,
        },
        "thresholds": thresholds,
        "reasons": reasons,
        "actions": actions,
    }
