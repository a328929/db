#!/usr/bin/env python3
"""One-shot SQLite compaction maintenance for the Telegram harvest DB."""

from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path("/root/db")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DB_PATH = ROOT / ".runtime" / "db" / "tg_data.db"
COMPACT_PATH = ROOT / ".runtime" / "db" / "tg_data.compact.db"
REPORT_PATH = ROOT / "bg.txt"
CONTAINER_NAME = "db"
PROGRESS_INTERVAL_SECONDS = 60
MIN_HEADROOM_BYTES = 1 * 1024 * 1024 * 1024

_log_lock = threading.Lock()
_stop_monitor = threading.Event()
_stage = {"name": "init"}
_expected_compact_bytes = {"value": 0}


def now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S %Z %z")


def fmt_bytes(value: int | float) -> str:
    value = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(value) < 1024 or unit == "TiB":
            if unit == "B":
                return f"{int(value)}B"
            return f"{value:.1f}{unit}"
        value /= 1024
    return f"{value:.1f}TiB"


def log(message: str) -> None:
    line = f"{now()} {message}"
    with _log_lock:
        with REPORT_PATH.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
        print(line, flush=True)


def run(
    args: list[str],
    *,
    check: bool = True,
    timeout: int | None = None,
    log_output: bool = True,
) -> subprocess.CompletedProcess[str]:
    log("$ " + " ".join(args))
    proc = subprocess.run(
        args,
        cwd=str(ROOT),
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if log_output:
        if proc.stdout.strip():
            log("stdout: " + proc.stdout.strip())
        if proc.stderr.strip():
            log("stderr: " + proc.stderr.strip())
    log(f"exit_code={proc.returncode}")
    if check and proc.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(args)}")
    return proc


def sql_quote_path(path: Path) -> str:
    return "'" + str(path).replace("'", "''") + "'"


def sqlite_stats(path: Path) -> dict[str, int | str]:
    conn = sqlite3.connect(str(path), timeout=60)
    try:
        cur = conn.cursor()
        values: dict[str, int | str] = {}
        for pragma in (
            "page_size",
            "page_count",
            "freelist_count",
            "auto_vacuum",
            "journal_mode",
        ):
            cur.execute("PRAGMA " + pragma)
            values[pragma] = cur.fetchone()[0]
        return values
    finally:
        conn.close()


def quick_check(path: Path) -> str:
    conn = sqlite3.connect(str(path), timeout=60)
    try:
        row = conn.execute("PRAGMA quick_check").fetchone()
        return str(row[0]) if row else ""
    finally:
        conn.close()


def remove_sidecars(base: Path) -> None:
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(str(base) + suffix)
        if sidecar.exists():
            log(f"remove sidecar {sidecar.name} size={fmt_bytes(sidecar.stat().st_size)}")
            sidecar.unlink()


def proc_io() -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        with Path("/proc/self/io").open(encoding="utf-8") as handle:
            for line in handle:
                key, value = line.split(":", 1)
                if key in {"read_bytes", "write_bytes"}:
                    values[key] = int(value.strip())
    except OSError:
        pass
    return values


def progress_monitor() -> None:
    last_io = proc_io()
    last_time = time.time()
    while not _stop_monitor.wait(PROGRESS_INTERVAL_SECONDS):
        current_time = time.time()
        current_io = proc_io()
        elapsed = max(current_time - last_time, 0.001)
        read_rate = None
        write_rate = None
        if last_io and current_io:
            read_rate = (current_io.get("read_bytes", 0) - last_io.get("read_bytes", 0)) / elapsed
            write_rate = (current_io.get("write_bytes", 0) - last_io.get("write_bytes", 0)) / elapsed
        last_io = current_io
        last_time = current_time

        compact_size = COMPACT_PATH.stat().st_size if COMPACT_PATH.exists() else 0
        free = shutil.disk_usage(DB_PATH.parent).free
        expected = _expected_compact_bytes["value"]
        pieces = [
            f"progress stage={_stage['name']}",
            f"compact={fmt_bytes(compact_size)}",
            f"free={fmt_bytes(free)}",
        ]
        if expected and _stage["name"] == "vacuum":
            pieces.insert(2, f"compact_pct={min(compact_size / expected * 100, 999):.1f}%")
        if read_rate is not None:
            pieces.append(f"read_rate={fmt_bytes(read_rate)}/s")
        if write_rate is not None:
            pieces.append(f"write_rate={fmt_bytes(write_rate)}/s")
        log(" ".join(pieces))


def start_monitor() -> threading.Thread:
    monitor = threading.Thread(target=progress_monitor, daemon=True)
    monitor.start()
    return monitor


def checkpoint_wal() -> None:
    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    try:
        row = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        log(f"wal_checkpoint(TRUNCATE)={row}")
    finally:
        conn.close()


def set_live_wal() -> str:
    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    try:
        return str(conn.execute("PRAGMA journal_mode=WAL").fetchone()[0])
    finally:
        conn.close()


def docker_is_running() -> bool:
    proc = run(
        ["docker", "inspect", "-f", "{{.State.Running}}", CONTAINER_NAME],
        check=False,
        log_output=False,
    )
    return proc.returncode == 0 and proc.stdout.strip() == "true"


def stop_container() -> None:
    if docker_is_running():
        run(["docker", "stop", "--time", "60", CONTAINER_NAME], timeout=90)
    else:
        log(f"container {CONTAINER_NAME} is not running before maintenance")


def start_container() -> None:
    if docker_is_running():
        log(f"container {CONTAINER_NAME} is already running")
        return
    run(["docker", "start", CONTAINER_NAME], timeout=60)


def curl_admin_login() -> None:
    run(
        [
            "curl",
            "-sS",
            "-I",
            "--max-time",
            "15",
            "http://127.0.0.1:8890/admin/login",
        ],
        check=False,
        timeout=20,
    )


def notify_ops_bot_result(exit_code: int) -> None:
    try:
        from tg_harvest.config import CFG
        from tg_harvest.ops_bot.client import is_notify_enabled, send_message_sync
    except Exception as exc:
        log(f"skip ops bot notification: import failed {exc!r}")
        return

    if not is_notify_enabled(CFG):
        return

    try:
        report_lines = REPORT_PATH.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        report_lines = [f"unable to read report: {exc!r}"]

    status = "成功" if exit_code == 0 else "失败"
    tail = "\n".join(report_lines[-30:])
    message = (
        f"SQLite 压缩任务{status}\n"
        f"exit_code={exit_code}\n"
        f"report={REPORT_PATH}\n\n"
        f"{tail}"
    )
    if not send_message_sync(CFG, message):
        log("ops bot notification not delivered")


def compact_database() -> None:
    if not DB_PATH.exists():
        raise RuntimeError(f"database not found: {DB_PATH}")

    if COMPACT_PATH.exists():
        log(f"remove stale compact candidate {COMPACT_PATH} size={fmt_bytes(COMPACT_PATH.stat().st_size)}")
        COMPACT_PATH.unlink()
    remove_sidecars(COMPACT_PATH)

    before = sqlite_stats(DB_PATH)
    page_size = int(before["page_size"])
    page_count = int(before["page_count"])
    freelist_count = int(before["freelist_count"])
    expected_compact = (page_count - freelist_count) * page_size
    reclaimable = freelist_count * page_size
    _expected_compact_bytes["value"] = expected_compact
    free = shutil.disk_usage(DB_PATH.parent).free

    log(f"source_path={DB_PATH}")
    log(f"before stats={before}")
    log(f"source_size={fmt_bytes(DB_PATH.stat().st_size)}")
    log(f"estimated_compact_size={fmt_bytes(expected_compact)}")
    log(f"estimated_reclaim={fmt_bytes(reclaimable)}")
    log(f"filesystem_free_before={fmt_bytes(free)}")

    if free < expected_compact + MIN_HEADROOM_BYTES:
        raise RuntimeError(
            "not enough free space: "
            f"free={fmt_bytes(free)} required_at_least={fmt_bytes(expected_compact + MIN_HEADROOM_BYTES)}"
        )

    _stage["name"] = "checkpoint"
    checkpoint_wal()
    remove_sidecars(DB_PATH)

    _stage["name"] = "vacuum"
    log("VACUUM INTO start")
    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    try:
        conn.execute("VACUUM INTO " + sql_quote_path(COMPACT_PATH))
    finally:
        conn.close()
    log(f"VACUUM INTO done compact_size={fmt_bytes(COMPACT_PATH.stat().st_size)}")

    _stage["name"] = "candidate_stats"
    candidate_stats = sqlite_stats(COMPACT_PATH)
    log(f"candidate stats={candidate_stats}")

    _stage["name"] = "candidate_quick_check"
    log("candidate quick_check start")
    check_result = quick_check(COMPACT_PATH)
    log(f"candidate quick_check={check_result}")
    if check_result != "ok":
        raise RuntimeError(f"candidate quick_check failed: {check_result}")

    timestamp = time.strftime("%Y%m%d-%H%M%S")
    backup_path = DB_PATH.with_name(f"tg_data.db.before-compact-{timestamp}")

    _stage["name"] = "replace"
    log(f"rename source to backup {backup_path.name}")
    DB_PATH.rename(backup_path)
    try:
        log("rename compact candidate to live database")
        COMPACT_PATH.rename(DB_PATH)

        journal_mode = set_live_wal()
        live_stats = sqlite_stats(DB_PATH)
        log(f"live journal_mode={journal_mode}")
        log(f"live stats={live_stats}")
    except Exception:
        log("replace failed; rolling back original database")
        failed_path = DB_PATH.with_name(f"tg_data.db.compact-failed-{timestamp}")
        if DB_PATH.exists():
            DB_PATH.rename(failed_path)
            log(f"moved failed live candidate to {failed_path.name}")
        backup_path.rename(DB_PATH)
        raise

    log(f"remove old database backup {backup_path.name} size={fmt_bytes(backup_path.stat().st_size)}")
    backup_path.unlink()
    remove_sidecars(backup_path)
    _stage["name"] = "done"
    log(f"filesystem_free_after={fmt_bytes(shutil.disk_usage(DB_PATH.parent).free)}")
    log(f"live_size_after={fmt_bytes(DB_PATH.stat().st_size)}")


def main() -> int:
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text("", encoding="utf-8")
    log("scheduled SQLite compaction job started")
    log(f"python={sys.version.split()[0]} sqlite={sqlite3.sqlite_version}")
    log(f"pid={os.getpid()}")

    monitor = start_monitor()
    stopped_container = False
    exit_code = 1
    try:
        _stage["name"] = "stop_container"
        stop_container()
        stopped_container = True
        compact_database()
        log("scheduled SQLite compaction job completed successfully")
        exit_code = 0
    except Exception as exc:
        log(f"ERROR {exc!r}")
        if COMPACT_PATH.exists():
            try:
                log(f"remove failed compact candidate {COMPACT_PATH.name} size={fmt_bytes(COMPACT_PATH.stat().st_size)}")
                COMPACT_PATH.unlink()
            except Exception as cleanup_exc:
                log(f"failed to remove compact candidate: {cleanup_exc!r}")
        remove_sidecars(COMPACT_PATH)
    finally:
        _stage["name"] = "start_container"
        if stopped_container:
            try:
                start_container()
                time.sleep(10)
                curl_admin_login()
            except Exception as exc:
                log(f"ERROR while restarting container: {exc!r}")
        else:
            log("container was not stopped by this job; skip restart")
        _stop_monitor.set()
        monitor.join(timeout=2)
        log("scheduled SQLite compaction job finished")
        notify_ops_bot_result(exit_code)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
