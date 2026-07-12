#!/usr/bin/env python3
import argparse
import shutil
import sqlite3
import sys
import time
from pathlib import Path
from typing import NoReturn
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_DEFAULT_COPY_BATCH_SIZE = 50000
_DB_SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")
_SEARCH_TERM_INSERT_FLUSH_SIZE = 100000
_SNAPSHOT_BUILD_HEADROOM_BYTES = 64 * 1024 * 1024

_TABLES_IN_COPY_ORDER = (
    "chats",
    "messages",
    "message_media",
    "media_groups",
    "dedupe_runs",
    "dedupe_actions",
    "message_search_terms_meta",
    "admin_jobs",
    "admin_job_logs",
    "admin_missing_chats",
    "admin_restricted_chats",
    "admin_recovery_chats",
    "admin_clone_runs",
    "admin_clone_plans",
    "admin_clone_migrations",
    "admin_clone_message_map",
    "account_runtime_state",
    "sync_chat_state",
    "sync_pending_updates",
    "sync_learning_events",
    "sync_model_state",
)

_COUNT_VERIFIED_COPY_TABLES = tuple(
    table for table in _TABLES_IN_COPY_ORDER if table != "message_search_terms_meta"
)


def _format_duration(seconds: float) -> str:
    seconds = max(0, float(seconds))
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, sec = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{sec:02d}s"
    return f"{minutes}m{sec:02d}s"


def _format_rate(rate: float, unit: str) -> str:
    rate = max(0.0, float(rate))
    if rate >= 1_000_000:
        return f"{rate / 1_000_000:.2f}M {unit}/s"
    if rate >= 1_000:
        return f"{rate / 1_000:.2f}K {unit}/s"
    return f"{rate:.1f} {unit}/s"


class Progress:
    def __init__(self, label: str, total: int, *, unit: str = "rows") -> None:
        self.label = label
        self.total = max(0, int(total or 0))
        self.unit = unit
        self.started_at = time.monotonic()
        self.last_print_at = 0.0

    def _line(self, current: int) -> str:
        current = max(0, int(current or 0))
        elapsed = max(0.001, time.monotonic() - self.started_at)
        rate = current / elapsed
        if self.total > 0:
            percent = min(100.0, current * 100.0 / self.total)
            remaining = max(0, self.total - current)
            eta = _format_duration(remaining / rate) if rate > 0 else "unknown"
            return (
                f"{self.label}: {current}/{self.total} {self.unit} "
                f"({percent:.1f}%) { _format_rate(rate, self.unit) } "
                f"elapsed={_format_duration(elapsed)} eta={eta}"
            )
        return (
            f"{self.label}: {current} {self.unit} "
            f"{ _format_rate(rate, self.unit) } elapsed={_format_duration(elapsed)}"
        )

    def update(self, current: int, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and now - self.last_print_at < 1.0:
            return
        self.last_print_at = now
        print("\r" + self._line(current), end="", flush=True)

    def done(self, current: int | None = None) -> None:
        self.update(self.total if current is None else current, force=True)
        print(flush=True)


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _sqlite_uri(path: Path, *, mode: str = "rw") -> str:
    return f"file:{quote(str(path))}?mode={mode}"


def _sidecar_paths(path: Path) -> list[Path]:
    return [Path(str(path) + suffix) for suffix in _DB_SIDECAR_SUFFIXES]


def _db_files(path: Path) -> tuple[Path, ...]:
    return (path, *_sidecar_paths(path))


def _remove_db_files(path: Path) -> None:
    for candidate in _db_files(path):
        if candidate.exists():
            candidate.unlink()


def _any_db_file_exists(path: Path) -> bool:
    return any(candidate.exists() for candidate in _db_files(path))


def _same_path(left: Path, right: Path) -> bool:
    try:
        return left.samefile(right)
    except FileNotFoundError:
        return left == right


def _work_path_for_target(target: Path) -> Path:
    return target.with_name(f"{target.name}.building")


def _source_snapshot_path_for_work_target(work_target: Path) -> Path:
    return work_target.with_name(f"{work_target.name}.source-snapshot")


def _secure_sqlite_artifacts(path: Path) -> None:
    from tg_harvest.runtime.paths import secure_sqlite_artifacts

    secure_sqlite_artifacts(path)


def _table_exists(cur: sqlite3.Cursor, schema: str, table: str) -> bool:
    cur.execute(
        f"SELECT 1 FROM {_quote_ident(schema)}.sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return cur.fetchone() is not None


def _table_columns(cur: sqlite3.Cursor, schema: str, table: str) -> list[str]:
    cur.execute(f"PRAGMA {_quote_ident(schema)}.table_xinfo({_quote_ident(table)})")
    rows = cur.fetchall()
    return [str(row[1]) for row in rows if int(row[6] or 0) == 0]


def _source_expr(table: str, column: str) -> str:
    quoted = _quote_ident(column)
    if table == "messages" and column == "content_norm":
        return "CASE WHEN content_norm = content THEN '' ELSE content_norm END"
    if table == "media_groups" and column == "caption_norm":
        return "CASE WHEN caption_norm = captions_concat THEN '' ELSE caption_norm END"
    return quoted


def _flush_search_term_inserts(
    cur: sqlite3.Cursor, inserts: list[tuple[str, int]]
) -> None:
    if not inserts:
        return
    cur.executemany(
        "INSERT OR IGNORE INTO message_search_terms(term, pk) VALUES (?, ?)",
        inserts,
    )
    inserts.clear()


def _copy_table(conn: sqlite3.Connection, table: str, *, batch_size: int) -> None:
    cur = conn.cursor()
    try:
        if not _table_exists(cur, "src", table) or not _table_exists(cur, "main", table):
            return
        src_cols = set(_table_columns(cur, "src", table))
        dst_cols = _table_columns(cur, "main", table)
        common_cols = [col for col in dst_cols if col in src_cols]
        if not common_cols:
            return
        columns_sql = ", ".join(_quote_ident(col) for col in common_cols)
        select_sql = ", ".join(_source_expr(table, col) for col in common_cols)
        cur.execute(f"SELECT COUNT(*) FROM src.{_quote_ident(table)}")
        total = int(cur.fetchone()[0] or 0)
        cur.execute(f"DELETE FROM main.{_quote_ident(table)}")
        conn.commit()
        progress = Progress(f"copy {table}", total)
        copied = 0
        last_rowid = 0
        while copied < total:
            cur.execute(
                f"""
                SELECT rowid
                FROM src.{_quote_ident(table)}
                WHERE rowid > ?
                ORDER BY rowid
                LIMIT ?
                """,
                (last_rowid, batch_size),
            )
            rowid_rows = cur.fetchall()
            if not rowid_rows:
                break
            next_last_rowid = int(rowid_rows[-1][0])
            cur.execute(
                f"""
                INSERT INTO main.{_quote_ident(table)}({columns_sql})
                SELECT {select_sql}
                FROM src.{_quote_ident(table)}
                WHERE rowid > ? AND rowid <= ?
                """,
                (last_rowid, next_last_rowid),
            )
            conn.commit()
            copied += len(rowid_rows)
            last_rowid = next_last_rowid
            progress.update(copied)
        progress.done(copied)
    finally:
        cur.close()


def _drop_search_sync_triggers(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    try:
        for trigger_name in (
            "trg_messages_fts_insert",
            "trg_messages_fts_delete",
            "trg_messages_fts_update",
            "trg_message_terms_queue_insert",
            "trg_message_terms_queue_update",
            "trg_message_terms_delete",
        ):
            cur.execute(f"DROP TRIGGER IF EXISTS main.{trigger_name}")
        conn.commit()
    finally:
        cur.close()


def _rebuild_search_terms(conn: sqlite3.Connection, *, batch_size: int) -> None:
    from tg_harvest.storage.search_terms import (
        _create_message_search_terms_queue_triggers,
        _finish_message_search_terms_rebuild,
        extract_cjk_search_terms,
    )
    from tg_harvest.storage.search_text_state import search_text_expression

    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM message_search_terms")
        cur.execute("SELECT COUNT(*) FROM messages")
        total = int(cur.fetchone()[0] or 0)
        progress = Progress("rebuild message_search_terms", total)
        last_pk = 0
        processed = 0
        while True:
            cur.execute(
                f"""
                SELECT pk, {search_text_expression()} AS search_text
                FROM messages
                WHERE pk > ?
                ORDER BY pk ASC
                LIMIT ?
                """,
                (last_pk, batch_size),
            )
            rows = cur.fetchall()
            if not rows:
                break

            inserts: list[tuple[str, int]] = []
            for row in rows:
                pk = int(row["pk"])
                last_pk = pk
                for token in extract_cjk_search_terms(str(row["search_text"] or "")):
                    inserts.append((token, pk))
                    if len(inserts) >= _SEARCH_TERM_INSERT_FLUSH_SIZE:
                        _flush_search_term_inserts(cur, inserts)
            _flush_search_term_inserts(cur, inserts)
            processed += len(rows)
            conn.commit()
            progress.update(processed)

        _finish_message_search_terms_rebuild(cur)
        conn.commit()
        progress.done(processed)
    finally:
        cur.close()
    conn.execute("DELETE FROM message_search_terms_rebuild_queue")
    cur = conn.cursor()
    try:
        _create_message_search_terms_queue_triggers(cur)
        conn.commit()
    finally:
        cur.close()


def _rebuild_fts(conn: sqlite3.Connection, *, batch_size: int) -> None:
    from tg_harvest.storage.fts import (
        _create_fts_table,
        _create_fts_triggers,
        _drop_fts_triggers,
        _write_fts_index_status,
    )
    from tg_harvest.storage.search_text_state import search_text_expression

    cur = conn.cursor()
    try:
        _drop_fts_triggers(cur)
        cur.execute("DROP TABLE IF EXISTS messages_fts")
        _create_fts_table(cur)
        _write_fts_index_status(cur, ready=False)
        cur.execute("SELECT COUNT(*) FROM messages")
        total = int(cur.fetchone()[0] or 0)
        progress = Progress("rebuild messages_fts", total)
        last_pk = 0
        processed = 0
        while True:
            cur.execute(
                """
                SELECT pk
                FROM messages
                WHERE pk > ?
                ORDER BY pk ASC
                LIMIT ?
                """,
                (last_pk, batch_size),
            )
            pk_rows = cur.fetchall()
            if not pk_rows:
                break
            next_last_pk = int(pk_rows[-1]["pk"])
            cur.execute(
                f"""
                INSERT INTO messages_fts(rowid, content)
                SELECT pk, {search_text_expression()}
                FROM messages
                WHERE pk > ? AND pk <= ?
                ORDER BY pk ASC
                """,
                (last_pk, next_last_pk),
            )
            conn.commit()
            processed += len(pk_rows)
            last_pk = next_last_pk
            progress.update(processed)

        _write_fts_index_status(cur, ready=True)
        _create_fts_triggers(cur)
        conn.commit()
        progress.done(processed)
    finally:
        cur.close()


def _fetch_count(cur: sqlite3.Cursor, schema: str, table: str) -> int:
    if not _table_exists(cur, schema, table):
        return 0
    cur.execute(f"SELECT COUNT(*) FROM {_quote_ident(schema)}.{_quote_ident(table)}")
    return int(cur.fetchone()[0] or 0)


def _fetch_meta_value(cur: sqlite3.Cursor, key: str) -> str:
    if not _table_exists(cur, "main", "message_search_terms_meta"):
        return ""
    cur.execute(
        """
        SELECT value
        FROM message_search_terms_meta
        WHERE key = ?
        LIMIT 1
        """,
        (key,),
    )
    row = cur.fetchone()
    if row is None:
        return ""
    return str(row["value"] if isinstance(row, sqlite3.Row) else row[0] or "")


def _ensure_integrity_check_ok(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    try:
        print("integrity_check: running")
        cur.execute("PRAGMA main.integrity_check")
        results = [str(row[0]) for row in cur.fetchall()]
    finally:
        cur.close()

    if results != ["ok"]:
        sample = "; ".join(results[:5])
        raise RuntimeError(f"integrity_check failed: {sample}")
    print("integrity_check: ok")


def _ensure_foreign_key_check_ok(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    try:
        print("foreign_key_check: running")
        cur.execute("PRAGMA main.foreign_key_check")
        failures = cur.fetchmany(10)
    finally:
        cur.close()

    if failures:
        sample = "; ".join(str(tuple(row)) for row in failures[:5])
        raise RuntimeError(f"foreign_key_check failed: {sample}")
    print("foreign_key_check: ok")


def _verify_counts(conn: sqlite3.Connection, *, strict: bool) -> None:
    from tg_harvest.storage.search_terms import message_search_terms_are_current

    cur = conn.cursor()
    failures: list[str] = []
    try:
        print("verification:")
        for table in _COUNT_VERIFIED_COPY_TABLES:
            src_count = _fetch_count(cur, "src", table)
            dst_count = _fetch_count(cur, "main", table)
            status = "ok" if src_count == dst_count else "mismatch"
            print(f"  {table}: src={src_count} dst={dst_count} {status}")
            if status != "ok":
                failures.append(f"{table}: src={src_count} dst={dst_count}")

        messages_count = _fetch_count(cur, "main", "messages")
        fts_docsize_count = _fetch_count(cur, "main", "messages_fts_docsize")
        fts_status = "ok" if messages_count == fts_docsize_count else "mismatch"
        print(
            "  messages_fts_docsize: "
            f"messages={messages_count} fts_docsize={fts_docsize_count} {fts_status}"
        )
        if fts_status != "ok":
            failures.append(
                "messages_fts_docsize: "
                f"messages={messages_count} fts_docsize={fts_docsize_count}"
            )

        cjk_version = _fetch_meta_value(cur, "cjk_terms_version")
        cjk_status = "ok" if message_search_terms_are_current(conn) else "mismatch"
        print(f"  cjk_terms_version: value={cjk_version or '<missing>'} {cjk_status}")
        if cjk_status != "ok":
            failures.append(f"cjk_terms_version={cjk_version or '<missing>'}")

        fts_ready = _fetch_meta_value(cur, "fts_index_status")
        fts_ready_status = "ok" if fts_ready == "ready" else "mismatch"
        print(f"  fts_index_status: value={fts_ready or '<missing>'} {fts_ready_status}")
        if fts_ready_status != "ok":
            failures.append(f"fts_index_status={fts_ready or '<missing>'}")
    finally:
        cur.close()

    if failures and strict:
        raise RuntimeError("verification failed: " + "; ".join(failures))


def _open_target(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(_sqlite_uri(path, mode="rwc"), uri=True)
    try:
        _secure_sqlite_artifacts(path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("PRAGMA main.journal_mode=OFF")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA temp_store=MEMORY")
        _secure_sqlite_artifacts(path)
        return conn
    except Exception:
        conn.close()
        raise


def _create_source_snapshot(source: Path, snapshot: Path) -> None:
    """Create a stable, read-only source image without modifying the source."""
    snapshot.parent.mkdir(parents=True, exist_ok=True)
    source_conn = sqlite3.connect(
        _sqlite_uri(source, mode="ro"),
        uri=True,
        timeout=300,
    )
    snapshot_conn = None
    try:
        snapshot_conn = sqlite3.connect(
            _sqlite_uri(snapshot, mode="rwc"),
            uri=True,
            timeout=300,
        )
        _secure_sqlite_artifacts(snapshot)
        source_conn.backup(snapshot_conn)
        snapshot_conn.commit()
        _secure_sqlite_artifacts(snapshot)
    finally:
        if snapshot_conn is not None:
            snapshot_conn.close()
        source_conn.close()


def _finalize_target_pragmas(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA optimize")
    conn.commit()
    # Keep the generated artifact transport-friendly; the app will switch it
    # back to WAL on normal startup.
    conn.execute("PRAGMA main.journal_mode=DELETE")
    conn.commit()


def _available_bytes(path: Path) -> int:
    target = path if path.exists() else path.parent
    while not target.exists() and target != target.parent:
        target = target.parent
    usage = shutil.disk_usage(target)
    return int(usage.free)


def _source_artifact_bytes(source: Path) -> int:
    total = 0
    for artifact in _db_files(source):
        try:
            total += max(0, int(artifact.stat().st_size))
        except FileNotFoundError:
            continue
    return total


def _required_free_bytes(source: Path, *, min_free_gb: float) -> int:
    configured_minimum = max(0, int(float(min_free_gb) * 1024 * 1024 * 1024))
    # The source remains untouched while the target filesystem holds both a
    # consistent backup image and the rebuilt compact database.
    snapshot_and_build = (
        _source_artifact_bytes(source) * 2 + _SNAPSHOT_BUILD_HEADROOM_BYTES
    )
    return max(configured_minimum, snapshot_and_build)


def _promote_work_target(work_target: Path, target: Path, *, force: bool) -> None:
    if force:
        _remove_db_files(target)
    work_target.replace(target)
    for suffix in _DB_SIDECAR_SUFFIXES:
        source_sidecar = Path(str(work_target) + suffix)
        if source_sidecar.exists():
            source_sidecar.replace(Path(str(target) + suffix))
    _secure_sqlite_artifacts(target)


def _fail(message: str) -> NoReturn:
    raise SystemExit(message)


def main() -> int:
    from tg_harvest.config import CFG
    from tg_harvest.storage.connection import detect_sqlite_features
    from tg_harvest.storage.schema import create_schema

    parser = argparse.ArgumentParser(
        description=(
            "Create a compact replacement SQLite database from a consistent "
            "source snapshot without modifying the source."
        )
    )
    parser.add_argument("--source", default=str(CFG.db_name), help="Source SQLite database")
    parser.add_argument("--target", required=True, help="Target compact SQLite database")
    parser.add_argument(
        "--min-free-gb",
        type=float,
        default=25.0,
        help=(
            "Required free space on the target filesystem before starting; "
            "allow room for both the temporary source snapshot and target"
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow overwriting an existing target database",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=_DEFAULT_COPY_BATCH_SIZE,
        help="Rows per copy/rebuild batch; lower this on memory-constrained devices",
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Skip post-build count and integrity verification",
    )
    args = parser.parse_args()

    source = Path(args.source).expanduser().resolve()
    target = Path(args.target).expanduser().resolve()
    work_target = _work_path_for_target(target)
    source_snapshot = _source_snapshot_path_for_work_target(work_target)
    batch_size = max(1, int(args.batch_size))
    if not source.exists():
        _fail(f"source database does not exist: {source}")
    if _same_path(source, target):
        _fail("target must be different from source database")
    if _same_path(source, work_target):
        _fail("temporary target path conflicts with source database")
    if _same_path(source, source_snapshot):
        _fail("temporary source snapshot path conflicts with source database")
    if target == work_target:
        _fail("target path conflicts with temporary build path")
    if _any_db_file_exists(target) and not args.force:
        _fail(f"target already exists; pass --force to replace: {target}")
    if _any_db_file_exists(work_target) and not args.force:
        _fail(f"temporary target already exists; pass --force to replace: {work_target}")
    if _any_db_file_exists(source_snapshot) and not args.force:
        _fail(
            "temporary source snapshot already exists; "
            f"pass --force to replace: {source_snapshot}"
        )

    if args.force:
        _remove_db_files(work_target)
        _remove_db_files(source_snapshot)

    free_bytes = _available_bytes(target)
    required_bytes = _required_free_bytes(source, min_free_gb=args.min_free_gb)
    if free_bytes < required_bytes:
        _fail(
            f"target filesystem free space is too low: {free_bytes} bytes, "
            f"required at least {required_bytes} bytes for the source snapshot "
            "and compact build"
        )

    conn = None
    try:
        print(f"creating consistent source snapshot: {source_snapshot}")
        _create_source_snapshot(source, source_snapshot)
        conn = _open_target(work_target)
        create_schema(
            conn,
            detect_sqlite_features(conn),
            skip_fts_auto_heal=1,
        )
        _drop_search_sync_triggers(conn)
        conn.execute(
            "ATTACH DATABASE ? AS src",
            (_sqlite_uri(source_snapshot, mode="ro"),),
        )
        for table in _TABLES_IN_COPY_ORDER:
            _copy_table(conn, table, batch_size=batch_size)
        conn.execute("DETACH DATABASE src")
        _rebuild_search_terms(conn, batch_size=batch_size)
        _rebuild_fts(conn, batch_size=batch_size)
        _finalize_target_pragmas(conn)
        if not args.no_verify:
            conn.execute(
                "ATTACH DATABASE ? AS src",
                (_sqlite_uri(source_snapshot, mode="ro"),),
            )
            _verify_counts(conn, strict=True)
            conn.execute("DETACH DATABASE src")
            _ensure_foreign_key_check_ok(conn)
            _ensure_integrity_check_ok(conn)
        _secure_sqlite_artifacts(work_target)
        conn.close()
        conn = None
        _promote_work_target(work_target, target, force=args.force)
    finally:
        try:
            if conn is not None:
                conn.close()
        finally:
            try:
                _secure_sqlite_artifacts(work_target)
            finally:
                _remove_db_files(source_snapshot)

    print(f"compact database ready: {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
