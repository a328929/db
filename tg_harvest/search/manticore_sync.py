from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
import time
from collections.abc import Callable, Iterable
from contextlib import closing
from dataclasses import dataclass
from typing import Any

from tg_harvest.search.manticore_client import (
    MANTICORE_STORED_DOCUMENT_FIELDS,
    ManticoreClient,
)
from tg_harvest.storage.manticore_outbox import (
    OUTBOX_TABLE,
    manticore_index_is_ready,
    record_manticore_validation,
    set_manticore_index_status,
)

_SYNC_LOCK = threading.Lock()
_SYNC_EVENT = threading.Event()
_SYNC_THREAD: threading.Thread | None = None
_SYNC_GET_CONN_FN: Callable[[], sqlite3.Connection] | None = None
_SYNC_CLIENT: ManticoreClient | None = None
_SYNC_BATCH_SIZE = 1000
_SYNC_IDLE_SECONDS = 2.0
_SYNC_ERROR_BACKOFF_SECONDS = 30.0
_SYNC_RECOVERY_VALIDATION_MIN_INTERVAL_SECONDS = 60.0
_SYNC_RECOVERY_VALIDATION_MAX_INTERVAL_SECONDS = 600.0
_SYNC_VALIDATION_INTERVAL_SECONDS = 600.0
_VALIDATION_SAMPLE_SIDE_SIZE = 32


@dataclass(frozen=True)
class OutboxItem:
    pk: int
    operation: str
    revision: int
    document: dict[str, Any] | None


def _message_type_code(value: Any) -> int:
    message_type = str(value or "").upper()
    if message_type == "TEXT":
        return 1
    if message_type == "PHOTO":
        return 2
    if message_type in {"VIDEO", "GIF", "VIDEO_NOTE"}:
        return 3
    if message_type in {"AUDIO", "VOICE"}:
        return 4
    return 0


def build_manticore_document(row: Any) -> dict[str, Any] | None:
    content = str(row["content"] or "") if row["message_pk"] is not None else ""
    if row["message_pk"] is None:
        return None
    return {
        "content": content,
        "chat_id": int(row["chat_id"]),
        "message_id": int(row["message_id"]),
        "msg_date_ts": int(row["msg_date_ts"]),
        "type_code": _message_type_code(row["msg_type"]),
        "file_size": int(row["file_size"] or 0),
        "duration_sec": int(row["duration_sec"] or 0),
        "is_promo": int(row["is_promo"] or 0),
    }


def _load_validation_sample(
    conn: sqlite3.Connection,
    *,
    side_size: int = _VALIDATION_SAMPLE_SIDE_SIZE,
) -> dict[int, dict[str, Any]]:
    """Load a bounded first/last sample using Manticore-readable attributes."""
    rows_by_pk: dict[int, Any] = {}
    cur = conn.cursor()
    try:
        for order in ("ASC", "DESC"):
            cur.execute(
                """
                SELECT
                    m.pk AS message_pk,
                    COALESCE(NULLIF(m.content_norm, ''), m.content, '') AS content,
                    m.chat_id,
                    m.message_id,
                    m.msg_date_ts,
                    m.msg_type,
                    m.is_promo,
                    COALESCE(mm.file_size, 0) AS file_size,
                    COALESCE(mm.duration_sec, 0) AS duration_sec
                FROM messages m
                LEFT JOIN message_media mm
                  ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id
                ORDER BY m.pk """
                + order
                + " LIMIT ?",
                (max(1, int(side_size)),),
            )
            for row in cur.fetchall():
                rows_by_pk[int(row["message_pk"])] = row
    finally:
        cur.close()

    documents: dict[int, dict[str, Any]] = {}
    for pk, row in rows_by_pk.items():
        document = build_manticore_document(row)
        if document is not None:
            documents[pk] = _validation_projection(document)
    return documents


def _document_digest(documents: dict[int, dict[str, Any]]) -> str:
    digest = hashlib.blake2b(digest_size=16)
    for document_id in sorted(documents):
        payload = json.dumps(
            {"id": int(document_id), "document": documents[document_id]},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8", "surrogatepass")
        digest.update(payload)
        digest.update(b"\n")
    return digest.hexdigest()


def _validation_projection(document: Any) -> dict[str, int]:
    if not isinstance(document, dict):
        raise ValueError("Manticore sample document is not an object")
    return {
        field: int(document.get(field) or 0)
        for field in MANTICORE_STORED_DOCUMENT_FIELDS
    }


def _load_outbox_batch(
    conn: sqlite3.Connection, *, batch_size: int
) -> list[OutboxItem]:
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT
                o.pk,
                o.operation,
                o.revision,
                m.pk AS message_pk,
                COALESCE(NULLIF(m.content_norm, ''), m.content, '') AS content,
                m.chat_id,
                m.message_id,
                m.msg_date_ts,
                m.msg_type,
                m.is_promo,
                COALESCE(mm.file_size, 0) AS file_size,
                COALESCE(mm.duration_sec, 0) AS duration_sec
            FROM {OUTBOX_TABLE} o
            LEFT JOIN messages m ON m.pk = o.pk
            LEFT JOIN message_media mm
              ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id
            ORDER BY o.queued_at ASC, o.pk ASC
            LIMIT ?
            """,
            (max(1, int(batch_size)),),
        )
        items: list[OutboxItem] = []
        for row in cur.fetchall():
            pk = int(row["pk"])
            revision = int(row["revision"])
            document = build_manticore_document(row)
            if row["operation"] == "delete" or document is None:
                items.append(OutboxItem(pk, "delete", revision, None))
                continue
            items.append(
                OutboxItem(
                    pk,
                    "upsert",
                    revision,
                    document,
                )
            )
        return items
    finally:
        cur.close()


def _build_bulk_operations(
    client: ManticoreClient, items: Iterable[OutboxItem]
) -> list[dict[str, Any]]:
    operations: list[dict[str, Any]] = []
    for item in items:
        if item.operation == "delete" or item.document is None:
            operations.append(client.delete_operation(item.pk))
        else:
            operations.append(client.replace_operation(item.pk, item.document))
    return operations


def _ack_outbox_items(conn: sqlite3.Connection, items: list[OutboxItem]) -> None:
    cur = conn.cursor()
    try:
        cur.executemany(
            f"DELETE FROM {OUTBOX_TABLE} WHERE pk = ? AND revision = ?",
            [(item.pk, item.revision) for item in items],
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def _record_outbox_failure(
    conn: sqlite3.Connection,
    items: list[OutboxItem],
    error: Exception,
    *,
    table: str,
) -> None:
    message = str(error).strip()[:500]
    cur = conn.cursor()
    try:
        cur.executemany(
            f"""
            UPDATE {OUTBOX_TABLE}
            SET attempts = attempts + 1, last_error = ?
            WHERE pk = ? AND revision = ?
            """,
            [(message, item.pk, item.revision) for item in items],
        )
        # This commit also persists the retry metadata above, keeping the
        # failure record and readiness transition atomic.
        set_manticore_index_status(conn, table, "stale")
    except Exception:
        conn.rollback()
        logging.exception("记录 Manticore 同步失败状态时发生数据库错误")
    finally:
        cur.close()


def drain_manticore_outbox(
    conn: sqlite3.Connection,
    client: ManticoreClient,
    *,
    batch_size: int = 1000,
) -> int:
    items = _load_outbox_batch(conn, batch_size=batch_size)
    if not items:
        return 0
    try:
        client.bulk(_build_bulk_operations(client, items))
    except Exception as exc:
        _record_outbox_failure(conn, items, exc, table=client.table)
        raise
    _ack_outbox_items(conn, items)
    return len(items)


def validate_manticore_state(
    get_conn_fn: Callable[[], sqlite3.Connection], client: ManticoreClient
) -> bool:
    """Compare the SQLite source and Manticore index without rebuilding data."""
    sqlite_count: int | None = None
    manticore_count: int | None = None
    outbox_pending: int | None = None
    source_sample_digest = ""
    manticore_sample_digest = ""
    sample_consistent = True
    validation_error = ""
    try:
        client.ensure_table()
        with closing(get_conn_fn()) as conn:
            sqlite_count = int(conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0] or 0)
            outbox_pending = int(
                conn.execute(f"SELECT COUNT(*) FROM {OUTBOX_TABLE}").fetchone()[0] or 0
            )
            manticore_count = client.document_count()
            source_documents = _load_validation_sample(conn)
            source_sample_digest = _document_digest(source_documents)
            sample_documents_fn = getattr(client, "sample_documents", None)
            if callable(sample_documents_fn):
                raw_index_documents = sample_documents_fn(list(source_documents))
                index_documents = {
                    int(document_id): _validation_projection(document)
                    for document_id, document in raw_index_documents.items()
                }
                manticore_sample_digest = _document_digest(index_documents)
                sample_consistent = source_sample_digest == manticore_sample_digest
                if not sample_consistent:
                    validation_error = "Manticore consistency sample mismatch"
            ready = (
                sqlite_count == manticore_count
                and outbox_pending == 0
                and sample_consistent
            )
            record_manticore_validation(
                conn,
                table=client.table,
                status="ready" if ready else "stale",
                sqlite_count=sqlite_count,
                manticore_count=manticore_count,
                outbox_pending=outbox_pending,
                error=validation_error,
                source_sample_digest=source_sample_digest,
                manticore_sample_digest=manticore_sample_digest,
            )
            return ready
    except Exception as exc:
        logging.exception("Manticore 状态校验失败")
        try:
            with closing(get_conn_fn()) as conn:
                record_manticore_validation(
                    conn,
                    table=client.table,
                    status="stale",
                    sqlite_count=sqlite_count,
                    manticore_count=manticore_count,
                    outbox_pending=outbox_pending,
                    error=str(exc),
                    source_sample_digest=source_sample_digest,
                    manticore_sample_digest=manticore_sample_digest,
                )
        except Exception:
            logging.exception("记录 Manticore 状态校验失败信息时发生数据库错误")
        return False


def configure_manticore_sync(
    get_conn_fn: Callable[[], sqlite3.Connection],
    client: ManticoreClient,
    *,
    batch_size: int = 1000,
    idle_seconds: float = 2.0,
    validation_interval_seconds: float = 600.0,
) -> None:
    global _SYNC_BATCH_SIZE, _SYNC_CLIENT, _SYNC_GET_CONN_FN
    global _SYNC_IDLE_SECONDS, _SYNC_THREAD, _SYNC_VALIDATION_INTERVAL_SECONDS

    with _SYNC_LOCK:
        _SYNC_GET_CONN_FN = get_conn_fn
        _SYNC_CLIENT = client
        _SYNC_BATCH_SIZE = max(1, int(batch_size))
        _SYNC_IDLE_SECONDS = max(0.2, float(idle_seconds))
        _SYNC_VALIDATION_INTERVAL_SECONDS = max(
            60.0, float(validation_interval_seconds)
        )
        if _SYNC_THREAD is not None and _SYNC_THREAD.is_alive():
            _SYNC_EVENT.set()
            return
        _SYNC_THREAD = threading.Thread(
            target=_manticore_sync_worker,
            name="manticore-search-sync",
            daemon=True,
        )
        _SYNC_THREAD.start()
        _SYNC_EVENT.set()


def schedule_manticore_sync() -> None:
    _SYNC_EVENT.set()


def _manticore_validation_is_due(
    *,
    now: float,
    last_validated_at: float,
    interval_seconds: float,
    index_ready: bool,
    recovery_interval_seconds: float = (
        _SYNC_RECOVERY_VALIDATION_MIN_INTERVAL_SECONDS
    ),
) -> bool:
    elapsed = now - last_validated_at
    if not index_ready:
        recovery_due_after = min(
            _SYNC_RECOVERY_VALIDATION_MAX_INTERVAL_SECONDS,
            max(
                _SYNC_RECOVERY_VALIDATION_MIN_INTERVAL_SECONDS,
                float(recovery_interval_seconds),
            ),
        )
        return elapsed >= recovery_due_after
    return elapsed >= interval_seconds


def _next_recovery_validation_interval(
    current_interval_seconds: float, *, validation_ready: bool
) -> float:
    if validation_ready:
        return _SYNC_RECOVERY_VALIDATION_MIN_INTERVAL_SECONDS
    return min(
        _SYNC_RECOVERY_VALIDATION_MAX_INTERVAL_SECONDS,
        max(
            _SYNC_RECOVERY_VALIDATION_MIN_INTERVAL_SECONDS,
            float(current_interval_seconds) * 2,
        ),
    )


def _manticore_sync_worker() -> None:
    table_ready = False
    # Application startup performs a bounded consistency validation before this
    # worker is configured. Normal outbox traffic must not turn that audit into
    # a per-batch operation.
    last_validated_at = time.monotonic()
    recovery_validation_interval = (
        _SYNC_RECOVERY_VALIDATION_MIN_INTERVAL_SECONDS
    )
    while True:
        with _SYNC_LOCK:
            get_conn_fn = _SYNC_GET_CONN_FN
            client = _SYNC_CLIENT
            batch_size = _SYNC_BATCH_SIZE
            idle_seconds = _SYNC_IDLE_SECONDS
            validation_interval_seconds = _SYNC_VALIDATION_INTERVAL_SECONDS
        _SYNC_EVENT.wait(timeout=idle_seconds)
        _SYNC_EVENT.clear()
        if get_conn_fn is None or client is None:
            continue

        try:
            if not table_ready:
                client.ensure_table()
                table_ready = True
            index_ready = False
            while True:
                with closing(get_conn_fn()) as conn:
                    drained = drain_manticore_outbox(
                        conn, client, batch_size=batch_size
                    )
                    if drained <= 0:
                        index_ready = manticore_index_is_ready(conn, client.table)
                if drained <= 0:
                    break
                time.sleep(0.02)
            now = time.monotonic()
            if _manticore_validation_is_due(
                now=now,
                last_validated_at=last_validated_at,
                interval_seconds=validation_interval_seconds,
                index_ready=index_ready,
                recovery_interval_seconds=recovery_validation_interval,
            ):
                validation_ready = validate_manticore_state(get_conn_fn, client)
                last_validated_at = now
                if validation_ready:
                    recovery_validation_interval = _next_recovery_validation_interval(
                        recovery_validation_interval, validation_ready=True
                    )
                elif index_ready:
                    # A healthy index just failed its periodic audit. Give the
                    # first recovery attempt the advertised minimum delay.
                    recovery_validation_interval = (
                        _SYNC_RECOVERY_VALIDATION_MIN_INTERVAL_SECONDS
                    )
                else:
                    recovery_validation_interval = _next_recovery_validation_interval(
                        recovery_validation_interval, validation_ready=False
                    )
        except Exception:
            table_ready = False
            logging.exception("Manticore 搜索索引后台同步失败")
            time.sleep(_SYNC_ERROR_BACKOFF_SECONDS)
