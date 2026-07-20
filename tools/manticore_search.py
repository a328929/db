#!/usr/bin/env python3
# ruff: noqa: E402
from __future__ import annotations

import argparse
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tg_harvest.config import CFG
from tg_harvest.search.manticore_client import ManticoreClient
from tg_harvest.search.manticore_sync import (
    build_manticore_document,
    drain_manticore_outbox,
    validate_manticore_state,
)
from tg_harvest.storage.connection import connect_db
from tg_harvest.storage.manticore_outbox import (
    OUTBOX_TABLE,
    configure_manticore_outbox_triggers,
    create_manticore_outbox_table,
    get_manticore_index_status,
    set_manticore_index_status,
)


def _client() -> ManticoreClient:
    return ManticoreClient(
        base_url=CFG.manticore_url,
        table=CFG.manticore_table,
        timeout_seconds=CFG.manticore_timeout_seconds,
        bearer_token=CFG.manticore_bearer_token,
    )


def _connect_db() -> sqlite3.Connection:
    conn, _features = connect_db(
        CFG.db_name,
        cache_mb=CFG.sqlite_cache_mb,
        mmap_mb=CFG.sqlite_mmap_mb,
    )
    return conn


def _enable_outbox(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    try:
        create_manticore_outbox_table(cur, " STRICT")
        configure_manticore_outbox_triggers(cur, enabled=True)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def _load_rebuild_batch(
    conn: sqlite3.Connection, *, after_pk: int, batch_size: int
) -> list[sqlite3.Row]:
    cur = conn.cursor()
    try:
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
            WHERE m.pk > ?
            ORDER BY m.pk ASC
            LIMIT ?
            """,
            (max(0, int(after_pk)), max(1, int(batch_size))),
        )
        return cur.fetchall()
    finally:
        cur.close()


def _rebuild(client: ManticoreClient, *, reset: bool, batch_size: int) -> None:
    with closing(_connect_db()) as conn:
        _enable_outbox(conn)
        set_manticore_index_status(conn, client.table, "building")
        client.ensure_table()
        if reset:
            client.truncate_table()
        last_pk = 0
        indexed = 0
        scanned = 0
        while True:
            rows = _load_rebuild_batch(
                conn, after_pk=last_pk, batch_size=batch_size
            )
            if not rows:
                break
            operations = []
            for row in rows:
                last_pk = int(row["message_pk"])
                scanned += 1
                document = build_manticore_document(row)
                if document is None:
                    continue
                operations.append(client.replace_operation(last_pk, document))
            client.bulk(operations)
            indexed += len(operations)
            print(
                f"scanned={scanned} indexed={indexed} last_pk={last_pk}",
                flush=True,
            )

        while drain_manticore_outbox(conn, client, batch_size=batch_size) > 0:
            pass
    if not validate_manticore_state(_connect_db, client):
        raise RuntimeError(
            "重建完成后 SQLite 与 Manticore 数量仍不一致，索引保持 stale"
        )
    print(f"rebuild complete: scanned={scanned} indexed={indexed}")


def _status(client: ManticoreClient) -> None:
    validate_manticore_state(_connect_db, client)
    table_status = client.table_status()
    manticore_count = client.document_count()
    with closing(_connect_db()) as conn:
        cur = conn.cursor()
        try:
            sqlite_count = int(
                cur.execute(
                    "SELECT COUNT(*) FROM messages"
                ).fetchone()[0]
            )
            outbox_count = int(
                cur.execute(f"SELECT COUNT(*) FROM {OUTBOX_TABLE}").fetchone()[0]
            )
            index_status = get_manticore_index_status(conn, client.table)
        finally:
            cur.close()
    print(f"sqlite_searchable={sqlite_count}")
    print(f"manticore_documents={manticore_count}")
    print(f"outbox_pending={outbox_count}")
    print(f"index_status={index_status or 'not_built'}")
    print(f"manticore_disk_bytes={table_status.get('disk_bytes', '0')}")
    print(f"manticore_ram_bytes={table_status.get('ram_bytes', '0')}")


def _repair_empty_documents(client: ManticoreClient, *, batch_size: int) -> None:
    with closing(_connect_db()) as conn:
        _enable_outbox(conn)
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT m.pk AS message_pk,
                       COALESCE(NULLIF(m.content_norm, ''), m.content, '') AS content,
                       m.chat_id, m.message_id, m.msg_date_ts, m.msg_type,
                       m.is_promo, COALESCE(mm.file_size, 0) AS file_size,
                       COALESCE(mm.duration_sec, 0) AS duration_sec
                FROM messages m
                LEFT JOIN message_media mm
                  ON mm.chat_id = m.chat_id AND mm.message_id = m.message_id
                WHERE NULLIF(
                    COALESCE(NULLIF(TRIM(m.content_norm), ''), TRIM(m.content), '')
                , '') IS NULL
                ORDER BY m.pk
                """
            )
            rows = cur.fetchall()
        finally:
            cur.close()
        operations = [
            client.replace_operation(int(row["message_pk"]), document)
            for row in rows
            if (document := build_manticore_document(row)) is not None
        ]
        for start in range(0, len(operations), max(1, int(batch_size))):
            client.bulk(operations[start : start + max(1, int(batch_size))])
        while drain_manticore_outbox(conn, client, batch_size=batch_size) > 0:
            pass
    print(f"empty-document repair complete: indexed={len(operations)}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Manage the Manticore search index")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init", help="Create the configured Manticore table")
    rebuild = subparsers.add_parser("rebuild", help="Backfill Manticore from SQLite")
    rebuild.add_argument("--reset", action="store_true", help="Truncate the table first")
    rebuild.add_argument("--batch-size", type=int, default=5000)
    subparsers.add_parser("status", help="Show SQLite/Manticore synchronization status")
    repair = subparsers.add_parser("repair-empty", help="Index messages with empty search text")
    repair.add_argument("--batch-size", type=int, default=5000)
    subparsers.add_parser("optimize", help="Merge Manticore disk chunks")
    args = parser.parse_args()

    client = _client()
    if args.command == "init":
        client.ensure_table()
    elif args.command == "rebuild":
        _rebuild(client, reset=bool(args.reset), batch_size=max(1, args.batch_size))
    elif args.command == "status":
        _status(client)
    elif args.command == "repair-empty":
        _repair_empty_documents(client, batch_size=max(1, args.batch_size))
    elif args.command == "optimize":
        client.optimize_table()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
