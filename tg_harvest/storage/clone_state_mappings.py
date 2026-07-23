import logging
import secrets
import sqlite3
from collections.abc import Iterable
from typing import Any

from tg_harvest.storage.clone_common import _clean_text, _now_iso, _optional_int
from tg_harvest.storage.clone_state_common import (
    _build_clone_message_mapping_filters,
    _clone_message_mapping_from_row,
    _clone_message_mapping_summary_from_row,
    _commit_and_load_required,
    _normalize_offset,
    _query_all,
    _query_count,
    _query_one,
)
from tg_harvest.storage.row_access import row_int as _row_int

_MAX_TELEGRAM_RANDOM_ID = (1 << 63) - 1
_TARGET_MESSAGE_ID_BATCH_SIZE = 500


def _new_delivery_random_id() -> int:
    return secrets.randbelow(_MAX_TELEGRAM_RANDOM_ID) + 1


def _valid_delivery_random_id(value: Any) -> int | None:
    normalized = _optional_int(value)
    if normalized is None or normalized <= 0 or normalized > _MAX_TELEGRAM_RANDOM_ID:
        return None
    return normalized


def _normalized_target_message_ids(message_ids: Iterable[Any]) -> list[int]:
    normalized_ids: list[int] = []
    seen: set[int] = set()
    for value in message_ids:
        message_id = _optional_int(value)
        if message_id is None or message_id <= 0 or message_id in seen:
            continue
        seen.add(message_id)
        normalized_ids.append(message_id)
    return normalized_ids


def _message_id_chunks(message_ids: list[int]) -> Iterable[list[int]]:
    for offset in range(0, len(message_ids), _TARGET_MESSAGE_ID_BATCH_SIZE):
        yield message_ids[offset : offset + _TARGET_MESSAGE_ID_BATCH_SIZE]


def load_clone_tail_delete_selection(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    target_chat_id: int,
    source_message_limit: int,
) -> dict[str, Any]:
    """Resolve the last successfully cloned source messages to target IDs.

    Source message IDs define the rollback boundary. Target message IDs are
    only immutable handles for deleting the already mapped deliveries; target
    announcements and other unmapped posts never consume the requested limit.
    Results are newest-source-first so a partially completed delete still
    leaves the missing clone state as a suffix of the source timeline.
    """

    normalized_run_id = _clean_text(run_id)
    normalized_target_chat_id = int(target_chat_id)
    normalized_limit = int(source_message_limit)
    if not normalized_run_id:
        raise ValueError("run_id 不能为空")
    if normalized_target_chat_id <= 0:
        raise ValueError("target_chat_id 参数非法")
    if normalized_limit <= 0:
        raise ValueError("回滚源消息数量必须为正整数")

    cur = conn.cursor()
    try:
        cur.execute(
            """
            WITH source_tail AS (
                SELECT source_chat_id, source_message_id
                FROM admin_clone_message_map
                WHERE run_id = ?
                  AND target_chat_id = ?
                  AND status = 'done'
                  AND target_message_id IS NOT NULL
                GROUP BY source_chat_id, source_message_id
                ORDER BY source_message_id DESC
                LIMIT ?
            )
            SELECT
                mapping.source_chat_id,
                mapping.source_message_id,
                mapping.target_message_id,
                mapping.chunk_index,
                mapping.mode
            FROM admin_clone_message_map mapping
            JOIN source_tail tail
              ON tail.source_chat_id = mapping.source_chat_id
             AND tail.source_message_id = mapping.source_message_id
            WHERE mapping.run_id = ?
              AND mapping.target_chat_id = ?
              AND mapping.status = 'done'
              AND mapping.target_message_id IS NOT NULL
            ORDER BY
                mapping.source_message_id DESC,
                mapping.chunk_index DESC,
                mapping.id DESC
            """,
            (
                normalized_run_id,
                normalized_target_chat_id,
                normalized_limit,
                normalized_run_id,
                normalized_target_chat_id,
            ),
        )
        rows = cur.fetchall()
    finally:
        cur.close()

    source_message_ids: list[int] = []
    target_message_ids: list[int] = []
    seen_source_ids: set[int] = set()
    seen_target_ids: set[int] = set()
    for row in rows:
        source_message_id = int(row["source_message_id"])
        target_message_id = int(row["target_message_id"])
        if source_message_id not in seen_source_ids:
            seen_source_ids.add(source_message_id)
            source_message_ids.append(source_message_id)
        if target_message_id not in seen_target_ids:
            seen_target_ids.add(target_message_id)
            target_message_ids.append(target_message_id)

    return {
        "requested_source_message_count": normalized_limit,
        "selected_source_message_count": len(source_message_ids),
        "selected_target_message_count": len(target_message_ids),
        "first_source_message_id": min(source_message_ids, default=0),
        "last_source_message_id": max(source_message_ids, default=0),
        "source_message_ids": source_message_ids,
        "target_message_ids": target_message_ids,
    }


def rewind_clone_mappings_for_deleted_target_messages(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    target_chat_id: int,
    target_message_ids: Iterable[Any],
) -> dict[str, int]:
    """Make successfully deleted target messages eligible for replay again.

    Telegram does not renumber messages after a deletion.  Replay state must
    therefore be rewound through the durable source-to-target mappings, never
    by treating a gap in target message IDs as a migration cursor.  Media
    transfer intents are reset together with their mappings so the next replay
    reserves fresh MTProto random IDs and actually sends replacement media
    instead of reusing an already deleted target message ID.  A relay item
    whose temporary relay message has not been cleaned up keeps that first hop
    so a retry can forward it with a new target-side random ID and then clean
    it up.
    """

    normalized_run_id = _clean_text(run_id)
    normalized_target_chat_id = int(target_chat_id)
    normalized_target_ids = _normalized_target_message_ids(target_message_ids)
    empty_result = {
        "selected_target_message_count": len(normalized_target_ids),
        "rewound_mapping_count": 0,
        "rewound_done_mapping_count": 0,
        "rewound_text_mapping_count": 0,
        "rewound_media_mapping_count": 0,
        "rewound_media_transfer_count": 0,
        "unmapped_target_message_count": len(normalized_target_ids),
        "first_rewound_source_message_id": 0,
    }
    if not normalized_run_id:
        raise ValueError("run_id 不能为空")
    if normalized_target_chat_id <= 0:
        raise ValueError("target_chat_id 参数非法")
    if not normalized_target_ids:
        return empty_result

    rewound_rows: list[sqlite3.Row] = []
    matched_target_ids: set[int] = set()
    transfers_to_rewind: dict[int, sqlite3.Row] = {}
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        for target_id_chunk in _message_id_chunks(normalized_target_ids):
            placeholders = ",".join("?" for _ in target_id_chunk)
            cur.execute(
                f"""
                SELECT
                    source_chat_id,
                    source_message_id,
                    target_message_id,
                    mode,
                    status
                FROM admin_clone_message_map
                WHERE run_id = ?
                  AND target_chat_id = ?
                  AND target_message_id IN ({placeholders})
                """,
                [
                    normalized_run_id,
                    normalized_target_chat_id,
                    *target_id_chunk,
                ],
            )
            rows = cur.fetchall()
            rewound_rows.extend(rows)
            matched_target_ids.update(
                int(row["target_message_id"])
                for row in rows
                if _optional_int(row["target_message_id"]) is not None
            )
            cur.execute(
                f"""
                DELETE FROM admin_clone_message_map
                WHERE run_id = ?
                  AND target_chat_id = ?
                  AND target_message_id IN ({placeholders})
                """,
                [
                    normalized_run_id,
                    normalized_target_chat_id,
                    *target_id_chunk,
                ],
            )
            cur.execute(
                f"""
                SELECT
                    id,
                    transfer_strategy,
                    source_hop_status,
                    cleanup_status,
                    relay_message_id
                FROM admin_clone_media_transfers
                WHERE run_id = ?
                  AND target_chat_id = ?
                  AND target_message_id IN ({placeholders})
                """,
                [
                    normalized_run_id,
                    normalized_target_chat_id,
                    *target_id_chunk,
                ],
            )
            for transfer in cur.fetchall():
                transfers_to_rewind[int(transfer["id"])] = transfer

        media_sources_by_chat: dict[int, set[int]] = {}
        for row in rewound_rows:
            if str(row["mode"] or "") not in {
                "media_copy",
                "media_group_copy",
            }:
                continue
            source_chat_id = int(row["source_chat_id"])
            source_message_id = int(row["source_message_id"])
            media_sources_by_chat.setdefault(source_chat_id, set()).add(
                source_message_id
            )

        rewound_media_transfer_count = 0
        now = _now_iso()
        for source_chat_id, source_message_ids in media_sources_by_chat.items():
            for source_id_chunk in _message_id_chunks(sorted(source_message_ids)):
                placeholders = ",".join("?" for _ in source_id_chunk)
                cur.execute(
                    f"""
                    SELECT
                        id,
                        transfer_strategy,
                        source_hop_status,
                        cleanup_status,
                        relay_message_id
                    FROM admin_clone_media_transfers
                    WHERE run_id = ?
                      AND target_chat_id = ?
                      AND source_chat_id = ?
                      AND source_message_id IN ({placeholders})
                    """,
                    [
                        normalized_run_id,
                        normalized_target_chat_id,
                        source_chat_id,
                        *source_id_chunk,
                    ],
                )
                for transfer in cur.fetchall():
                    transfers_to_rewind[int(transfer["id"])] = transfer

        rewound_media_transfer_count = 0
        for transfer in transfers_to_rewind.values():
            should_reuse_relay_message = (
                str(transfer["transfer_strategy"] or "") == "relay"
                and str(transfer["source_hop_status"] or "") == "sent"
                and _optional_int(transfer["relay_message_id"]) is not None
                and str(transfer["cleanup_status"] or "") != "done"
            )
            if should_reuse_relay_message:
                cur.execute(
                    """
                    UPDATE admin_clone_media_transfers
                    SET target_random_id = ?, target_message_id = NULL,
                        target_hop_status = 'pending', error_message = '',
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (_new_delivery_random_id(), now, int(transfer["id"])),
                )
            else:
                cur.execute(
                    "DELETE FROM admin_clone_media_transfers WHERE id = ?",
                    (int(transfer["id"]),),
                )
            rewound_media_transfer_count += 1

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()

    done_rows = [row for row in rewound_rows if row["status"] == "done"]
    text_count = sum(
        1 for row in rewound_rows if str(row["mode"] or "") == "text_replay"
    )
    media_count = sum(
        1
        for row in rewound_rows
        if str(row["mode"] or "") in {"media_copy", "media_group_copy"}
    )
    source_message_ids = [
        int(row["source_message_id"])
        for row in done_rows
        if int(row["source_message_id"] or 0) > 0
    ]
    return {
        "selected_target_message_count": len(normalized_target_ids),
        "rewound_mapping_count": len(rewound_rows),
        "rewound_done_mapping_count": len(done_rows),
        "rewound_text_mapping_count": text_count,
        "rewound_media_mapping_count": media_count,
        "rewound_media_transfer_count": rewound_media_transfer_count,
        "unmapped_target_message_count": max(
            0,
            len(normalized_target_ids) - len(matched_target_ids),
        ),
        "first_rewound_source_message_id": min(source_message_ids, default=0),
    }


def record_clone_message_mapping(
    conn: sqlite3.Connection,
    *,
    migration_id: str,
    run_id: str,
    plan_id: str = "",
    source_chat_id: int,
    source_message_id: int,
    source_msg_date_ts: Any = None,
    source_msg_date_text: Any = None,
    target_chat_id: int,
    target_message_id: Any = None,
    chunk_index: int = 0,
    chunk_count: int = 1,
    mode: str = "text_replay",
    status: str = "done",
    error_message: Any = None,
    sent_at: Any = None,
    delivery_random_id: Any = None,
    delivery_account: Any = None,
) -> dict:
    now = _now_iso()
    normalized_run_id = _clean_text(run_id)
    normalized_mode = _clean_text(mode) or "text_replay"
    normalized_sent_at = _clean_text(sent_at) if sent_at is not None else now
    normalized_delivery_account = _clean_text(delivery_account).lower()
    cur = conn.cursor()
    try:
        normalized_delivery_random_id = _valid_delivery_random_id(delivery_random_id)
        cur.execute(
            """
            INSERT INTO admin_clone_message_map(
                migration_id,
                run_id,
                plan_id,
                source_chat_id,
                source_message_id,
                source_msg_date_ts,
                source_msg_date_text,
                target_chat_id,
                target_message_id,
                delivery_random_id,
                delivery_account,
                chunk_index,
                chunk_count,
                mode,
                status,
                error_message,
                sent_at,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(
                run_id,
                source_chat_id,
                source_message_id,
                chunk_index,
                mode
            ) DO UPDATE SET
                migration_id = excluded.migration_id,
                plan_id = excluded.plan_id,
                source_msg_date_ts = excluded.source_msg_date_ts,
                source_msg_date_text = excluded.source_msg_date_text,
                target_chat_id = excluded.target_chat_id,
                target_message_id = excluded.target_message_id,
                delivery_random_id = COALESCE(
                    excluded.delivery_random_id,
                    admin_clone_message_map.delivery_random_id
                ),
                delivery_account = CASE
                    WHEN excluded.delivery_account <> '' THEN excluded.delivery_account
                    ELSE admin_clone_message_map.delivery_account
                END,
                chunk_count = excluded.chunk_count,
                status = excluded.status,
                error_message = excluded.error_message,
                sent_at = excluded.sent_at,
                updated_at = excluded.updated_at
            """,
            (
                _clean_text(migration_id),
                normalized_run_id,
                _clean_text(plan_id),
                int(source_chat_id),
                int(source_message_id),
                _optional_int(source_msg_date_ts),
                _clean_text(source_msg_date_text),
                int(target_chat_id),
                _optional_int(target_message_id),
                normalized_delivery_random_id,
                normalized_delivery_account,
                int(chunk_index),
                int(chunk_count),
                normalized_mode,
                _clean_text(status) or "done",
                _clean_text(error_message) if error_message is not None else "",
                normalized_sent_at,
                now,
                now,
            ),
        )
        return _commit_and_load_required(
            conn,
            load_fn=lambda: load_clone_message_mapping(
                conn,
                run_id=normalized_run_id,
                source_chat_id=int(source_chat_id),
                source_message_id=int(source_message_id),
                chunk_index=int(chunk_index),
                mode=normalized_mode,
            ),
            missing_message="clone message mapping 写入后读取失败",
        )
    except sqlite3.Error:
        try:
            conn.rollback()
        except sqlite3.Error:
            logging.exception(
                "克隆消息映射写入失败后的回滚也失败: run_id=%s source=%s/%s",
                normalized_run_id,
                source_chat_id,
                source_message_id,
            )
        logging.exception(
            "克隆消息映射写入失败，迁移不得确认成功: run_id=%s source=%s/%s mode=%s",
            normalized_run_id,
            source_chat_id,
            source_message_id,
            normalized_mode,
        )
        raise
    except Exception:
        try:
            conn.rollback()
        except sqlite3.Error:
            logging.exception(
                "克隆消息映射未知错误后的回滚也失败: run_id=%s source=%s/%s",
                normalized_run_id,
                source_chat_id,
                source_message_id,
            )
        logging.exception(
            "克隆消息映射写入发生未知错误，迁移不得确认成功: run_id=%s source=%s/%s mode=%s",
            normalized_run_id,
            source_chat_id,
            source_message_id,
            normalized_mode,
        )
        raise
    finally:
        cur.close()


def ensure_clone_text_delivery(
    conn: sqlite3.Connection,
    *,
    migration_id: str,
    run_id: str,
    plan_id: str,
    source_chat_id: int,
    source_message_id: int,
    source_msg_date_ts: Any = None,
    source_msg_date_text: Any = None,
    target_chat_id: int,
    target_account: str,
    chunk_index: int,
    chunk_count: int,
) -> dict:
    """Durably reserve a text delivery before sending it to Telegram.

    The stored MTProto random ID is reused after a timeout or process restart,
    preventing a successful but not-yet-mapped text message from being sent
    twice.
    """
    normalized_run_id = _clean_text(run_id)
    normalized_source_chat_id = int(source_chat_id)
    normalized_source_message_id = int(source_message_id)
    normalized_chunk_index = int(chunk_index)
    normalized_target_account = _clean_text(target_account).lower()
    existing = load_clone_message_mapping(
        conn,
        run_id=normalized_run_id,
        source_chat_id=normalized_source_chat_id,
        source_message_id=normalized_source_message_id,
        chunk_index=normalized_chunk_index,
        mode="text_replay",
    )
    if existing is not None and existing.get("status") == "done":
        return existing
    if existing is not None and int(existing.get("target_chat_id") or 0) not in (
        0,
        int(target_chat_id),
    ):
        raise RuntimeError("已存在文本交付记录指向不同目标，拒绝重复迁移")
    if (
        existing is not None
        and existing.get("delivery_account")
        and normalized_target_account
        and existing["delivery_account"] != normalized_target_account
    ):
        if not (
            existing.get("status") == "error"
            and existing.get("target_message_id") is None
        ):
            raise RuntimeError("已存在文本交付记录绑定不同目标侧账号，拒绝跨账号恢复")
        reset_failed_delivery = True
    else:
        reset_failed_delivery = False

    delivery_random_id = _valid_delivery_random_id(
        existing.get("delivery_random_id")
        if existing is not None and not reset_failed_delivery
        else None
    ) or _new_delivery_random_id()
    now = _now_iso()
    cur = conn.cursor()
    try:
        if reset_failed_delivery:
            cur.execute(
                """
                DELETE FROM admin_clone_message_map
                WHERE run_id = ?
                  AND source_chat_id = ?
                  AND source_message_id = ?
                  AND chunk_index = ?
                  AND mode = 'text_replay'
                """,
                (
                    normalized_run_id,
                    normalized_source_chat_id,
                    normalized_source_message_id,
                    normalized_chunk_index,
                ),
            )
        cur.execute(
            """
            INSERT INTO admin_clone_message_map(
                migration_id,
                run_id,
                plan_id,
                source_chat_id,
                source_message_id,
                source_msg_date_ts,
                source_msg_date_text,
                target_chat_id,
                target_message_id,
                delivery_random_id,
                delivery_account,
                chunk_index,
                chunk_count,
                mode,
                status,
                error_message,
                sent_at,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, 'text_replay', 'pending', '', NULL, ?, ?)
            ON CONFLICT(
                run_id,
                source_chat_id,
                source_message_id,
                chunk_index,
                mode
            ) DO UPDATE SET
                migration_id = excluded.migration_id,
                plan_id = excluded.plan_id,
                source_msg_date_ts = excluded.source_msg_date_ts,
                source_msg_date_text = excluded.source_msg_date_text,
                target_chat_id = excluded.target_chat_id,
                delivery_random_id = excluded.delivery_random_id,
                delivery_account = CASE
                    WHEN excluded.delivery_account <> '' THEN excluded.delivery_account
                    ELSE admin_clone_message_map.delivery_account
                END,
                chunk_count = excluded.chunk_count,
                status = 'pending',
                error_message = '',
                updated_at = excluded.updated_at
            """,
            (
                _clean_text(migration_id),
                normalized_run_id,
                _clean_text(plan_id),
                normalized_source_chat_id,
                normalized_source_message_id,
                _optional_int(source_msg_date_ts),
                _clean_text(source_msg_date_text),
                int(target_chat_id),
                delivery_random_id,
                normalized_target_account,
                normalized_chunk_index,
                int(chunk_count),
                now,
                now,
            ),
        )
        return _commit_and_load_required(
            conn,
            load_fn=lambda: load_clone_message_mapping(
                conn,
                run_id=normalized_run_id,
                source_chat_id=normalized_source_chat_id,
                source_message_id=normalized_source_message_id,
                chunk_index=normalized_chunk_index,
                mode="text_replay",
            ),
            missing_message="clone text delivery 写入后读取失败",
        )
    except Exception:
        try:
            conn.rollback()
        except sqlite3.Error:
            logging.exception(
                "克隆文本交付意图写入失败后的回滚也失败: run_id=%s source=%s/%s chunk=%s",
                normalized_run_id,
                normalized_source_chat_id,
                normalized_source_message_id,
                normalized_chunk_index,
            )
        raise
    finally:
        cur.close()


def load_clone_message_mapping(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    source_chat_id: int,
    source_message_id: int,
    chunk_index: int = 0,
    mode: str = "text_replay",
) -> dict | None:
    return _query_one(
        conn,
        """
        SELECT *
        FROM admin_clone_message_map
        WHERE run_id = ?
          AND source_chat_id = ?
          AND source_message_id = ?
          AND chunk_index = ?
          AND mode = ?
        LIMIT 1
        """,
        (
            _clean_text(run_id),
            int(source_chat_id),
            int(source_message_id),
            int(chunk_index),
            _clean_text(mode) or "text_replay",
        ),
        _clone_message_mapping_from_row,
    )


def load_clone_message_mapping_summary(
    conn: sqlite3.Connection,
    run_id: str,
) -> dict:
    normalized_run_id = _clean_text(run_id)
    return _query_one(
        conn,
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) AS done,
            SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error,
            SUM(CASE WHEN mode = 'text_replay' THEN 1 ELSE 0 END) AS text_total,
            SUM(CASE WHEN mode = 'text_replay' AND status = 'done' THEN 1 ELSE 0 END) AS text_done,
            SUM(CASE WHEN mode = 'text_replay' AND status = 'error' THEN 1 ELSE 0 END) AS text_error,
            SUM(CASE WHEN mode = 'media_copy' THEN 1 ELSE 0 END) AS media_total,
            SUM(CASE WHEN mode = 'media_copy' AND status = 'done' THEN 1 ELSE 0 END) AS media_done,
            SUM(CASE WHEN mode = 'media_copy' AND status = 'error' THEN 1 ELSE 0 END) AS media_error,
            SUM(CASE WHEN mode = 'media_group_copy' THEN 1 ELSE 0 END) AS media_group_total,
            SUM(CASE WHEN mode = 'media_group_copy' AND status = 'done' THEN 1 ELSE 0 END) AS media_group_done,
            SUM(CASE WHEN mode = 'media_group_copy' AND status = 'error' THEN 1 ELSE 0 END) AS media_group_error,
            MAX(sent_at) AS latest_sent_at,
            MAX(updated_at) AS latest_updated_at
        FROM admin_clone_message_map
        WHERE run_id = ?
        """,
        (normalized_run_id,),
        _clone_message_mapping_summary_from_row,
    )


def load_clone_run_progress(
    conn: sqlite3.Connection,
    run_id: str,
) -> dict[str, Any]:
    """Load the latest verified whole-group snapshot and deduplicated progress.

    Migration records retain the complete source snapshot calculated before an
    execution starts.  Message mappings are durable per-source-message records
    across all executions of a clone run.  Keeping the two together here makes
    the group-level progress independent from the counters of the latest task.
    """

    normalized_run_id = _clean_text(run_id)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                migration_id,
                text_total,
                media_total,
                media_group_total,
                updated_at
            FROM admin_clone_migrations
            WHERE run_id = ?
              AND mode = 'timeline_replay'
              AND (
                    text_total > 0
                 OR media_total > 0
                 OR media_group_total > 0
              )
            ORDER BY updated_at DESC, created_at DESC, migration_id DESC
            LIMIT 1
            """,
            (normalized_run_id,),
        )
        snapshot = cur.fetchone()

        cur.execute(
            """
            WITH mapping_state AS (
                SELECT
                    CASE WHEN mode = 'text_replay' THEN 'text' ELSE 'media' END
                        AS mapping_kind,
                    source_chat_id,
                    source_message_id,
                    CASE
                        WHEN MAX(CASE WHEN mode = 'text_replay' THEN 1 ELSE 0 END) = 1
                        THEN CASE
                            WHEN SUM(CASE WHEN status = 'done'
                                THEN 1 ELSE 0 END) >= MAX(
                                    CASE WHEN chunk_count > 0 THEN chunk_count ELSE 1 END
                                )
                            THEN 1 ELSE 0
                        END
                        WHEN MAX(CASE WHEN status = 'done' THEN 1 ELSE 0 END) = 1
                        THEN 1 ELSE 0
                    END AS is_done,
                    MAX(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS has_error,
                    MAX(CASE WHEN mode = 'media_group_copy' AND status = 'done'
                        THEN 1 ELSE 0 END) AS media_group_item_done
                FROM admin_clone_message_map
                WHERE run_id = ?
                  AND mode IN ('text_replay', 'media_copy', 'media_group_copy')
                GROUP BY
                    CASE WHEN mode = 'text_replay' THEN 'text' ELSE 'media' END,
                    source_chat_id,
                    source_message_id
            )
            SELECT
                SUM(CASE WHEN mapping_kind = 'text' AND is_done = 1
                    THEN 1 ELSE 0 END) AS text_done,
                SUM(CASE WHEN mapping_kind = 'text' AND is_done = 0
                              AND has_error = 1
                    THEN 1 ELSE 0 END) AS text_error,
                SUM(CASE WHEN mapping_kind = 'media' AND is_done = 1
                    THEN 1 ELSE 0 END) AS media_done,
                SUM(CASE WHEN mapping_kind = 'media' AND is_done = 0
                              AND has_error = 1
                    THEN 1 ELSE 0 END) AS media_error,
                SUM(CASE WHEN mapping_kind = 'media' AND is_done = 1
                              AND media_group_item_done = 1
                    THEN 1 ELSE 0 END) AS media_group_items_done
            FROM mapping_state
            """,
            (normalized_run_id,),
        )
        mapped = cur.fetchone()
    finally:
        cur.close()

    text_done = _row_int(mapped, "text_done")
    text_error = _row_int(mapped, "text_error")
    media_done = _row_int(mapped, "media_done")
    media_error = _row_int(mapped, "media_error")
    media_group_items_done = _row_int(mapped, "media_group_items_done")

    if snapshot is None:
        return {
            "assessment_state": "unverified",
            "snapshot_migration_id": "",
            "verified_at": "",
            "messages_total": 0,
            "messages_done": text_done + media_done,
            "messages_error": text_error + media_error,
            "messages_remaining": 0,
            "text_total": 0,
            "text_done": text_done,
            "text_error": text_error,
            "text_remaining": 0,
            "media_total": 0,
            "media_done": media_done,
            "media_error": media_error,
            "media_remaining": 0,
            "media_group_total": 0,
            "media_group_items_done": media_group_items_done,
        }

    text_total = _row_int(snapshot, "text_total")
    media_total = _row_int(snapshot, "media_total")
    message_total = text_total + media_total
    completed = min(message_total, text_done + media_done)
    text_completed = min(text_total, text_done)
    media_completed = min(media_total, media_done)
    return {
        "assessment_state": "verified",
        "snapshot_migration_id": str(snapshot["migration_id"] or ""),
        "verified_at": str(snapshot["updated_at"] or ""),
        "messages_total": message_total,
        "messages_done": completed,
        "messages_error": text_error + media_error,
        "messages_remaining": max(0, message_total - completed),
        "text_total": text_total,
        "text_done": text_completed,
        "text_error": text_error,
        "text_remaining": max(0, text_total - text_completed),
        "media_total": media_total,
        "media_done": media_completed,
        "media_error": media_error,
        "media_remaining": max(0, media_total - media_completed),
        "media_group_total": _row_int(snapshot, "media_group_total"),
        "media_group_items_done": media_group_items_done,
    }


def list_clone_message_mappings(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    status: Any = "",
    mode: Any = "",
    limit: Any = 100,
    offset: Any = 0,
) -> list[dict]:
    from tg_harvest.storage.clone_common import _normalize_bounded_int

    normalized_limit = _normalize_bounded_int(
        limit,
        default=100,
        minimum=1,
        maximum=1000,
    )
    normalized_offset = _normalize_offset(offset)
    where_sql, params = _build_clone_message_mapping_filters(
        run_id=run_id,
        status=status,
        mode=mode,
    )
    params.extend([normalized_limit, normalized_offset])
    return _query_all(
        conn,
        f"""
        SELECT *
        FROM admin_clone_message_map
        WHERE {where_sql}
        ORDER BY updated_at DESC, created_at DESC, id DESC
        LIMIT ? OFFSET ?
        """,
        params,
        _clone_message_mapping_from_row,
    )


def count_clone_message_mappings(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    status: Any = "",
    mode: Any = "",
) -> int:
    where_sql, params = _build_clone_message_mapping_filters(
        run_id=run_id,
        status=status,
        mode=mode,
    )
    return _query_count(
        conn,
        f"""
        SELECT COUNT(*) AS c
        FROM admin_clone_message_map
        WHERE {where_sql}
        """,
        params,
    )
