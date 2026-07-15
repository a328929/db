import secrets
import sqlite3
from collections.abc import Iterable
from typing import Any

from tg_harvest.domain.clone_target_permissions import clone_target_write_was_rejected
from tg_harvest.storage.clone_common import _clean_text, _now_iso, _optional_int
from tg_harvest.storage.row_access import row_int as _row_int

CLONE_MEDIA_TRANSFER_DIRECT = "direct"
CLONE_MEDIA_TRANSFER_RELAY = "relay"
_VALID_TRANSFER_STRATEGIES = frozenset(
    {CLONE_MEDIA_TRANSFER_DIRECT, CLONE_MEDIA_TRANSFER_RELAY}
)
_MAX_TELEGRAM_RANDOM_ID = (1 << 63) - 1


def _normalized_message_ids(message_ids: Iterable[Any]) -> list[int]:
    result: list[int] = []
    seen: set[int] = set()
    for value in message_ids:
        message_id = _optional_int(value)
        if message_id is None or message_id <= 0 or message_id in seen:
            continue
        seen.add(message_id)
        result.append(message_id)
    return result


def _new_random_id() -> int:
    return secrets.randbelow(_MAX_TELEGRAM_RANDOM_ID) + 1


def _insert_clone_media_transfer(
    cur: sqlite3.Cursor,
    *,
    migration_id: str,
    run_id: str,
    plan_id: str,
    source_chat_id: int,
    source_message_id: int,
    target_chat_id: int,
    strategy: str,
    relay_chat_id: int | None,
    source_account: str,
    target_account: str,
    now: str,
) -> None:
    cur.execute(
        """
        INSERT INTO admin_clone_media_transfers(
            migration_id, run_id, plan_id, source_chat_id,
            source_message_id, target_chat_id, transfer_strategy,
            relay_chat_id, source_account, target_account,
            source_random_id, target_random_id, source_hop_status,
            target_hop_status, cleanup_status, error_message,
            created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, '', ?, ?)
        """,
        (
            migration_id,
            run_id,
            plan_id,
            source_chat_id,
            source_message_id,
            target_chat_id,
            strategy,
            relay_chat_id,
            source_account,
            target_account,
            _new_random_id() if strategy == CLONE_MEDIA_TRANSFER_RELAY else None,
            _new_random_id(),
            "pending" if strategy == CLONE_MEDIA_TRANSFER_RELAY else "not_required",
            "pending" if strategy == CLONE_MEDIA_TRANSFER_RELAY else "not_required",
            now,
            now,
        ),
    )


def _transfer_plan_changed(
    existing: dict,
    *,
    strategy: str,
    relay_chat_id: int | None,
    source_account: str,
    target_account: str,
) -> bool:
    if existing["transfer_strategy"] != strategy:
        return True
    if strategy == CLONE_MEDIA_TRANSFER_RELAY and existing["relay_chat_id"] not in (
        None,
        relay_chat_id,
    ):
        return True
    return bool(
        existing["source_account"]
        and source_account
        and existing["source_account"] != source_account
    ) or bool(
        existing["target_account"]
        and target_account
        and existing["target_account"] != target_account
    )


def _relay_first_hop_is_reusable(existing: dict) -> bool:
    return (
        existing["transfer_strategy"] == CLONE_MEDIA_TRANSFER_RELAY
        and existing["source_hop_status"] == "sent"
        and existing["relay_message_id"] is not None
    )


def _can_replan_unsent_transfer(existing: dict) -> bool:
    return (
        existing["target_hop_status"] != "sent"
        and not _relay_first_hop_is_reusable(existing)
        and clone_target_write_was_rejected(existing["error_message"])
    )


def _transfer_from_row(row: sqlite3.Row | None) -> dict | None:
    if row is None:
        return None
    return {
        "id": _row_int(row, "id"),
        "migration_id": str(row["migration_id"] or ""),
        "run_id": str(row["run_id"] or ""),
        "plan_id": str(row["plan_id"] or ""),
        "source_chat_id": _row_int(row, "source_chat_id"),
        "source_message_id": _row_int(row, "source_message_id"),
        "target_chat_id": _row_int(row, "target_chat_id"),
        "transfer_strategy": str(row["transfer_strategy"] or ""),
        "relay_chat_id": _optional_int(row["relay_chat_id"]),
        "source_account": str(row["source_account"] or ""),
        "target_account": str(row["target_account"] or ""),
        "source_random_id": _optional_int(row["source_random_id"]),
        "target_random_id": _row_int(row, "target_random_id"),
        "relay_message_id": _optional_int(row["relay_message_id"]),
        "target_message_id": _optional_int(row["target_message_id"]),
        "source_hop_status": str(row["source_hop_status"] or ""),
        "target_hop_status": str(row["target_hop_status"] or ""),
        "cleanup_status": str(row["cleanup_status"] or ""),
        "error_message": str(row["error_message"] or ""),
        "created_at": str(row["created_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
    }


def _load_transfers(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    source_chat_id: int,
    source_message_ids: Iterable[Any],
) -> list[dict]:
    message_ids = _normalized_message_ids(source_message_ids)
    if not message_ids:
        return []
    placeholders = ",".join("?" for _ in message_ids)
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT *
            FROM admin_clone_media_transfers
            WHERE run_id = ?
              AND source_chat_id = ?
              AND source_message_id IN ({placeholders})
            ORDER BY source_message_id ASC
            """,
            [_clean_text(run_id), int(source_chat_id), *message_ids],
        )
        return [
            item
            for item in (_transfer_from_row(row) for row in cur.fetchall())
            if item is not None
        ]
    finally:
        cur.close()


def ensure_clone_media_transfers(
    conn: sqlite3.Connection,
    *,
    migration_id: str,
    run_id: str,
    plan_id: str,
    source_chat_id: int,
    source_message_ids: Iterable[Any],
    target_chat_id: int,
    transfer_strategy: str,
    relay_chat_id: Any = None,
    source_account: str = "",
    target_account: str = "",
) -> list[dict]:
    """Create durable delivery intents and preserve their Telegram random IDs.

    A transfer row is created before any Telegram write. Reusing the stored
    random IDs makes a retry of a timed-out forward idempotent at MTProto level.
    A target-side permission rejection may be replanned after an online
    preflight selects a different writable account.  Other failed or pending
    transfers remain account-bound because the remote outcome may be
    ambiguous; a sent target hop is always account-bound.
    """
    normalized_ids = _normalized_message_ids(source_message_ids)
    if not normalized_ids:
        return []

    strategy = _clean_text(transfer_strategy).lower()
    if strategy not in _VALID_TRANSFER_STRATEGIES:
        raise ValueError("未知媒体传输策略")
    normalized_relay_chat_id = _optional_int(relay_chat_id)
    if strategy == CLONE_MEDIA_TRANSFER_RELAY and not normalized_relay_chat_id:
        raise ValueError("中转媒体传输缺少固定中转频道")

    normalized_run_id = _clean_text(run_id)
    normalized_migration_id = _clean_text(migration_id)
    normalized_plan_id = _clean_text(plan_id)
    normalized_source_account = _clean_text(source_account).lower()
    normalized_target_account = _clean_text(target_account).lower()
    now = _now_iso()
    existing_by_id = {
        int(item["source_message_id"]): item
        for item in _load_transfers(
            conn,
            run_id=normalized_run_id,
            source_chat_id=int(source_chat_id),
            source_message_ids=normalized_ids,
        )
    }

    cur = conn.cursor()
    try:
        for source_message_id in normalized_ids:
            existing = existing_by_id.get(source_message_id)
            if existing is None:
                _insert_clone_media_transfer(
                    cur,
                    migration_id=normalized_migration_id,
                    run_id=normalized_run_id,
                    plan_id=normalized_plan_id,
                    source_chat_id=int(source_chat_id),
                    source_message_id=source_message_id,
                    target_chat_id=int(target_chat_id),
                    strategy=strategy,
                    relay_chat_id=normalized_relay_chat_id,
                    source_account=normalized_source_account,
                    target_account=normalized_target_account,
                    now=now,
                )
                continue

            if int(existing["target_chat_id"]) != int(target_chat_id):
                raise RuntimeError("已存在媒体传输记录指向不同目标，拒绝重复迁移")
            plan_changed = _transfer_plan_changed(
                existing,
                strategy=strategy,
                relay_chat_id=normalized_relay_chat_id,
                source_account=normalized_source_account,
                target_account=normalized_target_account,
            )
            if plan_changed and _can_replan_unsent_transfer(existing):
                cur.execute(
                    "DELETE FROM admin_clone_media_transfers WHERE id = ?",
                    (int(existing["id"]),),
                )
                _insert_clone_media_transfer(
                    cur,
                    migration_id=normalized_migration_id,
                    run_id=normalized_run_id,
                    plan_id=normalized_plan_id,
                    source_chat_id=int(source_chat_id),
                    source_message_id=source_message_id,
                    target_chat_id=int(target_chat_id),
                    strategy=strategy,
                    relay_chat_id=normalized_relay_chat_id,
                    source_account=normalized_source_account,
                    target_account=normalized_target_account,
                    now=now,
                )
                continue

            if plan_changed and _relay_first_hop_is_reusable(existing):
                if not clone_target_write_was_rejected(existing["error_message"]):
                    raise RuntimeError(
                        "媒体传输状态尚未确认可安全重规划，拒绝跨账号恢复"
                    )
                if (
                    existing["transfer_strategy"] == strategy
                    and existing["source_account"] == normalized_source_account
                    and existing["relay_chat_id"] in (None, normalized_relay_chat_id)
                ):
                    cur.execute(
                        """
                        UPDATE admin_clone_media_transfers
                        SET migration_id = ?, plan_id = ?, target_account = ?,
                            target_random_id = ?, target_message_id = NULL,
                            target_hop_status = 'pending', error_message = '',
                            updated_at = ?
                        WHERE id = ?
                        """,
                        (
                            normalized_migration_id,
                            normalized_plan_id,
                            normalized_target_account,
                            _new_random_id(),
                            now,
                            int(existing["id"]),
                        ),
                    )
                    continue
                raise RuntimeError(
                    "中转第一跳已完成，不能切换源侧账号、传输策略或中转频道"
                )

            if plan_changed:
                raise RuntimeError("媒体传输状态尚未确认可安全重规划，拒绝跨账号恢复")

            cur.execute(
                """
                UPDATE admin_clone_media_transfers
                SET migration_id = ?, plan_id = ?, source_account = ?,
                    target_account = ?, updated_at = ?
                WHERE run_id = ? AND source_chat_id = ? AND source_message_id = ?
                """,
                (
                    normalized_migration_id,
                    normalized_plan_id,
                    normalized_source_account,
                    normalized_target_account,
                    now,
                    normalized_run_id,
                    int(source_chat_id),
                    source_message_id,
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()

    transfers = _load_transfers(
        conn,
        run_id=normalized_run_id,
        source_chat_id=int(source_chat_id),
        source_message_ids=normalized_ids,
    )
    by_id = {int(item["source_message_id"]): item for item in transfers}
    missing = [message_id for message_id in normalized_ids if message_id not in by_id]
    if missing:
        raise RuntimeError("媒体传输意图写入后读取不完整")
    return [by_id[message_id] for message_id in normalized_ids]


def mark_clone_media_transfer_source_hop_sent(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    source_chat_id: int,
    relay_message_ids_by_source: dict[int, int],
) -> None:
    if not relay_message_ids_by_source:
        return
    now = _now_iso()
    cur = conn.cursor()
    try:
        for source_message_id, relay_message_id in relay_message_ids_by_source.items():
            cur.execute(
                """
                UPDATE admin_clone_media_transfers
                SET relay_message_id = ?, source_hop_status = 'sent',
                    cleanup_status = 'pending', error_message = '', updated_at = ?
                WHERE run_id = ? AND source_chat_id = ? AND source_message_id = ?
                  AND transfer_strategy = 'relay'
                """,
                (
                    int(relay_message_id),
                    now,
                    _clean_text(run_id),
                    int(source_chat_id),
                    int(source_message_id),
                ),
            )
            if cur.rowcount != 1:
                raise RuntimeError("中转第一跳状态写入失败")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def mark_clone_media_transfer_target_hop_sent(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    source_chat_id: int,
    target_message_ids_by_source: dict[int, int],
) -> None:
    if not target_message_ids_by_source:
        return
    now = _now_iso()
    cur = conn.cursor()
    try:
        for (
            source_message_id,
            target_message_id,
        ) in target_message_ids_by_source.items():
            cur.execute(
                """
                UPDATE admin_clone_media_transfers
                SET target_message_id = ?, target_hop_status = 'sent',
                    error_message = '', updated_at = ?
                WHERE run_id = ? AND source_chat_id = ? AND source_message_id = ?
                """,
                (
                    int(target_message_id),
                    now,
                    _clean_text(run_id),
                    int(source_chat_id),
                    int(source_message_id),
                ),
            )
            if cur.rowcount != 1:
                raise RuntimeError("目标媒体状态写入失败")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def mark_clone_media_transfer_target_hop_observed(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    source_chat_id: int,
    target_message_ids_by_source: dict[int, int],
) -> None:
    """Persist Telegram's returned IDs before independently confirming delivery."""
    if not target_message_ids_by_source:
        return
    now = _now_iso()
    cur = conn.cursor()
    try:
        for source_message_id, target_message_id in target_message_ids_by_source.items():
            cur.execute(
                """
                UPDATE admin_clone_media_transfers
                SET target_message_id = ?, target_hop_status = 'unconfirmed',
                    error_message = '', updated_at = ?
                WHERE run_id = ? AND source_chat_id = ? AND source_message_id = ?
                  AND target_hop_status != 'sent'
                """,
                (
                    int(target_message_id),
                    now,
                    _clean_text(run_id),
                    int(source_chat_id),
                    int(source_message_id),
                ),
            )
            if cur.rowcount != 1:
                raise RuntimeError("目标媒体待确认状态写入失败")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()


def list_pending_clone_relay_cleanup(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    source_chat_id: int,
    relay_chat_id: int,
) -> list[dict]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT *
            FROM admin_clone_media_transfers
            WHERE run_id = ?
              AND source_chat_id = ?
              AND transfer_strategy = 'relay'
              AND relay_chat_id = ?
              AND target_hop_status = 'sent'
              AND cleanup_status != 'done'
              AND relay_message_id IS NOT NULL
            ORDER BY source_message_id ASC
            """,
            (
                _clean_text(run_id),
                int(source_chat_id),
                int(relay_chat_id),
            ),
        )
        return [
            item
            for item in (_transfer_from_row(row) for row in cur.fetchall())
            if item is not None
        ]
    finally:
        cur.close()


def mark_clone_media_transfer_cleanup_done(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    source_chat_id: int,
    source_message_ids: Iterable[Any],
) -> None:
    normalized_ids = _normalized_message_ids(source_message_ids)
    if not normalized_ids:
        return
    placeholders = ",".join("?" for _ in normalized_ids)
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            UPDATE admin_clone_media_transfers
            SET cleanup_status = 'done', error_message = '', updated_at = ?
            WHERE run_id = ?
              AND source_chat_id = ?
              AND source_message_id IN ({placeholders})
              AND transfer_strategy = 'relay'
              AND target_hop_status = 'sent'
            """,
            [_now_iso(), _clean_text(run_id), int(source_chat_id), *normalized_ids],
        )
        conn.commit()
    finally:
        cur.close()


def record_clone_media_transfer_error(
    conn: sqlite3.Connection,
    *,
    run_id: str,
    source_chat_id: int,
    source_message_ids: Iterable[Any],
    message: Any,
) -> None:
    normalized_ids = _normalized_message_ids(source_message_ids)
    if not normalized_ids:
        return
    placeholders = ",".join("?" for _ in normalized_ids)
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            UPDATE admin_clone_media_transfers
            SET error_message = ?, updated_at = ?
            WHERE run_id = ?
              AND source_chat_id = ?
              AND source_message_id IN ({placeholders})
            """,
            [
                _clean_text(message),
                _now_iso(),
                _clean_text(run_id),
                int(source_chat_id),
                *normalized_ids,
            ],
        )
        conn.commit()
    finally:
        cur.close()
