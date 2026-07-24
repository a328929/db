import logging
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from types import SimpleNamespace
from typing import Any

from tg_harvest.admin_jobs.common import (
    admin_error_message,
    call_with_conn,
    classify_chat_access_failure,
    finish_job_heartbeat,
    mark_admin_job_running,
    start_admin_job_heartbeat,
    start_admin_job_thread,
    update_admin_job_progress,
)
from tg_harvest.admin_jobs.runtime import _admin_now_iso
from tg_harvest.admin_jobs.sessions import (
    _cleanup_isolated_worker_session,
    _create_isolated_worker_client,
    _disconnect_worker_client,
    bind_client_event_loop,
)
from tg_harvest.domain.chat_inventory import (
    ChatInventoryRow,
    RestrictedChatInventoryRow,
    chat_identity_candidates,
    chat_identity_key,
    find_missing_joined_chats,
    find_restricted_joined_chats,
    load_joined_chat_inventory,
    load_known_chat_identities,
    restricted_chat_row_from_entity,
    unavailable_chat_risk_row,
)
from tg_harvest.ingest.flood_wait import call_with_bounded_retry, is_flood_wait_error
from tg_harvest.storage.channel_management import (
    list_database_channels,
    list_restricted_chat_scan_results,
    replace_missing_chat_scan_results,
    replace_restricted_chat_scan_results,
)
from tg_harvest.admin_jobs.inventory_constants import (
    PUBLIC_ENTITY_BATCH_SIZE,
    DEFAULT_PUBLIC_RESOLVE_LIMIT,
    DEFAULT_PUBLIC_RESOLVE_GAP_SECONDS,
    DEFAULT_FLOOD_WAIT_THRESHOLD,
)

def _scan_joined_restricted_chats(
    *,
    accounts: list[_ScanAccount],
    job_id: str,
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> tuple[dict[tuple[str, int], RestrictedChatInventoryRow], set[tuple[str, int]], list[tuple[_ScanAccount, Any, str]]]:
    """Phase 1: Scan joined chats for restriction reasons and risk flags.

    Args:
        accounts: List of scan account configurations (primary, secondary).
        job_id: Unique job identifier for worker session naming.
        admin_job_append_log_fn: Callback to log progress messages.

    Returns:
        Tuple of:
        - merged_rows: Dict of chat identity to RestrictedChatInventoryRow
        - joined_identities: Set of chat identities for joined chats
        - active_clients: List of (account, client, worker_id) tuples for cleanup

    Raises:
        RuntimeError: If no authorized sessions are available.
    """
    merged_rows: dict[tuple[str, int], RestrictedChatInventoryRow] = {}
    joined_identities: set[tuple[str, int]] = set()
    active_clients: list[tuple[_ScanAccount, Any, str]] = []

    for account in accounts:
        admin_job_append_log_fn(
            job_id,
            f"正在连接{account.label}并扫描已加入会话风险标记...",
        )
        worker_id = f"{job_id}_restricted_chats_{account.key}"
        client = _create_isolated_worker_client(account.cfg, worker_id)
        if not client.is_user_authorized():
            admin_job_append_log_fn(
                job_id,
                f"{account.label} Telegram 会话未登录，本轮扫描已跳过",
            )
            try:
                _disconnect_worker_client(client)
            except Exception as exc:
                logging.warning(
                    "清理未授权会话时断开连接失败: account=%s worker_id=%s error=%s",
                    account.key,
                    worker_id,
                    admin_error_message(exc),
                )
            finally:
                _cleanup_isolated_worker_session(account.cfg, worker_id)
            continue
        active_clients.append((account, client, worker_id))
        with bind_client_event_loop(client):
            dialogs = list(
                client.iter_dialogs(
                    limit=None,
                    archived=None,
                    ignore_migrated=True,
                )
            )
        for joined_row in load_joined_chat_inventory(dialogs, account.key):
            joined_identities.update(
                chat_identity_candidates(joined_row.chat_id, joined_row.chat_type)
            )
        account_rows = find_restricted_joined_chats(dialogs, account.key)
        admin_job_append_log_fn(
            job_id,
            f"{account.label}已加入会话发现 {len(account_rows)} 个风险候选",
        )
        for row in account_rows:
            key = chat_identity_key(row.chat_id, row.chat_type)
            merged_rows[key] = _merge_restricted_chat_row(
                merged_rows.get(key), row
            )

    if not active_clients:
        raise RuntimeError("没有可用的 Telegram 会话可执行扫描")

    return merged_rows, joined_identities, active_clients


def _batch_refresh_cached_public_entities(
    *,
    public_rows: list[dict],
    active_clients: list[tuple[_ScanAccount, Any, str]],
    merged_rows: dict[tuple[str, int], RestrictedChatInventoryRow],
    job_id: str,
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> set[int]:
    """Phase 2: Batch refresh cached public entities.

    Args:
        public_rows: Database rows for public channels not joined by any account.
        active_clients: List of (account, client, worker_id) tuples from Phase 1.
        merged_rows: Dict to accumulate restricted chat results (modified in-place).
        job_id: Unique job identifier.
        admin_job_append_log_fn: Callback to log progress messages.

    Returns:
        Set of chat IDs that were successfully probed (either refreshed or confirmed unavailable).

    Notes:
        - Uses session cache (get_input_entity) to identify which entities are already cached
        - Batch fetches cached entities (PUBLIC_ENTITY_BATCH_SIZE per call)
        - Stops processing an account if FloodWait error occurs
        - Does not raise on individual entity failures (continues with next account)
    """
    probed_public_chat_ids: set[int] = set()
    uncached_by_id = {int(row["chat_id"]): row for row in public_rows}

    for account, client, _worker_id in active_clients:
        cached_pairs: list[tuple[dict[str, Any], Any]] = []
        for row in public_rows:
            try:
                input_peer = client.session.get_input_entity(row["chat_username"])
            except ValueError:
                continue
            cached_pairs.append((row, input_peer))

        for offset in range(0, len(cached_pairs), PUBLIC_ENTITY_BATCH_SIZE):
            batch = cached_pairs[offset : offset + PUBLIC_ENTITY_BATCH_SIZE]
            try:
                resolved_pairs = _fetch_cached_entities(client, batch)
            except Exception as exc:
                if is_flood_wait_error(exc):
                    admin_job_append_log_fn(
                        job_id,
                        f"{account.label}批量刷新公开频道缓存触发频控，已切换保守模式",
                    )
                    break
                raise
            for row, entity in resolved_pairs:
                chat_id = int(row["chat_id"])
                probed_public_chat_ids.add(chat_id)
                uncached_by_id.pop(chat_id, None)
                _merge_resolved_public_entity(
                    merged_rows,
                    row=row,
                    entity=entity,
                )

    return probed_public_chat_ids


def _resolve_uncached_public_entities(
    *,
    uncached_by_id: dict[int, dict],
    active_clients: list[tuple[_ScanAccount, Any, str]],
    merged_rows: dict[tuple[str, int], RestrictedChatInventoryRow],
    cfg: Any,
    job_id: str,
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> tuple[set[int], int]:
    """Phase 3: Incrementally resolve uncached public entities.

    Args:
        uncached_by_id: Dict of chat_id to database row for entities not in cache.
        active_clients: List of (account, client, worker_id) tuples.
        merged_rows: Dict to accumulate restricted chat results (modified in-place).
        cfg: Configuration with resolve_limit, resolve_gap, flood_wait_threshold.
        job_id: Unique job identifier.
        admin_job_append_log_fn: Callback to log progress messages.

    Returns:
        Tuple of:
        - probed_public_chat_ids: Set of successfully resolved chat IDs
        - public_access_failure_count: Count of confirmed access failures

    Notes:
        - Limited by admin_restricted_public_resolve_limit (see constants for details)
        - Rate-limited by admin_restricted_public_resolve_gap_seconds
        - Round-robins between accounts to distribute load and avoid single-account limits
        - Records access failures when all accounts fail consistently
        - Transient failures (FloodWait, network errors) are not recorded as access failures
    """
    # Phase 3: Incremental resolution of uncached public entities
    # ============================================================
    # IMPORTANT: This phase has a configurable limit to prevent excessive API calls
    # in a single scan cycle. The default limit (DEFAULT_PUBLIC_RESOLVE_LIMIT=40)
    # means only the first 40 uncached entities will be actively resolved.
    #
    # IMPLICATIONS:
    # - If the database has 1000+ public channels, most will NOT be resolved this scan
    # - Unresolved channels retain their previous scan results (if any)
    # - Risk flags for unresolved channels may become stale over time
    # - Access failures (entity_unavailable) may not be detected until resolution
    #
    # RATIONALE:
    # - Resolving thousands of entities would cause:
    #   * Long scan duration (1+ seconds per entity with rate limiting)
    #   * High risk of hitting Telegram flood control limits
    #   * Potential account penalties for aggressive API usage
    # - Most channels don't change restriction status frequently
    # - Cached batch refresh (Phase 2) covers recently-accessed channels
    #
    # CONFIGURATION:
    # - Set admin_restricted_public_resolve_limit=0 to skip active resolution entirely
    # - Set a high value (e.g., 10000) for exhaustive scans, but monitor for:
    #   * Scan duration (expect 1+ hours for 1000s of entities)
    #   * FloodWait errors triggering account switches
    #   * Telegram account rate limit warnings
    # - Consider running exhaustive scans off-hours or with dedicated scan accounts
    #
    # FUTURE IMPROVEMENT:
    # - Implement rotation strategy: resolve different subsets each scan
    # - Prioritize by: last_message_ts DESC, message_count DESC
    # - Track last_resolved_at per channel to ensure periodic coverage
    resolve_limit = max(
        0,
        int(getattr(cfg, "admin_restricted_public_resolve_limit", DEFAULT_PUBLIC_RESOLVE_LIMIT) or DEFAULT_PUBLIC_RESOLVE_LIMIT),
    )
    resolve_gap = max(
        0.0,
        float(
            getattr(cfg, "admin_restricted_public_resolve_gap_seconds", DEFAULT_PUBLIC_RESOLVE_GAP_SECONDS)
            or DEFAULT_PUBLIC_RESOLVE_GAP_SECONDS
        ),
    )

    probed_public_chat_ids: set[int] = set()
    public_access_failure_count = 0
    account_next_resolve_at: dict[str, float] = {}
    unresolved_rows = list(uncached_by_id.values())[:resolve_limit]

    for index, row in enumerate(unresolved_rows):
        access_failures: list[tuple[str, Exception]] = []
        had_transient_failure = False
        resolved = False
        for account_offset in range(len(active_clients)):
            account, client, _worker_id = active_clients[
                (index + account_offset) % len(active_clients)
            ]
            next_allowed = account_next_resolve_at.get(account.key, 0.0)
            wait_seconds = max(0.0, next_allowed - time.monotonic())
            if wait_seconds > 0:
                time.sleep(wait_seconds)
            account_next_resolve_at[account.key] = time.monotonic() + resolve_gap
            try:
                with bind_client_event_loop(client):
                    entity = call_with_bounded_retry(
                        client.get_entity,
                        row["chat_username"],
                        max_retries=2,
                        flood_wait_threshold_seconds=int(
                            getattr(account.cfg, "flood_wait_switch_threshold", DEFAULT_FLOOD_WAIT_THRESHOLD)
                            or DEFAULT_FLOOD_WAIT_THRESHOLD
                        ),
                        account_label=account.label,
                        scope="restricted-public-username",
                    )
            except Exception as exc:
                if is_flood_wait_error(exc):
                    had_transient_failure = True
                    continue
                risk_type = classify_chat_access_failure(exc)
                if risk_type:
                    access_failures.append((risk_type, exc))
                else:
                    had_transient_failure = True
                logging.info(
                    "公开频道风险补探测失败: chat_id=%s account=%s error=%s",
                    row.get("chat_id"),
                    account.key,
                    admin_error_message(exc),
                )
                continue
            chat_id = int(row["chat_id"])
            probed_public_chat_ids.add(chat_id)
            _merge_resolved_public_entity(
                merged_rows,
                row=row,
                entity=entity,
            )
            resolved = True
            break

        if (
            not resolved
            and not had_transient_failure
            and len(access_failures) == len(active_clients)
        ):
            risk_type, last_exc = access_failures[-1]
            chat_id = int(row["chat_id"])
            risk_row = unavailable_chat_risk_row(
                chat_id=chat_id,
                chat_title=str(row.get("chat_title") or ""),
                chat_username=str(row.get("chat_username") or ""),
                chat_type=str(row.get("chat_type") or ""),
                risk_type=risk_type,
                risk_message=admin_error_message(last_exc),
                membership_scope="public_unjoined",
                last_message_at=str(row.get("last_message_at") or ""),
                last_message_ts=row.get("last_message_ts"),
            )
            key = chat_identity_key(risk_row.chat_id, risk_row.chat_type)
            merged_rows[key] = _merge_restricted_chat_row(
                merged_rows.get(key), risk_row
            )
            probed_public_chat_ids.add(chat_id)
            public_access_failure_count += 1

    return probed_public_chat_ids, public_access_failure_count


_TOKEN_SEPARATOR_RE = re.compile(r"[、,，;；|/]+")


@dataclass(frozen=True)
class _ChannelInventoryScanSpec:
    worker_suffix: str
    logger_message: str
    scan_rows_fn: Any
    replace_results_fn: Any
    build_success_message_fn: Callable[[int], str]


@dataclass(frozen=True)
class _ScanAccount:
    key: str
    label: str
    cfg: Any


def _cfg_with_session_name(cfg: Any, session_name: str) -> Any:
    values = dict(getattr(cfg, "__dict__", {}) or {})
    if not values:
        values = {
            "api_id": getattr(cfg, "api_id", 0),
            "api_hash": getattr(cfg, "api_hash", ""),
        }
    values["session_name"] = session_name
    return SimpleNamespace(**values)


def _scan_accounts(cfg: Any) -> list[_ScanAccount]:
    accounts = [_ScanAccount(key="primary", label="主账号", cfg=cfg)]
    primary_session_name = str(getattr(cfg, "session_name", "") or "").strip()
    secondary_session_name = str(
        getattr(cfg, "secondary_session_name", "") or ""
    ).strip()
    if secondary_session_name and secondary_session_name != primary_session_name:
        accounts.append(
            _ScanAccount(
                key="secondary",
                label="第二账号",
                cfg=_cfg_with_session_name(cfg, secondary_session_name),
            )
        )
    return accounts


def _dedupe_texts(values: list[str]) -> str:
    items: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        items.append(text)
    return "；".join(items)


def _merge_tokens(values: list[str]) -> str:
    tokens: list[str] = []
    seen: set[str] = set()
    for value in values:
        for raw_token in _TOKEN_SEPARATOR_RE.split(str(value or "")):
            token = raw_token.strip()
            key = token.casefold()
            if not token or key in seen:
                continue
            seen.add(key)
            tokens.append(token)
    return "、".join(tokens)


def _chat_row_priority(row: ChatInventoryRow) -> tuple[int, int, int, str, int]:
    return (
        1 if str(row.unavailable_reason or "").strip() else 0,
        0 if row.chat_username else 1,
        -(row.last_message_ts or 0),
        row.chat_title.casefold(),
        row.chat_id,
    )


def _merge_chat_inventory_row(
    current: ChatInventoryRow | None,
    incoming: ChatInventoryRow,
) -> ChatInventoryRow:
    if current is None:
        return incoming
    preferred = current if _chat_row_priority(current) <= _chat_row_priority(incoming) else incoming
    other = incoming if preferred is current else current

    # Merge unavailable reasons from both accounts
    merged_reason = _dedupe_texts(
        [preferred.unavailable_reason, other.unavailable_reason]
    )

    # Merge source account labels to track which accounts saw this state
    merged_accounts = _merge_tokens(
        [preferred.scan_source_account, other.scan_source_account]
    )

    if merged_reason == str(preferred.unavailable_reason or "") and merged_accounts == str(preferred.scan_source_account or ""):
        return preferred
    return replace(
        preferred,
        unavailable_reason=merged_reason,
        scan_source_account=merged_accounts,
    )


def _restricted_row_priority(
    row: RestrictedChatInventoryRow,
) -> tuple[int, int, str, int]:
    return (
        0 if row.chat_username else 1,
        -(row.last_message_ts or 0),
        row.chat_title.casefold(),
        row.chat_id,
    )


def _merge_restricted_chat_row(
    current: RestrictedChatInventoryRow | None,
    incoming: RestrictedChatInventoryRow,
) -> RestrictedChatInventoryRow:
    if current is None:
        return incoming
    preferred = (
        current
        if _restricted_row_priority(current) <= _restricted_row_priority(incoming)
        else incoming
    )
    return replace(
        preferred,
        restriction_platforms=_merge_tokens(
            [current.restriction_platforms, incoming.restriction_platforms]
        ),
        restriction_reasons=_merge_tokens(
            [current.restriction_reasons, incoming.restriction_reasons]
        ),
        restriction_text=_dedupe_texts(
            [current.restriction_text, incoming.restriction_text]
        ),
        risk_flags=_merge_tokens([current.risk_flags, incoming.risk_flags]),
        membership_scope=(
            "joined"
            if "joined" in {current.membership_scope, incoming.membership_scope}
            else "public_unjoined"
        ),
        scanned_at=max(current.scanned_at, incoming.scanned_at),
    )


def _restricted_row_from_stored(item: dict[str, Any]) -> RestrictedChatInventoryRow:
    return RestrictedChatInventoryRow(
        chat_id=int(item.get("chat_id") or 0),
        chat_title=str(item.get("chat_title") or ""),
        chat_username=str(item.get("chat_username") or ""),
        chat_type=str(item.get("chat_type") or ""),
        is_public=int(item.get("is_public") or 0),
        restriction_platforms=str(item.get("restriction_platforms") or ""),
        restriction_reasons=str(item.get("restriction_reasons") or ""),
        restriction_text=str(item.get("restriction_text") or ""),
        risk_flags=str(item.get("risk_flags") or ""),
        membership_scope="public_unjoined",
        last_message_at=str(item.get("last_message_at") or ""),
        last_message_ts=item.get("last_message_ts"),
        scan_job_id=str(item.get("scan_job_id") or ""),
        scanned_at=str(item.get("scanned_at") or ""),
    )


def _resolved_entity_matches_database_row(entity: Any, row: dict[str, Any]) -> bool:
    entity_id = getattr(entity, "id", None)
    entity_type = entity.__class__.__name__
    if entity_id is None or not entity_type.lower().lstrip("_").startswith(
        ("channel", "chat")
    ):
        return False
    return not chat_identity_candidates(
        row.get("chat_id"), row.get("chat_type")
    ).isdisjoint(chat_identity_candidates(entity_id, entity_type))


def _merge_resolved_public_entity(
    merged_rows: dict[tuple[str, int], RestrictedChatInventoryRow],
    *,
    row: dict[str, Any],
    entity: Any,
) -> None:
    if not _resolved_entity_matches_database_row(entity, row):
        return
    restricted_row = restricted_chat_row_from_entity(
        entity,
        chat_id=int(row.get("chat_id") or 0),
        chat_title=str(row.get("chat_title") or ""),
        chat_username=str(row.get("chat_username") or ""),
        last_message_at=str(row.get("last_message_at") or ""),
        last_message_ts=row.get("last_message_ts"),
        membership_scope="public_unjoined",
    )
    if restricted_row is None:
        return
    key = chat_identity_key(restricted_row.chat_id, restricted_row.chat_type)
    merged_rows[key] = _merge_restricted_chat_row(merged_rows.get(key), restricted_row)


def _fetch_cached_entities(
    client: Any,
    pairs: list[tuple[dict[str, Any], Any]],
) -> list[tuple[dict[str, Any], Any]]:
    if not pairs:
        return []
    try:
        with bind_client_event_loop(client):
            entities = call_with_bounded_retry(
                client.get_entity,
                [input_peer for _row, input_peer in pairs],
                scope="restricted-public-cache-batch",
            )
        return list(zip((row for row, _input_peer in pairs), entities, strict=True))
    except Exception as exc:
        if is_flood_wait_error(exc):
            raise
        if len(pairs) <= 1:
            return []
        midpoint = len(pairs) // 2
        return [
            *_fetch_cached_entities(client, pairs[:midpoint]),
            *_fetch_cached_entities(client, pairs[midpoint:]),
        ]


def _scan_account_rows(
    account: _ScanAccount,
    *,
    job_id: str,
    worker_suffix: str,
    admin_job_append_log_fn: Callable[[str, str], Any],
    scan_fn: Callable[[Any], list[Any]],
) -> list[Any] | None:
    worker_id = f"{job_id}_{worker_suffix}_{account.key}"
    client = None
    try:
        client = _create_isolated_worker_client(account.cfg, worker_id)
        if not client.is_user_authorized():
            admin_job_append_log_fn(
                job_id,
                f"{account.label} Telegram 会话未登录，本轮扫描已跳过",
            )
            return None
        return scan_fn(client)
    finally:
        if client is not None:
            try:
                _disconnect_worker_client(client)
            except Exception as exc:
                logging.warning(
                    "清理扫描会话时断开连接失败: account=%s worker_id=%s error=%s",
                    account.key,
                    worker_id,
                    admin_error_message(exc),
                )
        _cleanup_isolated_worker_session(account.cfg, worker_id)


def _scan_missing_chat_rows(
    *,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_append_log_fn: Callable[[str, str], Any],
    job_id: str,
) -> list[Any]:
    """Scan for groups/channels that accounts have joined but are not in the database.

    This function orchestrates a multi-account scan to discover chats that should be
    added to the database. It merges results from all configured accounts (primary
    and secondary) to maximize coverage.

    Args:
        cfg: Configuration object with api_id, api_hash, session_name, and optional
            secondary_session_name.
        get_conn_fn: Callback returning a database connection.
        admin_job_append_log_fn: Callback to append log messages to the job.
            Called as (job_id, message).
        job_id: Unique identifier for this scan job.

    Returns:
        List of ChatInventoryRow for chats missing from the database.
        Sorted by: unavailable status, title (case-insensitive), chat_id.

    Raises:
        RuntimeError: If no authorized Telegram sessions are available.

    Notes:
        - Creates isolated worker sessions for each account to avoid state pollution
        - Merges results from multiple accounts, preferring accessible over unavailable
        - Tracks which accounts observed unavailable states via scan_source_account
        - Automatically cleans up worker sessions on completion
    """
    admin_job_append_log_fn(job_id, "正在读取数据库已有群组清单...")
    known_chat_ids = call_with_conn(get_conn_fn, load_known_chat_identities)
    admin_job_append_log_fn(job_id, f"数据库中已有 {len(known_chat_ids)} 个群组/频道身份")

    merged_rows: dict[tuple[str, int], ChatInventoryRow] = {}
    scanned_account_count = 0
    for account in _scan_accounts(cfg):
        account_rows = _scan_account_rows(
            account,
            job_id=job_id,
            worker_suffix="missing_chats",
            admin_job_append_log_fn=admin_job_append_log_fn,
            scan_fn=lambda client: find_missing_joined_chats(
                client.iter_dialogs(),
                known_chat_ids,
                include_unavailable=True,
                source_account=account.key,
            ),
        )
        if account_rows is None:
            continue
        scanned_account_count += 1
        unavailable_count = sum(
            1 for row in account_rows if str(row.unavailable_reason or "").strip()
        )
        admin_job_append_log_fn(
            job_id,
            f"{account.label}扫描到 {len(account_rows)} 个未入库候选，"
            f"其中 {unavailable_count} 个当前不可访问",
        )
        for row in account_rows:
            key = chat_identity_key(row.chat_id, row.chat_type)
            merged_rows[key] = _merge_chat_inventory_row(merged_rows.get(key), row)

    if scanned_account_count <= 0:
        raise RuntimeError("没有可用的 Telegram 会话可执行扫描")

    rows = list(merged_rows.values())
    rows.sort(
        key=lambda item: (
            1 if str(item.unavailable_reason or "").strip() else 0,
            item.chat_title.casefold(),
            item.chat_id,
        )
    )
    return rows


def _scan_restricted_chat_rows(
    *,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_append_log_fn: Callable[[str, str], Any],
    job_id: str,
) -> list[Any]:
    """Scan for groups/channels with Telegram risk flags or access restrictions.

    This function performs a comprehensive three-phase scan:

    Phase 1: Scan joined chats for restriction reasons and risk flags
    - Extracts restriction_reason, scam, fake flags from dialog entities
    - Identifies forbidden/unavailable chats

    Phase 2: Batch refresh cached public entities
    - For public chats in the database that accounts haven't joined
    - Uses session cache (get_input_entity) to avoid API calls when possible
    - Batch fetches entities (50 per call) to minimize network overhead

    Phase 3: Resolve uncached public entities
    - Limited by admin_restricted_public_resolve_limit (default 40)
    - Rate-limited by admin_restricted_public_resolve_gap_seconds (default 1.0)
    - Round-robins between accounts to distribute load
    - Records access failures (entity_unavailable, access_denied, etc.)

    Args:
        cfg: Configuration object with:
            - session_name, secondary_session_name: Account credentials
            - admin_restricted_public_resolve_limit: Max entities to resolve per scan
            - admin_restricted_public_resolve_gap_seconds: Delay between resolutions
            - flood_wait_switch_threshold: Seconds to wait before switching accounts
        get_conn_fn: Callback returning a database connection.
        admin_job_append_log_fn: Callback to log progress messages.
        job_id: Unique identifier for this scan job.

    Returns:
        List of RestrictedChatInventoryRow with risk flags, restriction reasons,
        and access failure information.
        Sorted by: title (case-insensitive), chat_id.

    Raises:
        RuntimeError: If no authorized Telegram sessions are available.

    Notes:
        - Creates isolated worker clients that are cleaned up on completion
        - Handles FloodWait errors by switching to conservative mode
        - Merges results from multiple accounts to get comprehensive risk picture
        - Public entities not resolved this scan retain previous scan results
        - Access failures recorded with account-level context
    """
    # Load database context
    database_rows = call_with_conn(
        get_conn_fn,
        list_database_channels,
        sort="message_count_desc",
    )
    previous_rows = call_with_conn(get_conn_fn, list_restricted_chat_scan_results)

    try:
        # Phase 1: Scan joined chats
        merged_rows, joined_identities, active_clients = _scan_joined_restricted_chats(
            accounts=_scan_accounts(cfg),
            job_id=job_id,
            admin_job_append_log_fn=admin_job_append_log_fn,
        )

        # Phase 2: Batch refresh cached public entities
        public_rows = [
            row
            for row in database_rows
            if str(row.get("chat_username") or "").strip()
            and joined_identities.isdisjoint(
                chat_identity_candidates(row.get("chat_id"), row.get("chat_type"))
            )
        ]
        admin_job_append_log_fn(
            job_id,
            f"正在补探测 {len(public_rows)} 个账号未加入的数据库公开群组/频道...",
        )

        probed_public_chat_ids = _batch_refresh_cached_public_entities(
            public_rows=public_rows,
            active_clients=active_clients,
            merged_rows=merged_rows,
            job_id=job_id,
            admin_job_append_log_fn=admin_job_append_log_fn,
        )

        # Phase 3: Resolve uncached public entities
        uncached_by_id = {
            int(row["chat_id"]): row
            for row in public_rows
            if int(row["chat_id"]) not in probed_public_chat_ids
        }
        additional_probed, public_access_failure_count = _resolve_uncached_public_entities(
            uncached_by_id=uncached_by_id,
            active_clients=active_clients,
            merged_rows=merged_rows,
            cfg=cfg,
            job_id=job_id,
            admin_job_append_log_fn=admin_job_append_log_fn,
        )
        probed_public_chat_ids.update(additional_probed)

        # Phase 4: Preserve previous scan results for unprobed channels
        candidate_ids = {int(row["chat_id"]) for row in public_rows}
        for previous in previous_rows:
            chat_id = int(previous.get("chat_id") or 0)
            if chat_id not in candidate_ids or chat_id in probed_public_chat_ids:
                continue
            row = _restricted_row_from_stored(previous)
            key = chat_identity_key(row.chat_id, row.chat_type)
            merged_rows[key] = _merge_restricted_chat_row(
                merged_rows.get(key), row
            )

        admin_job_append_log_fn(
            job_id,
            "公开群组补探测完成："
            f"成功刷新 {max(0, len(probed_public_chat_ids) - public_access_failure_count)} 个，"
            f"确认不可访问 {public_access_failure_count} 个，"
            f"本轮未主动解析 {max(0, len(uncached_by_id) - len(additional_probed))} 个",
        )
    finally:
        for account, client, worker_id in reversed(active_clients):
            try:
                _disconnect_worker_client(client)
            except Exception as exc:
                logging.warning(
                    "清理受限扫描会话时断开连接失败: account=%s worker_id=%s error=%s",
                    account.key,
                    worker_id,
                    admin_error_message(exc),
                )
            finally:
                _cleanup_isolated_worker_session(account.cfg, worker_id)

    rows = list(merged_rows.values())
    rows.sort(key=lambda item: (item.chat_title.casefold(), item.chat_id))
    return rows


def _run_channel_inventory_scan_job(
    job_id: str,
    *,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
    spec: _ChannelInventoryScanSpec,
) -> None:
    heartbeat_stop, heartbeat_thread = start_admin_job_heartbeat(job_id)
    try:
        mark_admin_job_running(
            job_id,
            admin_job_set_status_fn=admin_job_set_status_fn,
        )
        update_admin_job_progress(
            job_id,
            0,
            total=None,
            stage="running",
        )
        rows = spec.scan_rows_fn(
            cfg=cfg,
            get_conn_fn=get_conn_fn,
            admin_job_append_log_fn=admin_job_append_log_fn,
            job_id=job_id,
        )
        scanned_at = _admin_now_iso()

        admin_job_append_log_fn(job_id, "正在保存扫描结果...")
        saved_count = call_with_conn(
            get_conn_fn,
            spec.replace_results_fn,
            rows,
            scan_job_id=job_id,
            scanned_at=scanned_at,
        )

        update_admin_job_progress(
            job_id,
            saved_count,
            total=saved_count,
            stage="done",
        )
        admin_job_append_log_fn(job_id, spec.build_success_message_fn(saved_count))
        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        logging.exception(spec.logger_message, job_id)
        admin_job_append_log_fn(job_id, f"扫描失败：{admin_error_message(exc)}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        finish_job_heartbeat(heartbeat_stop, heartbeat_thread)


_MISSING_CHATS_SCAN_SPEC = _ChannelInventoryScanSpec(
    worker_suffix="missing_chats",
    logger_message="扫描未入库群组失败: job_id=%s",
    scan_rows_fn=_scan_missing_chat_rows,
    replace_results_fn=replace_missing_chat_scan_results,
    build_success_message_fn=(
        lambda saved_count: f"扫描完成：发现 {saved_count} 个已加入但未入库的群组/频道"
    ),
)


_RESTRICTED_CHATS_SCAN_SPEC = _ChannelInventoryScanSpec(
    worker_suffix="restricted_chats",
    logger_message="扫描内容限制群组失败: job_id=%s",
    scan_rows_fn=_scan_restricted_chat_rows,
    replace_results_fn=replace_restricted_chat_scan_results,
    build_success_message_fn=(
        lambda saved_count: (
            "扫描完成：发现 "
            f"{saved_count} 个带 Telegram 内容限制/风险标记的群组/频道"
        )
    ),
)


def _admin_missing_chats_scan_job_runner(
    job_id: str,
    *,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    _run_channel_inventory_scan_job(
        job_id,
        cfg=cfg,
        get_conn_fn=get_conn_fn,
        admin_job_set_status_fn=admin_job_set_status_fn,
        admin_job_append_log_fn=admin_job_append_log_fn,
        spec=_MISSING_CHATS_SCAN_SPEC,
    )


def _admin_start_missing_chats_scan_job_thread(job_id: str, **kwargs):
    return start_admin_job_thread(
        _admin_missing_chats_scan_job_runner,
        job_id,
        **kwargs,
    )


def _admin_restricted_chats_scan_job_runner(
    job_id: str,
    *,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    _run_channel_inventory_scan_job(
        job_id,
        cfg=cfg,
        get_conn_fn=get_conn_fn,
        admin_job_set_status_fn=admin_job_set_status_fn,
        admin_job_append_log_fn=admin_job_append_log_fn,
        spec=_RESTRICTED_CHATS_SCAN_SPEC,
    )


def _admin_start_restricted_chats_scan_job_thread(job_id: str, **kwargs):
    return start_admin_job_thread(
        _admin_restricted_chats_scan_job_runner,
        job_id,
        **kwargs,
    )
