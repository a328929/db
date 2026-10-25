from __future__ import annotations

import json
import logging
import re
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, fields, replace
from datetime import UTC, datetime, timedelta
from typing import Any

from telethon.tl.types import InputPeerChat, PeerChannel, PeerChat

from tg_harvest.admin_jobs.common import (
    admin_error_message,
    call_with_conn,
    cfg_with_session_name,
    classify_chat_access_failure,
    start_admin_job_thread,
)
from tg_harvest.admin_jobs.framework import (
    AdminJobContext,
    AdminJobResult,
    admin_job,
    legacy_admin_job_log_adapter,
)
from tg_harvest.admin_jobs.inventory_constants import (
    DEFAULT_FLOOD_WAIT_THRESHOLD,
    DEFAULT_PUBLIC_RESOLVE_GAP_SECONDS,
    DEFAULT_PUBLIC_RESOLVE_LIMIT,
    PUBLIC_ENTITY_BATCH_SIZE,
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
    RestrictedChatScanRows,
    chat_identity_candidates,
    chat_identity_key,
    find_missing_joined_chats,
    find_restricted_joined_chats,
    load_joined_chat_inventory,
    load_known_chat_identities,
    restricted_chat_row_from_entity,
    unavailable_chat_risk_row,
)
from tg_harvest.ingest.flood_wait import (
    call_with_bounded_retry,
    flood_wait_seconds,
    is_flood_wait_error,
    is_transient_telegram_error,
)
from tg_harvest.storage.channel_management import (
    list_chat_risk_scan_states,
    list_database_channels,
    list_stored_restricted_chat_scan_results,
    replace_missing_chat_scan_results,
    replace_restricted_chat_scan_results,
)


def _scan_joined_restricted_chats(
    *,
    accounts: list[_ScanAccount],
    job_id: str,
    admin_job_append_log_fn: Callable[[str, str], Any],
    active_clients: list | None = None,
    scan: _RiskScan | None = None,
) -> tuple[
    dict[tuple[str, int], RestrictedChatInventoryRow],
    set[tuple[str, int]],
    list[tuple[_ScanAccount, Any, str]],
]:
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
    if active_clients is None:
        active_clients = []

    for account in accounts:
        admin_job_append_log_fn(
            job_id,
            f"正在连接{account.label}并扫描已加入会话风险标记...",
        )
        worker_id = f"{job_id}_restricted_chats_{account.key}"
        client = _create_isolated_worker_client(account.cfg, worker_id)
        active_clients.append((account, client, worker_id))
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
            active_clients.pop()
            continue
        if scan is not None and scan.is_cooling_down(account.key):
            admin_job_append_log_fn(
                job_id, f"{account.label}仍在 Telegram 冷却期，保留该账号历史观察"
            )
            continue
        try:
            with bind_client_event_loop(client):
                dialogs = list(
                    client.iter_dialogs(
                        limit=None,
                        archived=None,
                        ignore_migrated=True,
                    )
                )
        except Exception as exc:
            if scan is None or not (
                is_flood_wait_error(exc) or is_transient_telegram_error(exc)
            ):
                raise
            scan.defer_account(account.key, exc)
            admin_job_append_log_fn(
                job_id,
                f"{account.label}会话枚举暂未完成，保留历史观察：{admin_error_message(exc)}",
            )
            continue
        if scan is not None:
            for dialog in dialogs:
                inventory = load_joined_chat_inventory([dialog], account.key)
                if inventory:
                    scan.observe(inventory[0], account.key, dialog.entity, joined=True)
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
            merged_rows[key] = _merge_restricted_chat_row(merged_rows.get(key), row)

    if not active_clients:
        raise RuntimeError("没有可用的 Telegram 会话可执行扫描")

    return merged_rows, joined_identities, active_clients


class _RiskScan:
    """Keep account observations separate until all probes have finished."""

    def __init__(self, cfg, database_rows, previous_rows, states, job_id):
        self.cfg = cfg
        self.job_id = job_id
        self.started_at = _admin_now_iso()
        self.targets = {}
        for row in [*database_rows, *previous_rows]:
            self.targets.setdefault(
                chat_identity_key(row["chat_id"], row["chat_type"]), dict(row)
            )
        self.states = {
            (
                *chat_identity_key(state["chat_id"], state["chat_type"]),
                state["source_account"],
            ): dict(state)
            for state in states
        }
        self.loaded_state_keys = set(self.states)
        self.previous_rows = previous_rows
        self.current = set()
        self.verified = set()
        self.joined = set()
        self.cooldowns = {}
        self.next_call_at = {}
        for state in states:
            if state["status"] == "flood_wait":
                account = state["source_account"]
                self.cooldowns[account] = max(
                    self.cooldowns.get(account, ""), state["next_retry_at"]
                )
        for old in previous_rows:
            identity, _target = self.target(old)
            sources = set(
                _TOKEN_SEPARATOR_RE.split(old.get("scan_source_account", ""))
            ) - {""}
            if not sources or not all(
                (*identity, account) in self.loaded_state_keys for account in sources
            ):
                legacy = self.state(identity, "__legacy__")
                if (*identity, "__legacy__") not in self.loaded_state_keys:
                    legacy["risk_json"] = json.dumps(
                        asdict(_restricted_row_from_stored(old)), ensure_ascii=False
                    )

    def target(self, row):
        identity = (
            chat_identity_key(row.chat_id, row.chat_type)
            if isinstance(row, ChatInventoryRow)
            else chat_identity_key(row["chat_id"], row["chat_type"])
        )
        if identity in self.targets:
            return identity, self.targets[identity]
        candidates = chat_identity_candidates(identity[1], identity[0])
        matches = [
            key
            for key in self.targets
            if not candidates.isdisjoint(chat_identity_candidates(key[1], key[0]))
        ]
        if len(matches) == 1:
            return matches[0], self.targets[matches[0]]
        item = asdict(row) if isinstance(row, ChatInventoryRow) else dict(row)
        self.targets[identity] = item
        return identity, item

    def state(self, identity, account):
        key = (*identity, account)
        if key not in self.states:
            row = self.targets[identity]
            self.states[key] = dict(
                chat_id=int(row["chat_id"]),
                chat_type=identity[0],
                source_account=account,
                last_attempt_at="",
                last_checked_at="",
                next_retry_at="",
                status="not_checked",
                risk_json="null",
                error_json="null",
            )
        self.states[key]["chat_id"] = int(self.targets[identity]["chat_id"])
        return self.states[key]

    def is_cooling_down(self, account):
        return self.cooldowns.get(account, "") > _admin_now_iso()

    def defer_account(self, account, exc):
        seconds = flood_wait_seconds(exc) if is_flood_wait_error(exc) else 60
        self.cooldowns[account] = (
            datetime.now(UTC) + timedelta(seconds=seconds)
        ).isoformat()
        for identity in self.targets:
            if (*identity, account) not in self.verified:
                self.state(identity, account).update(
                    status="flood_wait"
                    if is_flood_wait_error(exc)
                    else "transient_error",
                    next_retry_at=self.cooldowns[account],
                )

    def due(self, identity, account):
        state = self.state(identity, account)
        return (
            (*identity, account) not in self.current
            and not self.is_cooling_down(account)
            and state["next_retry_at"] <= _admin_now_iso()
        )

    def may_resolve(self, identity, account):
        state = self.state(identity, account)
        return self.due(identity, account) or (
            (*identity, account) in self.current
            and state["status"] == "lookup_failed"
            and not self.is_cooling_down(account)
        )

    def scope(self, identity, row):
        if identity in self.joined:
            return "joined"
        return "public_unjoined" if row.get("chat_username") else "access_unknown"

    def observe(self, row, account, entity, *, joined=False):
        identity, target = self.target(row)
        entity_type = entity.__class__.__name__.lower().lstrip("_")
        forbidden = entity_type in {"channelforbidden", "chatforbidden"}
        if joined and not forbidden:
            self.joined.add(identity)
        risk = restricted_chat_row_from_entity(
            entity,
            chat_id=target["chat_id"],
            chat_title=target.get("chat_title", ""),
            chat_username=target.get("chat_username", ""),
            last_message_at=getattr(row, "last_message_at", "")
            or target.get("last_message_at", ""),
            last_message_ts=getattr(row, "last_message_ts", None)
            or target.get("last_message_ts"),
            membership_scope=self.scope(identity, target),
            source_account=account,
        )
        now = _admin_now_iso()
        state = self.state(identity, account)
        state.update(
            last_attempt_at=now,
            next_retry_at="",
            status="access_error" if forbidden else "checked",
        )
        if risk is not None:
            risk = replace(risk, scan_job_id=self.job_id, scanned_at=now)
        if forbidden:
            # Forbidden constructors carry no current content restriction fields.
            state["error_json"] = json.dumps(asdict(risk), ensure_ascii=False)
        else:
            state.update(
                last_checked_at=now,
                risk_json=json.dumps(
                    asdict(risk) if risk else None, ensure_ascii=False
                ),
                error_json="null",
            )
        self.current.add((*identity, account))
        self.verified.add((*identity, account))

    def failure(self, identity, account, exc, *, risk_type=None, attempted=True):
        state = self.state(identity, account)
        row = self.targets[identity]
        now = _admin_now_iso()
        if attempted:
            self.current.add((*identity, account))
        status, delay = "transient_error", 60
        error = None
        if is_flood_wait_error(exc):
            self.defer_account(account, exc)
            status, delay = "flood_wait", flood_wait_seconds(exc)
        else:
            kind = risk_type or classify_chat_access_failure(exc)
            if kind:
                status = (
                    "access_error"
                    if kind in {"access_denied", "account_banned"}
                    else "lookup_failed"
                )
                delay = 300
                error = unavailable_chat_risk_row(
                    chat_id=row["chat_id"],
                    chat_title=row.get("chat_title", ""),
                    chat_username=row.get("chat_username", ""),
                    chat_type=row.get("chat_type", ""),
                    risk_type=kind,
                    risk_message=admin_error_message(exc),
                    membership_scope=self.scope(identity, row),
                    source_account=account,
                    last_message_at=row.get("last_message_at", ""),
                    last_message_ts=row.get("last_message_ts"),
                )
                if kind in {"username_unavailable", "identity_mismatch"}:
                    error = replace(
                        error,
                        restriction_text=(
                            "账号无法解析该用户名，尚未确认原群组/频道是否存在或可访问"
                            if kind == "username_unavailable"
                            else "用户名或缓存指向其他实体，原群组状态尚未核实"
                        ),
                    )
                error = replace(
                    error,
                    scanned_at=now,
                    scan_job_id=self.job_id,
                    scan_status="checked" if status == "access_error" else "unverified",
                )
                if status == "access_error":
                    self.verified.add((*identity, account))
        state.update(
            last_attempt_at=now,
            next_retry_at=(datetime.now(UTC) + timedelta(seconds=delay)).isoformat(),
            status=status,
        )
        # A transient failure must not erase the last explicit access/lookup error.
        if error is not None:
            state["error_json"] = json.dumps(asdict(error), ensure_ascii=False)

    def accept(self, identity, account, entity):
        row = self.targets[identity]
        if not _resolved_entity_matches_database_row(entity, row):
            self.failure(
                identity,
                account,
                ValueError(
                    f"用户名或缓存指向其他实体，原群组状态尚未核实：expected={row['chat_id']}, "
                    f"actual={entity.__class__.__name__}:{getattr(entity, 'id', None)}"
                ),
                risk_type="identity_mismatch",
            )
            return False
        self.observe(row, account, entity)
        return True

    def call(self, account, client, value):
        gap_value = getattr(
            self.cfg, "admin_restricted_public_resolve_gap_seconds", None
        )
        gap = max(
            0.0,
            float(
                DEFAULT_PUBLIC_RESOLVE_GAP_SECONDS if gap_value is None else gap_value
            ),
        )
        wait = max(0.0, self.next_call_at.get(account.key, 0.0) - time.monotonic())
        if wait:
            time.sleep(wait)
        self.next_call_at[account.key] = time.monotonic() + gap
        with bind_client_event_loop(client):
            return call_with_bounded_retry(
                client.get_entity,
                value,
                max_retries=2,
                flood_wait_threshold_seconds=int(
                    getattr(
                        account.cfg,
                        "flood_wait_switch_threshold",
                        DEFAULT_FLOOD_WAIT_THRESHOLD,
                    )
                    or DEFAULT_FLOOD_WAIT_THRESHOLD
                ),
                account_label=account.label,
                scope="restricted-chat-probe",
            )

    def fetch_batch(self, account, client, pairs):
        if not pairs or self.is_cooling_down(account.key):
            return []
        try:
            entities = self.call(account, client, [peer for _identity, peer in pairs])
            return [
                (identity, entity, None)
                for (identity, _peer), entity in zip(pairs, entities, strict=True)
            ]
        except Exception as exc:
            if is_flood_wait_error(exc):
                self.defer_account(account.key, exc)
            if (
                len(pairs) == 1
                or is_flood_wait_error(exc)
                or is_transient_telegram_error(exc)
            ):
                return [(identity, None, exc) for identity, _peer in pairs]
            midpoint = len(pairs) // 2
            return [
                *self.fetch_batch(account, client, pairs[:midpoint]),
                *self.fetch_batch(account, client, pairs[midpoint:]),
            ]

    def cached_peer(self, client, identity):
        row = self.targets[identity]
        kinds = [identity[0]] if identity[0] else ["channel", "chat"]
        for kind in kinds:
            peer = (
                PeerChannel(identity[1]) if kind == "channel" else PeerChat(identity[1])
            )
            try:
                return client.session.get_input_entity(peer)
            except (ValueError, TypeError):
                pass
        if identity[0] == "chat":
            return InputPeerChat(identity[1])
        if row.get("chat_username"):
            try:
                return client.session.get_input_entity(row["chat_username"])
            except (ValueError, TypeError):
                pass
        return None

    def refresh_cached(self, active_clients):
        for account, client, _worker_id in active_clients:
            pairs = []
            for identity in list(self.targets):
                if not self.due(identity, account.key):
                    continue
                peer = self.cached_peer(client, identity)
                if peer is not None:
                    pairs.append((identity, peer))
            for offset in range(0, len(pairs), PUBLIC_ENTITY_BATCH_SIZE):
                for identity, entity, error in self.fetch_batch(
                    account, client, pairs[offset : offset + PUBLIC_ENTITY_BATCH_SIZE]
                ):
                    if error is not None:
                        self.failure(identity, account.key, error)
                    else:
                        self.accept(identity, account.key, entity)
                if self.is_cooling_down(account.key):
                    break

    def resolve_uncached(self, active_clients):
        limit_value = getattr(self.cfg, "admin_restricted_public_resolve_limit", None)
        limit = max(
            0, int(DEFAULT_PUBLIC_RESOLVE_LIMIT if limit_value is None else limit_value)
        )
        pending = []
        for identity, row in self.targets.items():
            accounts = [
                account
                for account, _client, _worker in active_clients
                if self.may_resolve(identity, account.key)
            ]
            if accounts and row.get("chat_username"):
                pending.append(
                    (
                        min(
                            self.state(identity, account.key)["last_attempt_at"]
                            for account in accounts
                        ),
                        identity,
                    )
                )
        for index, (_last_attempt, identity) in enumerate(sorted(pending)[:limit]):
            for offset in range(len(active_clients)):
                account, client, _worker_id = active_clients[
                    (index + offset) % len(active_clients)
                ]
                if not self.may_resolve(identity, account.key):
                    continue
                try:
                    entity = self.call(
                        account, client, self.targets[identity]["chat_username"]
                    )
                except Exception as exc:
                    self.failure(identity, account.key, exc)
                else:
                    self.accept(identity, account.key, entity)

    def results(self, accounts):
        merged = {}
        account_keys = {account.key for account in accounts}
        for identity, target in self.targets.items():
            for account in account_keys:
                state = self.state(identity, account)
                for field in ("risk_json", "error_json"):
                    payload = json.loads(state[field])
                    if payload is None:
                        continue
                    risk = _restricted_row_from_stored(payload)
                    fresh = (*identity, account) in self.verified and (
                        field == "error_json" or state["status"] == "checked"
                    )
                    status = risk.scan_status if fresh else "stale"
                    if (
                        field == "error_json"
                        and (*identity, account) in self.current
                        and state["status"] == "lookup_failed"
                    ):
                        status = "unverified"
                    risk = replace(
                        risk,
                        chat_id=target["chat_id"],
                        membership_scope=self.scope(identity, target),
                        scan_status=status,
                    )
                    merged[identity] = _merge_restricted_chat_row(
                        merged.get(identity), risk
                    )
        for identity, target in self.targets.items():
            legacy = self.states.get((*identity, "__legacy__"))
            if not legacy or legacy["risk_json"] == "null":
                continue
            old = json.loads(legacy["risk_json"])
            # Old aggregate snapshots have no reliable account provenance.
            # Keep them separately until every configured account has checked
            # the content entity, even across repeated forbidden results.
            if all(
                self.state(identity, account)["last_checked_at"]
                >= (old.get("scanned_at") or self.started_at)
                for account in account_keys
            ):
                legacy["risk_json"] = "null"
                continue
            risk = replace(
                _restricted_row_from_stored(old),
                chat_id=target["chat_id"],
                membership_scope=self.scope(identity, target),
                scan_status="stale",
            )
            merged[identity] = _merge_restricted_chat_row(merged.get(identity), risk)
        rows = list(merged.values())
        rows.sort(key=lambda row: (row.chat_title.casefold(), row.chat_id))
        complete = partial = failed = 0
        for identity in self.targets:
            verified = sum(
                (*identity, account) in self.verified for account in account_keys
            )
            if verified == len(account_keys):
                complete += 1
            elif verified:
                partial += 1
            elif any((*identity, account) in self.current for account in account_keys):
                failed += 1
        summary = dict(
            total=len(self.targets),
            checked=complete,
            partial=partial,
            failed=failed,
            not_checked=len(self.targets) - complete - partial - failed,
            scanned_at=self.started_at,
            accounts=sorted(account_keys),
        )
        return RestrictedChatScanRows(
            rows, scan_states=list(self.states.values()), summary=summary
        )


_TOKEN_SEPARATOR_RE = re.compile(r"[、,，;；|/]+")


@dataclass(frozen=True)
class _ScanAccount:
    key: str
    label: str
    cfg: Any


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
                cfg=cfg_with_session_name(cfg, secondary_session_name),
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
    preferred = (
        current
        if _chat_row_priority(current) <= _chat_row_priority(incoming)
        else incoming
    )
    other = incoming if preferred is current else current

    # Merge unavailable reasons from both accounts
    merged_reason = _dedupe_texts(
        [preferred.unavailable_reason, other.unavailable_reason]
    )

    # Merge source account labels to track which accounts saw this state
    merged_accounts = _merge_tokens(
        [preferred.scan_source_account, other.scan_source_account]
    )

    if merged_reason == str(
        preferred.unavailable_reason or ""
    ) and merged_accounts == str(preferred.scan_source_account or ""):
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
        restriction_details=tuple(
            item
            for index, item in enumerate(
                (*current.restriction_details, *incoming.restriction_details)
            )
            if item
            not in (*current.restriction_details, *incoming.restriction_details)[:index]
        ),
        scan_source_account=_merge_tokens(
            [current.scan_source_account, incoming.scan_source_account]
        ),
        access_source_account=_merge_tokens(
            [current.access_source_account, incoming.access_source_account]
        ),
        scan_status=(
            "stale"
            if "stale" in {current.scan_status, incoming.scan_status}
            else "unverified"
            if "unverified" in {current.scan_status, incoming.scan_status}
            else "checked"
        ),
        membership_scope=(
            "joined"
            if "joined" in {current.membership_scope, incoming.membership_scope}
            else "access_unknown"
            if "access_unknown" in {current.membership_scope, incoming.membership_scope}
            else "public_unjoined"
        ),
        scanned_at=max(current.scanned_at, incoming.scanned_at),
    )


def _restricted_row_from_stored(item: dict[str, Any]) -> RestrictedChatInventoryRow:
    values = {
        field.name: item[field.name]
        for field in fields(RestrictedChatInventoryRow)
        if field.name in item
    }
    details = values.get("restriction_details") or ()
    values["restriction_details"] = tuple(
        json.loads(details) if isinstance(details, str) else details
    )
    return RestrictedChatInventoryRow(**values)


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
    admin_job_append_log_fn(
        job_id, f"数据库中已有 {len(known_chat_ids)} 个群组/频道身份"
    )

    merged_rows: dict[tuple[str, int], ChatInventoryRow] = {}
    scanned_account_count = 0
    accounts = _scan_accounts(cfg)
    for account in accounts:
        account_rows = _scan_account_rows(
            account,
            job_id=job_id,
            worker_suffix="missing_chats",
            admin_job_append_log_fn=admin_job_append_log_fn,
            scan_fn=lambda client, account_key=account.key: find_missing_joined_chats(
                client.iter_dialogs(),
                known_chat_ids,
                include_unavailable=True,
                source_account=account_key,
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
    if scanned_account_count != len(accounts):
        raise RuntimeError(
            "部分账号未完成扫描，已保留上次完整扫描结果；请恢复账号登录后重试"
        )

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
) -> RestrictedChatScanRows:
    """Scan dialogs and database targets without equating unknown with healthy."""
    database_rows = call_with_conn(
        get_conn_fn, list_database_channels, sort="message_count_desc"
    )
    previous_rows = call_with_conn(
        get_conn_fn, list_stored_restricted_chat_scan_results
    )
    states = call_with_conn(get_conn_fn, list_chat_risk_scan_states)
    scan = _RiskScan(cfg, database_rows, previous_rows, states, job_id)
    active_clients = []
    try:
        accounts = _scan_accounts(cfg)
        _merged, _joined, active_clients = _scan_joined_restricted_chats(
            accounts=accounts,
            job_id=job_id,
            admin_job_append_log_fn=admin_job_append_log_fn,
            active_clients=active_clients,
            scan=scan,
        )
        if len(active_clients) != len(accounts):
            raise RuntimeError(
                "部分账号未完成登录，已保留上次扫描结果；请恢复账号登录后重试"
            )
        admin_job_append_log_fn(
            job_id,
            f"正在按账号补查数据库及历史风险目标，共 {len(scan.targets)} 个群组/频道...",
        )
        scan.refresh_cached(active_clients)
        scan.resolve_uncached(active_clients)
        rows = scan.results(accounts)
        summary = rows.summary
        admin_job_append_log_fn(
            job_id,
            f"本轮扫描覆盖：共 {summary['total']} 个，完整核实 {summary['checked']} 个，"
            f"部分账号核实 {summary['partial']} 个，未能核实 {summary['failed']} 个，"
            f"未检查 {summary['not_checked']} 个；未核实的历史观察已保留",
        )
        return rows
    finally:
        for account, client, worker_id in reversed(active_clients):
            try:
                _disconnect_worker_client(client)
            except Exception as exc:
                logging.warning(
                    "清理风险扫描会话时断开连接失败: account=%s worker_id=%s error=%s",
                    account.key,
                    worker_id,
                    admin_error_message(exc),
                )
            finally:
                _cleanup_isolated_worker_session(account.cfg, worker_id)


@admin_job(name="扫描未入库群组", requires_client=False)
def _admin_missing_chats_scan_job_runner(ctx: AdminJobContext) -> AdminJobResult:
    """Scan for groups/channels joined by accounts but not in database.

    This job orchestrates scanning with Telegram clients internally, but doesn't
    require a client from the framework since it manages multiple isolated clients
    for primary and secondary accounts.

    Args:
        ctx: Admin job context with database access

    Returns:
        AdminJobResult with success status and count of missing chats found
    """
    ctx.progress.start(total=None, stage="running")

    rows = _scan_missing_chat_rows(
        cfg=ctx.config,
        get_conn_fn=lambda: ctx.get_connection(),
        admin_job_append_log_fn=legacy_admin_job_log_adapter(ctx),
        job_id=ctx.job_id,
    )

    scanned_at = _admin_now_iso()
    ctx.log("正在保存扫描结果...")

    conn = ctx.get_connection()
    try:
        saved_count = replace_missing_chat_scan_results(
            conn,
            rows,
            scan_job_id=ctx.job_id,
            scanned_at=scanned_at,
        )
    finally:
        conn.close()

    ctx.progress.update(current=saved_count)
    ctx.progress.finish()

    return AdminJobResult(
        success=True,
        message=f"扫描完成：发现 {saved_count} 个已加入但未入库的群组/频道",
    )


def _admin_start_missing_chats_scan_job_thread(job_id: str, **kwargs) -> Any:
    return start_admin_job_thread(
        _admin_missing_chats_scan_job_runner,
        job_id,
        **kwargs,
    )


@admin_job(name="扫描内容限制群组", requires_client=False)
def _admin_restricted_chats_scan_job_runner(ctx: AdminJobContext) -> AdminJobResult:
    """Scan for groups/channels with Telegram risk flags or access restrictions.

    This job orchestrates scanning with Telegram clients internally, but doesn't
    require a client from the framework since it manages multiple isolated clients
    for primary and secondary accounts.

    Args:
        ctx: Admin job context with database access

    Returns:
        AdminJobResult with success status and count of restricted chats found
    """
    ctx.progress.start(total=None, stage="running")

    rows = _scan_restricted_chat_rows(
        cfg=ctx.config,
        get_conn_fn=lambda: ctx.get_connection(),
        admin_job_append_log_fn=legacy_admin_job_log_adapter(ctx),
        job_id=ctx.job_id,
    )

    scanned_at = _admin_now_iso()
    ctx.log("正在保存扫描结果...")

    conn = ctx.get_connection()
    try:
        saved_count = replace_restricted_chat_scan_results(
            conn,
            rows,
            scan_job_id=ctx.job_id,
            scanned_at=scanned_at,
        )
    finally:
        conn.close()

    ctx.progress.update(current=saved_count)
    ctx.progress.finish()

    return AdminJobResult(
        success=True,
        message=f"扫描完成：发现 {saved_count} 个带 Telegram 内容限制/风险标记的群组/频道",
    )


def _admin_start_restricted_chats_scan_job_thread(job_id: str, **kwargs) -> Any:
    return start_admin_job_thread(
        _admin_restricted_chats_scan_job_runner,
        job_id,
        **kwargs,
    )
