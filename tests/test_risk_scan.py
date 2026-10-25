"""Risk scans must preserve uncertainty, account scope and progress across runs."""

import json
import sqlite3
from contextlib import ExitStack
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from telethon.errors import ChannelPrivateError, UsernameNotOccupiedError
from telethon.sessions import MemorySession
from telethon.tl.types import (
    Channel,
    ChannelForbidden,
    ChatPhotoEmpty,
    RestrictionReason,
)

from tg_harvest.admin_jobs.channel_inventory import _scan_restricted_chat_rows
from tg_harvest.admin_jobs.runners import _delete_chat_data, _delete_empty_chats_data
from tg_harvest.domain.chat_inventory import (
    RestrictedChatInventoryRow,
    entity_has_all_platform_terms_restriction,
)
from tg_harvest.ingest.flood_wait import AccountFloodWaitError
from tg_harvest.storage.channel_management import (
    get_chat_risk_scan_summary,
    list_chat_risk_scan_states,
    list_restricted_chat_scan_results,
    list_stored_restricted_chat_scan_results,
    record_chat_access_risk,
    replace_restricted_chat_scan_results,
    resolve_chat_access_risk,
)
from tg_harvest.storage.connection import detect_sqlite_features
from tg_harvest.storage.schema import create_schema


def channel(chat_id, **kwargs):
    return Channel(
        chat_id,
        f"Channel {chat_id}",
        ChatPhotoEmpty(),
        datetime.now(UTC),
        access_hash=chat_id * 100,
        **kwargs,
    )


class Client:
    def __init__(
        self, *, cached=(), resolve=None, dialogs=(), failures=None, dialog_error=None
    ):
        self.session = MemorySession()
        self.session.process_entities(list(cached))
        self.by_id = {entity.id: entity for entity in cached}
        self.resolve = resolve or {}
        self.dialogs = list(dialogs)
        self.failures = failures or {}
        self.dialog_error = dialog_error
        self.calls = []

    def is_user_authorized(self):
        return True

    def iter_dialogs(self, **kwargs):
        assert kwargs == dict(limit=None, archived=None, ignore_migrated=True)
        if self.dialog_error:
            raise self.dialog_error
        return iter(self.dialogs)

    def get_entity(self, value):
        self.calls.append(value)
        if isinstance(value, list):
            return [self._resolve(item) for item in value]
        return self._resolve(value)

    def _resolve(self, value):
        if isinstance(value, str):
            entity = self.resolve.get(value)
            if entity is None:
                try:
                    raise UsernameNotOccupiedError(request=None)
                except UsernameNotOccupiedError as exc:
                    raise ValueError(f'No user has "{value}" as username') from exc
        else:
            chat_id = getattr(value, "channel_id", None) or value.chat_id
            if chat_id in self.failures:
                raise self.failures[chat_id]
            entity = self.by_id[chat_id]
        if isinstance(entity, Exception):
            raise entity
        self.session.process_entities([entity])
        return entity


@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    create_schema(conn, detect_sqlite_features(conn))
    yield conn
    conn.close()


def add_target(conn, chat_id, *, username="public", chat_type="Channel"):
    conn.execute(
        "INSERT INTO chats(chat_id, chat_title, chat_username, chat_type) VALUES (?, ?, ?, ?)",
        (chat_id, f"Channel {chat_id}", username, chat_type),
    )
    conn.commit()


def seed_risk(conn, chat_id, **kwargs):
    row = RestrictedChatInventoryRow(
        chat_id=chat_id,
        chat_title=f"Channel {chat_id}",
        chat_type="Channel",
        risk_flags="scam",
        **kwargs,
    )
    replace_restricted_chat_scan_results(
        conn, [row], scan_job_id="old", scanned_at="2026-01-01T00:00:00+00:00"
    )


def run_scan(conn, primary, secondary=None, *, limit=40, save=True, gap=0):
    cfg = SimpleNamespace(
        session_name="primary",
        secondary_session_name="secondary" if secondary else "",
        admin_restricted_public_resolve_limit=limit,
        admin_restricted_public_resolve_gap_seconds=gap,
        flood_wait_switch_threshold=30,
    )
    logs = []
    clients = {"primary": primary, "secondary": secondary}
    with ExitStack() as stack:
        stack.enter_context(
            patch(
                "tg_harvest.admin_jobs.channel_inventory.call_with_conn",
                side_effect=lambda _get, fn, *args, **kwargs: fn(conn, *args, **kwargs),
            )
        )
        stack.enter_context(
            patch(
                "tg_harvest.admin_jobs.channel_inventory._create_isolated_worker_client",
                side_effect=lambda account_cfg, _worker: clients[
                    account_cfg.session_name
                ],
            )
        )
        stack.enter_context(
            patch("tg_harvest.admin_jobs.channel_inventory._disconnect_worker_client")
        )
        stack.enter_context(
            patch(
                "tg_harvest.admin_jobs.channel_inventory._cleanup_isolated_worker_session"
            )
        )
        sleep = stack.enter_context(
            patch("tg_harvest.admin_jobs.channel_inventory.time.sleep")
        )
        rows = _scan_restricted_chat_rows(
            cfg=cfg,
            get_conn_fn=lambda: conn,
            admin_job_append_log_fn=lambda _job, text: logs.append(text),
            job_id="scan",
        )
    if save:
        replace_restricted_chat_scan_results(
            conn, rows, scan_job_id="scan", scanned_at=datetime.now(UTC).isoformat()
        )
    return rows, logs, sleep


def test_forbidden_cached_entity_is_an_access_risk(db):
    add_target(db, 42)
    seed_risk(db, 42)
    client = Client(cached=[ChannelForbidden(42, 4200, "Forbidden")])
    rows, logs, _ = run_scan(db, client)
    assert len(rows) == 1
    assert set(rows[0].risk_flags.split("、")) == {"scam", "access_unavailable"}
    assert rows[0].access_source_account == "primary"
    assert rows[0].scan_status == "stale"
    assert rows.summary["checked"] == 1
    assert "成功刷新" not in logs[-1]


def test_legacy_content_risk_survives_repeated_forbidden_results(db):
    add_target(db, 42)
    seed_risk(db, 42)
    for _ in range(2):
        rows, _, _ = run_scan(
            db, Client(cached=[ChannelForbidden(42, 4200, "Forbidden")])
        )
        assert set(rows[0].risk_flags.split("、")) == {"scam", "access_unavailable"}
    rows, _, _ = run_scan(db, Client(cached=[channel(42)]))
    assert rows == []


def test_reassigned_username_preserves_old_risk_and_disables_wrong_link(db):
    add_target(db, 42)
    seed_risk(db, 42, chat_username="public")
    client = Client(resolve={"public": channel(99, username="public", fake=True)})
    rows, _, _ = run_scan(db, client)
    assert len(rows) == 1 and rows[0].chat_id == 42
    assert set(rows[0].risk_flags.split("、")) == {"scam", "identity_mismatch"}
    assert list_restricted_chat_scan_results(db)[0]["chat_username"] == ""
    assert list_stored_restricted_chat_scan_results(db)[0]["chat_username"] == "public"
    assert "fake" not in rows[0].risk_flags
    assert rows.summary["checked"] == 0 and rows.summary["failed"] == 1


def test_cached_id_takes_precedence_over_reassigned_username(db):
    add_target(db, 42)
    seed_risk(db, 42, chat_username="public")
    client = Client(cached=[channel(42)], resolve={"public": channel(99, fake=True)})
    rows, _, _ = run_scan(db, client)
    assert rows == []
    assert not any(isinstance(value, str) for value in client.calls)
    assert rows.summary["checked"] == 1


def test_failed_usernames_do_not_starve_later_targets_after_restart(db):
    for chat_id in range(1, 5):
        add_target(db, chat_id, username=f"public{chat_id}")
    first = Client()
    rows, _, _ = run_scan(db, first, limit=2)
    assert first.calls == ["public1", "public2"]
    assert all(
        row.risk_flags == "username_unavailable" and row.scan_status == "unverified"
        for row in rows
    )
    assert rows.summary["not_checked"] == 2
    second = Client(resolve={"public3": channel(3, scam=True), "public4": channel(4)})
    rows, _, _ = run_scan(db, second, limit=2)
    assert second.calls == ["public3", "public4"]
    assert any(row.chat_id == 3 and row.risk_flags == "scam" for row in rows)
    assert len(list_chat_risk_scan_states(db)) == 4


def test_zero_disables_username_resolution_and_gap_accepts_zero(db):
    add_target(db, 42)
    client = Client(resolve={"public": channel(42, scam=True)})
    rows, _, sleep = run_scan(db, client, limit=0)
    assert client.calls == [] and rows == []
    assert rows.summary["not_checked"] == 1
    assert not sleep.called


def test_zero_gap_does_not_restore_default_delay(db):
    add_target(db, 1, username="one")
    add_target(db, 2, username="two")
    rows, _, sleep = run_scan(
        db, Client(resolve={"one": channel(1), "two": channel(2)}), gap=0
    )
    assert rows.summary["checked"] == 2
    assert not sleep.called


def test_private_channel_is_checked_by_cached_id_without_username(db):
    add_target(db, -10042, username="")
    seed_risk(db, -10042)
    rows, _, _ = run_scan(db, Client(cached=[channel(42, scam=True)]), limit=0)
    assert len(rows) == 1 and rows[0].chat_id == -10042
    assert rows[0].risk_flags == "scam" and rows[0].scan_status == "checked"
    assert rows.summary["checked"] == 1


def test_private_channel_without_credentials_preserves_historical_timestamp(db):
    add_target(db, 42, username="")
    seed_risk(db, 42)
    rows, _, _ = run_scan(db, Client(), limit=0)
    assert len(rows) == 1 and rows[0].scan_status == "stale"
    assert rows[0].scanned_at == "2026-01-01T00:00:00+00:00"
    assert rows.summary["not_checked"] == 1


def test_external_access_risk_is_never_written_into_scan_snapshot(db):
    add_target(db, 42)
    record_chat_access_risk(
        db,
        chat_id=42,
        chat_title="Private",
        chat_username="public",
        chat_type="Channel",
        risk_type="access_denied",
        risk_message="Previously inaccessible",
        source_account="primary",
    )
    run_scan(db, Client(), limit=0)
    assert list_stored_restricted_chat_scan_results(db) == []
    assert resolve_chat_access_risk(db, chat_id=42)
    assert list_restricted_chat_scan_results(db) == []


def test_each_account_keeps_its_access_status_even_when_other_account_succeeds(db):
    add_target(db, 42)
    primary = Client(
        cached=[channel(42)], failures={42: ChannelPrivateError(request=None)}
    )
    secondary = Client(cached=[channel(42)])
    rows, _, _ = run_scan(db, primary, secondary)
    assert len(rows) == 1 and rows[0].risk_flags == "access_denied"
    assert rows[0].access_source_account == "primary"
    assert rows.summary["checked"] == 1
    assert len(list_chat_risk_scan_states(db)) == 2
    db.execute("UPDATE admin_chat_risk_scan_state SET next_retry_at = ''")
    db.commit()
    rows, _, _ = run_scan(
        db, Client(cached=[channel(42)]), Client(cached=[channel(42)])
    )
    assert rows == []
    assert list_restricted_chat_scan_results(db) == []


def test_success_does_not_stop_username_probe_for_other_account(db):
    add_target(db, 42)
    primary = Client(resolve={"public": channel(42)})
    secondary = Client(resolve={"public": ChannelPrivateError(request=None)})
    rows, _, _ = run_scan(db, primary, secondary)
    assert primary.calls == ["public"] and secondary.calls == ["public"]
    assert len(rows) == 1 and rows[0].access_source_account == "secondary"


def test_account_risk_survives_other_account_success_and_transient_failure(db):
    add_target(db, 42)
    run_scan(db, Client(cached=[channel(42, scam=True)]), Client(cached=[channel(42)]))
    primary = Client(
        cached=[channel(42)], failures={42: RuntimeError("unexpected network outage")}
    )
    rows, _, _ = run_scan(db, primary, Client(cached=[channel(42)]))
    assert (
        len(rows) == 1
        and rows[0].risk_flags == "scam"
        and rows[0].scan_status == "stale"
    )
    assert rows[0].scan_source_account == "primary"
    assert rows.summary["partial"] == 1


def test_flood_wait_stops_account_calls_and_survives_next_scan(db):
    for chat_id in range(1, 4):
        add_target(db, chat_id, username=f"public{chat_id}")
    wait = AccountFloodWaitError(seconds=180, threshold_seconds=30)
    primary = Client(resolve={"public1": wait})
    secondary = Client(resolve={f"public{i}": channel(i) for i in range(1, 4)})
    rows, _, _ = run_scan(db, primary, secondary)
    assert primary.calls == ["public1"]
    assert len(secondary.calls) == 3
    assert rows == [] and rows.summary["partial"] == 3
    next_primary = Client(resolve={f"public{i}": channel(i) for i in range(1, 4)})
    run_scan(db, next_primary, Client(cached=[channel(i) for i in range(1, 4)]))
    assert next_primary.calls == []


def test_dialog_flood_wait_preserves_risk_and_continues_other_account(db):
    add_target(db, 42)
    seed_risk(db, 42)
    primary = Client(
        dialog_error=AccountFloodWaitError(seconds=90, threshold_seconds=30)
    )
    rows, _, _ = run_scan(db, primary, Client(cached=[channel(42)]))
    assert (
        len(rows) == 1
        and rows[0].risk_flags == "scam"
        and rows[0].scan_status == "stale"
    )
    assert rows.summary["partial"] == 1
    assert any(
        state["status"] == "flood_wait" for state in list_chat_risk_scan_states(db)
    )


def test_one_bad_cached_entity_does_not_hide_other_entities(db):
    for chat_id in (1, 2, 3):
        add_target(db, chat_id, username=f"public{chat_id}")
    client = Client(
        cached=[channel(1, scam=True), channel(2), channel(3, fake=True)],
        failures={2: ChannelPrivateError(request=None)},
    )
    rows, _, _ = run_scan(db, client)
    assert {row.chat_id: row.risk_flags for row in rows} == {
        1: "scam",
        2: "access_denied",
        3: "fake",
    }
    assert rows.summary["checked"] == 3


def test_restriction_tuples_and_accounts_round_trip_without_false_pairs(db):
    add_target(db, 42)
    first = channel(
        42,
        restricted=True,
        restriction_reason=[
            RestrictionReason("all", "porno", "Sensitive"),
            RestrictionReason("android-ios", "terms", "Platform restriction"),
        ],
    )
    second = channel(42, fake=True)
    rows, _, _ = run_scan(db, Client(cached=[first]), Client(cached=[second]))
    result = list_restricted_chat_scan_results(db)[0]
    assert len(result["restriction_details"]) == 2
    assert [
        (item["platform"], item["reason"]) for item in result["restriction_details"]
    ] == [("all", "porno"), ("android-ios", "terms")]
    assert all(
        item["source_account"] == "primary" for item in result["restriction_details"]
    )
    assert set(rows[0].scan_source_account.split("、")) == {"primary", "secondary"}
    assert not entity_has_all_platform_terms_restriction(first)


def test_scanner_saves_progress_and_results_atomically(db):
    add_target(db, 42)
    seed_risk(db, 42)
    rows, _, _ = run_scan(db, Client(cached=[channel(42, fake=True)]), save=False)
    db.execute(
        "CREATE TRIGGER reject_scan_progress BEFORE INSERT ON admin_chat_risk_scan_state BEGIN SELECT RAISE(ABORT, 'failed state write'); END"
    )
    db.commit()
    with pytest.raises(sqlite3.IntegrityError, match="failed state write"):
        replace_restricted_chat_scan_results(
            db, rows, scan_job_id="scan", scanned_at="now"
        )
    assert list_stored_restricted_chat_scan_results(db)[0]["risk_flags"] == "scam"
    assert list_chat_risk_scan_states(db) == []
    assert get_chat_risk_scan_summary(db) is None


@pytest.mark.parametrize("bulk", [False, True])
def test_deleting_chat_also_removes_scan_progress(db, bulk):
    add_target(db, 42)
    run_scan(db, Client(cached=[channel(42, fake=True)]))
    assert list_chat_risk_scan_states(db)
    if bulk:
        _delete_empty_chats_data(db)
    else:
        _delete_chat_data(db, 42)
    assert list_chat_risk_scan_states(db) == []
    assert list_stored_restricted_chat_scan_results(db) == []


def test_existing_database_migrates_without_losing_old_risk():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE admin_restricted_chats(chat_id INTEGER PRIMARY KEY, chat_title TEXT NOT NULL, risk_flags TEXT, scanned_at TEXT NOT NULL)"
    )
    conn.execute(
        "INSERT INTO admin_restricted_chats VALUES(42, 'Old risk', 'scam', '2026-01-01')"
    )
    conn.commit()
    try:
        create_schema(conn, detect_sqlite_features(conn))
        create_schema(conn, detect_sqlite_features(conn))
        risk = list_stored_restricted_chat_scan_results(conn)[0]
        assert risk["risk_flags"] == "scam"
        assert json.loads(risk["restriction_details"]) == []
        assert risk["scan_source_account"] == ""
        assert list_chat_risk_scan_states(conn) == []
    finally:
        conn.close()
