from types import SimpleNamespace

from tg_harvest.admin_jobs.clone_preflight import _build_deep_preflight_outcome
from tg_harvest.domain.clone_target_permissions import (
    clone_target_send_permission,
    clone_target_write_was_rejected,
)


def test_target_write_rejection_recognizes_telethon_admin_privileges_message():
    error = (
        "Chat admin privileges are required to do that in the specified chat "
        "(for example, in a channel which is not yours), or invalid permissions "
        "used for the channel or group"
    )

    assert clone_target_write_was_rejected(error)


def test_broadcast_channel_requires_creator_or_post_messages_permission():
    non_admin = SimpleNamespace(
        broadcast=True,
        megagroup=False,
        creator=False,
        admin_rights=None,
    )
    publisher = SimpleNamespace(
        broadcast=True,
        megagroup=False,
        creator=False,
        admin_rights=SimpleNamespace(post_messages=True),
    )

    assert clone_target_send_permission(non_admin) == "blocked"
    assert clone_target_send_permission(publisher) == "ok"


def test_megagroup_allows_members_unless_send_messages_is_restricted():
    writable_member = SimpleNamespace(
        broadcast=False,
        megagroup=True,
        creator=False,
        admin_rights=None,
        banned_rights=None,
        default_banned_rights=SimpleNamespace(send_messages=False),
    )
    restricted_member = SimpleNamespace(
        broadcast=False,
        megagroup=True,
        creator=False,
        admin_rights=None,
        banned_rights=SimpleNamespace(send_messages=True),
        default_banned_rights=SimpleNamespace(send_messages=False),
    )

    assert clone_target_send_permission(writable_member) == "ok"
    assert clone_target_send_permission(restricted_member) == "blocked"


def test_preflight_selects_a_writable_owner_over_an_accessible_channel_member():
    accounts = [
        {
            "account": "primary",
            "session_status": "ok",
            "source_access": "ok",
            "source_forwarding_permission": "ok",
            "target_access": "ok",
            "target_send_permission": "blocked",
            "relay_access": "not_configured",
            "relay_send_permission": "unknown",
            "relay_safety": "not_configured",
            "source_latest_message": {"message_id": 10},
            "source_latest_error": "",
        },
        {
            "account": "secondary",
            "session_status": "ok",
            "source_access": "missing",
            "source_forwarding_permission": "unknown",
            "target_access": "ok",
            "target_send_permission": "ok",
            "relay_access": "not_configured",
            "relay_send_permission": "unknown",
            "relay_safety": "not_configured",
            "source_latest_message": {},
            "source_latest_error": "",
        },
    ]

    outcome = _build_deep_preflight_outcome(
        run={"run_id": "run-1", "target_chat_id": 777},
        accounts=accounts,
        network_access_checked=True,
        source_snapshot={"latest_message_id": 10, "message_count": 10},
        cfg=SimpleNamespace(),
    )

    assert outcome["capabilities"]["target_write_account"] == "secondary"
    assert outcome["text_strategy"] == "database_replay"
    assert not any("没有账号拥有" in item for item in outcome["blocking_issues"])
    assert any("primary" in item for item in outcome["warnings"])


def test_relay_target_account_only_needs_read_access_to_relay():
    accounts = [
        {
            "account": "primary",
            "session_status": "ok",
            "source_access": "ok",
            "source_forwarding_permission": "ok",
            "target_access": "missing",
            "target_send_permission": "unknown",
            "relay_access": "ok",
            "relay_send_permission": "ok",
            "relay_cleanup_permission": "ok",
            "relay_safety": "private_channel",
            "source_latest_message": {"message_id": 10},
            "source_latest_error": "",
        },
        {
            "account": "secondary",
            "session_status": "ok",
            "source_access": "missing",
            "source_forwarding_permission": "unknown",
            "target_access": "ok",
            "target_send_permission": "ok",
            "relay_access": "ok",
            "relay_send_permission": "blocked",
            "relay_cleanup_permission": "blocked",
            "relay_safety": "private_channel",
            "source_latest_message": {},
            "source_latest_error": "",
        },
    ]

    outcome = _build_deep_preflight_outcome(
        run={"run_id": "run-1", "source_chat_id": 100, "target_chat_id": 777},
        accounts=accounts,
        network_access_checked=True,
        source_snapshot={"latest_message_id": 10, "message_count": 10},
        cfg=SimpleNamespace(
            clone_relay_chat_id=999,
            clone_relay_chat_username="",
        ),
    )

    relay = outcome["capabilities"]["media_relay"]
    assert relay["enabled"] is True
    assert relay["source_account"] == "primary"
    assert relay["target_account"] == "secondary"


def test_preflight_blocks_relay_that_matches_source_or_has_extra_viewers():
    base_account = {
        "session_status": "ok",
        "source_forwarding_permission": "ok",
        "relay_access": "ok",
        "relay_send_permission": "ok",
        "relay_cleanup_permission": "ok",
        "relay_safety": "private_channel",
        "source_latest_error": "",
    }
    accounts = [
        {
            **base_account,
            "account": "primary",
            "source_access": "ok",
            "target_access": "missing",
            "target_send_permission": "unknown",
            "source_latest_message": {"message_id": 10},
        },
        {
            **base_account,
            "account": "secondary",
            "source_access": "missing",
            "target_access": "ok",
            "target_send_permission": "ok",
            "source_latest_message": {},
        },
    ]
    cfg = SimpleNamespace(clone_relay_chat_id=999, clone_relay_chat_username="")

    same_chat = _build_deep_preflight_outcome(
        run={"run_id": "run-1", "source_chat_id": 999, "target_chat_id": 777},
        accounts=accounts,
        network_access_checked=True,
        source_snapshot={"latest_message_id": 10, "message_count": 10},
        cfg=cfg,
    )
    assert any("不能与源群或克隆目标相同" in item for item in same_chat["blocking_issues"])
    assert same_chat["capabilities"]["media_relay"]["enabled"] is False

    accounts[0]["relay_safety"] = "unsafe_extra_participants"
    accounts[1]["relay_safety"] = "unsafe_extra_participants"
    extra_viewers = _build_deep_preflight_outcome(
        run={"run_id": "run-1", "source_chat_id": 100, "target_chat_id": 777},
        accounts=accounts,
        network_access_checked=True,
        source_snapshot={"latest_message_id": 10, "message_count": 10},
        cfg=cfg,
    )
    assert any("额外成员" in item for item in extra_viewers["blocking_issues"])
