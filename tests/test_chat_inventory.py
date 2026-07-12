from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import patch

from tg_harvest.admin_jobs.channel_inventory import (
    _merge_restricted_chat_row,
    _scan_restricted_chat_rows,
)
from tg_harvest.domain.chat_inventory import (
    ChatInventoryRow,
    RestrictedChatInventoryRow,
    entity_has_all_platform_terms_restriction,
    filter_database_chats_to_joined,
    find_missing_joined_chats,
    find_restricted_joined_chats,
    load_joined_chat_inventory,
)


class _Entity:
    def __init__(
        self,
        chat_id,
        *,
        left=False,
        deactivated=False,
        title=None,
        restricted=False,
        restriction_reason=None,
        scam=False,
        fake=False,
    ):
        self.id = chat_id
        self.left = left
        self.deactivated = deactivated
        self.title = title
        self.restricted = restricted
        self.restriction_reason = restriction_reason or []
        self.scam = scam
        self.fake = fake


class Channel(_Entity):
    pass


class _CachedSession:
    def __init__(self, cached_usernames):
        self.cached_usernames = set(cached_usernames)

    def get_input_entity(self, username):
        if username not in self.cached_usernames:
            raise ValueError(username)
        return username


class _RiskScanClient:
    def __init__(self, dialogs, resolved_entities):
        self._dialogs = list(dialogs)
        self._resolved_entities = dict(resolved_entities)
        self.session = _CachedSession(self._resolved_entities)

    def is_user_authorized(self):
        return True

    def iter_dialogs(self, **_kwargs):
        return iter(self._dialogs)

    def get_entity(self, values):
        if isinstance(values, list):
            return [self._resolved_entities[value] for value in values]
        return self._resolved_entities[values]


class _Dialog:
    def __init__(
        self,
        *,
        chat_id,
        title,
        is_group=False,
        is_channel=False,
        left=False,
        entity=None,
        message=None,
    ):
        self.title = title
        self.is_group = is_group
        self.is_channel = is_channel
        self.entity = entity or _Entity(chat_id, left=left, title=title)
        self.message = message


class _ChannelForbidden:
    def __init__(self, chat_id, *, title):
        self.id = chat_id
        self.title = title


class _RestrictionReason:
    def __init__(self, *, text="", reason="", platform=""):
        self.text = text
        self.reason = reason
        self.platform = platform


def test_find_missing_joined_chats_filters_and_sorts():
    dialogs = [
        _Dialog(chat_id=3, title="Beta", is_channel=True),
        _Dialog(chat_id=1, title="Alpha", is_group=True),
        _Dialog(chat_id=2, title="Known", is_group=True),
        _Dialog(chat_id=4, title="Left", is_group=True, left=True),
        _Dialog(chat_id=3, title="Beta Duplicate", is_channel=True),
    ]

    rows = find_missing_joined_chats(dialogs, known_chat_ids={2})

    assert [row.chat_id for row in rows] == [1, 3]
    assert [row.chat_title for row in rows] == ["Alpha", "Beta"]
    assert [row.chat_type for row in rows] == ["_Entity", "_Entity"]


def test_joined_chat_inventory_extracts_dialog_last_message_time():
    dialogs = [
        _Dialog(
            chat_id=1,
            title="With Last Message",
            is_channel=True,
            message=type(
                "Message",
                (),
                {"date": datetime(2026, 4, 1, 10, 0, 0, tzinfo=UTC)},
            )(),
        )
    ]

    rows = load_joined_chat_inventory(dialogs)

    assert rows[0].last_message_at == "2026-04-01 10:00:00"
    assert rows[0].last_message_ts == 1775037600


def test_filter_database_chats_to_joined_keeps_only_accessible_dialogs():
    database_rows = [
        {"chat_id": 1, "chat_title": "Joined"},
        {"chat_id": 2, "chat_title": "Recovered But Not Joined"},
        {"chat_id": -1003, "chat_title": "Stored As Entity Id"},
        {"chat_id": 4, "chat_title": "Forbidden"},
    ]
    joined_rows = [
        ChatInventoryRow(chat_id=1, chat_title="Joined"),
        ChatInventoryRow(chat_id=3, chat_title="Stored As Positive Id"),
        ChatInventoryRow(
            chat_id=4,
            chat_title="Forbidden",
            unavailable_reason="Telegram 返回该会话不可访问",
        ),
    ]

    rows = filter_database_chats_to_joined(database_rows, joined_rows)

    assert [row["chat_id"] for row in rows] == [1, -1003]


def test_restricted_joined_chats_still_count_as_joined():
    dialogs = [
        _Dialog(
            chat_id=1,
            title="Restricted",
            is_channel=True,
            entity=_Entity(
                1,
                title="Restricted",
                restricted=True,
                restriction_reason=[
                    _RestrictionReason(
                        text="This channel can't be displayed because it violated Telegram's Terms of Service."
                    )
                ],
            ),
        ),
        _Dialog(
            chat_id=2,
            title="Forbidden",
            entity=_ChannelForbidden(2, title="Forbidden"),
        ),
        _Dialog(chat_id=3, title="Visible", is_channel=True),
    ]

    inventory_rows = load_joined_chat_inventory(dialogs)
    inventory_by_id = {row.chat_id: row for row in inventory_rows}

    assert inventory_by_id[1].unavailable_reason == ""
    assert inventory_by_id[2].unavailable_reason == "Telegram 返回该会话不可访问"

    missing_rows = find_missing_joined_chats(dialogs, known_chat_ids=set())
    assert [row.chat_id for row in missing_rows] == [1, 3]

def test_find_restricted_joined_chats_reports_reasons_and_risk_flags():
    dialogs = [
        _Dialog(
            chat_id=1,
            title="Restricted",
            is_channel=True,
            entity=_Entity(
                1,
                title="Restricted",
                restricted=True,
                restriction_reason=[
                    _RestrictionReason(
                        platform="all",
                        reason="porn",
                        text="This channel can't be displayed because it was used to spread pornographic content.",
                    )
                ],
            ),
        ),
        _Dialog(
            chat_id=2,
            title="Scam",
            is_channel=True,
            entity=_Entity(2, title="Scam", scam=True),
        ),
        _Dialog(
            chat_id=3,
            title="Forbidden",
            entity=_ChannelForbidden(3, title="Forbidden"),
        ),
        _Dialog(chat_id=4, title="Visible", is_channel=True),
    ]

    rows = find_restricted_joined_chats(dialogs)
    rows_by_id = {row.chat_id: row for row in rows}

    assert [row.chat_id for row in rows] == [1, 2]
    assert rows_by_id[1].restriction_platforms == "all"
    assert rows_by_id[1].restriction_reasons == "porn"
    assert "pornographic content" in rows_by_id[1].restriction_text
    assert rows_by_id[1].risk_flags == "restricted"
    assert rows_by_id[2].risk_flags == "scam"


def test_merge_restricted_chat_rows_preserves_risks_seen_by_both_accounts():
    primary = RestrictedChatInventoryRow(
        chat_id=1,
        chat_title="Risk",
        chat_username="risk",
        chat_type="Channel",
        restriction_platforms="all",
        restriction_reasons="porn",
        restriction_text="Content restricted",
        risk_flags="restricted",
        last_message_ts=10,
    )
    secondary = RestrictedChatInventoryRow(
        chat_id=1,
        chat_title="Risk",
        chat_type="Channel",
        restriction_platforms="android",
        restriction_reasons="copyright",
        restriction_text="Copyright report",
        risk_flags="scam、fake",
        last_message_ts=20,
    )

    merged = _merge_restricted_chat_row(primary, secondary)

    assert merged.chat_username == "risk"
    assert merged.restriction_platforms == "all、android"
    assert merged.restriction_reasons == "porn、copyright"
    assert merged.restriction_text == "Content restricted；Copyright report"
    assert merged.risk_flags == "restricted、scam、fake"


def test_restricted_scan_combines_joined_and_cached_public_unjoined_results():
    joined_entity = Channel(
        1,
        title="Joined Risk",
        restricted=True,
        restriction_reason=[_RestrictionReason(platform="all", reason="porn")],
    )
    public_entity = Channel(2, title="Public Risk", scam=True)
    client = _RiskScanClient(
        [
            _Dialog(
                chat_id=1,
                title="Joined Risk",
                is_channel=True,
                entity=joined_entity,
            )
        ],
        {"public-risk": public_entity},
    )
    database_rows = [
        {
            "chat_id": 1,
            "chat_title": "Joined Risk",
            "chat_username": "joined-risk",
            "chat_type": "Channel",
        },
        {
            "chat_id": 2,
            "chat_title": "Public Risk",
            "chat_username": "public-risk",
            "chat_type": "Channel",
        },
    ]

    def fake_call_with_conn(_get_conn_fn, fn, *args, **kwargs):
        del args, kwargs
        if fn.__name__ == "list_database_channels":
            return database_rows
        if fn.__name__ == "list_restricted_chat_scan_results":
            return []
        raise AssertionError(fn.__name__)

    cfg = SimpleNamespace(
        session_name="primary",
        secondary_session_name="",
        admin_restricted_public_resolve_limit=0,
        admin_restricted_public_resolve_gap_seconds=0,
    )
    with (
        patch(
            "tg_harvest.admin_jobs.channel_inventory.call_with_conn",
            side_effect=fake_call_with_conn,
        ),
        patch(
            "tg_harvest.admin_jobs.channel_inventory._create_isolated_worker_client",
            return_value=client,
        ),
        patch(
            "tg_harvest.admin_jobs.channel_inventory._disconnect_worker_client"
        ),
        patch(
            "tg_harvest.admin_jobs.channel_inventory._cleanup_isolated_worker_session"
        ),
    ):
        rows = _scan_restricted_chat_rows(
            cfg=cfg,
            get_conn_fn=lambda: None,
            admin_job_append_log_fn=lambda *_args: None,
            job_id="job-risk",
        )

    rows_by_id = {row.chat_id: row for row in rows}
    assert set(rows_by_id) == {1, 2}
    assert rows_by_id[1].membership_scope == "joined"
    assert rows_by_id[1].risk_flags == "restricted"
    assert rows_by_id[2].membership_scope == "public_unjoined"
    assert rows_by_id[2].risk_flags == "scam"


def test_all_platform_terms_restriction_marks_entity_unavailable():
    assert entity_has_all_platform_terms_restriction(
        _Entity(
            1,
            restriction_reason=[
                _RestrictionReason(platform="all", reason="terms"),
            ],
        )
    )
    assert entity_has_all_platform_terms_restriction(
        _Entity(
            2,
            restriction_reason=[
                _RestrictionReason(platform="all", reason="tos"),
            ],
        )
    )
    assert not entity_has_all_platform_terms_restriction(
        _Entity(
            3,
            restriction_reason=[
                _RestrictionReason(platform="all", reason="porn"),
            ],
        )
    )
    assert not entity_has_all_platform_terms_restriction(
        _Entity(
            4,
            restriction_reason=[
                _RestrictionReason(platform="ios", reason="terms"),
            ],
        )
    )
