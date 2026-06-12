from datetime import UTC, datetime

from tg_harvest.domain.chat_inventory import (
    ChatInventoryRow,
    entity_has_all_platform_terms_restriction,
    filter_database_chats_to_joined,
    find_database_chats_not_joined,
    find_missing_joined_chats,
    find_restricted_joined_chats,
    load_joined_chat_inventory,
    write_missing_chat_report,
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


def test_find_database_chats_not_joined_filters_by_joined_identity():
    rows = find_database_chats_not_joined(
        [
            {
                "chat_id": 1,
                "chat_title": "Absent",
                "chat_username": "absent",
                "chat_type": "Channel",
                "message_count": 12,
                "last_seen_at": "2026-01-01 00:00:00",
            },
            {
                "chat_id": -1002,
                "chat_title": "Joined",
                "message_count": 3,
                "last_seen_at": "2026-01-02 00:00:00",
            },
        ],
        joined_chat_ids={2},
    )

    assert [row["chat_id"] for row in rows] == [1]
    assert rows[0]["chat_username"] == "absent"
    assert rows[0]["message_count"] == 12
    assert rows[0]["scan_reason"] == "账号未加入"


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

    absent_rows = find_database_chats_not_joined(
        [
            {"chat_id": 1, "chat_title": "Restricted"},
            {"chat_id": 2, "chat_title": "Forbidden"},
            {"chat_id": 3, "chat_title": "Visible"},
        ],
        joined_chat_ids={1, 3},
        unavailable_chat_reasons={
            2: "Telegram 返回该会话不可访问",
        },
    )

    assert [row["chat_id"] for row in absent_rows] == [2]
    assert absent_rows[0]["scan_reason"] == "Telegram 返回该会话不可访问"


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


def test_write_missing_chat_report(tmp_path):
    output = write_missing_chat_report(
        [ChatInventoryRow(chat_id=100, chat_title="Test Chat")],
        tmp_path / "missing.txt",
    )

    assert output.exists()
    assert output.read_text(encoding="utf-8") == "Test Chat | ID: 100\n"
