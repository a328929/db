from tg_harvest.domain.chat_inventory import ChatInventoryRow
from tg_harvest.domain.chat_inventory import find_database_chats_not_joined
from tg_harvest.domain.chat_inventory import find_missing_joined_chats
from tg_harvest.domain.chat_inventory import load_joined_chat_inventory
from tg_harvest.domain.chat_inventory import write_missing_chat_report


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
    ):
        self.id = chat_id
        self.left = left
        self.deactivated = deactivated
        self.title = title
        self.restricted = restricted
        self.restriction_reason = restriction_reason or []


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
    ):
        self.title = title
        self.is_group = is_group
        self.is_channel = is_channel
        self.entity = entity or _Entity(chat_id, left=left, title=title)


class _ChannelForbidden:
    def __init__(self, chat_id, *, title):
        self.id = chat_id
        self.title = title


class _RestrictionReason:
    def __init__(self, *, text="", reason=""):
        self.text = text
        self.reason = reason


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


def test_unavailable_joined_chats_are_treated_as_absent():
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

    assert "violated Telegram" in inventory_by_id[1].unavailable_reason
    assert inventory_by_id[2].unavailable_reason == "Telegram 返回该会话不可访问"

    missing_rows = find_missing_joined_chats(dialogs, known_chat_ids=set())
    assert [row.chat_id for row in missing_rows] == [3]

    absent_rows = find_database_chats_not_joined(
        [
            {"chat_id": 1, "chat_title": "Restricted"},
            {"chat_id": 2, "chat_title": "Forbidden"},
            {"chat_id": 3, "chat_title": "Visible"},
        ],
        joined_chat_ids={3},
        unavailable_chat_reasons={
            1: "Telegram 限制显示：This channel can't be displayed because it violated Telegram's Terms of Service.",
            2: "Telegram 返回该会话不可访问",
        },
    )

    assert [row["chat_id"] for row in absent_rows] == [1, 2]
    assert "violated Telegram" in absent_rows[0]["scan_reason"]
    assert absent_rows[1]["scan_reason"] == "Telegram 返回该会话不可访问"


def test_write_missing_chat_report(tmp_path):
    output = write_missing_chat_report(
        [ChatInventoryRow(chat_id=100, chat_title="Test Chat")],
        tmp_path / "missing.txt",
    )

    assert output.exists()
    assert output.read_text(encoding="utf-8") == "Test Chat | ID: 100\n"
