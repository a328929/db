from tg_harvest.domain.chat_inventory import ChatInventoryRow
from tg_harvest.domain.chat_inventory import find_database_chats_not_joined
from tg_harvest.domain.chat_inventory import find_missing_joined_chats
from tg_harvest.domain.chat_inventory import write_missing_chat_report


class _Entity:
    def __init__(self, chat_id, *, left=False, deactivated=False, title=None):
        self.id = chat_id
        self.left = left
        self.deactivated = deactivated
        self.title = title


class _Dialog:
    def __init__(self, *, chat_id, title, is_group=False, is_channel=False, left=False):
        self.title = title
        self.is_group = is_group
        self.is_channel = is_channel
        self.entity = _Entity(chat_id, left=left, title=title)


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


def test_write_missing_chat_report(tmp_path):
    output = write_missing_chat_report(
        [ChatInventoryRow(chat_id=100, chat_title="Test Chat")],
        tmp_path / "missing.txt",
    )

    assert output.exists()
    assert output.read_text(encoding="utf-8") == "Test Chat | ID: 100\n"
