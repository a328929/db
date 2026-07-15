import re
from dataclasses import dataclass

CLONE_MESSAGE_DELETE_MAX_COUNT = 100_000
CLONE_MESSAGE_DELETE_MAX_MESSAGE_ID = 2_147_483_647

_DELETE_SELECTION_RE = re.compile(r"^(?P<first>[1-9]\d*)(?:-(?P<last>[1-9]\d*))?$")


@dataclass(frozen=True)
class CloneMessageDeleteSelection:
    mode: str
    requested_count: int
    first_message_id: int | None = None
    last_message_id: int | None = None

    @property
    def description(self) -> str:
        if self.mode == "latest":
            return (
                f"最后 {self.requested_count} 条已克隆源消息"
                "（按源消息 ID 从新到旧回滚）"
            )
        return (
            f"目标消息 ID {self.first_message_id}-{self.last_message_id}"
            "（仅清理目标消息，不修改克隆映射）"
        )


def parse_clone_message_delete_selection(value: object) -> CloneMessageDeleteSelection:
    text = str(value or "").strip()
    match = _DELETE_SELECTION_RE.fullmatch(text)
    if match is None:
        raise ValueError("删除规则必须是正整数或正整数区间，例如 1000 或 200-1000")

    first = int(match.group("first"))
    last_text = match.group("last")
    if last_text is None:
        _validate_requested_count(first)
        return CloneMessageDeleteSelection(
            mode="latest",
            requested_count=first,
        )

    last = int(last_text)
    if first > last:
        raise ValueError("删除区间的起始消息 ID 不能大于结束消息 ID")
    if last > CLONE_MESSAGE_DELETE_MAX_MESSAGE_ID:
        raise ValueError("消息 ID 超出 Telegram 支持范围")

    requested_count = last - first + 1
    _validate_requested_count(requested_count)
    return CloneMessageDeleteSelection(
        mode="range",
        requested_count=requested_count,
        first_message_id=first,
        last_message_id=last,
    )


def _validate_requested_count(value: int) -> None:
    if value <= 0:
        raise ValueError("删除消息数量必须为正整数")
    if value > CLONE_MESSAGE_DELETE_MAX_COUNT:
        raise ValueError(
            f"单次最多删除 {CLONE_MESSAGE_DELETE_MAX_COUNT} 条消息，请分批处理"
        )
