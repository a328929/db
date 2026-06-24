from typing import Any


def clean_text(value: Any) -> str:
    return str(value or "").strip()


def optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
