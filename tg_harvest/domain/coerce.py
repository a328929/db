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


def safe_int(value: Any, default: int = 0) -> int:
    parsed = optional_int(value)
    return parsed if parsed is not None else int(default)
