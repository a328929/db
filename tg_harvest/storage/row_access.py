from typing import Any

from tg_harvest.domain.coerce import safe_int


def scan_row_value(row: Any, key: str, default: Any = "") -> Any:
    if row is None:
        return default
    if isinstance(row, dict):
        value = row.get(key, default)
    else:
        # sqlite3.Row exposes named values through ``__getitem__`` rather
        # than attributes.  Keep dataclass/object scans working as well.
        try:
            value = row[key]
        except (IndexError, KeyError, TypeError):
            value = getattr(row, key, default)
    return default if value is None else value


def scan_row_int(row: Any, key: str, default: int = 0) -> int:
    return safe_int(scan_row_value(row, key, default), default)


def row_int(row: Any, key: Any, default: int = 0) -> int:
    if row is None:
        return int(default)
    try:
        value = row[key]
    except (IndexError, KeyError, TypeError):
        try:
            value = getattr(row, key)
        except AttributeError:
            return int(default)
    return safe_int(value, default)
