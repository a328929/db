from collections.abc import Callable
from contextlib import suppress
from typing import Any

from tg_harvest.admin_jobs.common import call_with_conn as _call_with_conn
from tg_harvest.domain.coerce import clean_text as _clean_text


def _load_required_record(
    *,
    get_conn_fn: Callable[[], Any],
    load_fn: Callable[..., Any],
    missing_message: str,
    **kwargs: Any,
) -> Any:
    record = _call_with_conn(get_conn_fn, load_fn, **kwargs)
    if record is None:
        raise RuntimeError(missing_message)
    return record


def _update_required_record(
    *,
    get_conn_fn: Callable[[], Any],
    update_fn: Callable[..., Any],
    missing_message: str,
    **kwargs: Any,
) -> Any:
    record = _call_with_conn(get_conn_fn, update_fn, **kwargs)
    if record is None:
        raise RuntimeError(missing_message)
    return record


def _try_update_record(
    *,
    get_conn_fn: Callable[[], Any],
    update_fn: Callable[..., Any],
    **kwargs: Any,
) -> None:
    with suppress(Exception):
        _call_with_conn(get_conn_fn, update_fn, **kwargs)
