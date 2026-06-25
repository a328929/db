import threading
from collections.abc import Callable
from typing import Any

from tg_harvest.admin_jobs.core import (
    _admin_job_heartbeat,
    _admin_job_update_progress,
    job_context,
)
from tg_harvest.admin_jobs.sessions import _start_job_heartbeat, bind_client_event_loop
from tg_harvest.domain.chat_ids import candidate_chat_entity_ids
from tg_harvest.domain.coerce import clean_username
from tg_harvest.ingest.flood_wait import is_flood_wait_error


class UsernameFallbackSkippedError(RuntimeError):
    def __init__(self, chat_id: int, chat_username: str):
        self.chat_id = int(chat_id)
        self.chat_username = clean_username(chat_username)
        super().__init__(
            "本地实体缓存未命中，未执行公开 username 解析"
            f": chat_id={self.chat_id}, username={self.chat_username}"
        )


def is_entity_lookup_miss_error(exc: Exception) -> bool:
    if isinstance(exc, UsernameFallbackSkippedError):
        return True
    err_str = f"{type(exc).__name__}: {exc}".lower()
    return "not exist" in err_str or "could not find the input entity" in err_str


def admin_error_message(exc: Exception) -> str:
    if isinstance(exc, UsernameFallbackSkippedError):
        return "账号本地实体缓存未命中，未执行公开 username 解析"
    err_str = str(exc).lower()
    if "channelprivate" in err_str:
        return "您已被踢出该群组，或该群组已转为私有且您不在其中"
    if "userbanned" in err_str:
        return "您的账号已被该群组/频道封禁"
    if "not exist" in err_str or "could not find the input entity" in err_str:
        return "该群组/频道已解散或不存在"
    if "chatrestrictd" in err_str or "chatwriteforbidden" in err_str:
        return "账号被限制或禁言"
    if "floodwait" in err_str:
        return "触发 Telegram 频控限制，请稍后再试"
    return f"{type(exc).__name__}: {exc}"


def resolve_chat_entity(
    client: Any,
    chat_id: int,
    chat_username: str | None = None,
    *,
    allow_username_fallback: bool = True,
    username_fallback_gate: Callable[[], bool] | None = None,
) -> Any:
    with bind_client_event_loop(client):
        try:
            return client.get_entity(chat_id)
        except Exception as exc:
            if is_flood_wait_error(exc):
                raise
            err_msg = str(exc).lower()
            if "could not find the input entity" not in err_msg:
                raise

            for fallback_id in candidate_chat_entity_ids(chat_id):
                if fallback_id == int(chat_id):
                    continue
                try:
                    return client.get_entity(fallback_id)
                except Exception as fallback_exc:
                    if is_flood_wait_error(fallback_exc):
                        raise
                    pass

            if chat_username and allow_username_fallback:
                if username_fallback_gate is not None and not username_fallback_gate():
                    raise UsernameFallbackSkippedError(chat_id, chat_username) from exc
                return client.get_entity(chat_username)
            if chat_username and not allow_username_fallback:
                raise UsernameFallbackSkippedError(chat_id, chat_username) from exc
            raise exc


def finish_job_heartbeat(heartbeat_stop, heartbeat_thread) -> None:
    heartbeat_stop.set()
    heartbeat_thread.join(timeout=1.0)


def start_admin_job_heartbeat(job_id: str):
    job_context.set(str(job_id))
    return _start_job_heartbeat(job_id, _admin_job_heartbeat)


def mark_admin_job_running(
    job_id: str,
    *,
    admin_job_set_status_fn: Callable[[str, str], bool],
) -> None:
    admin_job_set_status_fn(job_id, "running")


def update_admin_job_progress(
    job_id: str,
    current: int,
    *,
    total: int | None,
    stage: str,
) -> None:
    _admin_job_update_progress(
        job_id,
        current,
        total=total,
        stage=stage,
        log_step=0,
        auto_log=False,
    )


def call_with_conn(
    get_conn_fn: Callable[[], Any],
    fn: Callable[..., Any],
    /,
    *args: Any,
    **kwargs: Any,
) -> Any:
    conn = get_conn_fn()
    try:
        return fn(conn, *args, **kwargs)
    finally:
        conn.close()


def start_admin_job_thread(target, *args, **kwargs):
    thread = threading.Thread(
        target=target,
        args=args,
        kwargs=kwargs,
        daemon=True,
    )
    thread.start()
    return thread


def read_chat_username(
    get_conn_fn: Callable[[], Any], chat_id: int
) -> str | None:
    try:
        def _load_chat_username(conn: Any, target_chat_id: int) -> str | None:
            cur = conn.cursor()
            cur.execute(
                "SELECT chat_username FROM chats WHERE chat_id = ?",
                (target_chat_id,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            username = row["chat_username"]
            return str(username) if username else None

        username = call_with_conn(get_conn_fn, _load_chat_username, chat_id)
        return str(username) if username else None
    except Exception:
        return None
