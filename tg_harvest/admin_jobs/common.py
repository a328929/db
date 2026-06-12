import threading
from collections.abc import Callable
from typing import Any

from tg_harvest.domain.chat_ids import candidate_chat_entity_ids


def admin_error_message(exc: Exception) -> str:
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
    client: Any, chat_id: int, chat_username: str | None = None
) -> Any:
    try:
        return client.get_entity(chat_id)
    except Exception as exc:
        err_msg = str(exc).lower()
        if "could not find the input entity" not in err_msg:
            raise

        for fallback_id in candidate_chat_entity_ids(chat_id):
            if fallback_id == int(chat_id):
                continue
            try:
                return client.get_entity(fallback_id)
            except Exception:
                pass

        if chat_username:
            return client.get_entity(chat_username)
        raise exc


def finish_job_heartbeat(heartbeat_stop, heartbeat_thread) -> None:
    heartbeat_stop.set()
    heartbeat_thread.join(timeout=1.0)


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
    conn = None
    try:
        conn = get_conn_fn()
        cur = conn.cursor()
        cur.execute("SELECT chat_username FROM chats WHERE chat_id = ?", (chat_id,))
        row = cur.fetchone()
        if row is None:
            return None
        username = row["chat_username"]
        return str(username) if username else None
    except Exception:
        return None
    finally:
        if conn:
            conn.close()
