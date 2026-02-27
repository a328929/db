import re

with open('/root/db/tg_harvest/admin_jobs_runners.py', 'r', encoding='utf-8') as f:
    content = f.read()

helper_code = """
def _admin_update_all_chats(
    job_id, client, incremental, get_conn_fn, admin_job_append_log_fn
):
    conn = get_conn_fn()
    try:
        cur = conn.cursor()
        cur.execute(
            '''
            SELECT chat_id, chat_title
            FROM chats
            ORDER BY chat_title COLLATE NOCASE ASC, chat_id ASC
            '''
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        admin_job_append_log_fn(job_id, "当前无可更新群聊，任务结束")
        return False

    total = len(rows)
    success_count = 0
    failed_count = 0
    failed_chats = []
    total_added_messages = 0
    admin_job_append_log_fn(job_id, f"读取到 {total} 个群组，开始逐个执行增量采集")
    
    for idx, row in enumerate(rows, start=1):
        current_chat_id = int(row["chat_id"])
        current_chat_title = str(row["chat_title"] or current_chat_id)
        try:
            before_count = _admin_get_chat_message_count(get_conn_fn, current_chat_id)
            _admin_process_single_chat_update(
                job_id=job_id,
                client=client,
                get_conn_fn=get_conn_fn,
                admin_job_append_log_fn=admin_job_append_log_fn,
                chat_id=current_chat_id,
                chat_title=current_chat_title,
                idx=idx,
                total=total,
            )
            after_count = _admin_get_chat_message_count(get_conn_fn, current_chat_id)
            added_count = max(0, after_count - before_count)
            total_added_messages += added_count
            success_count += 1
            admin_job_append_log_fn(
                job_id,
                f"[{idx}/{total}] {current_chat_title} 新增 {added_count} 条消息",
            )
        except Exception as chat_exc:
            failed_count += 1
            failed_chats.append(current_chat_title)
            admin_job_append_log_fn(
                job_id,
                f"[{idx}/{total}] 增量采集失败：群组={current_chat_title}，群组ID={current_chat_id}，错误={chat_exc}",
            )

    final_log_msg = f"全部群组增量采集完成：成功 {success_count} 个，失败 {failed_count} 个，总计 {total} 个，本次共新增 {total_added_messages} 条消息"
    if failed_chats:
        final_log_msg += f"。失败群组列表：{', '.join(failed_chats)}"
    
    admin_job_append_log_fn(job_id, final_log_msg)
    return True
"""

new_runner = """
def _admin_update_job_runner(
    job_id: str,
    chat_id: Any,
    chat_title: str,
    incremental: bool,
    *,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_make_job_log_handler_fn: Callable[[str], logging.Handler],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    from telethon.sync import TelegramClient

    root_logger = logging.getLogger()
    job_log_handler = admin_make_job_log_handler_fn(job_id)
    root_logger.addHandler(job_log_handler)
    try:
        admin_job_set_status_fn(job_id, "running")
        mode_label = "增量" if incremental else "全量"
        is_all_scope = isinstance(chat_id, str) and chat_id.strip().lower() == "all"
        if is_all_scope:
            admin_job_append_log_fn(job_id, f"开始新增数据采集：模式={mode_label}，范围=全部群组")
        else:
            admin_job_append_log_fn(job_id, f"开始新增数据采集：模式={mode_label}，目标群组={chat_title}，群组ID={chat_id}")

        asyncio.set_event_loop(asyncio.new_event_loop())
        client = TelegramClient(cfg.session_name, cfg.api_id, cfg.api_hash)
        client.connect()
        try:
            if not client.is_user_authorized():
                admin_job_append_log_fn(job_id, "Telegram 未登录！请先在终端运行 python jb.py 完成登录授权。")
                admin_job_set_status_fn(job_id, "error")
                return

            if is_all_scope:
                did_run = _admin_update_all_chats(
                    job_id, client, incremental, get_conn_fn, admin_job_append_log_fn
                )
                if not did_run:
                    admin_job_set_status_fn(job_id, "done")
                    return
            else:
                _admin_process_single_chat_update(
                    job_id=job_id,
                    client=client,
                    get_conn_fn=get_conn_fn,
                    admin_job_append_log_fn=admin_job_append_log_fn,
                    chat_id=int(chat_id),
                    chat_title=chat_title,
                    idx=1,
                    total=1,
                )

            admin_job_set_status_fn(job_id, "done")
        finally:
            client.disconnect()

    except Exception as exc:
        admin_job_append_log_fn(job_id, f"采集失败：{exc}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        root_logger.removeHandler(job_log_handler)
"""

start_idx = content.find("def _admin_update_job_runner(")
end_idx = content.find("def _admin_delete_job_runner(")

if start_idx != -1 and end_idx != -1:
    new_content = content[:start_idx] + helper_code + "\n" + new_runner + "\n\n\n" + content[end_idx:]
    with open('/root/db/tg_harvest/admin_jobs_runners.py', 'w', encoding='utf-8') as f:
        f.write(new_content)
    print("Success")
else:
    print("Could not find boundaries")

