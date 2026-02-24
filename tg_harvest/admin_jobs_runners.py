# -*- coding: utf-8 -*-
import asyncio
import logging
import sqlite3
import threading
from typing import Any, Callable, Optional, Tuple


def _admin_harvest_job_runner(
    job_id: str,
    target: str,
    *,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_make_job_log_handler_fn: Callable[[str], logging.Handler],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    from telethon.sync import TelegramClient
    from tg_harvest.harvest_runner import _process_entity
    from tg_harvest.harvest_parse import resolve_target_entity

    root_logger = logging.getLogger()
    job_log_handler = admin_make_job_log_handler_fn(job_id)
    root_logger.addHandler(job_log_handler)
    try:
        admin_job_set_status_fn(job_id, "running")
        admin_job_append_log_fn(job_id, f"开始抓取目标：{target}")
        asyncio.set_event_loop(asyncio.new_event_loop())
        client = TelegramClient(cfg.session_name, cfg.api_id, cfg.api_hash)
        client.connect()
        try:
            if not client.is_user_authorized():
                admin_job_append_log_fn(job_id, "Telegram 未登录！请先在终端运行 python jb.py 完成登录授权。")
                admin_job_set_status_fn(job_id, "error")
                return

            entity = resolve_target_entity(client, target)
            if entity is None:
                admin_job_append_log_fn(job_id, "未找到该群组/频道，请检查名称或链接")
                admin_job_set_status_fn(job_id, "error")
                return

            entity_title = getattr(entity, "title", None) or getattr(entity, "username", None) or str(target)
            admin_job_append_log_fn(job_id, f"成功解析目标：{entity_title}")
            conn = get_conn_fn()
            try:
                _process_entity(conn, client, entity, idx=1, total=1)
            finally:
                conn.close()
        finally:
            client.disconnect()
        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        admin_job_append_log_fn(job_id, f"抓取失败：{exc}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        root_logger.removeHandler(job_log_handler)



def _admin_update_job_runner(
    job_id: str,
    chat_id: int,
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
    from tg_harvest.harvest_runner import _process_entity

    root_logger = logging.getLogger()
    job_log_handler = admin_make_job_log_handler_fn(job_id)
    root_logger.addHandler(job_log_handler)
    try:
        admin_job_set_status_fn(job_id, "running")
        mode_label = "增量" if incremental else "全量"
        admin_job_append_log_fn(job_id, f"开始{mode_label}更新：{chat_title} ({chat_id})")
        asyncio.set_event_loop(asyncio.new_event_loop())
        client = TelegramClient(cfg.session_name, cfg.api_id, cfg.api_hash)
        client.connect()
        try:
            if not client.is_user_authorized():
                admin_job_append_log_fn(job_id, "Telegram 未登录！请先在终端运行 python jb.py 完成登录授权。")
                admin_job_set_status_fn(job_id, "error")
                return

            entity = client.get_entity(chat_id)
            entity_title = getattr(entity, "title", None) or getattr(entity, "username", None) or str(chat_id)
            admin_job_append_log_fn(job_id, f"成功连接并获取实体：{entity_title}")
            conn = get_conn_fn()
            try:
                _process_entity(conn, client, entity, idx=1, total=1)
            finally:
                conn.close()
        finally:
            client.disconnect()
        admin_job_set_status_fn(job_id, "done")
    except Exception as exc:
        admin_job_append_log_fn(job_id, f"更新失败：{exc}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        root_logger.removeHandler(job_log_handler)



def _admin_delete_job_runner(
    job_id: str,
    chat_id: int,
    chat_title: str,
    *,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> None:
    conn = None
    try:
        admin_job_set_status_fn(job_id, "running")
        admin_job_append_log_fn(job_id, f"开始删除目标：{chat_title} ({chat_id})")

        conn = get_conn_fn()
        cur = conn.cursor()
        try:
            admin_job_append_log_fn(job_id, "统计待删除消息数量")
            cur.execute("SELECT COUNT(*) AS cnt FROM messages WHERE chat_id = ?", (chat_id,))
            count_row = cur.fetchone()
            message_count = int((count_row["cnt"] if count_row and "cnt" in count_row.keys() else 0) or 0)
            admin_job_append_log_fn(job_id, f"待删除消息数量：{message_count}")

            cur.execute("DELETE FROM dedupe_actions WHERE chat_id = ?", (chat_id,))
            cur.execute("DELETE FROM dedupe_runs WHERE chat_id = ?", (chat_id,))
            cur.execute("DELETE FROM media_groups WHERE chat_id = ?", (chat_id,))
            cur.execute("DELETE FROM message_media WHERE chat_id = ?", (chat_id,))
            admin_job_append_log_fn(job_id, "清理关联表数据完成")

            admin_job_append_log_fn(job_id, "删除 messages 表数据")
            cur.execute("DELETE FROM messages WHERE chat_id = ?", (chat_id,))
            deleted_messages = int(cur.rowcount or 0)
            admin_job_append_log_fn(job_id, f"messages 删除行数：{deleted_messages}")

            admin_job_append_log_fn(job_id, "删除 chats 表记录")
            cur.execute("DELETE FROM chats WHERE chat_id = ?", (chat_id,))
            deleted_chats = int(cur.rowcount or 0)
            admin_job_append_log_fn(job_id, f"chats 删除行数：{deleted_chats}")

            if deleted_chats != 1:
                raise RuntimeError(f"chats 删除异常，预期 1 行，实际 {deleted_chats} 行")

            conn.commit()
            admin_job_append_log_fn(job_id, "事务已提交")
            admin_job_append_log_fn(job_id, f"删除完成：消息 {deleted_messages} 条，chat 记录删除 {deleted_chats} 条")
            admin_job_set_status_fn(job_id, "done")
        finally:
            cur.close()
    except Exception as exc:
        if conn is not None:
            try:
                conn.rollback()
                admin_job_append_log_fn(job_id, "删除失败，事务已回滚")
            except Exception as rollback_exc:
                admin_job_append_log_fn(job_id, f"删除失败，回滚异常：{rollback_exc}")

        admin_job_append_log_fn(job_id, f"删除失败：{exc}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        if conn is not None:
            conn.close()


def _admin_cleanup_job_runner(
    job_id: str,
    keyword: str,
    scope: str,
    chat_id: Optional[int],
    target_label: str,
    *,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
    has_fts_fn: Callable[[Any], bool],
) -> None:
    conn: Optional[sqlite3.Connection] = None
    try:
        admin_job_set_status_fn(job_id, "running")
        admin_job_append_log_fn(job_id, f"开始垃圾清理任务：范围={scope} 目标={target_label}")

        like_pattern = f"%{keyword}%"
        scope_filter_sql = ""
        scope_filter_params: Tuple[Any, ...] = tuple()
        if scope == "chat" and isinstance(chat_id, int):
            scope_filter_sql = " AND m.chat_id = ?"
            scope_filter_params = (chat_id,)

        conn = get_conn_fn()
        cur = conn.cursor()
        try:
            admin_job_append_log_fn(job_id, "构建待清理消息集合")
            cur.execute("DROP TABLE IF EXISTS temp_cleanup_targets")
            cur.execute(
                """
                CREATE TEMP TABLE temp_cleanup_targets (
                    chat_id INTEGER NOT NULL,
                    pk INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    grouped_id INTEGER
                )
                """
            )

            target_insert_sql = (
                """
                INSERT INTO temp_cleanup_targets (chat_id, pk, message_id, grouped_id)
                SELECT m.chat_id, m.pk, m.message_id, m.grouped_id
                FROM messages m
                WHERE COALESCE(m.content_norm, m.content, '') LIKE ?
                """
                + scope_filter_sql
            )
            cur.execute(target_insert_sql, (like_pattern, *scope_filter_params))

            cur.execute("SELECT COUNT(*) AS cnt FROM temp_cleanup_targets")
            target_count = int(cur.fetchone()["cnt"] or 0)
            admin_job_append_log_fn(job_id, f"命中待清理消息数量：{target_count}")

            # 历史数据可能在外键未开启时期写入，先清理孤儿记录，避免后续校验误报导致整体回滚。
            orphan_media_scope_sql = ""
            orphan_media_scope_params: Tuple[Any, ...] = tuple()
            if scope == "chat" and isinstance(chat_id, int):
                orphan_media_scope_sql = " AND mm.chat_id = ?"
                orphan_media_scope_params = (chat_id,)

            cur.execute(
                (
                    "DELETE FROM message_media "
                    "WHERE EXISTS ("
                    "SELECT 1 FROM message_media mm "
                    "LEFT JOIN messages m ON m.chat_id = mm.chat_id AND m.message_id = mm.message_id "
                    "WHERE mm.chat_id = message_media.chat_id "
                    "AND mm.message_id = message_media.message_id "
                    "AND m.pk IS NULL"
                    + orphan_media_scope_sql
                    + ")"
                ),
                orphan_media_scope_params,
            )
            deleted_orphan_media = int(cur.rowcount or 0)
            if deleted_orphan_media > 0:
                admin_job_append_log_fn(job_id, f"历史孤儿 message_media 清理行数：{deleted_orphan_media}")

            if target_count == 0:
                conn.commit()
                admin_job_append_log_fn(job_id, "未命中任何消息，无需清理")
                admin_job_set_status_fn(job_id, "done")
                return

            cur.execute(
                """
                DELETE FROM dedupe_actions
                WHERE EXISTS (
                    SELECT 1
                    FROM temp_cleanup_targets t
                    WHERE t.chat_id = dedupe_actions.chat_id
                      AND (t.pk = dedupe_actions.pk OR t.message_id = dedupe_actions.message_id)
                )
                """
            )
            deleted_actions = int(cur.rowcount or 0)
            admin_job_append_log_fn(job_id, f"dedupe_actions 删除行数：{deleted_actions}")

            cur.execute(
                """
                DELETE FROM message_media
                WHERE EXISTS (
                    SELECT 1
                    FROM temp_cleanup_targets t
                    WHERE t.chat_id = message_media.chat_id
                      AND t.message_id = message_media.message_id
                )
                """
            )
            deleted_media = int(cur.rowcount or 0)
            admin_job_append_log_fn(job_id, f"message_media 删除行数：{deleted_media}")

            cur.execute(
                """
                DELETE FROM messages
                WHERE pk IN (SELECT pk FROM temp_cleanup_targets)
                """
            )
            deleted_messages = int(cur.rowcount or 0)
            admin_job_append_log_fn(job_id, f"messages 删除行数：{deleted_messages}")

            media_group_scope_sql = ""
            media_group_scope_params: Tuple[Any, ...] = tuple()
            if scope == "chat" and isinstance(chat_id, int):
                media_group_scope_sql = "WHERE mg.chat_id = ?"
                media_group_scope_params = (chat_id,)

            cur.execute(
                f"""
                DELETE FROM media_groups AS mg
                {media_group_scope_sql}
                AND NOT EXISTS (
                    SELECT 1 FROM messages m
                    WHERE m.chat_id = mg.chat_id
                      AND m.grouped_id = mg.grouped_id
                      AND m.grouped_id IS NOT NULL
                )
                """ if media_group_scope_sql else """
                DELETE FROM media_groups AS mg
                WHERE NOT EXISTS (
                    SELECT 1 FROM messages m
                    WHERE m.chat_id = mg.chat_id
                      AND m.grouped_id = mg.grouped_id
                      AND m.grouped_id IS NOT NULL
                )
                """,
                media_group_scope_params,
            )
            deleted_groups = int(cur.rowcount or 0)
            admin_job_append_log_fn(job_id, f"media_groups 清理行数：{deleted_groups}")

            cur.execute("DROP TABLE IF EXISTS temp_cleanup_empty_chats")
            cur.execute("CREATE TEMP TABLE temp_cleanup_empty_chats (chat_id INTEGER PRIMARY KEY)")
            if scope == "chat" and isinstance(chat_id, int):
                cur.execute(
                    """
                    INSERT OR IGNORE INTO temp_cleanup_empty_chats (chat_id)
                    SELECT c.chat_id
                    FROM chats c
                    WHERE c.chat_id = ?
                      AND NOT EXISTS (SELECT 1 FROM messages m WHERE m.chat_id = c.chat_id)
                    """,
                    (chat_id,),
                )
            else:
                cur.execute(
                    """
                    INSERT OR IGNORE INTO temp_cleanup_empty_chats (chat_id)
                    SELECT c.chat_id
                    FROM chats c
                    WHERE NOT EXISTS (SELECT 1 FROM messages m WHERE m.chat_id = c.chat_id)
                    """
                )

            cur.execute(
                "DELETE FROM dedupe_actions WHERE chat_id IN (SELECT chat_id FROM temp_cleanup_empty_chats)"
            )
            deleted_actions_empty_chat = int(cur.rowcount or 0)

            cur.execute(
                "DELETE FROM message_media WHERE chat_id IN (SELECT chat_id FROM temp_cleanup_empty_chats)"
            )
            deleted_media_empty_chat = int(cur.rowcount or 0)

            cur.execute(
                "DELETE FROM dedupe_runs WHERE chat_id IN (SELECT chat_id FROM temp_cleanup_empty_chats)"
            )
            deleted_runs_empty_chat = int(cur.rowcount or 0)

            cur.execute(
                "DELETE FROM media_groups WHERE chat_id IN (SELECT chat_id FROM temp_cleanup_empty_chats)"
            )
            deleted_groups_empty_chat = int(cur.rowcount or 0)

            cur.execute("DELETE FROM chats WHERE chat_id IN (SELECT chat_id FROM temp_cleanup_empty_chats)")
            deleted_empty_chats = int(cur.rowcount or 0)
            admin_job_append_log_fn(
                job_id,
                "空 chat 清理："
                f"dedupe_actions {deleted_actions_empty_chat} 行，"
                f"message_media {deleted_media_empty_chat} 行，"
                f"dedupe_runs {deleted_runs_empty_chat} 行，"
                f"media_groups {deleted_groups_empty_chat} 行，"
                f"chats {deleted_empty_chats} 行",
            )

            verify_scope_sql = ""
            verify_scope_params: Tuple[Any, ...] = tuple()
            if scope == "chat" and isinstance(chat_id, int):
                verify_scope_sql = " AND m.chat_id = ?"
                verify_scope_params = (chat_id,)

            dedupe_scope_sql = ""
            dedupe_scope_params: Tuple[Any, ...] = tuple()
            if scope == "chat" and isinstance(chat_id, int):
                dedupe_scope_sql = " AND da.chat_id = ?"
                dedupe_scope_params = (chat_id,)

            admin_job_append_log_fn(job_id, "执行彻底性校验：关键字残留")
            cur.execute(
                (
                    "SELECT COUNT(*) AS cnt FROM messages m "
                    "WHERE COALESCE(m.content_norm, m.content, '') LIKE ?"
                    + verify_scope_sql
                ),
                (like_pattern, *verify_scope_params),
            )
            remaining_matches = int(cur.fetchone()["cnt"] or 0)
            if remaining_matches != 0:
                raise RuntimeError(f"清理校验失败：仍存在 {remaining_matches} 条 content LIKE 命中消息")

            admin_job_append_log_fn(job_id, "执行彻底性校验：message_media 孤儿")
            cur.execute(
                (
                    "SELECT COUNT(*) AS cnt FROM message_media mm "
                    "LEFT JOIN messages m ON m.chat_id = mm.chat_id AND m.message_id = mm.message_id "
                    "WHERE m.pk IS NULL"
                    + orphan_media_scope_sql
                ),
                orphan_media_scope_params,
            )
            orphan_media = int(cur.fetchone()["cnt"] or 0)
            if orphan_media != 0:
                raise RuntimeError(f"清理校验失败：message_media 存在 {orphan_media} 条孤立记录")

            orphan_groups_scope_sql = ""
            orphan_groups_scope_params: Tuple[Any, ...] = tuple()
            if scope == "chat" and isinstance(chat_id, int):
                orphan_groups_scope_sql = " AND mg.chat_id = ?"
                orphan_groups_scope_params = (chat_id,)

            admin_job_append_log_fn(job_id, "执行彻底性校验：media_groups 孤儿")
            cur.execute(
                (
                    "SELECT COUNT(*) AS cnt FROM media_groups mg "
                    "WHERE NOT EXISTS ("
                    "SELECT 1 FROM messages m "
                    "WHERE m.chat_id = mg.chat_id "
                    "AND m.grouped_id = mg.grouped_id "
                    "AND m.grouped_id IS NOT NULL"
                    ")"
                    + orphan_groups_scope_sql
                ),
                orphan_groups_scope_params,
            )
            orphan_groups = int(cur.fetchone()["cnt"] or 0)
            if orphan_groups != 0:
                raise RuntimeError(f"清理校验失败：media_groups 存在 {orphan_groups} 条孤立记录")

            admin_job_append_log_fn(job_id, "执行彻底性校验：dedupe_actions 无效引用")
            cur.execute(
                (
                    "SELECT COUNT(*) AS cnt FROM dedupe_actions da "
                    "LEFT JOIN chats c ON c.chat_id = da.chat_id "
                    "LEFT JOIN messages m ON m.pk = da.pk "
                    "WHERE (c.chat_id IS NULL OR m.pk IS NULL OR m.chat_id <> da.chat_id OR m.message_id <> da.message_id)"
                    + dedupe_scope_sql
                ),
                dedupe_scope_params,
            )
            invalid_dedupe_actions = int(cur.fetchone()["cnt"] or 0)
            if invalid_dedupe_actions != 0:
                raise RuntimeError(f"清理校验失败：dedupe_actions 存在 {invalid_dedupe_actions} 条无效引用")

            admin_job_append_log_fn(job_id, "执行彻底性校验：dedupe_runs 无效引用")
            if scope == "chat" and isinstance(chat_id, int):
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM dedupe_runs dr
                    LEFT JOIN chats c ON c.chat_id = dr.chat_id
                    WHERE c.chat_id IS NULL
                      AND dr.chat_id = ?
                    """,
                    (chat_id,),
                )
            else:
                cur.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM dedupe_runs dr
                    LEFT JOIN chats c ON c.chat_id = dr.chat_id
                    WHERE c.chat_id IS NULL
                    """
                )
            invalid_dedupe_runs = int(cur.fetchone()["cnt"] or 0)
            if invalid_dedupe_runs != 0:
                raise RuntimeError(f"清理校验失败：dedupe_runs 存在 {invalid_dedupe_runs} 条无效引用")

            if has_fts_fn(conn):
                admin_job_append_log_fn(job_id, "执行 FTS 一致性校验")
                cur.execute("SELECT COUNT(*) AS cnt FROM messages")
                total_messages = int(cur.fetchone()["cnt"] or 0)
                cur.execute("SELECT COUNT(*) AS cnt FROM messages_fts")
                total_fts = int(cur.fetchone()["cnt"] or 0)
                if total_messages != total_fts:
                    admin_job_append_log_fn(
                        job_id,
                        f"FTS 检测到漂移（messages={total_messages}, messages_fts={total_fts}），执行重建",
                    )
                    cur.execute("DELETE FROM messages_fts")
                    cur.execute(
                        """
                        INSERT INTO messages_fts(rowid, content)
                        SELECT pk, COALESCE(NULLIF(content_norm, ''), content, '')
                        FROM messages
                        """
                    )
                    cur.execute("SELECT COUNT(*) AS cnt FROM messages_fts")
                    rebuilt_fts = int(cur.fetchone()["cnt"] or 0)
                    if rebuilt_fts != total_messages:
                        raise RuntimeError(
                            f"清理校验失败：FTS 重建后仍不一致（messages={total_messages}, messages_fts={rebuilt_fts}）"
                        )

            conn.commit()
            admin_job_append_log_fn(
                job_id,
                f"垃圾清理完成：messages {deleted_messages}，message_media {deleted_media}，"
                f"dedupe_actions {deleted_actions}，media_groups {deleted_groups}，chats {deleted_empty_chats}",
            )
            admin_job_set_status_fn(job_id, "done")
        finally:
            cur.close()
    except Exception as exc:
        if conn is not None:
            try:
                conn.rollback()
                admin_job_append_log_fn(job_id, "垃圾清理失败，事务已回滚")
            except Exception as rollback_exc:
                admin_job_append_log_fn(job_id, f"垃圾清理失败，回滚异常：{rollback_exc}")
        admin_job_append_log_fn(job_id, f"垃圾清理任务执行失败：{exc}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        if conn is not None:
            conn.close()


def _admin_start_harvest_job_thread(
    job_id: str,
    target: str,
    *,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_make_job_log_handler_fn: Callable[[str], logging.Handler],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> threading.Thread:
    worker = threading.Thread(
        target=_admin_harvest_job_runner,
        args=(job_id, target),
        kwargs={
            "cfg": cfg,
            "get_conn_fn": get_conn_fn,
            "admin_make_job_log_handler_fn": admin_make_job_log_handler_fn,
            "admin_job_set_status_fn": admin_job_set_status_fn,
            "admin_job_append_log_fn": admin_job_append_log_fn,
        },
        daemon=True,
    )
    worker.start()
    return worker


def _admin_start_update_job_thread(
    job_id: str,
    chat_id: int,
    chat_title: str,
    incremental: bool,
    *,
    cfg: Any,
    get_conn_fn: Callable[[], Any],
    admin_make_job_log_handler_fn: Callable[[str], logging.Handler],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> threading.Thread:
    worker = threading.Thread(
        target=_admin_update_job_runner,
        args=(job_id, chat_id, chat_title, incremental),
        kwargs={
            "cfg": cfg,
            "get_conn_fn": get_conn_fn,
            "admin_make_job_log_handler_fn": admin_make_job_log_handler_fn,
            "admin_job_set_status_fn": admin_job_set_status_fn,
            "admin_job_append_log_fn": admin_job_append_log_fn,
        },
        daemon=True,
    )
    worker.start()
    return worker


def _admin_start_delete_job_thread(
    job_id: str,
    chat_id: int,
    chat_title: str,
    *,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
) -> threading.Thread:
    worker = threading.Thread(
        target=_admin_delete_job_runner,
        args=(job_id, chat_id, chat_title),
        kwargs={
            "get_conn_fn": get_conn_fn,
            "admin_job_set_status_fn": admin_job_set_status_fn,
            "admin_job_append_log_fn": admin_job_append_log_fn,
        },
        daemon=True,
    )
    worker.start()
    return worker


def _admin_start_cleanup_job_thread(
    job_id: str,
    keyword: str,
    scope: str,
    chat_id: Optional[int],
    target_label: str,
    *,
    get_conn_fn: Callable[[], Any],
    admin_job_set_status_fn: Callable[[str, str], bool],
    admin_job_append_log_fn: Callable[[str, str], Any],
    has_fts_fn: Callable[[Any], bool],
) -> threading.Thread:
    worker = threading.Thread(
        target=_admin_cleanup_job_runner,
        args=(job_id, keyword, scope, chat_id, target_label),
        kwargs={
            "get_conn_fn": get_conn_fn,
            "admin_job_set_status_fn": admin_job_set_status_fn,
            "admin_job_append_log_fn": admin_job_append_log_fn,
            "has_fts_fn": has_fts_fn,
        },
        daemon=True,
    )
    worker.start()
    return worker
