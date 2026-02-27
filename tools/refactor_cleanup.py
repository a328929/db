import re

with open('/root/db/tg_harvest/admin_jobs_runners.py', 'r', encoding='utf-8') as f:
    content = f.read()

# We will create helper functions and insert them BEFORE _admin_cleanup_job_runner.

helper_1 = """
def _build_cleanup_targets_table(
    cur, job_id, mode, scope_filter_sql, scope_filter_params, like_pattern, admin_job_append_log_fn
):
    admin_job_append_log_fn(job_id, "构建待清理消息集合")
    cur.execute("DROP TABLE IF EXISTS temp_cleanup_targets")
    cur.execute(
        '''
        CREATE TEMP TABLE temp_cleanup_targets (
            chat_id INTEGER NOT NULL,
            pk INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            grouped_id INTEGER,
            PRIMARY KEY (chat_id, pk)
        )
        '''
    )
    cur.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_temp_cleanup_targets_chat_message
        ON temp_cleanup_targets(chat_id, message_id)
        '''
    )
    cur.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_temp_cleanup_targets_chat_grouped
        ON temp_cleanup_targets(chat_id, grouped_id)
        '''
    )

    if mode == "empty_media":
        # 1) 清理孤立空消息（无 grouped_id，且文本为空；可包含媒体或非媒体）。
        target_insert_sql = (
            '''
            INSERT INTO temp_cleanup_targets (chat_id, pk, message_id, grouped_id)
            SELECT m.chat_id, m.pk, m.message_id, m.grouped_id
            FROM messages m
            WHERE m.grouped_id IS NULL
              AND COALESCE(NULLIF(m.content_norm, ''), NULLIF(m.content, ''), '') = ''
            '''
            + scope_filter_sql
        )
        cur.execute(target_insert_sql, scope_filter_params)

        # 2) 仅当整个媒体组都无文本时，才将该 grouped_id 全组纳入删除。
        target_group_insert_sql = (
            '''
            INSERT INTO temp_cleanup_targets (chat_id, pk, message_id, grouped_id)
            SELECT m.chat_id, m.pk, m.message_id, m.grouped_id
            FROM messages m
            WHERE m.grouped_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM messages m2
                  WHERE m2.chat_id = m.chat_id
                    AND m2.grouped_id = m.grouped_id
                    AND COALESCE(NULLIF(m2.content_norm, ''), NULLIF(m2.content, ''), '') <> ''
              )
              AND NOT EXISTS (
                  SELECT 1 FROM temp_cleanup_targets t
                  WHERE t.chat_id = m.chat_id
                    AND t.pk = m.pk
              )
            '''
            + scope_filter_sql
        )
        cur.execute(target_group_insert_sql, scope_filter_params)
    else:
        target_insert_sql = (
            '''
            INSERT INTO temp_cleanup_targets (chat_id, pk, message_id, grouped_id)
            SELECT m.chat_id, m.pk, m.message_id, m.grouped_id
            FROM messages m
            WHERE COALESCE(m.content_norm, m.content, '') LIKE ?
            '''
            + scope_filter_sql
        )
        cur.execute(target_insert_sql, (like_pattern, *scope_filter_params))

        # 将同属一个媒体组的其他消息也一并纳入清理
        cur.execute("DROP TABLE IF EXISTS temp_cleanup_grouped_ids")
        cur.execute(
            '''
            CREATE TEMP TABLE temp_cleanup_grouped_ids AS
            SELECT DISTINCT chat_id, grouped_id
            FROM temp_cleanup_targets
            WHERE grouped_id IS NOT NULL
            '''
        )
        target_group_insert_sql = (
            '''
            INSERT INTO temp_cleanup_targets (chat_id, pk, message_id, grouped_id)
            SELECT m.chat_id, m.pk, m.message_id, m.grouped_id
            FROM messages m
            JOIN temp_cleanup_grouped_ids g
              ON g.chat_id = m.chat_id
             AND g.grouped_id = m.grouped_id
            WHERE NOT EXISTS (
                SELECT 1 FROM temp_cleanup_targets t
                WHERE t.chat_id = m.chat_id
                  AND t.pk = m.pk
            )
            '''
        )
        cur.execute(target_group_insert_sql)

    cur.execute("SELECT COUNT(*) AS cnt FROM temp_cleanup_targets")
    target_count = int(cur.fetchone()["cnt"] or 0)
    return target_count
"""

helper_2 = """
def _execute_cleanup_deletion_batches(
    conn, cur, job_id, target_count, dedupe_scope_sql, dedupe_scope_params, admin_job_append_log_fn
):
    admin_job_append_log_fn(job_id, f"待清理消息记录共计：{target_count} 条，准备执行分批删除")
    
    cur.execute("DROP TABLE IF EXISTS temp_cleanup_pending")
    cur.execute(
        '''
        CREATE TEMP TABLE temp_cleanup_pending AS
        SELECT chat_id, pk, message_id
        FROM temp_cleanup_targets
        '''
    )
    cur.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_temp_cleanup_pending_pk
        ON temp_cleanup_pending(pk)
        '''
    )

    deleted_messages = 0
    deleted_media = 0
    batch_no = 0
    
    cur.execute("DROP TABLE IF EXISTS temp_cleanup_batch")
    cur.execute(
        '''
        CREATE TEMP TABLE temp_cleanup_batch (
            chat_id INTEGER,
            pk INTEGER,
            message_id INTEGER
        )
        '''
    )
    cur.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_temp_cleanup_batch_pk
        ON temp_cleanup_batch(pk)
        '''
    )
    cur.execute(
        '''
        CREATE INDEX IF NOT EXISTS idx_temp_cleanup_batch_chat_message
        ON temp_cleanup_batch(chat_id, message_id)
        '''
    )

    while True:
        batch_no += 1
        cur.execute("DELETE FROM temp_cleanup_batch")
        cur.execute(
            f'''
            INSERT INTO temp_cleanup_batch (chat_id, pk, message_id)
            SELECT chat_id, pk, message_id
            FROM temp_cleanup_pending
            LIMIT ?
            ''',
            (CLEANUP_DELETE_BATCH_SIZE,),
        )
        cur.execute("SELECT COUNT(*) AS cnt FROM temp_cleanup_batch")
        batch_size = int(cur.fetchone()["cnt"] or 0)
        if batch_size == 0:
            break

        cur.execute(
            '''
            DELETE FROM message_media
            WHERE (chat_id, message_id) IN (
                SELECT chat_id, message_id FROM temp_cleanup_batch
            )
            '''
        )
        deleted_media += int(cur.rowcount or 0)

        cur.execute(
            '''
            DELETE FROM messages
            WHERE pk IN (SELECT pk FROM temp_cleanup_batch)
            '''
        )
        deleted_messages += int(cur.rowcount or 0)

        cur.execute(
            '''
            DELETE FROM temp_cleanup_pending
            WHERE pk IN (SELECT pk FROM temp_cleanup_batch)
            '''
        )

        should_log_batch = (batch_no == 1) or (batch_no % 10 == 0)
        if should_log_batch:
            cur.execute("SELECT COUNT(*) AS cnt FROM temp_cleanup_pending")
            pending_count = int(cur.fetchone()["cnt"] or 0)
            admin_job_append_log_fn(
                job_id,
                f"分批删除进度：第 {batch_no} 批，已删除消息 {deleted_messages}/{target_count}，剩余 {pending_count}",
            )
        conn.commit()
    
    cur.execute("DROP TABLE IF EXISTS temp_cleanup_pending")
    cur.execute("DROP TABLE IF EXISTS temp_cleanup_batch")

    admin_job_append_log_fn(job_id, "清理孤儿去重记录 (dedupe_actions)")
    cur.execute(
        f'''
        DELETE FROM dedupe_actions
        WHERE 1=1 {dedupe_scope_sql}
          AND NOT EXISTS (
              SELECT 1 FROM messages m
              WHERE m.chat_id = dedupe_actions.chat_id
                AND m.message_id = dedupe_actions.message_id
          )
        ''',
        dedupe_scope_params,
    )
    deleted_actions = int(cur.rowcount or 0)

    admin_job_append_log_fn(job_id, "清理孤儿媒体分组 (media_groups)")
    cur.execute(
        '''
        DELETE FROM media_groups
        WHERE NOT EXISTS (
            SELECT 1 FROM messages m
            WHERE m.chat_id = media_groups.chat_id
              AND m.grouped_id = media_groups.grouped_id
              AND m.grouped_id IS NOT NULL
        )
        '''
    )
    deleted_groups = int(cur.rowcount or 0)

    admin_job_append_log_fn(job_id, "检查并清理空群组记录 (chats)")
    cur.execute("DROP TABLE IF EXISTS temp_cleanup_empty_chats")
    cur.execute("CREATE TEMP TABLE temp_cleanup_empty_chats (chat_id INTEGER PRIMARY KEY)")
    
    cur.execute("SELECT DISTINCT chat_id FROM temp_cleanup_targets")
    affected_chats = [row["chat_id"] for row in cur.fetchall()]
    
    for c_id in affected_chats:
        cur.execute("SELECT 1 FROM messages WHERE chat_id = ? LIMIT 1", (c_id,))
        if not cur.fetchone():
            cur.execute(
                '''
                INSERT OR IGNORE INTO temp_cleanup_empty_chats (chat_id)
                VALUES (?)
                ''',
                (c_id,),
            )
            
    deleted_empty_chats = 0
    cur.execute("SELECT COUNT(*) AS cnt FROM temp_cleanup_empty_chats")
    if int(cur.fetchone()["cnt"] or 0) > 0:
        admin_job_append_log_fn(job_id, "删除空群聊相关的关联数据")
        for cleanup_sql in [
            "DELETE FROM dedupe_actions WHERE chat_id IN (SELECT chat_id FROM temp_cleanup_empty_chats)",
            "DELETE FROM message_media WHERE chat_id IN (SELECT chat_id FROM temp_cleanup_empty_chats)",
            "DELETE FROM dedupe_runs WHERE chat_id IN (SELECT chat_id FROM temp_cleanup_empty_chats)",
            "DELETE FROM media_groups WHERE chat_id IN (SELECT chat_id FROM temp_cleanup_empty_chats)",
        ]:
            cur.execute(cleanup_sql)
        admin_job_append_log_fn(job_id, "删除空群聊主表记录")
        cur.execute("DELETE FROM chats WHERE chat_id IN (SELECT chat_id FROM temp_cleanup_empty_chats)")
        deleted_empty_chats = int(cur.rowcount or 0)
        conn.commit()

    return deleted_messages, deleted_media, deleted_actions, deleted_groups, deleted_empty_chats
"""

helper_3 = """
def _verify_cleanup_consistency(
    cur, job_id, mode, like_pattern, verify_scope_sql, verify_scope_params, has_fts, admin_job_append_log_fn
):
    if mode == "empty_media":
        admin_job_append_log_fn(job_id, "执行彻底性校验：无文本消息残留")
        cur.execute(
            (
                "SELECT COUNT(*) AS cnt FROM messages m "
                "WHERE ("
                "  (m.grouped_id IS NULL AND COALESCE(NULLIF(m.content_norm, ''), NULLIF(m.content, ''), '') = '')"
                "  OR ("
                "    m.grouped_id IS NOT NULL AND NOT EXISTS ("
                "      SELECT 1 FROM messages m2 "
                "      WHERE m2.chat_id = m.chat_id "
                "        AND m2.grouped_id = m.grouped_id "
                "        AND COALESCE(NULLIF(m2.content_norm, ''), NULLIF(m2.content, ''), '') <> ''"
                "    )"
                "  )"
                ")"
                + verify_scope_sql
            ),
            verify_scope_params,
        )
        remaining_matches = int(cur.fetchone()["cnt"] or 0)
        if remaining_matches != 0:
            raise RuntimeError(f"清理校验失败：仍存在 {remaining_matches} 条无文本消息")
    else:
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
            raise RuntimeError(f"清理校验失败：仍存在 {remaining_matches} 条关键字命中消息")

    admin_job_append_log_fn(job_id, "执行彻底性校验：消息媒体关联孤儿")
    cur.execute(
        (
            "SELECT COUNT(*) AS cnt FROM message_media mm "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM messages m "
            "  WHERE m.chat_id = mm.chat_id AND m.message_id = mm.message_id"
            ")"
        )
    )
    remaining_media = int(cur.fetchone()["cnt"] or 0)
    if remaining_media != 0:
        raise RuntimeError(f"清理校验失败：存在 {remaining_media} 条孤儿媒体记录")

    admin_job_append_log_fn(job_id, "执行彻底性校验：媒体分组关联孤儿")
    cur.execute(
        (
            "SELECT COUNT(*) AS cnt FROM media_groups mg "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM messages m "
            "  WHERE m.chat_id = mg.chat_id AND m.grouped_id = mg.grouped_id"
            ")"
        )
    )
    remaining_groups = int(cur.fetchone()["cnt"] or 0)
    if remaining_groups != 0:
        raise RuntimeError(f"清理校验失败：存在 {remaining_groups} 条孤儿媒体分组记录")

    if has_fts:
        admin_job_append_log_fn(job_id, "执行 FTS 一致性校验")
        cur.execute("SELECT COUNT(*) AS cnt FROM messages")
        total_messages = int(cur.fetchone()["cnt"] or 0)
        cur.execute("SELECT COUNT(*) AS cnt FROM messages_fts")
        total_fts = int(cur.fetchone()["cnt"] or 0)
        if total_messages != total_fts:
            admin_job_append_log_fn(
                job_id,
                f"FTS 检测到漂移（消息记录={total_messages}，全文索引记录={total_fts}），执行重建",
            )
            cur.execute("DELETE FROM messages_fts")
            cur.execute(
                '''
                INSERT INTO messages_fts(rowid, content)
                SELECT pk, COALESCE(NULLIF(content_norm, ''), content, '')
                FROM messages
                '''
            )
            cur.execute("SELECT COUNT(*) AS cnt FROM messages_fts")
            rebuilt_fts = int(cur.fetchone()["cnt"] or 0)
            if rebuilt_fts != total_messages:
                raise RuntimeError(
                    f"清理校验失败：FTS 重建后仍不一致（消息记录={total_messages}，全文索引记录={rebuilt_fts}）"
                )
"""

new_runner = """
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
    cleanup_mode: str = "keyword",
) -> None:
    scope_label = "当前群组" if scope == "chat" and isinstance(chat_id, int) else "全部数据"
    mode = (cleanup_mode or "keyword").strip().lower()
    if mode not in {"keyword", "empty_media"}:
        mode = "keyword"
    mode_label = "垃圾清理" if mode == "keyword" else "无文本媒体清理"

    conn: Optional[sqlite3.Connection] = None
    try:
        admin_job_set_status_fn(job_id, "running")
        admin_job_append_log_fn(job_id, f"开始{mode_label}任务：范围={scope_label}，目标={target_label}")

        like_pattern = f"%{keyword}%"
        scope_filter_sql = ""
        scope_filter_params: Tuple[Any, ...] = tuple()
        if scope == "chat" and isinstance(chat_id, int):
            scope_filter_sql = " AND m.chat_id = ?"
            scope_filter_params = (chat_id,)

        conn = get_conn_fn()
        cur = conn.cursor()
        try:
            dedupe_scope_sql = ""
            dedupe_scope_params: Tuple[Any, ...] = tuple()
            if scope == "chat" and isinstance(chat_id, int):
                dedupe_scope_sql = " AND dedupe_actions.chat_id = ?"
                dedupe_scope_params = (chat_id,)

            # Phase 1: Build target list
            target_count = _build_cleanup_targets_table(
                cur, job_id, mode, scope_filter_sql, scope_filter_params, like_pattern, admin_job_append_log_fn
            )

            if target_count == 0:
                admin_job_append_log_fn(job_id, f"未找到匹配的{mode_label}数据，无需执行删除")
                admin_job_set_status_fn(job_id, "done")
                return

            # Phase 2: Execute deletion batches
            deleted_messages, deleted_media, deleted_actions, deleted_groups, deleted_empty_chats = _execute_cleanup_deletion_batches(
                conn, cur, job_id, target_count, dedupe_scope_sql, dedupe_scope_params, admin_job_append_log_fn
            )

            # Phase 3: Verify consistency
            has_fts = has_fts_fn(conn)
            _verify_cleanup_consistency(
                cur, job_id, mode, like_pattern, scope_filter_sql, scope_filter_params, has_fts, admin_job_append_log_fn
            )

            conn.commit()
            admin_job_append_log_fn(
                job_id,
                f"{mode_label}完成：命中待清理消息 {target_count}，消息记录 {deleted_messages}，消息媒体关联 {deleted_media}，"
                f"去重动作记录 {deleted_actions}，媒体分组 {deleted_groups}，群组记录 {deleted_empty_chats}",
            )
            admin_job_set_status_fn(job_id, "done")

        except Exception as query_exc:
            conn.rollback()
            raise query_exc
        finally:
            cur.close()
    except Exception as exc:
        if conn is not None:
            try:
                conn.rollback()
            except Exception as rollback_exc:
                admin_job_append_log_fn(job_id, f"清理失败，回滚异常：{rollback_exc}")

        admin_job_append_log_fn(job_id, f"清理失败：{exc}")
        admin_job_set_status_fn(job_id, "error")
    finally:
        if conn is not None:
            conn.close()
"""

# Now we need to find the exact boundary in the original file to replace.
# Looking for `def _admin_cleanup_job_runner(` to the next `def _admin_start_harvest_job_thread(`

start_idx = content.find("def _admin_cleanup_job_runner(")
end_idx = content.find("def _admin_start_harvest_job_thread(")

if start_idx != -1 and end_idx != -1:
    new_content = content[:start_idx] + helper_1 + "\n" + helper_2 + "\n" + helper_3 + "\n" + new_runner + "\n\n\n" + content[end_idx:]
    with open('/root/db/tg_harvest/admin_jobs_runners.py', 'w', encoding='utf-8') as f:
        f.write(new_content)
    print("Success")
else:
    print("Could not find boundaries")
