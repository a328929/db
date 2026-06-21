import re
import sqlite3

from tg_harvest.storage.search_text_state import (
    SEARCH_TEXT_PRESENT_COLUMN,
    table_has_column,
)

_OBSOLETE_INDEXES = (
    # Covered by sqlite_autoindex_messages_1 (UNIQUE(chat_id, message_id)).
    "idx_messages_msg_id",
    # Covered by the left prefix of idx_messages_chat_date for current lookups.
    "idx_messages_chat_id",
    # Exact duplicates of table primary-key autoindexes.
    "idx_media_file_ref",
    "idx_dedupe_runs_batch",
    "idx_admin_job_logs_job_seq",
    # Historical duplicate of idx_mg_pure_hash.
    "idx_mg_hash",
)


def _normalize_index_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", str(sql or "").strip()).lower()


def _stored_index_sql(cur: sqlite3.Cursor, index_name: str) -> str:
    cur.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type = 'index' AND name = ?
        LIMIT 1
        """,
        (index_name,),
    )
    row = cur.fetchone()
    if row is None:
        return ""
    return str(row["sql"] if isinstance(row, sqlite3.Row) else row[0] or "")


def _create_index_sql(expected_sql: str) -> str:
    return re.sub(
        r"^CREATE\s+INDEX\s+",
        "CREATE INDEX IF NOT EXISTS ",
        str(expected_sql).strip(),
        count=1,
        flags=re.IGNORECASE,
    )


def _ensure_index(cur: sqlite3.Cursor, index_name: str, expected_sql: str) -> None:
    existing_sql = _stored_index_sql(cur, index_name)
    if existing_sql and _normalize_index_sql(existing_sql) != _normalize_index_sql(expected_sql):
        cur.execute(f"DROP INDEX IF EXISTS {index_name}")
    cur.execute(_create_index_sql(expected_sql))


def _drop_obsolete_indexes(cur: sqlite3.Cursor) -> None:
    for index_name in _OBSOLETE_INDEXES:
        cur.execute(f"DROP INDEX IF EXISTS {index_name}")


def _create_message_indexes(cur: sqlite3.Cursor):
    # 核心业务索引：按频道日期倒序排序（Web 列表主视图）
    _ensure_index(
        cur,
        "idx_messages_chat_date",
        "CREATE INDEX idx_messages_chat_date "
        "ON messages(chat_id, msg_date_ts DESC, message_id DESC, pk DESC)"
    )

    # 媒体组关联索引（支持相册视图）
    _ensure_index(
        cur,
        "idx_messages_grouped_id",
        "CREATE INDEX idx_messages_grouped_id "
        "ON messages(chat_id, grouped_id, message_id) WHERE grouped_id IS NOT NULL"
    )

    # 去重与内容标识索引
    _ensure_index(
        cur,
        "idx_messages_pure_hash",
        "CREATE INDEX idx_messages_pure_hash "
        "ON messages(chat_id, pure_hash) WHERE pure_hash <> ''"
    )
    _ensure_index(
        cur,
        "idx_messages_dedupe_hash",
        "CREATE INDEX idx_messages_dedupe_hash "
        "ON messages(chat_id, dedupe_hash) WHERE dedupe_hash <> ''"
    )
    _ensure_index(
        cur,
        "idx_messages_dedupe_promo_solo",
        "CREATE INDEX idx_messages_dedupe_promo_solo "
        "ON messages(chat_id, dedupe_hash, msg_date_ts ASC, message_id ASC, pk ASC) "
        "WHERE dedupe_hash <> '' AND grouped_id IS NULL AND is_promo = 1 AND dedupe_eligible = 1"
    )

    # 推广内容识别与排序索引
    _ensure_index(
        cur,
        "idx_messages_promo",
        "CREATE INDEX idx_messages_promo "
        "ON messages(chat_id, is_promo, promo_score DESC, msg_date_ts DESC, message_id DESC, pk DESC)"
    )

    # 发送者与消息类型聚合索引（支持筛选）
    _ensure_index(
        cur,
        "idx_messages_sender",
        "CREATE INDEX idx_messages_sender "
        "ON messages(chat_id, sender_id, msg_date_ts DESC, message_id DESC, pk DESC)"
    )
    _ensure_index(
        cur,
        "idx_messages_type",
        "CREATE INDEX idx_messages_type "
        "ON messages(chat_id, msg_type, msg_date_ts DESC, message_id DESC, pk DESC)"
    )
    _ensure_index(
        cur,
        "idx_messages_type_global",
        "CREATE INDEX idx_messages_type_global "
        "ON messages(msg_type, msg_date_ts DESC, message_id DESC, pk DESC)"
    )

    # 全局时间轴（用于跨频道搜索展示）
    _ensure_index(
        cur,
        "idx_messages_date_global",
        "CREATE INDEX idx_messages_date_global "
        "ON messages(msg_date_ts DESC, message_id DESC, pk DESC)"
    )

    if table_has_column(cur, "messages", SEARCH_TEXT_PRESENT_COLUMN):
        _ensure_index(
            cur,
            "idx_messages_unsearchable_pk",
            "CREATE INDEX idx_messages_unsearchable_pk "
            "ON messages(pk, chat_id, message_id, grouped_id) "
            "WHERE search_text_present = 0"
        )
        _ensure_index(
            cur,
            "idx_messages_unsearchable_chat",
            "CREATE INDEX idx_messages_unsearchable_chat "
            "ON messages(chat_id, pk, message_id, grouped_id) "
            "WHERE search_text_present = 0"
        )


def _create_chat_indexes(cur: sqlite3.Cursor):
    _ensure_index(
        cur,
        "idx_chats_title",
        "CREATE INDEX idx_chats_title "
        "ON chats(chat_title COLLATE NOCASE ASC, chat_id ASC)"
    )
    _ensure_index(
        cur,
        "idx_chats_last_seen",
        "CREATE INDEX idx_chats_last_seen "
        "ON chats(last_seen_at DESC, chat_id ASC)"
    )
    _ensure_index(
        cur,
        "idx_chats_message_count_desc",
        "CREATE INDEX idx_chats_message_count_desc "
        "ON chats(message_count DESC, chat_title COLLATE NOCASE ASC, chat_id ASC)"
    )
    _ensure_index(
        cur,
        "idx_chats_message_count_asc",
        "CREATE INDEX idx_chats_message_count_asc "
        "ON chats(message_count ASC, chat_title COLLATE NOCASE ASC, chat_id ASC)"
    )


def _create_media_indexes(cur: sqlite3.Cursor):
    # 文件唯一性索引
    _ensure_index(
        cur,
        "idx_media_unique_id",
        "CREATE INDEX idx_media_unique_id "
        "ON message_media(chat_id, file_unique_id) "
        "WHERE file_unique_id IS NOT NULL AND file_unique_id <> ''"
    )

    # 核心性能索引：文件指纹（用于跨频道去重检测）
    _ensure_index(
        cur,
        "idx_media_fingerprint",
        "CREATE INDEX idx_media_fingerprint "
        "ON message_media(chat_id, media_fingerprint) "
        "WHERE media_fingerprint IS NOT NULL AND media_fingerprint <> ''"
    )

    # 核心排序索引：按文件大小排序
    _ensure_index(
        cur,
        "idx_media_sort_size",
        "CREATE INDEX idx_media_sort_size "
        "ON message_media(chat_id, file_size DESC, message_id DESC)"
    )
    _ensure_index(
        cur,
        "idx_media_sort_size_global",
        "CREATE INDEX idx_media_sort_size_global "
        "ON message_media(file_size DESC, chat_id DESC, message_id DESC)"
    )

    # 核心排序索引：按媒体时长排序
    _ensure_index(
        cur,
        "idx_media_sort_duration",
        "CREATE INDEX idx_media_sort_duration "
        "ON message_media(chat_id, duration_sec DESC, message_id DESC)"
    )
    _ensure_index(
        cur,
        "idx_media_sort_duration_global",
        "CREATE INDEX idx_media_sort_duration_global "
        "ON message_media(duration_sec DESC, chat_id DESC, message_id DESC)"
    )

    # 类型与元数据过滤索引
    _ensure_index(
        cur,
        "idx_media_kind",
        "CREATE INDEX idx_media_kind "
        "ON message_media(chat_id, media_kind)"
    )
    _ensure_index(
        cur,
        "idx_media_mime",
        "CREATE INDEX idx_media_mime "
        "ON message_media(chat_id, mime_type)"
    )
    _ensure_index(
        cur,
        "idx_media_grouped_id",
        "CREATE INDEX idx_media_grouped_id "
        "ON message_media(chat_id, grouped_id) WHERE grouped_id IS NOT NULL"
    )


def _create_media_group_indexes(cur: sqlite3.Cursor):
    _ensure_index(
        cur,
        "idx_mg_pure_hash",
        "CREATE INDEX idx_mg_pure_hash "
        "ON media_groups(chat_id, pure_hash) WHERE pure_hash <> ''"
    )
    _ensure_index(
        cur,
        "idx_mg_pure_hash_promo",
        "CREATE INDEX idx_mg_pure_hash_promo "
        "ON media_groups(chat_id, is_promo, dedupe_eligible, pure_hash, item_count, first_message_id, grouped_id) "
        "WHERE pure_hash <> ''"
    )
    _ensure_index(
        cur,
        "idx_mg_media_sig",
        "CREATE INDEX idx_mg_media_sig "
        "ON media_groups(chat_id, media_sig_hash) WHERE media_sig_hash <> ''"
    )
    _ensure_index(
        cur,
        "idx_mg_media_sig_promo",
        "CREATE INDEX idx_mg_media_sig_promo "
        "ON media_groups(chat_id, is_promo, dedupe_eligible, media_sig_hash, item_count, first_message_id, grouped_id) "
        "WHERE media_sig_hash <> ''"
    )
    _ensure_index(
        cur,
        "idx_mg_dedupe_hash",
        "CREATE INDEX idx_mg_dedupe_hash "
        "ON media_groups(chat_id, dedupe_hash) WHERE dedupe_hash <> ''"
    )
    _ensure_index(
        cur,
        "idx_mg_promo",
        "CREATE INDEX idx_mg_promo "
        "ON media_groups(chat_id, is_promo, dedupe_eligible, item_count)"
    )
    _ensure_index(
        cur,
        "idx_mg_time",
        "CREATE INDEX idx_mg_time "
        "ON media_groups(chat_id, first_msg_date_ts DESC)"
    )


def _create_dedupe_indexes(cur: sqlite3.Cursor):
    _ensure_index(
        cur,
        "idx_dedupe_runs_chat",
        "CREATE INDEX idx_dedupe_runs_chat ON dedupe_runs(chat_id)"
    )
    _ensure_index(
        cur,
        "idx_dedupe_actions_batch",
        "CREATE INDEX idx_dedupe_actions_batch ON dedupe_actions(batch_id)"
    )
    _ensure_index(
        cur,
        "idx_dedupe_actions_chat_time",
        "CREATE INDEX idx_dedupe_actions_chat_time "
        "ON dedupe_actions(chat_id, created_at DESC)"
    )


def _create_message_search_term_indexes(cur: sqlite3.Cursor):
    _ensure_index(
        cur,
        "idx_message_search_terms_pk",
        "CREATE INDEX idx_message_search_terms_pk ON message_search_terms(pk)"
    )
    _ensure_index(
        cur,
        "idx_message_search_terms_queue_order",
        "CREATE INDEX idx_message_search_terms_queue_order "
        "ON message_search_terms_rebuild_queue(queued_at, pk)"
    )


def _create_admin_job_indexes(cur: sqlite3.Cursor):
    _ensure_index(
        cur,
        "idx_admin_jobs_updated_created",
        "CREATE INDEX idx_admin_jobs_updated_created "
        "ON admin_jobs(updated_at ASC, created_at ASC)"
    )
    _ensure_index(
        cur,
        "idx_admin_jobs_status_updated",
        "CREATE INDEX idx_admin_jobs_status_updated ON admin_jobs(status, updated_at)"
    )
    _ensure_index(
        cur,
        "idx_admin_jobs_target_chat",
        "CREATE INDEX idx_admin_jobs_target_chat ON admin_jobs(target_chat_id, status)"
    )
    _ensure_index(
        cur,
        "idx_admin_jobs_status_heartbeat",
        "CREATE INDEX idx_admin_jobs_status_heartbeat ON admin_jobs(status, heartbeat_at)"
    )


def _create_admin_missing_chat_indexes(cur: sqlite3.Cursor):
    _ensure_index(
        cur,
        "idx_admin_missing_chats_scanned",
        "CREATE INDEX idx_admin_missing_chats_scanned "
        "ON admin_missing_chats(scanned_at DESC)"
    )
    _ensure_index(
        cur,
        "idx_admin_missing_chats_title",
        "CREATE INDEX idx_admin_missing_chats_title "
        "ON admin_missing_chats(chat_title COLLATE NOCASE)"
    )
    _ensure_index(
        cur,
        "idx_admin_missing_chats_last_message",
        "CREATE INDEX idx_admin_missing_chats_last_message "
        "ON admin_missing_chats(last_message_ts DESC)"
    )


def _create_admin_absent_chat_indexes(cur: sqlite3.Cursor):
    _ensure_index(
        cur,
        "idx_admin_absent_chats_scanned",
        "CREATE INDEX idx_admin_absent_chats_scanned "
        "ON admin_absent_chats(scanned_at DESC)"
    )
    _ensure_index(
        cur,
        "idx_admin_absent_chats_count",
        "CREATE INDEX idx_admin_absent_chats_count "
        "ON admin_absent_chats(message_count DESC, last_message_ts DESC)"
    )
    _ensure_index(
        cur,
        "idx_admin_absent_chats_title",
        "CREATE INDEX idx_admin_absent_chats_title "
        "ON admin_absent_chats(chat_title COLLATE NOCASE)"
    )
    _ensure_index(
        cur,
        "idx_admin_absent_chats_last_message",
        "CREATE INDEX idx_admin_absent_chats_last_message "
        "ON admin_absent_chats(last_message_ts DESC)"
    )


def _create_admin_restricted_chat_indexes(cur: sqlite3.Cursor):
    _ensure_index(
        cur,
        "idx_admin_restricted_chats_scanned",
        "CREATE INDEX idx_admin_restricted_chats_scanned "
        "ON admin_restricted_chats(scanned_at DESC)"
    )
    _ensure_index(
        cur,
        "idx_admin_restricted_chats_title",
        "CREATE INDEX idx_admin_restricted_chats_title "
        "ON admin_restricted_chats(chat_title COLLATE NOCASE)"
    )
    _ensure_index(
        cur,
        "idx_admin_restricted_chats_public",
        "CREATE INDEX idx_admin_restricted_chats_public "
        "ON admin_restricted_chats(is_public, chat_title COLLATE NOCASE)"
    )
    _ensure_index(
        cur,
        "idx_admin_restricted_chats_last_message",
        "CREATE INDEX idx_admin_restricted_chats_last_message "
        "ON admin_restricted_chats(last_message_ts DESC)"
    )


def _create_admin_recovery_chat_indexes(cur: sqlite3.Cursor):
    _ensure_index(
        cur,
        "idx_admin_recovery_chats_scanned",
        "CREATE INDEX idx_admin_recovery_chats_scanned "
        "ON admin_recovery_chats(scanned_at DESC)"
    )
    _ensure_index(
        cur,
        "idx_admin_recovery_chats_title",
        "CREATE INDEX idx_admin_recovery_chats_title "
        "ON admin_recovery_chats(chat_title COLLATE NOCASE)"
    )
    _ensure_index(
        cur,
        "idx_admin_recovery_chats_recovered",
        "CREATE INDEX idx_admin_recovery_chats_recovered "
        "ON admin_recovery_chats(recovered_at, chat_title COLLATE NOCASE)"
    )
    _ensure_index(
        cur,
        "idx_admin_recovery_chats_session_ts",
        "CREATE INDEX idx_admin_recovery_chats_session_ts "
        "ON admin_recovery_chats(session_entity_ts DESC)"
    )


def _create_admin_clone_run_indexes(cur: sqlite3.Cursor):
    _ensure_index(
        cur,
        "idx_admin_clone_runs_source_updated",
        "CREATE INDEX idx_admin_clone_runs_source_updated "
        "ON admin_clone_runs(source_chat_id, updated_at DESC)",
    )
    _ensure_index(
        cur,
        "idx_admin_clone_runs_status_updated",
        "CREATE INDEX idx_admin_clone_runs_status_updated "
        "ON admin_clone_runs(status, updated_at DESC)",
    )
    _ensure_index(
        cur,
        "idx_admin_clone_runs_target",
        "CREATE INDEX idx_admin_clone_runs_target "
        "ON admin_clone_runs(target_chat_id, target_title COLLATE NOCASE)",
    )


def _create_admin_clone_plan_indexes(cur: sqlite3.Cursor):
    _ensure_index(
        cur,
        "idx_admin_clone_plans_run_updated",
        "CREATE INDEX idx_admin_clone_plans_run_updated "
        "ON admin_clone_plans(run_id, updated_at DESC)",
    )
    _ensure_index(
        cur,
        "idx_admin_clone_plans_status_updated",
        "CREATE INDEX idx_admin_clone_plans_status_updated "
        "ON admin_clone_plans(status, updated_at DESC)",
    )


def _create_admin_clone_migration_indexes(cur: sqlite3.Cursor):
    _ensure_index(
        cur,
        "idx_admin_clone_migrations_run_updated",
        "CREATE INDEX idx_admin_clone_migrations_run_updated "
        "ON admin_clone_migrations(run_id, updated_at DESC)",
    )
    _ensure_index(
        cur,
        "idx_admin_clone_migrations_status_updated",
        "CREATE INDEX idx_admin_clone_migrations_status_updated "
        "ON admin_clone_migrations(status, updated_at DESC)",
    )
    _ensure_index(
        cur,
        "idx_admin_clone_message_map_source",
        "CREATE INDEX idx_admin_clone_message_map_source "
        "ON admin_clone_message_map("
        "run_id, source_chat_id, source_message_id, chunk_index, mode"
        ")",
    )
    _ensure_index(
        cur,
        "idx_admin_clone_message_map_migration",
        "CREATE INDEX idx_admin_clone_message_map_migration "
        "ON admin_clone_message_map(migration_id, status, updated_at DESC)",
    )


def _create_indexes(cur: sqlite3.Cursor):
    _drop_obsolete_indexes(cur)
    _create_chat_indexes(cur)
    _create_message_indexes(cur)
    _create_media_indexes(cur)
    _create_media_group_indexes(cur)
    _create_dedupe_indexes(cur)
    _create_message_search_term_indexes(cur)
    _create_admin_job_indexes(cur)
    _create_admin_missing_chat_indexes(cur)
    _create_admin_absent_chat_indexes(cur)
    _create_admin_restricted_chat_indexes(cur)
    _create_admin_recovery_chat_indexes(cur)
    _create_admin_clone_run_indexes(cur)
    _create_admin_clone_plan_indexes(cur)
    _create_admin_clone_migration_indexes(cur)
