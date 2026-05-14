# -*- coding: utf-8 -*-
import sqlite3

from tg_harvest.storage.search_text_state import SEARCH_TEXT_PRESENT_COLUMN
from tg_harvest.storage.search_text_state import table_has_column


def _create_message_indexes(cur: sqlite3.Cursor):
    # 基础与主键引用索引
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_chat_id ON messages(chat_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_msg_id ON messages(message_id)")

    # 核心业务索引：按频道日期倒序排序（Web 列表主视图）
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_chat_date "
        "ON messages(chat_id, msg_date_ts DESC)"
    )

    # 媒体组关联索引（支持相册视图）
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_grouped_id "
        "ON messages(chat_id, grouped_id) WHERE grouped_id IS NOT NULL"
    )

    # 去重与内容标识索引
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_pure_hash "
        "ON messages(chat_id, pure_hash) WHERE pure_hash <> ''"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_dedupe_hash "
        "ON messages(chat_id, dedupe_hash) WHERE dedupe_hash <> ''"
    )

    # 推广内容识别与排序索引
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_promo "
        "ON messages(chat_id, is_promo, promo_score DESC, msg_date_ts DESC)"
    )

    # 发送者与消息类型聚合索引（支持筛选）
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_sender "
        "ON messages(chat_id, sender_id, msg_date_ts DESC)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_type "
        "ON messages(chat_id, msg_type, msg_date_ts DESC)"
    )

    # 全局时间轴（用于跨频道搜索展示）
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_date_global "
        "ON messages(msg_date_ts DESC)"
    )

    if table_has_column(cur, "messages", SEARCH_TEXT_PRESENT_COLUMN):
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_messages_unsearchable_pk "
            "ON messages(pk, chat_id, message_id, grouped_id) "
            "WHERE search_text_present = 0"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_messages_unsearchable_chat "
            "ON messages(chat_id, pk, message_id, grouped_id) "
            "WHERE search_text_present = 0"
        )


def _create_media_indexes(cur: sqlite3.Cursor):
    # 媒体引用与文件唯一性索引
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_media_file_ref "
        "ON message_media(chat_id, message_id)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_media_unique_id "
        "ON message_media(chat_id, file_unique_id) "
        "WHERE file_unique_id IS NOT NULL AND file_unique_id <> ''"
    )

    # 核心性能索引：文件指纹（用于跨频道去重检测）
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_media_fingerprint "
        "ON message_media(chat_id, media_fingerprint) "
        "WHERE media_fingerprint IS NOT NULL AND media_fingerprint <> ''"
    )

    # 核心排序索引：按文件大小排序
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_media_sort_size "
        "ON message_media(chat_id, file_size DESC, message_id DESC)"
    )

    # 核心排序索引：按媒体时长排序
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_media_sort_duration "
        "ON message_media(chat_id, duration_sec DESC, message_id DESC)"
    )

    # 类型与元数据过滤索引
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_media_kind "
        "ON message_media(chat_id, media_kind)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_media_mime "
        "ON message_media(chat_id, mime_type)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_media_grouped_id "
        "ON message_media(chat_id, grouped_id) WHERE grouped_id IS NOT NULL"
    )


def _create_media_group_indexes(cur: sqlite3.Cursor):
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_mg_pure_hash "
        "ON media_groups(chat_id, pure_hash) WHERE pure_hash <> ''"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_mg_media_sig "
        "ON media_groups(chat_id, media_sig_hash) WHERE media_sig_hash <> ''"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_mg_dedupe_hash "
        "ON media_groups(chat_id, dedupe_hash) WHERE dedupe_hash <> ''"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_mg_promo "
        "ON media_groups(chat_id, is_promo, item_count DESC)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_mg_time "
        "ON media_groups(chat_id, first_msg_date_ts DESC)"
    )


def _create_dedupe_indexes(cur: sqlite3.Cursor):
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dedupe_runs_batch ON dedupe_runs(batch_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dedupe_actions_batch ON dedupe_actions(batch_id)")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_dedupe_actions_chat_time "
        "ON dedupe_actions(chat_id, created_at DESC)"
    )


def _create_message_search_term_indexes(cur: sqlite3.Cursor):
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_message_search_terms_pk ON message_search_terms(pk)"
    )


def _create_admin_job_indexes(cur: sqlite3.Cursor):
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_admin_jobs_status_updated ON admin_jobs(status, updated_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_admin_jobs_target_chat ON admin_jobs(target_chat_id, status)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_admin_jobs_status_heartbeat ON admin_jobs(status, heartbeat_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_admin_job_logs_job_seq ON admin_job_logs(job_id, seq)"
    )


def _create_admin_missing_chat_indexes(cur: sqlite3.Cursor):
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_admin_missing_chats_scanned "
        "ON admin_missing_chats(scanned_at DESC)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_admin_missing_chats_title "
        "ON admin_missing_chats(chat_title COLLATE NOCASE)"
    )


def _create_admin_absent_chat_indexes(cur: sqlite3.Cursor):
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_admin_absent_chats_scanned "
        "ON admin_absent_chats(scanned_at DESC)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_admin_absent_chats_count "
        "ON admin_absent_chats(message_count DESC, last_seen_at DESC)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_admin_absent_chats_title "
        "ON admin_absent_chats(chat_title COLLATE NOCASE)"
    )


def _create_indexes(cur: sqlite3.Cursor):
    _create_message_indexes(cur)
    _create_media_indexes(cur)
    _create_media_group_indexes(cur)
    _create_dedupe_indexes(cur)
    _create_message_search_term_indexes(cur)
    _create_admin_job_indexes(cur)
    _create_admin_missing_chat_indexes(cur)
    _create_admin_absent_chat_indexes(cur)
