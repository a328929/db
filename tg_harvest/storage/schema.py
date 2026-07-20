import logging
import re
import sqlite3
from collections.abc import Iterable, Sequence
from contextlib import suppress

from tg_harvest.domain.coerce import optional_int, safe_int

from . import connection as _db_runtime
from . import indexes as _indexes
from . import manticore_outbox as _manticore_outbox
from .connection import SqliteFeatures

_CHAT_SUMMARY_BATCH_SIZE = 500

_LEGACY_SEARCH_TRIGGERS = (
    "trg_messages_fts_insert",
    "trg_messages_fts_delete",
    "trg_messages_fts_update",
    "trg_message_terms_queue_insert",
    "trg_message_terms_queue_update",
    "trg_message_terms_delete",
    "trg_message_terms_queue_count_insert",
    "trg_message_terms_queue_count_delete",
)


def _remove_legacy_search_objects(cur: sqlite3.Cursor) -> None:
    for trigger_name in _LEGACY_SEARCH_TRIGGERS:
        cur.execute(f"DROP TRIGGER IF EXISTS {trigger_name}")
    for index_name in (
        "idx_message_search_terms_pk",
        "idx_message_search_terms_queue_order",
    ):
        cur.execute(f"DROP INDEX IF EXISTS {index_name}")
    for table_name in (
        "message_search_terms_rebuild_queue",
        "message_search_terms",
        "message_search_terms_meta",
        "messages_fts",
        "messages_fts_config",
        "messages_fts_data",
        "messages_fts_docsize",
        "messages_fts_idx",
        "messages_fts_content",
    ):
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")

# =========================
# Schema 初始化
# =========================


def _create_chats_table(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS chats (
        chat_id          INTEGER PRIMARY KEY,
        chat_title       TEXT NOT NULL,
        chat_username    TEXT,
        is_public        INTEGER NOT NULL DEFAULT 0,
        chat_type        TEXT,
        message_count    INTEGER NOT NULL DEFAULT 0,
        last_message_created_at TEXT NOT NULL DEFAULT '',
        first_seen_at    TEXT NOT NULL DEFAULT (datetime('now')),
        last_seen_at     TEXT NOT NULL DEFAULT (datetime('now'))
    ){strict_suffix}
    """)


def _create_messages_table(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS messages (
        pk                   INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id              INTEGER NOT NULL,
        message_id           INTEGER NOT NULL,
        msg_date_text        TEXT NOT NULL,
        msg_date_ts          INTEGER NOT NULL,
        sender_id            INTEGER,

        content              TEXT,
        content_norm         TEXT,
        pure_hash            TEXT,
        dedupe_hash          TEXT,

        msg_type             TEXT NOT NULL,
        grouped_id           INTEGER,
        has_media            INTEGER NOT NULL DEFAULT 0,

        is_promo             INTEGER NOT NULL DEFAULT 0,
        promo_score          INTEGER NOT NULL DEFAULT 0,
        promo_reasons        TEXT,
        dedupe_eligible      INTEGER NOT NULL DEFAULT 0,
        guard_reason         TEXT,
        text_len             INTEGER NOT NULL DEFAULT 0,

        visual_hash          TEXT,
        visual_hash_algo     TEXT,
        visual_embed_ref     TEXT,
        created_at           TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at           TEXT NOT NULL DEFAULT (datetime('now')),

        UNIQUE(chat_id, message_id),
        FOREIGN KEY(chat_id) REFERENCES chats(chat_id)
    ){strict_suffix}
    """)


def _create_message_media_table(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS message_media (
        chat_id              INTEGER NOT NULL,
        message_id           INTEGER NOT NULL,
        media_kind           TEXT,
        file_unique_id       TEXT,
        file_name            TEXT,
        file_ext             TEXT,
        mime_type            TEXT,
        file_size            INTEGER,
        width                INTEGER,
        height               INTEGER,
        duration_sec         INTEGER,
        grouped_id           INTEGER,

        media_fingerprint    TEXT,
        meta_json            TEXT,

        updated_at           TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (chat_id, message_id),
        FOREIGN KEY(chat_id, message_id) REFERENCES messages(chat_id, message_id) ON DELETE CASCADE
    ){strict_suffix}
    """)


def _create_media_groups_table(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS media_groups (
        chat_id              INTEGER NOT NULL,
        grouped_id           INTEGER NOT NULL,

        first_message_id     INTEGER,
        first_msg_date_ts    INTEGER,
        last_message_id      INTEGER,
        last_msg_date_ts     INTEGER,

        item_count           INTEGER NOT NULL DEFAULT 0,
        active_items         INTEGER NOT NULL DEFAULT 0,

        types_csv            TEXT,
        captions_concat      TEXT,
        caption_norm         TEXT,
        pure_hash            TEXT,      -- caption 模板 hash
        media_sig_hash       TEXT,      -- 组内媒体指纹签名
        dedupe_hash          TEXT,      -- 当前主去重键（文本优先）

        is_promo             INTEGER NOT NULL DEFAULT 0,
        promo_score          INTEGER NOT NULL DEFAULT 0,
        promo_reasons        TEXT,
        dedupe_eligible      INTEGER NOT NULL DEFAULT 0,
        guard_reason         TEXT,

        created_at           TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at           TEXT NOT NULL DEFAULT (datetime('now')),

        PRIMARY KEY(chat_id, grouped_id),
        FOREIGN KEY(chat_id) REFERENCES chats(chat_id)
    ){strict_suffix}
    """)


def _create_dedupe_tables(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS dedupe_runs (
        batch_id                 TEXT PRIMARY KEY,
        chat_id                  INTEGER NOT NULL,
        mode                     TEXT NOT NULL,
        threshold                INTEGER NOT NULL,
        promo_threshold          INTEGER NOT NULL,
        dup_hash_count_solo      INTEGER NOT NULL DEFAULT 0,
        dup_hash_count_group_txt INTEGER NOT NULL DEFAULT 0,
        dup_hash_count_group_med INTEGER NOT NULL DEFAULT 0,
        target_count             INTEGER NOT NULL DEFAULT 0,
        started_at               TEXT NOT NULL DEFAULT (datetime('now')),
        finished_at              TEXT
    ){strict_suffix}
    """)

    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS dedupe_actions (
        id                   INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id             TEXT NOT NULL,
        chat_id              INTEGER NOT NULL,
        pk                   INTEGER NOT NULL,
        message_id           INTEGER NOT NULL,
        grouped_id           INTEGER,
        dedupe_hash          TEXT,
        pure_hash            TEXT,
        action               TEXT NOT NULL,
        reason               TEXT NOT NULL,
        created_at           TEXT NOT NULL DEFAULT (datetime('now'))
    ){strict_suffix}
    """)


def _create_admin_job_tables(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS admin_jobs (
        job_id               TEXT PRIMARY KEY,
        job_type             TEXT NOT NULL,
        status               TEXT NOT NULL,
        target_chat_id       INTEGER,
        target_label         TEXT,
        created_at           TEXT NOT NULL,
        updated_at           TEXT NOT NULL,
        owner_instance_id    TEXT,
        owner_pid            INTEGER,
        owner_host           TEXT,
        heartbeat_at         TEXT NOT NULL DEFAULT (datetime('now')),
        progress_current     INTEGER NOT NULL DEFAULT 0,
        progress_total       INTEGER,
        progress_stage       TEXT NOT NULL DEFAULT 'queued',
        last_logged_current  INTEGER NOT NULL DEFAULT 0,
        stop_requested       INTEGER NOT NULL DEFAULT 0
    ){strict_suffix}
    """)

    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS admin_job_logs (
        job_id               TEXT NOT NULL,
        seq                  INTEGER NOT NULL,
        ts                   TEXT NOT NULL,
        message              TEXT NOT NULL,
        PRIMARY KEY (job_id, seq),
        FOREIGN KEY(job_id) REFERENCES admin_jobs(job_id) ON DELETE CASCADE
    ){strict_suffix}
    """)


def _create_admin_missing_chats_table(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS admin_missing_chats (
        chat_id              INTEGER PRIMARY KEY,
        chat_title           TEXT NOT NULL,
        chat_username        TEXT,
        chat_type            TEXT,
        is_public            INTEGER NOT NULL DEFAULT 0,
        unavailable_reason   TEXT,
        last_message_at      TEXT,
        last_message_ts      INTEGER,
        scan_job_id          TEXT,
        scanned_at           TEXT NOT NULL
    ){strict_suffix}
    """)


def _create_admin_restricted_chats_table(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS admin_restricted_chats (
        chat_id                  INTEGER PRIMARY KEY,
        chat_title               TEXT NOT NULL,
        chat_username            TEXT,
        chat_type                TEXT,
        is_public                INTEGER NOT NULL DEFAULT 0,
        restriction_platforms    TEXT,
        restriction_reasons      TEXT,
        restriction_text         TEXT,
        risk_flags               TEXT,
        membership_scope         TEXT NOT NULL DEFAULT 'joined',
        last_message_at          TEXT,
        last_message_ts          INTEGER,
        scan_job_id              TEXT,
        scanned_at               TEXT NOT NULL
    ){strict_suffix}
    """)


def _create_admin_recovery_chats_table(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS admin_recovery_chats (
        chat_id                  INTEGER PRIMARY KEY,
        chat_title               TEXT NOT NULL,
        chat_username            TEXT,
        chat_type                TEXT,
        is_public                INTEGER NOT NULL DEFAULT 0,
        source_session           TEXT,
        source_entity_id         INTEGER,
        source_access_hash       INTEGER,
        availability_reason      TEXT,
        session_entity_date      TEXT,
        session_entity_ts        INTEGER,
        recovered_at             TEXT,
        recovered_job_id         TEXT,
        scan_job_id              TEXT,
        scanned_at               TEXT NOT NULL
    ){strict_suffix}
    """)


def _create_admin_clone_runs_table(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS admin_clone_runs (
        run_id                   TEXT PRIMARY KEY,
        job_id                   TEXT NOT NULL UNIQUE,
        deletion_job_id         TEXT NOT NULL DEFAULT '',
        source_chat_id           INTEGER NOT NULL,
        source_title             TEXT NOT NULL,
        source_chat_username     TEXT,
        source_chat_type         TEXT,
        source_message_count     INTEGER NOT NULL DEFAULT 0,
        source_last_message_at   TEXT,
        source_last_message_ts   INTEGER,
        target_chat_id           INTEGER,
        target_access_hash       TEXT,
        target_title             TEXT NOT NULL,
        target_kind              TEXT NOT NULL,
        target_username          TEXT,
        target_owner_session     TEXT,
        phase                    TEXT NOT NULL DEFAULT 'queued',
        status                   TEXT NOT NULL DEFAULT 'queued',
        plan_json                TEXT,
        error_message            TEXT,
        target_created_at        TEXT,
        completed_at             TEXT,
        created_at               TEXT NOT NULL,
        updated_at               TEXT NOT NULL
    ){strict_suffix}
    """)


def _create_admin_clone_plans_table(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS admin_clone_plans (
        plan_id                  TEXT PRIMARY KEY,
        run_id                   TEXT NOT NULL,
        job_id                   TEXT,
        status                   TEXT NOT NULL DEFAULT 'queued',
        source_access            TEXT NOT NULL DEFAULT 'unknown',
        target_access            TEXT NOT NULL DEFAULT 'unknown',
        primary_session_status   TEXT NOT NULL DEFAULT 'unknown',
        secondary_session_status TEXT NOT NULL DEFAULT 'unknown',
        migration_account        TEXT NOT NULL DEFAULT '',
        text_strategy            TEXT NOT NULL DEFAULT '',
        media_strategy           TEXT NOT NULL DEFAULT '',
        media_group_strategy     TEXT NOT NULL DEFAULT '',
        avatar_strategy          TEXT NOT NULL DEFAULT '',
        blocking_issues_json     TEXT NOT NULL DEFAULT '[]',
        warnings_json            TEXT NOT NULL DEFAULT '[]',
        capabilities_json        TEXT NOT NULL DEFAULT '{{}}',
        plan_json                TEXT NOT NULL DEFAULT '{{}}',
        error_message            TEXT,
        created_at               TEXT NOT NULL,
        updated_at               TEXT NOT NULL,
        completed_at             TEXT,
        FOREIGN KEY(run_id) REFERENCES admin_clone_runs(run_id) ON DELETE CASCADE
    ){strict_suffix}
    """)


def _create_admin_clone_migrations_table(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS admin_clone_migrations (
        migration_id             TEXT PRIMARY KEY,
        run_id                   TEXT NOT NULL,
        plan_id                  TEXT,
        job_id                   TEXT,
        mode                     TEXT NOT NULL DEFAULT 'text_replay',
        status                   TEXT NOT NULL DEFAULT 'queued',
        phase                    TEXT NOT NULL DEFAULT 'queued',
        target_chat_id           INTEGER,
        target_title             TEXT,
        target_write_account     TEXT NOT NULL DEFAULT '',
        requested_limit          INTEGER NOT NULL DEFAULT 0,
        send_delay_ms            INTEGER NOT NULL DEFAULT 0,
        text_total               INTEGER NOT NULL DEFAULT 0,
        text_sent                INTEGER NOT NULL DEFAULT 0,
        text_skipped             INTEGER NOT NULL DEFAULT 0,
        text_failed              INTEGER NOT NULL DEFAULT 0,
        media_total              INTEGER NOT NULL DEFAULT 0,
        media_sent               INTEGER NOT NULL DEFAULT 0,
        media_skipped            INTEGER NOT NULL DEFAULT 0,
        media_failed             INTEGER NOT NULL DEFAULT 0,
        media_group_total        INTEGER NOT NULL DEFAULT 0,
        media_group_sent         INTEGER NOT NULL DEFAULT 0,
        media_group_skipped      INTEGER NOT NULL DEFAULT 0,
        media_group_failed       INTEGER NOT NULL DEFAULT 0,
        plan_json                TEXT NOT NULL DEFAULT '{{}}',
        error_message            TEXT,
        created_at               TEXT NOT NULL,
        updated_at               TEXT NOT NULL,
        completed_at             TEXT,
        FOREIGN KEY(run_id) REFERENCES admin_clone_runs(run_id) ON DELETE CASCADE,
        FOREIGN KEY(plan_id) REFERENCES admin_clone_plans(plan_id) ON DELETE SET NULL
    ){strict_suffix}
    """)


def _create_admin_clone_message_map_table(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS admin_clone_message_map (
        id                       INTEGER PRIMARY KEY AUTOINCREMENT,
        migration_id             TEXT NOT NULL,
        run_id                   TEXT NOT NULL,
        plan_id                  TEXT,
        source_chat_id           INTEGER NOT NULL,
        source_message_id        INTEGER NOT NULL,
        source_msg_date_ts       INTEGER,
        source_msg_date_text     TEXT,
        target_chat_id           INTEGER NOT NULL,
        target_message_id        INTEGER,
        delivery_random_id       INTEGER,
        delivery_account         TEXT NOT NULL DEFAULT '',
        chunk_index              INTEGER NOT NULL DEFAULT 0,
        chunk_count              INTEGER NOT NULL DEFAULT 1,
        mode                     TEXT NOT NULL DEFAULT 'text_replay',
        status                   TEXT NOT NULL DEFAULT 'done',
        error_message            TEXT,
        sent_at                  TEXT,
        created_at               TEXT NOT NULL,
        updated_at               TEXT NOT NULL,
        UNIQUE(
            run_id,
            source_chat_id,
            source_message_id,
            chunk_index,
            mode
        ),
        FOREIGN KEY(migration_id) REFERENCES admin_clone_migrations(migration_id)
            ON DELETE CASCADE,
        FOREIGN KEY(run_id) REFERENCES admin_clone_runs(run_id) ON DELETE CASCADE,
        FOREIGN KEY(plan_id) REFERENCES admin_clone_plans(plan_id) ON DELETE SET NULL
    ){strict_suffix}
    """)


def _create_admin_clone_media_transfers_table(
    cur: sqlite3.Cursor,
    strict_suffix: str,
):
    """Persist every media delivery hop so a restart can resume idempotently."""
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS admin_clone_media_transfers (
        id                       INTEGER PRIMARY KEY AUTOINCREMENT,
        migration_id             TEXT NOT NULL,
        run_id                   TEXT NOT NULL,
        plan_id                  TEXT,
        source_chat_id           INTEGER NOT NULL,
        source_message_id        INTEGER NOT NULL,
        target_chat_id           INTEGER NOT NULL,
        transfer_strategy        TEXT NOT NULL,
        relay_chat_id            INTEGER,
        source_account           TEXT NOT NULL DEFAULT '',
        target_account           TEXT NOT NULL DEFAULT '',
        source_random_id         INTEGER,
        target_random_id         INTEGER NOT NULL,
        relay_message_id         INTEGER,
        target_message_id        INTEGER,
        source_hop_status        TEXT NOT NULL DEFAULT 'not_required',
        target_hop_status        TEXT NOT NULL DEFAULT 'pending',
        cleanup_status           TEXT NOT NULL DEFAULT 'not_required',
        error_message            TEXT NOT NULL DEFAULT '',
        created_at               TEXT NOT NULL,
        updated_at               TEXT NOT NULL,
        UNIQUE(run_id, source_chat_id, source_message_id),
        FOREIGN KEY(migration_id) REFERENCES admin_clone_migrations(migration_id)
            ON DELETE CASCADE,
        FOREIGN KEY(run_id) REFERENCES admin_clone_runs(run_id) ON DELETE CASCADE,
        FOREIGN KEY(plan_id) REFERENCES admin_clone_plans(plan_id) ON DELETE SET NULL
    ){strict_suffix}
    """)


def _create_sync_scheduler_tables(cur: sqlite3.Cursor, strict_suffix: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS account_runtime_state (
        account_key              TEXT PRIMARY KEY,
        session_name             TEXT NOT NULL DEFAULT '',
        label                    TEXT NOT NULL DEFAULT '',
        cooldown_until           TEXT NOT NULL DEFAULT '',
        public_resolve_used      INTEGER NOT NULL DEFAULT 0,
        recent_success_count     INTEGER NOT NULL DEFAULT 0,
        recent_failure_count     INTEGER NOT NULL DEFAULT 0,
        avg_duration_seconds     REAL NOT NULL DEFAULT 0,
        last_success_at          TEXT NOT NULL DEFAULT '',
        last_failure_at          TEXT NOT NULL DEFAULT '',
        last_failure_message     TEXT NOT NULL DEFAULT '',
        in_flight_count          INTEGER NOT NULL DEFAULT 0,
        updated_at               TEXT NOT NULL DEFAULT (datetime('now'))
    ){strict_suffix}
    """)

    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS sync_chat_state (
        chat_id                  INTEGER PRIMARY KEY,
        chat_title               TEXT NOT NULL DEFAULT '',
        chat_username            TEXT,
        membership_scope         TEXT NOT NULL DEFAULT 'unknown',
        status                   TEXT NOT NULL DEFAULT 'idle',
        last_event_at            TEXT NOT NULL DEFAULT '',
        last_event_reason        TEXT NOT NULL DEFAULT '',
        last_probe_at            TEXT NOT NULL DEFAULT '',
        last_probe_status        TEXT NOT NULL DEFAULT '',
        last_update_at           TEXT NOT NULL DEFAULT '',
        last_success_at          TEXT NOT NULL DEFAULT '',
        last_failure_at          TEXT NOT NULL DEFAULT '',
        last_failure_message     TEXT NOT NULL DEFAULT '',
        remote_last_id           INTEGER NOT NULL DEFAULT 0,
        local_last_id            INTEGER NOT NULL DEFAULT 0,
        failure_count            INTEGER NOT NULL DEFAULT 0,
        unavailable_count        INTEGER NOT NULL DEFAULT 0,
        quarantine_reason        TEXT NOT NULL DEFAULT '',
        next_probe_at            TEXT NOT NULL DEFAULT '',
        next_update_at           TEXT NOT NULL DEFAULT '',
        model_delay_seconds      INTEGER NOT NULL DEFAULT 0,
        priority_score           REAL NOT NULL DEFAULT 0,
        source_accounts          TEXT NOT NULL DEFAULT '',
        last_source_account      TEXT NOT NULL DEFAULT '',
        is_active                INTEGER NOT NULL DEFAULT 1,
        created_at               TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at               TEXT NOT NULL DEFAULT (datetime('now'))
    ){strict_suffix}
    """)

    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS sync_pending_updates (
        chat_id                  INTEGER PRIMARY KEY,
        chat_title               TEXT NOT NULL DEFAULT '',
        chat_username            TEXT,
        first_event_at           TEXT NOT NULL DEFAULT '',
        last_event_at            TEXT NOT NULL DEFAULT '',
        event_count              INTEGER NOT NULL DEFAULT 0,
        source_accounts          TEXT NOT NULL DEFAULT '',
        reasons                  TEXT NOT NULL DEFAULT '',
        preferred_source_account TEXT NOT NULL DEFAULT '',
        due_at                   TEXT NOT NULL DEFAULT '',
        priority_score           REAL NOT NULL DEFAULT 0,
        quiet_delay_seconds      INTEGER NOT NULL DEFAULT 0,
        generation               INTEGER NOT NULL DEFAULT 0,
        in_flight                INTEGER NOT NULL DEFAULT 0,
        in_flight_generation     INTEGER NOT NULL DEFAULT 0,
        in_flight_owner_instance_id TEXT NOT NULL DEFAULT '',
        in_flight_owner_pid      INTEGER NOT NULL DEFAULT 0,
        in_flight_owner_host     TEXT NOT NULL DEFAULT '',
        dirty_generation         INTEGER NOT NULL DEFAULT 0,
        created_at               TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at               TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY(chat_id) REFERENCES chats(chat_id) ON DELETE CASCADE
    ){strict_suffix}
    """)

    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS sync_learning_events (
        id                       INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id                  INTEGER NOT NULL,
        event_type               TEXT NOT NULL,
        reason                   TEXT NOT NULL DEFAULT '',
        source_account           TEXT NOT NULL DEFAULT '',
        membership_scope         TEXT NOT NULL DEFAULT 'unknown',
        status                   TEXT NOT NULL DEFAULT '',
        features_json            TEXT NOT NULL DEFAULT '{{}}',
        prediction_json          TEXT NOT NULL DEFAULT '{{}}',
        outcome_json             TEXT NOT NULL DEFAULT '{{}}',
        quiet_delay_seconds      INTEGER NOT NULL DEFAULT 0,
        priority_score           REAL NOT NULL DEFAULT 0,
        added_message_count      INTEGER NOT NULL DEFAULT 0,
        wait_seconds             INTEGER NOT NULL DEFAULT 0,
        api_cost                 REAL NOT NULL DEFAULT 0,
        failure_type             TEXT NOT NULL DEFAULT '',
        created_at               TEXT NOT NULL DEFAULT (datetime('now'))
    ){strict_suffix}
    """)

    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS sync_model_state (
        model_key                TEXT PRIMARY KEY,
        model_version            TEXT NOT NULL DEFAULT '',
        backend                  TEXT NOT NULL DEFAULT '',
        metrics_json             TEXT NOT NULL DEFAULT '{{}}',
        trained_at               TEXT NOT NULL DEFAULT '',
        artifact_path            TEXT NOT NULL DEFAULT '',
        state_json               TEXT NOT NULL DEFAULT '{{}}',
        updated_at               TEXT NOT NULL DEFAULT (datetime('now'))
    ){strict_suffix}
    """)


def _create_tables(cur: sqlite3.Cursor, strict_suffix: str):
    _create_chats_table(cur, strict_suffix)
    _create_messages_table(cur, strict_suffix)
    _create_message_media_table(cur, strict_suffix)
    _create_media_groups_table(cur, strict_suffix)
    _create_dedupe_tables(cur, strict_suffix)
    _create_admin_job_tables(cur, strict_suffix)
    _create_admin_missing_chats_table(cur, strict_suffix)
    _create_admin_restricted_chats_table(cur, strict_suffix)
    _create_admin_recovery_chats_table(cur, strict_suffix)
    _create_admin_clone_runs_table(cur, strict_suffix)
    _create_admin_clone_plans_table(cur, strict_suffix)
    _create_admin_clone_migrations_table(cur, strict_suffix)
    _create_admin_clone_message_map_table(cur, strict_suffix)
    _create_admin_clone_media_transfers_table(cur, strict_suffix)
    _create_sync_scheduler_tables(cur, strict_suffix)
    _manticore_outbox.create_manticore_outbox_table(cur, strict_suffix)


def _column_exists(cur: sqlite3.Cursor, table_name: str, column_name: str) -> bool:
    try:
        cur.execute(f"PRAGMA table_xinfo({table_name})")
    except sqlite3.Error:
        cur.execute(f"PRAGMA table_info({table_name})")
    return any(str(row[1]) == column_name for row in cur.fetchall())


_DYNAMIC_DATETIME_DEFAULT_RE = re.compile(
    r"\s+DEFAULT\s*\(\s*datetime\s*\(\s*['\"]now['\"]\s*\)\s*\)",
    re.IGNORECASE,
)


def _column_sql_has_dynamic_datetime_default(column_sql: str) -> bool:
    return _DYNAMIC_DATETIME_DEFAULT_RE.search(str(column_sql or "")) is not None


def _sqlite_add_column_sql(column_sql: str) -> str:
    # SQLite cannot ADD COLUMN with DEFAULT(datetime('now')) on existing tables.
    # Use a constant default, then populate current rows below.
    return _DYNAMIC_DATETIME_DEFAULT_RE.sub(" DEFAULT ''", str(column_sql or ""))


def _add_table_column(
    cur: sqlite3.Cursor, table_name: str, column_name: str, column_sql: str
) -> None:
    if not _column_sql_has_dynamic_datetime_default(column_sql):
        cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_sql}")
        return

    cur.execute(
        f"ALTER TABLE {table_name} ADD COLUMN {_sqlite_add_column_sql(column_sql)}"
    )
    cur.execute(
        f"""
        UPDATE {table_name}
        SET {column_name} = datetime('now')
        WHERE {column_name} IS NULL OR {column_name} = ''
        """
    )


def _ensure_table_columns(
    cur: sqlite3.Cursor, table_name: str, column_defs: Sequence[tuple[str, str]]
) -> None:
    if not _table_exists(cur, table_name):
        return
    for column_name, column_sql in column_defs:
        if _column_exists(cur, table_name, column_name):
            continue
        _add_table_column(cur, table_name, column_name, column_sql)


def _ensure_chats_schema(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "chats",
        [
            ("is_public", "is_public INTEGER NOT NULL DEFAULT 0"),
            ("chat_type", "chat_type TEXT"),
            ("message_count", "message_count INTEGER NOT NULL DEFAULT 0"),
            (
                "last_message_created_at",
                "last_message_created_at TEXT NOT NULL DEFAULT ''",
            ),
            ("first_seen_at", "first_seen_at TEXT NOT NULL DEFAULT (datetime('now'))"),
            ("last_seen_at", "last_seen_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )


def _ensure_messages_runtime_columns(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "messages",
        [
            ("msg_date_text", "msg_date_text TEXT NOT NULL DEFAULT ''"),
            ("msg_date_ts", "msg_date_ts INTEGER NOT NULL DEFAULT 0"),
            ("sender_id", "sender_id INTEGER"),
            ("content", "content TEXT"),
            ("content_norm", "content_norm TEXT"),
            ("pure_hash", "pure_hash TEXT"),
            ("dedupe_hash", "dedupe_hash TEXT"),
            ("msg_type", "msg_type TEXT NOT NULL DEFAULT 'TEXT'"),
            ("grouped_id", "grouped_id INTEGER"),
            ("has_media", "has_media INTEGER NOT NULL DEFAULT 0"),
            ("is_promo", "is_promo INTEGER NOT NULL DEFAULT 0"),
            ("promo_score", "promo_score INTEGER NOT NULL DEFAULT 0"),
            ("promo_reasons", "promo_reasons TEXT"),
            ("dedupe_eligible", "dedupe_eligible INTEGER NOT NULL DEFAULT 0"),
            ("guard_reason", "guard_reason TEXT"),
            ("text_len", "text_len INTEGER NOT NULL DEFAULT 0"),
            ("visual_hash", "visual_hash TEXT"),
            ("visual_hash_algo", "visual_hash_algo TEXT"),
            ("visual_embed_ref", "visual_embed_ref TEXT"),
            ("created_at", "created_at TEXT NOT NULL DEFAULT (datetime('now'))"),
            ("updated_at", "updated_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )


def _ensure_message_media_runtime_columns(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "message_media",
        [
            ("media_kind", "media_kind TEXT"),
            ("file_unique_id", "file_unique_id TEXT"),
            ("file_name", "file_name TEXT"),
            ("file_ext", "file_ext TEXT"),
            ("mime_type", "mime_type TEXT"),
            ("file_size", "file_size INTEGER"),
            ("width", "width INTEGER"),
            ("height", "height INTEGER"),
            ("duration_sec", "duration_sec INTEGER"),
            ("grouped_id", "grouped_id INTEGER"),
            ("media_fingerprint", "media_fingerprint TEXT"),
            ("meta_json", "meta_json TEXT"),
            ("updated_at", "updated_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )


def _ensure_media_groups_runtime_columns(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "media_groups",
        [
            ("first_message_id", "first_message_id INTEGER"),
            ("first_msg_date_ts", "first_msg_date_ts INTEGER"),
            ("last_message_id", "last_message_id INTEGER"),
            ("last_msg_date_ts", "last_msg_date_ts INTEGER"),
            ("item_count", "item_count INTEGER NOT NULL DEFAULT 0"),
            ("active_items", "active_items INTEGER NOT NULL DEFAULT 0"),
            ("types_csv", "types_csv TEXT"),
            ("captions_concat", "captions_concat TEXT"),
            ("caption_norm", "caption_norm TEXT"),
            ("pure_hash", "pure_hash TEXT"),
            ("media_sig_hash", "media_sig_hash TEXT"),
            ("dedupe_hash", "dedupe_hash TEXT"),
            ("is_promo", "is_promo INTEGER NOT NULL DEFAULT 0"),
            ("promo_score", "promo_score INTEGER NOT NULL DEFAULT 0"),
            ("promo_reasons", "promo_reasons TEXT"),
            ("dedupe_eligible", "dedupe_eligible INTEGER NOT NULL DEFAULT 0"),
            ("guard_reason", "guard_reason TEXT"),
            ("created_at", "created_at TEXT NOT NULL DEFAULT (datetime('now'))"),
            ("updated_at", "updated_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )


def _ensure_dedupe_schema(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "dedupe_runs",
        [
            ("dup_hash_count_solo", "dup_hash_count_solo INTEGER NOT NULL DEFAULT 0"),
            (
                "dup_hash_count_group_txt",
                "dup_hash_count_group_txt INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "dup_hash_count_group_med",
                "dup_hash_count_group_med INTEGER NOT NULL DEFAULT 0",
            ),
            ("target_count", "target_count INTEGER NOT NULL DEFAULT 0"),
            ("started_at", "started_at TEXT NOT NULL DEFAULT (datetime('now'))"),
            ("finished_at", "finished_at TEXT"),
        ],
    )
    _ensure_table_columns(
        cur,
        "dedupe_actions",
        [
            ("grouped_id", "grouped_id INTEGER"),
            ("dedupe_hash", "dedupe_hash TEXT"),
            ("pure_hash", "pure_hash TEXT"),
            ("created_at", "created_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )


def _ensure_admin_job_schema(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "admin_jobs",
        [
            ("owner_instance_id", "owner_instance_id TEXT"),
            ("owner_pid", "owner_pid INTEGER"),
            ("owner_host", "owner_host TEXT"),
            ("heartbeat_at", "heartbeat_at TEXT NOT NULL DEFAULT (datetime('now'))"),
            ("progress_current", "progress_current INTEGER NOT NULL DEFAULT 0"),
            ("progress_total", "progress_total INTEGER"),
            ("progress_stage", "progress_stage TEXT NOT NULL DEFAULT 'queued'"),
            ("last_logged_current", "last_logged_current INTEGER NOT NULL DEFAULT 0"),
            ("stop_requested", "stop_requested INTEGER NOT NULL DEFAULT 0"),
        ],
    )


def _ensure_admin_missing_chats_schema(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "admin_missing_chats",
        [
            ("chat_username", "chat_username TEXT"),
            ("chat_type", "chat_type TEXT"),
            ("is_public", "is_public INTEGER NOT NULL DEFAULT 0"),
            ("unavailable_reason", "unavailable_reason TEXT"),
            ("last_message_at", "last_message_at TEXT"),
            ("last_message_ts", "last_message_ts INTEGER"),
            ("scan_job_id", "scan_job_id TEXT"),
            ("scanned_at", "scanned_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )


def _ensure_admin_restricted_chats_schema(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "admin_restricted_chats",
        [
            ("chat_username", "chat_username TEXT"),
            ("chat_type", "chat_type TEXT"),
            ("is_public", "is_public INTEGER NOT NULL DEFAULT 0"),
            ("restriction_platforms", "restriction_platforms TEXT"),
            ("restriction_reasons", "restriction_reasons TEXT"),
            ("restriction_text", "restriction_text TEXT"),
            ("risk_flags", "risk_flags TEXT"),
            (
                "membership_scope",
                "membership_scope TEXT NOT NULL DEFAULT 'joined'",
            ),
            ("last_message_at", "last_message_at TEXT"),
            ("last_message_ts", "last_message_ts INTEGER"),
            ("scan_job_id", "scan_job_id TEXT"),
            ("scanned_at", "scanned_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )


def _ensure_admin_recovery_chats_schema(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "admin_recovery_chats",
        [
            ("chat_username", "chat_username TEXT"),
            ("chat_type", "chat_type TEXT"),
            ("is_public", "is_public INTEGER NOT NULL DEFAULT 0"),
            ("source_session", "source_session TEXT"),
            ("source_entity_id", "source_entity_id INTEGER"),
            ("source_access_hash", "source_access_hash INTEGER"),
            ("availability_reason", "availability_reason TEXT"),
            ("session_entity_date", "session_entity_date TEXT"),
            ("session_entity_ts", "session_entity_ts INTEGER"),
            ("recovered_at", "recovered_at TEXT"),
            ("recovered_job_id", "recovered_job_id TEXT"),
            ("scan_job_id", "scan_job_id TEXT"),
            ("scanned_at", "scanned_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )


def _ensure_admin_clone_runs_schema(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "admin_clone_runs",
        [
            ("job_id", "job_id TEXT"),
            ("deletion_job_id", "deletion_job_id TEXT NOT NULL DEFAULT ''"),
            ("source_chat_id", "source_chat_id INTEGER NOT NULL DEFAULT 0"),
            ("source_title", "source_title TEXT NOT NULL DEFAULT ''"),
            ("source_chat_username", "source_chat_username TEXT"),
            ("source_chat_type", "source_chat_type TEXT"),
            (
                "source_message_count",
                "source_message_count INTEGER NOT NULL DEFAULT 0",
            ),
            ("source_last_message_at", "source_last_message_at TEXT"),
            ("source_last_message_ts", "source_last_message_ts INTEGER"),
            ("target_chat_id", "target_chat_id INTEGER"),
            ("target_access_hash", "target_access_hash TEXT"),
            ("target_title", "target_title TEXT NOT NULL DEFAULT ''"),
            ("target_kind", "target_kind TEXT NOT NULL DEFAULT 'channel'"),
            ("target_username", "target_username TEXT"),
            ("target_owner_session", "target_owner_session TEXT"),
            ("phase", "phase TEXT NOT NULL DEFAULT 'queued'"),
            ("status", "status TEXT NOT NULL DEFAULT 'queued'"),
            ("plan_json", "plan_json TEXT"),
            ("error_message", "error_message TEXT"),
            ("target_created_at", "target_created_at TEXT"),
            ("completed_at", "completed_at TEXT"),
            ("created_at", "created_at TEXT NOT NULL DEFAULT (datetime('now'))"),
            ("updated_at", "updated_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )


def _ensure_admin_clone_plans_schema(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "admin_clone_plans",
        [
            ("run_id", "run_id TEXT NOT NULL DEFAULT ''"),
            ("job_id", "job_id TEXT"),
            ("status", "status TEXT NOT NULL DEFAULT 'queued'"),
            ("source_access", "source_access TEXT NOT NULL DEFAULT 'unknown'"),
            ("target_access", "target_access TEXT NOT NULL DEFAULT 'unknown'"),
            (
                "primary_session_status",
                "primary_session_status TEXT NOT NULL DEFAULT 'unknown'",
            ),
            (
                "secondary_session_status",
                "secondary_session_status TEXT NOT NULL DEFAULT 'unknown'",
            ),
            ("migration_account", "migration_account TEXT NOT NULL DEFAULT ''"),
            ("text_strategy", "text_strategy TEXT NOT NULL DEFAULT ''"),
            ("media_strategy", "media_strategy TEXT NOT NULL DEFAULT ''"),
            (
                "media_group_strategy",
                "media_group_strategy TEXT NOT NULL DEFAULT ''",
            ),
            ("avatar_strategy", "avatar_strategy TEXT NOT NULL DEFAULT ''"),
            (
                "blocking_issues_json",
                "blocking_issues_json TEXT NOT NULL DEFAULT '[]'",
            ),
            ("warnings_json", "warnings_json TEXT NOT NULL DEFAULT '[]'"),
            ("capabilities_json", "capabilities_json TEXT NOT NULL DEFAULT '{}'"),
            ("plan_json", "plan_json TEXT NOT NULL DEFAULT '{}'"),
            ("error_message", "error_message TEXT"),
            ("created_at", "created_at TEXT NOT NULL DEFAULT (datetime('now'))"),
            ("updated_at", "updated_at TEXT NOT NULL DEFAULT (datetime('now'))"),
            ("completed_at", "completed_at TEXT"),
        ],
    )


def _ensure_admin_clone_migrations_schema(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "admin_clone_migrations",
        [
            ("run_id", "run_id TEXT NOT NULL DEFAULT ''"),
            ("plan_id", "plan_id TEXT"),
            ("job_id", "job_id TEXT"),
            ("mode", "mode TEXT NOT NULL DEFAULT 'text_replay'"),
            ("status", "status TEXT NOT NULL DEFAULT 'queued'"),
            ("phase", "phase TEXT NOT NULL DEFAULT 'queued'"),
            ("target_chat_id", "target_chat_id INTEGER"),
            ("target_title", "target_title TEXT"),
            (
                "target_write_account",
                "target_write_account TEXT NOT NULL DEFAULT ''",
            ),
            ("requested_limit", "requested_limit INTEGER NOT NULL DEFAULT 0"),
            ("send_delay_ms", "send_delay_ms INTEGER NOT NULL DEFAULT 0"),
            ("text_total", "text_total INTEGER NOT NULL DEFAULT 0"),
            ("text_sent", "text_sent INTEGER NOT NULL DEFAULT 0"),
            ("text_skipped", "text_skipped INTEGER NOT NULL DEFAULT 0"),
            ("text_failed", "text_failed INTEGER NOT NULL DEFAULT 0"),
            ("media_total", "media_total INTEGER NOT NULL DEFAULT 0"),
            ("media_sent", "media_sent INTEGER NOT NULL DEFAULT 0"),
            ("media_skipped", "media_skipped INTEGER NOT NULL DEFAULT 0"),
            ("media_failed", "media_failed INTEGER NOT NULL DEFAULT 0"),
            ("media_group_total", "media_group_total INTEGER NOT NULL DEFAULT 0"),
            ("media_group_sent", "media_group_sent INTEGER NOT NULL DEFAULT 0"),
            ("media_group_skipped", "media_group_skipped INTEGER NOT NULL DEFAULT 0"),
            ("media_group_failed", "media_group_failed INTEGER NOT NULL DEFAULT 0"),
            ("plan_json", "plan_json TEXT NOT NULL DEFAULT '{}'"),
            ("error_message", "error_message TEXT"),
            ("created_at", "created_at TEXT NOT NULL DEFAULT (datetime('now'))"),
            ("updated_at", "updated_at TEXT NOT NULL DEFAULT (datetime('now'))"),
            ("completed_at", "completed_at TEXT"),
        ],
    )


def _ensure_admin_clone_message_map_schema(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "admin_clone_message_map",
        [
            ("migration_id", "migration_id TEXT NOT NULL DEFAULT ''"),
            ("run_id", "run_id TEXT NOT NULL DEFAULT ''"),
            ("plan_id", "plan_id TEXT"),
            ("source_chat_id", "source_chat_id INTEGER NOT NULL DEFAULT 0"),
            ("source_message_id", "source_message_id INTEGER NOT NULL DEFAULT 0"),
            ("source_msg_date_ts", "source_msg_date_ts INTEGER"),
            ("source_msg_date_text", "source_msg_date_text TEXT"),
            ("target_chat_id", "target_chat_id INTEGER NOT NULL DEFAULT 0"),
            ("target_message_id", "target_message_id INTEGER"),
            ("delivery_random_id", "delivery_random_id INTEGER"),
            ("delivery_account", "delivery_account TEXT NOT NULL DEFAULT ''"),
            ("chunk_index", "chunk_index INTEGER NOT NULL DEFAULT 0"),
            ("chunk_count", "chunk_count INTEGER NOT NULL DEFAULT 1"),
            ("mode", "mode TEXT NOT NULL DEFAULT 'text_replay'"),
            ("status", "status TEXT NOT NULL DEFAULT 'done'"),
            ("error_message", "error_message TEXT"),
            ("sent_at", "sent_at TEXT"),
            ("created_at", "created_at TEXT NOT NULL DEFAULT (datetime('now'))"),
            ("updated_at", "updated_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )


def _ensure_admin_clone_media_transfers_schema(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "admin_clone_media_transfers",
        [
            ("migration_id", "migration_id TEXT NOT NULL DEFAULT ''"),
            ("run_id", "run_id TEXT NOT NULL DEFAULT ''"),
            ("plan_id", "plan_id TEXT"),
            ("source_chat_id", "source_chat_id INTEGER NOT NULL DEFAULT 0"),
            ("source_message_id", "source_message_id INTEGER NOT NULL DEFAULT 0"),
            ("target_chat_id", "target_chat_id INTEGER NOT NULL DEFAULT 0"),
            ("transfer_strategy", "transfer_strategy TEXT NOT NULL DEFAULT ''"),
            ("relay_chat_id", "relay_chat_id INTEGER"),
            ("source_account", "source_account TEXT NOT NULL DEFAULT ''"),
            ("target_account", "target_account TEXT NOT NULL DEFAULT ''"),
            ("source_random_id", "source_random_id INTEGER"),
            (
                "target_random_id",
                "target_random_id INTEGER NOT NULL DEFAULT 0",
            ),
            ("relay_message_id", "relay_message_id INTEGER"),
            ("target_message_id", "target_message_id INTEGER"),
            (
                "source_hop_status",
                "source_hop_status TEXT NOT NULL DEFAULT 'not_required'",
            ),
            (
                "target_hop_status",
                "target_hop_status TEXT NOT NULL DEFAULT 'pending'",
            ),
            (
                "cleanup_status",
                "cleanup_status TEXT NOT NULL DEFAULT 'not_required'",
            ),
            ("error_message", "error_message TEXT NOT NULL DEFAULT ''"),
            ("created_at", "created_at TEXT NOT NULL DEFAULT (datetime('now'))"),
            ("updated_at", "updated_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )
    cur.execute(
        """
        UPDATE admin_clone_media_transfers
        SET target_random_id = COALESCE(NULLIF(random(), 0), 1)
        WHERE target_random_id IS NULL OR target_random_id = 0
        """
    )
    cur.execute(
        """
        UPDATE admin_clone_media_transfers
        SET source_random_id = COALESCE(NULLIF(random(), 0), 1)
        WHERE transfer_strategy = 'relay'
          AND (source_random_id IS NULL OR source_random_id = 0)
        """
    )


def _ensure_sync_scheduler_schema(cur: sqlite3.Cursor) -> None:
    _ensure_table_columns(
        cur,
        "account_runtime_state",
        [
            ("session_name", "session_name TEXT NOT NULL DEFAULT ''"),
            ("label", "label TEXT NOT NULL DEFAULT ''"),
            ("cooldown_until", "cooldown_until TEXT NOT NULL DEFAULT ''"),
            ("public_resolve_used", "public_resolve_used INTEGER NOT NULL DEFAULT 0"),
            ("recent_success_count", "recent_success_count INTEGER NOT NULL DEFAULT 0"),
            ("recent_failure_count", "recent_failure_count INTEGER NOT NULL DEFAULT 0"),
            ("avg_duration_seconds", "avg_duration_seconds REAL NOT NULL DEFAULT 0"),
            ("last_success_at", "last_success_at TEXT NOT NULL DEFAULT ''"),
            ("last_failure_at", "last_failure_at TEXT NOT NULL DEFAULT ''"),
            ("last_failure_message", "last_failure_message TEXT NOT NULL DEFAULT ''"),
            ("in_flight_count", "in_flight_count INTEGER NOT NULL DEFAULT 0"),
            ("updated_at", "updated_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )
    _ensure_table_columns(
        cur,
        "sync_chat_state",
        [
            ("chat_title", "chat_title TEXT NOT NULL DEFAULT ''"),
            ("chat_username", "chat_username TEXT"),
            ("membership_scope", "membership_scope TEXT NOT NULL DEFAULT 'unknown'"),
            ("status", "status TEXT NOT NULL DEFAULT 'idle'"),
            ("last_event_at", "last_event_at TEXT NOT NULL DEFAULT ''"),
            ("last_event_reason", "last_event_reason TEXT NOT NULL DEFAULT ''"),
            ("last_probe_at", "last_probe_at TEXT NOT NULL DEFAULT ''"),
            ("last_probe_status", "last_probe_status TEXT NOT NULL DEFAULT ''"),
            ("last_update_at", "last_update_at TEXT NOT NULL DEFAULT ''"),
            ("last_success_at", "last_success_at TEXT NOT NULL DEFAULT ''"),
            ("last_failure_at", "last_failure_at TEXT NOT NULL DEFAULT ''"),
            (
                "last_failure_message",
                "last_failure_message TEXT NOT NULL DEFAULT ''",
            ),
            ("remote_last_id", "remote_last_id INTEGER NOT NULL DEFAULT 0"),
            ("local_last_id", "local_last_id INTEGER NOT NULL DEFAULT 0"),
            ("failure_count", "failure_count INTEGER NOT NULL DEFAULT 0"),
            ("unavailable_count", "unavailable_count INTEGER NOT NULL DEFAULT 0"),
            ("quarantine_reason", "quarantine_reason TEXT NOT NULL DEFAULT ''"),
            ("next_probe_at", "next_probe_at TEXT NOT NULL DEFAULT ''"),
            ("next_update_at", "next_update_at TEXT NOT NULL DEFAULT ''"),
            ("model_delay_seconds", "model_delay_seconds INTEGER NOT NULL DEFAULT 0"),
            ("priority_score", "priority_score REAL NOT NULL DEFAULT 0"),
            ("source_accounts", "source_accounts TEXT NOT NULL DEFAULT ''"),
            ("last_source_account", "last_source_account TEXT NOT NULL DEFAULT ''"),
            ("is_active", "is_active INTEGER NOT NULL DEFAULT 1"),
            ("created_at", "created_at TEXT NOT NULL DEFAULT (datetime('now'))"),
            ("updated_at", "updated_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )
    _ensure_table_columns(
        cur,
        "sync_pending_updates",
        [
            ("chat_title", "chat_title TEXT NOT NULL DEFAULT ''"),
            ("chat_username", "chat_username TEXT"),
            ("first_event_at", "first_event_at TEXT NOT NULL DEFAULT ''"),
            ("last_event_at", "last_event_at TEXT NOT NULL DEFAULT ''"),
            ("event_count", "event_count INTEGER NOT NULL DEFAULT 0"),
            ("source_accounts", "source_accounts TEXT NOT NULL DEFAULT ''"),
            ("reasons", "reasons TEXT NOT NULL DEFAULT ''"),
            (
                "preferred_source_account",
                "preferred_source_account TEXT NOT NULL DEFAULT ''",
            ),
            ("due_at", "due_at TEXT NOT NULL DEFAULT ''"),
            ("priority_score", "priority_score REAL NOT NULL DEFAULT 0"),
            (
                "quiet_delay_seconds",
                "quiet_delay_seconds INTEGER NOT NULL DEFAULT 0",
            ),
            ("generation", "generation INTEGER NOT NULL DEFAULT 0"),
            ("in_flight", "in_flight INTEGER NOT NULL DEFAULT 0"),
            (
                "in_flight_generation",
                "in_flight_generation INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "in_flight_owner_instance_id",
                "in_flight_owner_instance_id TEXT NOT NULL DEFAULT ''",
            ),
            (
                "in_flight_owner_pid",
                "in_flight_owner_pid INTEGER NOT NULL DEFAULT 0",
            ),
            (
                "in_flight_owner_host",
                "in_flight_owner_host TEXT NOT NULL DEFAULT ''",
            ),
            ("dirty_generation", "dirty_generation INTEGER NOT NULL DEFAULT 0"),
            ("created_at", "created_at TEXT NOT NULL DEFAULT (datetime('now'))"),
            ("updated_at", "updated_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )
    _ensure_table_columns(
        cur,
        "sync_learning_events",
        [
            ("chat_id", "chat_id INTEGER NOT NULL DEFAULT 0"),
            ("event_type", "event_type TEXT NOT NULL DEFAULT ''"),
            ("reason", "reason TEXT NOT NULL DEFAULT ''"),
            ("source_account", "source_account TEXT NOT NULL DEFAULT ''"),
            ("membership_scope", "membership_scope TEXT NOT NULL DEFAULT 'unknown'"),
            ("status", "status TEXT NOT NULL DEFAULT ''"),
            ("features_json", "features_json TEXT NOT NULL DEFAULT '{}'"),
            ("prediction_json", "prediction_json TEXT NOT NULL DEFAULT '{}'"),
            ("outcome_json", "outcome_json TEXT NOT NULL DEFAULT '{}'"),
            (
                "quiet_delay_seconds",
                "quiet_delay_seconds INTEGER NOT NULL DEFAULT 0",
            ),
            ("priority_score", "priority_score REAL NOT NULL DEFAULT 0"),
            ("added_message_count", "added_message_count INTEGER NOT NULL DEFAULT 0"),
            ("wait_seconds", "wait_seconds INTEGER NOT NULL DEFAULT 0"),
            ("api_cost", "api_cost REAL NOT NULL DEFAULT 0"),
            ("failure_type", "failure_type TEXT NOT NULL DEFAULT ''"),
            ("created_at", "created_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )
    _ensure_table_columns(
        cur,
        "sync_model_state",
        [
            ("model_version", "model_version TEXT NOT NULL DEFAULT ''"),
            ("backend", "backend TEXT NOT NULL DEFAULT ''"),
            ("metrics_json", "metrics_json TEXT NOT NULL DEFAULT '{}'"),
            ("trained_at", "trained_at TEXT NOT NULL DEFAULT ''"),
            ("artifact_path", "artifact_path TEXT NOT NULL DEFAULT ''"),
            ("state_json", "state_json TEXT NOT NULL DEFAULT '{}'"),
            ("updated_at", "updated_at TEXT NOT NULL DEFAULT (datetime('now'))"),
        ],
    )


def _ensure_chat_summary_columns(cur: sqlite3.Cursor) -> None:
    if not _column_exists(cur, "chats", "message_count"):
        cur.execute(
            "ALTER TABLE chats ADD COLUMN message_count INTEGER NOT NULL DEFAULT 0"
        )
    if not _column_exists(cur, "chats", "last_message_created_at"):
        cur.execute(
            "ALTER TABLE chats ADD COLUMN last_message_created_at TEXT NOT NULL DEFAULT ''"
        )


def _heal_chat_message_summaries_if_needed(cur: sqlite3.Cursor) -> None:
    mismatch_count = _refresh_chat_message_counts(cur, chat_ids=None)
    if mismatch_count <= 0:
        return
    logging.warning(
        f"检测到 chats 消息摘要与 messages 实际数据不一致，已修复 {mismatch_count} 个群聊摘要"
    )


def _table_exists(cur: sqlite3.Cursor, table_name: str) -> bool:
    cur.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    )
    return cur.fetchone() is not None


def _ensure_messages_schema(cur: sqlite3.Cursor) -> None:
    _remove_legacy_search_columns(cur)
    _ensure_messages_runtime_columns(cur)


def _remove_legacy_search_columns(cur: sqlite3.Cursor) -> None:
    """Remove the SQLite-only helper state used by the retired search path."""
    cur.execute("DROP INDEX IF EXISTS idx_messages_unsearchable_pk")
    cur.execute("DROP INDEX IF EXISTS idx_messages_unsearchable_chat")
    if not _column_exists(cur, "messages", "search_text_present"):
        return
    try:
        cur.execute("ALTER TABLE messages DROP COLUMN search_text_present")
    except sqlite3.OperationalError as exc:
        raise RuntimeError(
            "无法移除旧 search_text_present 列，请确认 SQLite 支持 DROP COLUMN"
        ) from exc


def _ensure_message_media_schema(cur: sqlite3.Cursor) -> None:
    _ensure_message_media_runtime_columns(cur)


def _refresh_chat_message_counts(
    cur: sqlite3.Cursor, chat_ids: Sequence[int] | None = None
) -> int:
    if chat_ids is None:
        batches: Iterable[Sequence[int] | None] = (None,)
    else:
        normalized_chat_ids = sorted({int(chat_id) for chat_id in chat_ids})
        if not normalized_chat_ids:
            return 0
        batches = (
            normalized_chat_ids[start : start + _CHAT_SUMMARY_BATCH_SIZE]
            for start in range(0, len(normalized_chat_ids), _CHAT_SUMMARY_BATCH_SIZE)
        )

    # Aggregate once, then update only rows whose denormalized values differ.
    # The previous correlated UPDATE evaluated COUNT/MAX repeatedly for every
    # chat, which was especially costly during startup healing on large DBs.
    update_count = 0
    for batch in batches:
        if batch is None:
            scope_sql = "1 = 1"
            scope_params: Sequence[int] = ()
        else:
            placeholders = ",".join("?" for _ in batch)
            scope_sql = f"c.chat_id IN ({placeholders})"
            scope_params = batch
        cur.execute(
            f"""
            SELECT
                c.chat_id,
                COUNT(m.chat_id) AS actual_message_count,
                COALESCE(MAX(m.created_at), '') AS actual_last_message_created_at,
                COALESCE(c.message_count, 0) AS stored_message_count,
                COALESCE(c.last_message_created_at, '') AS stored_last_message_created_at
            FROM chats AS c
            LEFT JOIN messages AS m ON m.chat_id = c.chat_id
            WHERE {scope_sql}
            GROUP BY c.chat_id
            """,
            scope_params,
        )
        updates = []
        for row in cur.fetchall():
            # Use positional access here: callers may pass a default SQLite
            # connection whose rows are tuples rather than ``sqlite3.Row``.
            actual_count = safe_int(row[1])
            actual_latest = str(row[2] or "")
            stored_count = optional_int(row[3])
            stored_latest = str(row[4] or "")
            if (
                stored_count is None
                or actual_count != stored_count
                or actual_latest != stored_latest
            ):
                updates.append((actual_count, actual_latest, safe_int(row[0])))

        if updates:
            cur.executemany(
                """
                UPDATE chats
                SET message_count = ?, last_message_created_at = ?
                WHERE chat_id = ?
                """,
                updates,
            )
            update_count += len(updates)
    return update_count


def _optimize_query_planner_stats(cur: sqlite3.Cursor) -> None:
    try:
        cur.execute("PRAGMA analysis_limit=1000;")
        cur.execute("PRAGMA optimize;")
    except sqlite3.Error:
        logging.debug("SQLite 查询规划统计优化跳过", exc_info=True)


@_db_runtime.synchronized_write
def create_schema(
    conn: sqlite3.Connection,
    feats: SqliteFeatures,
):
    cur = conn.cursor()
    try:
        strict_suffix = " STRICT" if feats.supports_strict else ""
        _create_tables(cur, strict_suffix)
        _ensure_chats_schema(cur)
        _ensure_messages_schema(cur)
        _ensure_message_media_schema(cur)
        _ensure_media_groups_runtime_columns(cur)
        _ensure_dedupe_schema(cur)
        _ensure_admin_job_schema(cur)
        _ensure_admin_missing_chats_schema(cur)
        _ensure_admin_restricted_chats_schema(cur)
        _ensure_admin_recovery_chats_schema(cur)
        _ensure_admin_clone_runs_schema(cur)
        _ensure_admin_clone_plans_schema(cur)
        _ensure_admin_clone_migrations_schema(cur)
        _ensure_admin_clone_message_map_schema(cur)
        _ensure_admin_clone_media_transfers_schema(cur)
        _ensure_sync_scheduler_schema(cur)
        _ensure_chat_summary_columns(cur)
        _indexes._create_indexes(cur)
        _heal_chat_message_summaries_if_needed(cur)
        _remove_legacy_search_objects(cur)
        _manticore_outbox.configure_manticore_outbox_triggers(cur, enabled=True)

        _optimize_query_planner_stats(cur)
        conn.commit()
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        cur.close()

@_db_runtime.synchronized_write
def refresh_chat_message_counts(
    conn: sqlite3.Connection, chat_ids: Iterable[int] | None = None
) -> None:
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        _refresh_chat_message_counts(
            cur, None if chat_ids is None else list(chat_ids)
        )
        conn.commit()
    except Exception:
        with suppress(Exception):
            conn.rollback()
        raise
    finally:
        cur.close()
