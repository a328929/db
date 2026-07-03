CREATE TABLE chats (
        chat_id          INTEGER PRIMARY KEY,
        chat_title       TEXT NOT NULL,
        chat_username    TEXT,
        is_public        INTEGER NOT NULL DEFAULT 0,
        chat_type        TEXT,
        message_count    INTEGER NOT NULL DEFAULT 0,
        first_seen_at    TEXT NOT NULL DEFAULT (datetime('now')),
        last_seen_at     TEXT NOT NULL DEFAULT (datetime('now'))
    , last_message_created_at TEXT NOT NULL DEFAULT '') STRICT
    ;
CREATE TABLE messages (
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
        search_text_present  INTEGER GENERATED ALWAYS AS (
            CASE WHEN COALESCE(NULLIF(TRIM(content_norm), ''), NULLIF(TRIM(content), ''), '') <> ''
                 THEN 1 ELSE 0 END
        ) VIRTUAL,

        created_at           TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at           TEXT NOT NULL DEFAULT (datetime('now')),

        UNIQUE(chat_id, message_id),
        FOREIGN KEY(chat_id) REFERENCES chats(chat_id)
    ) STRICT
    ;
CREATE TABLE sqlite_sequence(name,seq);
CREATE TABLE message_media (
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
    ) STRICT
    ;
CREATE TABLE media_groups (
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
    ) STRICT
    ;
CREATE TABLE dedupe_runs (
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
    ) STRICT
    ;
CREATE TABLE dedupe_actions (
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
    ) STRICT
    ;
CREATE TABLE message_search_terms (
        pk      INTEGER NOT NULL,
        term    TEXT NOT NULL,
        PRIMARY KEY (term, pk),
        FOREIGN KEY(pk) REFERENCES messages(pk) ON DELETE CASCADE
    ) STRICT
    ;
CREATE TABLE message_search_terms_rebuild_queue (
        pk         INTEGER PRIMARY KEY,
        reason     TEXT,
        queued_at  TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY(pk) REFERENCES messages(pk) ON DELETE CASCADE
    ) STRICT
    ;
CREATE TABLE message_search_terms_meta (
        key     TEXT PRIMARY KEY,
        value   TEXT NOT NULL
    ) STRICT
    ;
CREATE TABLE admin_jobs (
        job_id               TEXT PRIMARY KEY,
        job_type             TEXT NOT NULL,
        status               TEXT NOT NULL,
        target_chat_id       INTEGER,
        target_label         TEXT,
        created_at           TEXT NOT NULL,
        updated_at           TEXT NOT NULL,
        owner_instance_id    TEXT,
        owner_pid            INTEGER,
        heartbeat_at         TEXT NOT NULL DEFAULT (datetime('now')),
        progress_current     INTEGER NOT NULL DEFAULT 0,
        progress_total       INTEGER,
        progress_stage       TEXT NOT NULL DEFAULT 'queued',
        last_logged_current  INTEGER NOT NULL DEFAULT 0
    , stop_requested INTEGER NOT NULL DEFAULT 0) STRICT
    ;
CREATE TABLE admin_job_logs (
        job_id               TEXT NOT NULL,
        seq                  INTEGER NOT NULL,
        ts                   TEXT NOT NULL,
        message              TEXT NOT NULL,
        PRIMARY KEY (job_id, seq),
        FOREIGN KEY(job_id) REFERENCES admin_jobs(job_id) ON DELETE CASCADE
    ) STRICT
    ;
CREATE TABLE admin_missing_chats (
        chat_id              INTEGER PRIMARY KEY,
        chat_title           TEXT NOT NULL,
        chat_username        TEXT,
        chat_type            TEXT,
        is_public            INTEGER NOT NULL DEFAULT 0,
        last_message_at      TEXT,
        last_message_ts      INTEGER,
        scan_job_id          TEXT,
        scanned_at           TEXT NOT NULL
    , unavailable_reason TEXT) STRICT
    ;
CREATE TABLE admin_absent_chats (
        chat_id              INTEGER PRIMARY KEY,
        chat_title           TEXT NOT NULL,
        chat_username        TEXT,
        chat_type            TEXT,
        message_count        INTEGER NOT NULL DEFAULT 0,
        last_seen_at         TEXT,
        last_message_at      TEXT,
        last_message_ts      INTEGER,
        scan_reason          TEXT,
        scan_job_id          TEXT,
        scanned_at           TEXT NOT NULL
    ) STRICT
    ;
CREATE TABLE admin_restricted_chats (
        chat_id                  INTEGER PRIMARY KEY,
        chat_title               TEXT NOT NULL,
        chat_username            TEXT,
        chat_type                TEXT,
        is_public                INTEGER NOT NULL DEFAULT 0,
        restriction_platforms    TEXT,
        restriction_reasons      TEXT,
        restriction_text         TEXT,
        risk_flags               TEXT,
        last_message_at          TEXT,
        last_message_ts          INTEGER,
        scan_job_id              TEXT,
        scanned_at               TEXT NOT NULL
    ) STRICT
    ;
CREATE TABLE admin_recovery_chats (
        chat_id                  INTEGER PRIMARY KEY,
        chat_title               TEXT NOT NULL,
        chat_username            TEXT,
        chat_type                TEXT,
        is_public                INTEGER NOT NULL DEFAULT 0,
        source_session           TEXT,
        source_entity_id         INTEGER,
        session_entity_date      TEXT,
        session_entity_ts        INTEGER,
        recovered_at             TEXT,
        recovered_job_id         TEXT,
        scan_job_id              TEXT,
        scanned_at               TEXT NOT NULL
    , source_access_hash INTEGER, availability_reason TEXT) STRICT
    ;
CREATE TABLE IF NOT EXISTS 'messages_fts_data'(id INTEGER PRIMARY KEY, block BLOB);
CREATE TABLE IF NOT EXISTS 'messages_fts_idx'(segid, term, pgno, PRIMARY KEY(segid, term)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS 'messages_fts_docsize'(id INTEGER PRIMARY KEY, sz BLOB);
CREATE TABLE IF NOT EXISTS 'messages_fts_config'(k PRIMARY KEY, v) WITHOUT ROWID;
CREATE TABLE sqlite_stat1(tbl,idx,stat);
CREATE INDEX idx_chats_title ON chats(chat_title COLLATE NOCASE ASC, chat_id ASC);
CREATE INDEX idx_chats_last_seen ON chats(last_seen_at DESC, chat_id ASC);
CREATE INDEX idx_chats_message_count_desc ON chats(message_count DESC, chat_title COLLATE NOCASE ASC, chat_id ASC);
CREATE INDEX idx_chats_message_count_asc ON chats(message_count ASC, chat_title COLLATE NOCASE ASC, chat_id ASC);
CREATE INDEX idx_messages_chat_date ON messages(chat_id, msg_date_ts DESC, message_id DESC, pk DESC);
CREATE INDEX idx_messages_grouped_id ON messages(chat_id, grouped_id, message_id) WHERE grouped_id IS NOT NULL;
CREATE INDEX idx_messages_pure_hash ON messages(chat_id, pure_hash) WHERE pure_hash <> '';
CREATE INDEX idx_messages_dedupe_hash ON messages(chat_id, dedupe_hash) WHERE dedupe_hash <> '';
CREATE INDEX idx_messages_dedupe_promo_solo ON messages(chat_id, dedupe_hash, msg_date_ts ASC, message_id ASC, pk ASC) WHERE dedupe_hash <> '' AND grouped_id IS NULL AND is_promo = 1 AND dedupe_eligible = 1;
CREATE INDEX idx_messages_promo ON messages(chat_id, is_promo, promo_score DESC, msg_date_ts DESC, message_id DESC, pk DESC);
CREATE INDEX idx_messages_sender ON messages(chat_id, sender_id, msg_date_ts DESC, message_id DESC, pk DESC);
CREATE INDEX idx_messages_type ON messages(chat_id, msg_type, msg_date_ts DESC, message_id DESC, pk DESC);
CREATE INDEX idx_messages_type_global ON messages(msg_type, msg_date_ts DESC, message_id DESC, pk DESC);
CREATE INDEX idx_messages_date_global ON messages(msg_date_ts DESC, message_id DESC, pk DESC);
CREATE INDEX idx_messages_unsearchable_pk ON messages(pk, chat_id, message_id, grouped_id) WHERE search_text_present = 0;
CREATE INDEX idx_messages_unsearchable_chat ON messages(chat_id, pk, message_id, grouped_id) WHERE search_text_present = 0;
CREATE INDEX idx_media_unique_id ON message_media(chat_id, file_unique_id) WHERE file_unique_id IS NOT NULL AND file_unique_id <> '';
CREATE INDEX idx_media_fingerprint ON message_media(chat_id, media_fingerprint) WHERE media_fingerprint IS NOT NULL AND media_fingerprint <> '';
CREATE INDEX idx_media_sort_size ON message_media(chat_id, file_size DESC, message_id DESC);
CREATE INDEX idx_media_sort_size_global ON message_media(file_size DESC, chat_id DESC, message_id DESC);
CREATE INDEX idx_media_sort_duration ON message_media(chat_id, duration_sec DESC, message_id DESC);
CREATE INDEX idx_media_sort_duration_global ON message_media(duration_sec DESC, chat_id DESC, message_id DESC);
CREATE INDEX idx_media_kind ON message_media(chat_id, media_kind);
CREATE INDEX idx_media_mime ON message_media(chat_id, mime_type);
CREATE INDEX idx_media_grouped_id ON message_media(chat_id, grouped_id) WHERE grouped_id IS NOT NULL;
CREATE INDEX idx_mg_pure_hash ON media_groups(chat_id, pure_hash) WHERE pure_hash <> '';
CREATE INDEX idx_mg_pure_hash_promo ON media_groups(chat_id, is_promo, dedupe_eligible, pure_hash, item_count, first_message_id, grouped_id) WHERE pure_hash <> '';
CREATE INDEX idx_mg_media_sig ON media_groups(chat_id, media_sig_hash) WHERE media_sig_hash <> '';
CREATE INDEX idx_mg_media_sig_promo ON media_groups(chat_id, is_promo, dedupe_eligible, media_sig_hash, item_count, first_message_id, grouped_id) WHERE media_sig_hash <> '';
CREATE INDEX idx_mg_dedupe_hash ON media_groups(chat_id, dedupe_hash) WHERE dedupe_hash <> '';
CREATE INDEX idx_mg_promo ON media_groups(chat_id, is_promo, dedupe_eligible, item_count);
CREATE INDEX idx_mg_time ON media_groups(chat_id, first_msg_date_ts DESC);
CREATE INDEX idx_dedupe_runs_chat ON dedupe_runs(chat_id);
CREATE INDEX idx_dedupe_actions_batch ON dedupe_actions(batch_id);
CREATE INDEX idx_dedupe_actions_chat_time ON dedupe_actions(chat_id, created_at DESC);
CREATE INDEX idx_message_search_terms_pk ON message_search_terms(pk);
CREATE INDEX idx_message_search_terms_queue_order ON message_search_terms_rebuild_queue(queued_at, pk);
CREATE INDEX idx_admin_jobs_updated_created ON admin_jobs(updated_at ASC, created_at ASC);
CREATE INDEX idx_admin_jobs_status_updated ON admin_jobs(status, updated_at);
CREATE INDEX idx_admin_jobs_target_chat ON admin_jobs(target_chat_id, status);
CREATE INDEX idx_admin_jobs_status_heartbeat ON admin_jobs(status, heartbeat_at);
CREATE INDEX idx_admin_missing_chats_scanned ON admin_missing_chats(scanned_at DESC);
CREATE INDEX idx_admin_missing_chats_title ON admin_missing_chats(chat_title COLLATE NOCASE);
CREATE INDEX idx_admin_missing_chats_last_message ON admin_missing_chats(last_message_ts DESC);
CREATE INDEX idx_admin_absent_chats_scanned ON admin_absent_chats(scanned_at DESC);
CREATE INDEX idx_admin_absent_chats_count ON admin_absent_chats(message_count DESC, last_message_ts DESC);
CREATE INDEX idx_admin_absent_chats_title ON admin_absent_chats(chat_title COLLATE NOCASE);
CREATE INDEX idx_admin_absent_chats_last_message ON admin_absent_chats(last_message_ts DESC);
CREATE INDEX idx_admin_restricted_chats_scanned ON admin_restricted_chats(scanned_at DESC);
CREATE INDEX idx_admin_restricted_chats_title ON admin_restricted_chats(chat_title COLLATE NOCASE);
CREATE INDEX idx_admin_restricted_chats_public ON admin_restricted_chats(is_public, chat_title COLLATE NOCASE);
CREATE INDEX idx_admin_restricted_chats_last_message ON admin_restricted_chats(last_message_ts DESC);
CREATE INDEX idx_admin_recovery_chats_scanned ON admin_recovery_chats(scanned_at DESC);
CREATE INDEX idx_admin_recovery_chats_title ON admin_recovery_chats(chat_title COLLATE NOCASE);
CREATE INDEX idx_admin_recovery_chats_recovered ON admin_recovery_chats(recovered_at, chat_title COLLATE NOCASE);
CREATE INDEX idx_admin_recovery_chats_session_ts ON admin_recovery_chats(session_entity_ts DESC);
CREATE VIRTUAL TABLE messages_fts
    USING fts5(
        content,
        content='messages',
        content_rowid='pk',
        tokenize='trigram'
    )
/* messages_fts(content) */;
CREATE TABLE admin_clone_runs (
        run_id                   TEXT PRIMARY KEY,
        job_id                   TEXT NOT NULL UNIQUE,
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
    ) STRICT
    ;
CREATE INDEX idx_admin_clone_runs_source_updated ON admin_clone_runs(source_chat_id, updated_at DESC);
CREATE INDEX idx_admin_clone_runs_status_updated ON admin_clone_runs(status, updated_at DESC);
CREATE INDEX idx_admin_clone_runs_target ON admin_clone_runs(target_chat_id, target_title COLLATE NOCASE);
CREATE TABLE admin_clone_plans (
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
        capabilities_json        TEXT NOT NULL DEFAULT '{}',
        plan_json                TEXT NOT NULL DEFAULT '{}',
        error_message            TEXT,
        created_at               TEXT NOT NULL,
        updated_at               TEXT NOT NULL,
        completed_at             TEXT,
        FOREIGN KEY(run_id) REFERENCES admin_clone_runs(run_id) ON DELETE CASCADE
    ) STRICT
    ;
CREATE INDEX idx_admin_clone_plans_run_updated ON admin_clone_plans(run_id, updated_at DESC);
CREATE INDEX idx_admin_clone_plans_status_updated ON admin_clone_plans(status, updated_at DESC);
CREATE TABLE admin_clone_migrations (
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
        text_total               INTEGER NOT NULL DEFAULT 0,
        text_sent                INTEGER NOT NULL DEFAULT 0,
        text_skipped             INTEGER NOT NULL DEFAULT 0,
        text_failed              INTEGER NOT NULL DEFAULT 0,
        media_skipped            INTEGER NOT NULL DEFAULT 0,
        plan_json                TEXT NOT NULL DEFAULT '{}',
        error_message            TEXT,
        created_at               TEXT NOT NULL,
        updated_at               TEXT NOT NULL,
        completed_at             TEXT, requested_limit INTEGER NOT NULL DEFAULT 0, send_delay_ms INTEGER NOT NULL DEFAULT 0, media_total INTEGER NOT NULL DEFAULT 0, media_sent INTEGER NOT NULL DEFAULT 0, media_failed INTEGER NOT NULL DEFAULT 0, media_group_total INTEGER NOT NULL DEFAULT 0, media_group_sent INTEGER NOT NULL DEFAULT 0, media_group_skipped INTEGER NOT NULL DEFAULT 0, media_group_failed INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY(run_id) REFERENCES admin_clone_runs(run_id) ON DELETE CASCADE,
        FOREIGN KEY(plan_id) REFERENCES admin_clone_plans(plan_id) ON DELETE SET NULL
    ) STRICT
    ;
CREATE TABLE admin_clone_message_map (
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
    ) STRICT
    ;
CREATE INDEX idx_admin_clone_migrations_run_updated ON admin_clone_migrations(run_id, updated_at DESC);
CREATE INDEX idx_admin_clone_migrations_status_updated ON admin_clone_migrations(status, updated_at DESC);
CREATE INDEX idx_admin_clone_message_map_source ON admin_clone_message_map(run_id, source_chat_id, source_message_id, chunk_index, mode);
CREATE INDEX idx_admin_clone_message_map_migration ON admin_clone_message_map(migration_id, status, updated_at DESC);
CREATE INDEX idx_messages_created_at ON messages(created_at DESC, chat_id DESC, message_id DESC, pk DESC);
CREATE INDEX idx_chats_last_message_created_at ON chats(last_message_created_at DESC, chat_id ASC);
CREATE INDEX idx_messages_chat_created_at ON messages(chat_id, created_at DESC, pk DESC);
CREATE TRIGGER trg_message_terms_queue_insert
    AFTER INSERT ON messages
    WHEN new.search_text_present = 1 BEGIN
        INSERT INTO message_search_terms_rebuild_queue(pk, reason, queued_at)
        VALUES (new.pk, 'insert', datetime('now'))
        ON CONFLICT(pk) DO UPDATE SET
            reason = excluded.reason,
            queued_at = excluded.queued_at;
    END;
CREATE TRIGGER trg_message_terms_queue_update
    AFTER UPDATE OF content, content_norm ON messages
    WHEN new.search_text_present = 1 BEGIN
        INSERT INTO message_search_terms_rebuild_queue(pk, reason, queued_at)
        VALUES (new.pk, 'update', datetime('now'))
        ON CONFLICT(pk) DO UPDATE SET
            reason = excluded.reason,
            queued_at = excluded.queued_at;
    END;
CREATE TRIGGER trg_message_terms_delete
    AFTER DELETE ON messages BEGIN
        DELETE FROM message_search_terms WHERE pk = old.pk;
        DELETE FROM message_search_terms_rebuild_queue WHERE pk = old.pk;
    END;
CREATE TRIGGER trg_messages_fts_insert AFTER INSERT ON messages BEGIN
        INSERT INTO messages_fts(rowid, content)
        VALUES (new.pk, COALESCE(NULLIF(new.content_norm, ''), new.content, ''));
    END;
CREATE TRIGGER trg_messages_fts_delete AFTER DELETE ON messages BEGIN
        INSERT INTO messages_fts(messages_fts, rowid, content)
        VALUES ('delete', old.pk, COALESCE(NULLIF(old.content_norm, ''), old.content, ''));
    END;
CREATE TRIGGER trg_messages_fts_update AFTER UPDATE OF content, content_norm ON messages BEGIN
        INSERT INTO messages_fts(messages_fts, rowid, content)
        VALUES ('delete', old.pk, COALESCE(NULLIF(old.content_norm, ''), old.content, ''));
        INSERT INTO messages_fts(rowid, content)
        VALUES (new.pk, COALESCE(NULLIF(new.content_norm, ''), new.content, ''));
    END;
