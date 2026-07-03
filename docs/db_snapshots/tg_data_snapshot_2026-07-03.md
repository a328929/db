# tg_data.db Snapshot - 2026-07-03

This snapshot captures the runtime SQLite database before moving the large database file away from this machine.

## Source

- Database path: `.runtime/db/tg_data.db`
- Snapshot date: 2026-07-03 Asia/Shanghai
- Full schema SQL: `docs/db_snapshots/tg_data_schema_2026-07-03.sql`
- The workspace root `tg_data.db` file is not the active runtime database; the active large database is the file above.

## File And PRAGMA State

- Page size: 4096
- Page count: 5150880
- Estimated database pages bytes: 21,098,004,480 bytes, about 19.65 GiB
- Freelist pages: 0
- Journal mode: wal
- Auto vacuum: 0
- User version: 0

## Core Row Counts

| Table | Rows |
| --- | ---: |
| chats | 868 |
| messages | 5,549,441 |
| message_media | 5,291,213 |
| media_groups | 1,285,919 |
| message_search_terms | 174,722,094 |
| message_search_terms_rebuild_queue | 0 |
| messages_fts_docsize | 5,549,441 |
| admin_jobs | 3 |
| admin_job_logs | 60 |
| admin_clone_message_map | 75 |

## Important Existing Storage Facts

- Existing `message_search_terms` is still a rowid table in the large source DB.
- Existing source DB still has both `message_search_terms` and `sqlite_autoindex_message_search_terms_1`.
- New compact DBs created by current code use `message_search_terms WITHOUT ROWID`.
- Existing `messages.search_text_present` generated column uses the old inline expression in this source snapshot; current code generates the equivalent centralized expression.
- Existing FTS table is `messages_fts` using FTS5 trigram with external content table `messages` and rowid `pk`.

## Critical Tables

| Table | Purpose |
| --- | --- |
| `chats` | Chat/channel metadata and message count summary. |
| `messages` | Main Telegram message records, text, normalized text, hashes, promo flags, timestamps. |
| `message_media` | Per-message media metadata and fingerprints. |
| `media_groups` | Grouped media aggregate rows and dedupe keys. |
| `message_search_terms` | Auxiliary CJK 1/2-character search index. |
| `messages_fts` | SQLite FTS5 trigram text search index. |
| `dedupe_runs`, `dedupe_actions` | Promotion duplicate cleanup bookkeeping. |
| `admin_*` | Background job, channel inventory, recovery, clone, and migration state. |

## Important Indexes

The full list is preserved in the schema SQL file. The large/high-value indexes include:

- `idx_message_search_terms_pk` on `message_search_terms(pk)`
- `sqlite_autoindex_message_search_terms_1` for `message_search_terms(term, pk)` primary key in the source rowid table
- `idx_messages_chat_date` on `messages(chat_id, msg_date_ts DESC, message_id DESC, pk DESC)`
- `idx_messages_date_global` on `messages(msg_date_ts DESC, message_id DESC, pk DESC)`
- `idx_messages_pure_hash` and `idx_messages_dedupe_hash`
- `idx_messages_created_at` and `idx_messages_chat_created_at`
- `idx_media_sort_size`, `idx_media_sort_size_global`
- `idx_media_sort_duration`, `idx_media_sort_duration_global`
- `idx_mg_pure_hash_promo`, `idx_mg_media_sig_promo`

## Triggers

- `trg_messages_fts_insert`
- `trg_messages_fts_delete`
- `trg_messages_fts_update`
- `trg_message_terms_queue_insert`
- `trg_message_terms_queue_update`
- `trg_message_terms_delete`

## Compression Notes

The compact script should preserve core row counts for `chats`, `messages`, `message_media`, and `media_groups`, then rebuild:

- `message_search_terms`
- `message_search_terms_rebuild_queue` triggers
- `messages_fts`
- `messages_fts` triggers

The compact script intentionally does not replace the source database automatically.

Current compaction safety behavior:

- The source database is attached read-only while rows are copied.
- The target is first built as `<target>.building`.
- The final `<target>` file is only created or replaced after the copy, search index rebuild, row-count verification, foreign-key check, and SQLite integrity check pass.
- Existing target files require `--force`; without it, the script refuses to overwrite.
- The generated compact file is left in SQLite `DELETE` journal mode so it can be transferred as a single database file. Normal application startup can switch it back to WAL.
- Progress output includes rows processed, percent, speed, elapsed time, and ETA for table copy, CJK short-term index rebuild, and FTS rebuild.

Typical command:

```bash
python3 tools/compact_sqlite_db.py --source /path/to/tg_data.db --target /path/to/tg_data.compact.db
```

For low-memory devices:

```bash
python3 tools/compact_sqlite_db.py --source /path/to/tg_data.db --target /path/to/tg_data.compact.db --batch-size 10000
```

## Text Storage Stats

| Metric | Value |
| --- | ---: |
| messages.content bytes | 287,185,924 |
| messages.content_norm bytes | 274,745,213 |
| messages rows where content_norm equals content | 2,117,635 |
| media_groups.captions_concat bytes | 171,389,067 |
| media_groups.caption_norm bytes | 167,141,221 |
| media_groups rows where caption_norm equals captions_concat | 59,507 |

## Object-Level Space Ranking

The direct `dbstat` object ranking query was started on the source database but interrupted after several minutes because it was too slow for this turn. The earlier analysis found the main large objects were:

| Object | Approx Size |
| --- | ---: |
| message_search_terms | about 3.24 GiB |
| sqlite_autoindex_message_search_terms_1 | about 3.64 GiB |
| idx_message_search_terms_pk | about 2.52 GiB |
| messages_fts_data | about 2.44 GiB |
| messages | about 2.37 GiB |
| message_media | about 1.26 GiB |
| media_groups | about 1.02 GiB |

Use `python3 tools/db_space_report.py --db <db> --top 50` later if a fresh object ranking is needed.
