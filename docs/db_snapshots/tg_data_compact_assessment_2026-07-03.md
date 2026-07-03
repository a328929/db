# tg_data.db Compact Assessment - 2026-07-03

This assessment captures the current compact runtime database after the source
database was rebuilt and vacuumed.

## Source

- Runtime database: `.runtime/db/tg_data.db`
- Workspace root `tg_data.db`: empty placeholder, not the active runtime database
- File size: 14,042,923,008 bytes, about 13.08 GiB
- SQLite page size: 65,536
- Page count: 214,278
- Freelist pages: 0
- Journal mode at inspection time: `delete`

## Core Row Counts

| Table | Rows |
| --- | ---: |
| chats | 868 |
| messages | 5,549,446 |
| message_media | 5,291,217 |
| media_groups | 1,285,920 |
| message_search_terms | 174,722,287 |
| message_search_terms_rebuild_queue | 0 |
| messages_fts_docsize | 5,549,446 |
| admin_jobs | 3 |
| admin_job_logs | 60 |
| admin_clone_message_map | 75 |

## Compact Schema State

- `message_search_terms` is `STRICT, WITHOUT ROWID`.
- The old `sqlite_autoindex_message_search_terms_1` object is absent.
- `idx_message_search_terms_pk` is present for reverse lookup by message pk.
- `messages_fts` is present and uses FTS5 trigram with external content table `messages`.
- `messages.search_text_present` generated column is present.
- FTS sync triggers are present.
- CJK search-term queue triggers are present.

## Search Index State

| Metric | Value |
| --- | ---: |
| `cjk_terms_version` | 2 |
| `fts_index_status` | ready |
| `message_search_terms_rebuild_queue` | 0 |
| `messages_fts_docsize` vs `messages` | equal |
| `message_search_terms` sample orphan check | 0 |

Search-path samples succeeded for:

- Single-character CJK auxiliary search.
- Two-character CJK auxiliary search.
- FTS trigram search.
- Mixed CJK plus FTS search.

## Text Storage State

| Metric | Value |
| --- | ---: |
| messages.content bytes | 287,185,924 |
| messages.content_norm bytes | 230,312,503 |
| rows where content_norm equals content | 0 |
| media_groups.captions_concat bytes | 171,389,067 |
| media_groups.caption_norm bytes | 166,287,087 |
| rows where caption_norm equals captions_concat | 0 |
| messages with search text | 5,546,819 |
| messages without search text | 2,627 |

## Relationship Checks

- `SUM(chats.message_count)` equals `COUNT(messages)`.
- No chat rows have stale `message_count`.
- No sampled/core orphan rows found in:
  - `messages` to `chats`
  - `message_media` to `messages`
  - `media_groups` to `chats`
  - admin job log/map relationships
  - sampled `message_search_terms` rows to `messages`

## Object-Level Space Ranking

| Object | Approx Size |
| --- | ---: |
| message_search_terms | 2.29 GiB |
| idx_message_search_terms_pk | 2.29 GiB |
| messages | 1.91 GiB |
| message_media | 1.08 GiB |
| messages_fts_data | 970.00 MiB |
| media_groups | 843.19 MiB |
| idx_messages_dedupe_hash | 251.75 MiB |
| idx_messages_pure_hash | 251.50 MiB |
| idx_media_fingerprint | 242.81 MiB |
| idx_messages_created_at | 225.94 MiB |

## Integrity And Project Checks

- SQLite `quick_check` returned `ok`.
- Targeted database/search tests passed.
- Targeted lint checks for touched code passed.
- Flask app creation and route registration smoke check passed under the project virtual environment.

The full SQLite `integrity_check` was attempted but is expensive on this 13 GiB
database and did not finish within the interactive inspection window. The
lighter `quick_check`, schema checks, row-count checks, index-state checks, and
search-path checks passed.

## Assessment

The compact runtime database satisfies the second-stage completion criteria:

- The active runtime database is the compact rebuilt database.
- Historical redundant normalized text storage has been removed.
- `message_search_terms` has moved to the compact `WITHOUT ROWID` layout.
- FTS and CJK auxiliary search indexes are rebuilt and marked ready.
- Search behavior remains compatible with the current project code.
- Core counts, summaries, sampled relationships, and quick integrity checks pass.

Keep the pre-compact or 16 GiB intermediate database only as a short-term backup
until the compact database has run under normal workload for a maintenance
window.
