import sqlite3
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tg_harvest.storage.schema import create_schema
from tg_harvest.storage.connection import ensure_configured_db
from tg_harvest.storage.connection import detect_sqlite_features
from tg_harvest.storage.search_terms import backfill_message_search_terms_upgrade_batch
from tg_harvest.storage.search_terms import drain_message_search_terms_rebuild_queue
from tg_harvest.storage.search_terms import extract_cjk_bigrams
from tg_harvest.storage.search_terms import extract_cjk_search_terms
from tg_harvest.search.result_mapper import _map_search_items
from tg_harvest.storage.access import has_fts
from tg_harvest.ingest.store import batch_upsert
from tg_harvest.ingest.store import upsert_chat


class DbSchemaMigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row

    def tearDown(self) -> None:
        self.conn.close()

    def test_create_schema_heals_old_messages_and_chat_columns(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE chats (
                chat_id INTEGER PRIMARY KEY,
                chat_title TEXT NOT NULL,
                chat_username TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE messages (
                pk INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                msg_type TEXT NOT NULL,
                link TEXT,
                UNIQUE(chat_id, message_id)
            )
            """
        )
        cur.execute(
            "INSERT INTO chats(chat_id, chat_title, chat_username) VALUES (1, 'Legacy Chat', 'legacy')"
        )
        cur.execute(
            "INSERT INTO messages(chat_id, message_id, msg_type, link) VALUES (1, 10, 'TEXT', 'old-link')"
        )
        self.conn.commit()

        feats = detect_sqlite_features(self.conn)
        create_schema(self.conn, feats)

        cur.execute("PRAGMA table_info(chats)")
        chat_columns = {row[1] for row in cur.fetchall()}
        self.assertIn("message_count", chat_columns)
        self.assertIn("is_public", chat_columns)
        self.assertIn("chat_type", chat_columns)
        self.assertIn("first_seen_at", chat_columns)
        self.assertIn("last_seen_at", chat_columns)

        cur.execute("PRAGMA table_xinfo(messages)")
        message_columns = {row[1] for row in cur.fetchall()}
        self.assertNotIn("link", message_columns)
        self.assertIn("msg_date_text", message_columns)
        self.assertIn("msg_date_ts", message_columns)
        self.assertIn("content_norm", message_columns)
        self.assertIn("search_text_present", message_columns)
        self.assertIn("is_promo", message_columns)
        self.assertIn("updated_at", message_columns)

        cur.execute("PRAGMA index_list(messages)")
        message_indexes = {row[1] for row in cur.fetchall()}
        self.assertIn("idx_messages_unsearchable_pk", message_indexes)
        self.assertIn("idx_messages_unsearchable_chat", message_indexes)

        cur.execute(
            "SELECT message_count, first_seen_at, last_seen_at FROM chats WHERE chat_id = 1"
        )
        chat_row = cur.fetchone()
        self.assertEqual(1, int(chat_row["message_count"]))
        self.assertTrue(chat_row["first_seen_at"])
        self.assertTrue(chat_row["last_seen_at"])

        cur.execute("SELECT created_at, updated_at FROM messages WHERE chat_id = 1")
        message_row = cur.fetchone()
        self.assertTrue(message_row["created_at"])
        self.assertTrue(message_row["updated_at"])

    def test_unsearchable_message_lookup_uses_partial_index(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE chats (
                chat_id INTEGER PRIMARY KEY,
                chat_title TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE messages (
                pk INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                msg_date_text TEXT NOT NULL DEFAULT '',
                msg_date_ts INTEGER NOT NULL DEFAULT 0,
                content TEXT,
                content_norm TEXT,
                msg_type TEXT NOT NULL DEFAULT 'TEXT',
                UNIQUE(chat_id, message_id)
            )
            """
        )
        self.conn.commit()

        create_schema(self.conn, detect_sqlite_features(self.conn))
        cur.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT pk FROM messages
            INDEXED BY idx_messages_unsearchable_chat
            WHERE search_text_present = 0 AND chat_id = 1
            ORDER BY pk ASC
            LIMIT 10
            """
        )
        plan_text = " ".join(str(row[3]) for row in cur.fetchall())

        self.assertIn("idx_messages_unsearchable_chat", plan_text)

    def test_migrated_legacy_tables_get_timestamps_on_future_writes(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE chats (
                chat_id INTEGER PRIMARY KEY,
                chat_title TEXT NOT NULL,
                chat_username TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE messages (
                pk INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                msg_type TEXT NOT NULL,
                UNIQUE(chat_id, message_id)
            )
            """
        )
        self.conn.commit()

        create_schema(self.conn, detect_sqlite_features(self.conn))

        upsert_chat(self.conn, (2, "New Chat", None, 0, "Channel"))
        batch_upsert(
            self.conn,
            [
                (
                    2,
                    20,
                    "2026-01-01 00:00:00",
                    1,
                    None,
                    "hello",
                    "hello",
                    "hash",
                    "hash",
                    "TEXT",
                    None,
                    0,
                    0,
                    0,
                    "[]",
                    0,
                    None,
                    5,
                )
            ],
            [],
        )

        cur.execute("SELECT first_seen_at, last_seen_at FROM chats WHERE chat_id = 2")
        chat_row = cur.fetchone()
        self.assertTrue(chat_row["first_seen_at"])
        self.assertTrue(chat_row["last_seen_at"])
        cur.execute("SELECT created_at, updated_at FROM messages WHERE chat_id = 2")
        message_row = cur.fetchone()
        self.assertTrue(message_row["created_at"])
        self.assertTrue(message_row["updated_at"])

    def test_create_schema_heals_auxiliary_tables(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "CREATE TABLE chats (chat_id INTEGER PRIMARY KEY, chat_title TEXT NOT NULL)"
        )
        cur.execute(
            """
            CREATE TABLE messages (
                pk INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                msg_date_text TEXT NOT NULL DEFAULT '',
                msg_date_ts INTEGER NOT NULL DEFAULT 0,
                msg_type TEXT NOT NULL DEFAULT 'TEXT',
                UNIQUE(chat_id, message_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE message_media (
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                PRIMARY KEY (chat_id, message_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE media_groups (
                chat_id INTEGER NOT NULL,
                grouped_id INTEGER NOT NULL,
                PRIMARY KEY (chat_id, grouped_id)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE dedupe_runs (
                batch_id TEXT PRIMARY KEY,
                chat_id INTEGER NOT NULL,
                mode TEXT NOT NULL,
                threshold INTEGER NOT NULL,
                promo_threshold INTEGER NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE dedupe_actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id TEXT NOT NULL,
                chat_id INTEGER NOT NULL,
                pk INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                action TEXT NOT NULL,
                reason TEXT NOT NULL
            )
            """
        )
        cur.execute("INSERT INTO chats(chat_id, chat_title) VALUES (1, 'Chat 1')")
        cur.execute(
            """
            INSERT INTO messages(chat_id, message_id, msg_date_text, msg_date_ts, msg_type)
            VALUES (1, 10, '2026-01-01 00:00:00', 1, 'TEXT')
            """
        )
        cur.execute("INSERT INTO message_media(chat_id, message_id) VALUES (1, 10)")
        cur.execute("INSERT INTO media_groups(chat_id, grouped_id) VALUES (1, 100)")
        cur.execute(
            """
            INSERT INTO dedupe_runs(batch_id, chat_id, mode, threshold, promo_threshold)
            VALUES ('run-1', 1, 'PURGE_ALL', 2, 0)
            """
        )
        cur.execute(
            """
            INSERT INTO dedupe_actions(batch_id, chat_id, pk, message_id, action, reason)
            VALUES ('run-1', 1, 1, 10, 'HARD_DELETE', 'test')
            """
        )
        self.conn.commit()

        feats = detect_sqlite_features(self.conn)
        create_schema(self.conn, feats)

        cur.execute("PRAGMA table_info(message_media)")
        media_columns = {row[1] for row in cur.fetchall()}
        self.assertIn("file_name", media_columns)
        self.assertIn("duration_sec", media_columns)
        self.assertIn("updated_at", media_columns)

        cur.execute("PRAGMA table_info(media_groups)")
        media_group_columns = {row[1] for row in cur.fetchall()}
        self.assertIn("item_count", media_group_columns)
        self.assertIn("dedupe_hash", media_group_columns)
        self.assertIn("updated_at", media_group_columns)

        cur.execute("PRAGMA table_info(dedupe_runs)")
        dedupe_run_columns = {row[1] for row in cur.fetchall()}
        self.assertIn("dup_hash_count_solo", dedupe_run_columns)
        self.assertIn("target_count", dedupe_run_columns)

        cur.execute("PRAGMA table_info(dedupe_actions)")
        dedupe_action_columns = {row[1] for row in cur.fetchall()}
        self.assertIn("grouped_id", dedupe_action_columns)
        self.assertIn("dedupe_hash", dedupe_action_columns)
        self.assertIn("created_at", dedupe_action_columns)

        cur.execute("PRAGMA table_info(message_search_terms)")
        term_columns = {row[1] for row in cur.fetchall()}
        self.assertIn("pk", term_columns)
        self.assertIn("term", term_columns)

        cur.execute("PRAGMA table_info(message_search_terms_rebuild_queue)")
        queue_columns = {row[1] for row in cur.fetchall()}
        self.assertIn("pk", queue_columns)
        self.assertIn("reason", queue_columns)

        cur.execute("PRAGMA table_info(message_search_terms_meta)")
        term_meta_columns = {row[1] for row in cur.fetchall()}
        self.assertIn("key", term_meta_columns)
        self.assertIn("value", term_meta_columns)

        cur.execute("PRAGMA table_info(admin_jobs)")
        admin_job_columns = {row[1] for row in cur.fetchall()}
        self.assertIn("job_id", admin_job_columns)
        self.assertIn("status", admin_job_columns)
        self.assertIn("progress_stage", admin_job_columns)

        cur.execute("PRAGMA table_info(admin_job_logs)")
        admin_job_log_columns = {row[1] for row in cur.fetchall()}
        self.assertIn("job_id", admin_job_log_columns)
        self.assertIn("seq", admin_job_log_columns)
        self.assertIn("message", admin_job_log_columns)

        cur.execute("PRAGMA table_info(admin_missing_chats)")
        admin_missing_chat_columns = {row[1] for row in cur.fetchall()}
        self.assertIn("chat_id", admin_missing_chat_columns)
        self.assertIn("chat_title", admin_missing_chat_columns)
        self.assertIn("chat_username", admin_missing_chat_columns)
        self.assertIn("scanned_at", admin_missing_chat_columns)

        cur.execute("PRAGMA table_info(admin_absent_chats)")
        admin_absent_chat_columns = {row[1] for row in cur.fetchall()}
        self.assertIn("chat_id", admin_absent_chat_columns)
        self.assertIn("chat_title", admin_absent_chat_columns)
        self.assertIn("message_count", admin_absent_chat_columns)
        self.assertIn("last_seen_at", admin_absent_chat_columns)
        self.assertIn("scan_reason", admin_absent_chat_columns)
        self.assertIn("scanned_at", admin_absent_chat_columns)

        cur.execute("PRAGMA table_info(admin_restricted_chats)")
        admin_restricted_chat_columns = {row[1] for row in cur.fetchall()}
        self.assertIn("chat_id", admin_restricted_chat_columns)
        self.assertIn("chat_title", admin_restricted_chat_columns)
        self.assertIn("chat_username", admin_restricted_chat_columns)
        self.assertIn("restriction_platforms", admin_restricted_chat_columns)
        self.assertIn("restriction_reasons", admin_restricted_chat_columns)
        self.assertIn("restriction_text", admin_restricted_chat_columns)
        self.assertIn("risk_flags", admin_restricted_chat_columns)
        self.assertIn("scanned_at", admin_restricted_chat_columns)

        cur.execute("SELECT updated_at FROM message_media WHERE chat_id = 1")
        self.assertTrue(cur.fetchone()["updated_at"])
        cur.execute(
            "SELECT created_at, updated_at FROM media_groups WHERE chat_id = 1"
        )
        media_group_row = cur.fetchone()
        self.assertTrue(media_group_row["created_at"])
        self.assertTrue(media_group_row["updated_at"])
        cur.execute("SELECT started_at FROM dedupe_runs WHERE batch_id = 'run-1'")
        self.assertTrue(cur.fetchone()["started_at"])
        cur.execute("SELECT created_at FROM dedupe_actions WHERE batch_id = 'run-1'")
        self.assertTrue(cur.fetchone()["created_at"])

    def test_create_schema_drops_legacy_restricted_absent_scan_false_positives(self) -> None:
        create_schema(self.conn, detect_sqlite_features(self.conn))
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO admin_absent_chats(
                chat_id,
                chat_title,
                scan_reason,
                scanned_at
            )
            VALUES
                (1, 'False Positive', 'Telegram 限制显示：This channel can be opened', '2026-01-01'),
                (2, 'Forbidden', 'Telegram 返回该会话不可访问', '2026-01-01')
            """
        )
        self.conn.commit()

        create_schema(self.conn, detect_sqlite_features(self.conn))

        cur.execute("SELECT chat_id FROM admin_absent_chats ORDER BY chat_id")
        self.assertEqual([2], [int(row["chat_id"]) for row in cur.fetchall()])

    def test_create_schema_drops_obsolete_duplicate_indexes(self) -> None:
        feats = detect_sqlite_features(self.conn)
        create_schema(self.conn, feats)
        cur = self.conn.cursor()
        stale_index_sql = [
            "CREATE INDEX idx_messages_chat_id ON messages(chat_id)",
            "CREATE INDEX idx_messages_msg_id ON messages(message_id)",
            "CREATE INDEX idx_media_file_ref ON message_media(chat_id, message_id)",
            "CREATE INDEX idx_dedupe_runs_batch ON dedupe_runs(batch_id)",
            "CREATE INDEX idx_admin_job_logs_job_seq ON admin_job_logs(job_id, seq)",
            "CREATE INDEX idx_mg_hash ON media_groups(chat_id, pure_hash) WHERE pure_hash <> ''",
        ]
        for sql in stale_index_sql:
            cur.execute(sql)
        self.conn.commit()

        create_schema(self.conn, feats)

        cur.execute("PRAGMA index_list(messages)")
        message_indexes = {row[1] for row in cur.fetchall()}
        self.assertNotIn("idx_messages_chat_id", message_indexes)
        self.assertNotIn("idx_messages_msg_id", message_indexes)
        self.assertIn("idx_messages_chat_date", message_indexes)

        cur.execute("PRAGMA index_list(message_media)")
        media_indexes = {row[1] for row in cur.fetchall()}
        self.assertNotIn("idx_media_file_ref", media_indexes)

        cur.execute("PRAGMA index_list(dedupe_runs)")
        dedupe_run_indexes = {row[1] for row in cur.fetchall()}
        self.assertNotIn("idx_dedupe_runs_batch", dedupe_run_indexes)

        cur.execute("PRAGMA index_list(admin_job_logs)")
        admin_log_indexes = {row[1] for row in cur.fetchall()}
        self.assertNotIn("idx_admin_job_logs_job_seq", admin_log_indexes)

        cur.execute("PRAGMA index_list(media_groups)")
        media_group_indexes = {row[1] for row in cur.fetchall()}
        self.assertNotIn("idx_mg_hash", media_group_indexes)
        self.assertIn("idx_mg_pure_hash", media_group_indexes)

    def test_create_schema_replaces_stale_named_index_definitions(self) -> None:
        feats = detect_sqlite_features(self.conn)
        create_schema(self.conn, feats)
        cur = self.conn.cursor()
        cur.execute("DROP INDEX IF EXISTS idx_mg_promo")
        cur.execute(
            """
            CREATE INDEX idx_mg_promo
            ON media_groups(chat_id, is_promo, item_count DESC)
            """
        )
        self.conn.commit()

        create_schema(self.conn, feats)

        cur.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_mg_promo'"
        )
        index_sql = str(cur.fetchone()["sql"])
        self.assertIn("dedupe_eligible", index_sql)
        self.assertNotIn("item_count DESC", index_sql)

    def test_schema_without_fts5_does_not_install_broken_fts_triggers(self) -> None:
        feats = SimpleNamespace(supports_strict=False, supports_fts5=False)

        create_schema(self.conn, feats)
        cur = self.conn.cursor()
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'trg_messages_fts_%'"
        )
        self.assertEqual([], [row["name"] for row in cur.fetchall()])

        cur.execute("INSERT INTO chats(chat_id, chat_title) VALUES (1, 'Chat 1')")
        cur.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type,
                content, content_norm, has_media
            ) VALUES (1, 10, '2026-01-01 00:00:00', 1, 'TEXT', 'hello', 'hello', 0)
            """
        )
        cur.execute("UPDATE messages SET content_norm = '福利文本' WHERE chat_id = 1")
        cur.execute("DELETE FROM messages WHERE chat_id = 1")
        self.conn.commit()

    def test_create_schema_replaces_stale_fts_triggers(self) -> None:
        feats = detect_sqlite_features(self.conn)
        if not feats.supports_fts5:
            self.skipTest("SQLite build does not support FTS5")

        create_schema(self.conn, feats)
        cur = self.conn.cursor()
        cur.execute("DROP TRIGGER IF EXISTS trg_messages_fts_insert")
        cur.execute(
            """
            CREATE TRIGGER trg_messages_fts_insert AFTER INSERT ON messages BEGIN
                INSERT INTO messages_fts(rowid, content) VALUES (new.pk, new.content);
            END;
            """
        )
        cur.execute("INSERT INTO chats(chat_id, chat_title) VALUES (1, 'Chat 1')")
        cur.execute(
            """
            INSERT INTO messages(
                pk, chat_id, message_id, msg_date_text, msg_date_ts, msg_type,
                content, content_norm, has_media
            ) VALUES (1, 1, 10, '2026-01-01 00:00:00', 1, 'TEXT', 'rawonly', 'normtarget', 0)
            """
        )
        self.conn.commit()

        create_schema(self.conn, feats)

        cur.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='trg_messages_fts_insert'"
        )
        trigger_sql = str(cur.fetchone()["sql"])
        self.assertIn("NULLIF(new.content_norm, '')", trigger_sql)
        cur.execute(
            "SELECT rowid FROM messages_fts WHERE messages_fts MATCH ?",
            ('"normtarget"',),
        )
        self.assertEqual([1], [int(row["rowid"]) for row in cur.fetchall()])

    def test_create_schema_rebuilds_nonempty_incomplete_fts_index(self) -> None:
        feats = detect_sqlite_features(self.conn)
        if not feats.supports_fts5:
            self.skipTest("SQLite build does not support FTS5")

        create_schema(self.conn, feats)
        cur = self.conn.cursor()
        cur.execute("INSERT INTO chats(chat_id, chat_title) VALUES (1, 'Chat 1')")
        cur.executemany(
            """
            INSERT INTO messages(
                pk, chat_id, message_id, msg_date_text, msg_date_ts, msg_type,
                content, content_norm, has_media
            ) VALUES (?, 1, ?, '2026-01-01 00:00:00', ?, 'TEXT', ?, ?, 0)
            """,
            [
                (1, 10, 1, "firstneedle", "firstneedle"),
                (2, 11, 2, "missingneedle", "missingneedle"),
            ],
        )
        cur.execute("INSERT INTO messages_fts(messages_fts) VALUES ('delete-all')")
        cur.execute(
            "INSERT INTO messages_fts(rowid, content) VALUES (1, 'firstneedle')"
        )
        self.conn.commit()

        cur.execute("SELECT COUNT(*) AS c FROM messages_fts_docsize")
        self.assertEqual(1, int(cur.fetchone()["c"]))
        cur.execute(
            "SELECT rowid FROM messages_fts WHERE messages_fts MATCH ?",
            ('"missingneedle"',),
        )
        self.assertEqual([], cur.fetchall())

        create_schema(self.conn, feats)

        cur.execute("SELECT COUNT(*) AS c FROM messages_fts_docsize")
        self.assertEqual(2, int(cur.fetchone()["c"]))
        cur.execute(
            "SELECT rowid FROM messages_fts WHERE messages_fts MATCH ?",
            ('"missingneedle"',),
        )
        self.assertEqual([2], [int(row["rowid"]) for row in cur.fetchall()])

    def test_create_schema_replaces_stale_message_search_queue_triggers(self) -> None:
        feats = detect_sqlite_features(self.conn)
        create_schema(self.conn, feats)
        cur = self.conn.cursor()
        cur.execute("DROP TRIGGER IF EXISTS trg_message_terms_queue_insert")
        cur.execute(
            """
            CREATE TRIGGER trg_message_terms_queue_insert AFTER INSERT ON messages BEGIN
                INSERT INTO message_search_terms_rebuild_queue(pk, reason, queued_at)
                VALUES (new.pk, 'stale', datetime('now'));
            END;
            """
        )
        self.conn.commit()

        create_schema(self.conn, feats)

        cur.execute(
            "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='trg_message_terms_queue_insert'"
        )
        trigger_sql = str(cur.fetchone()["sql"])
        self.assertIn("ON CONFLICT(pk) DO UPDATE", trigger_sql)

    def test_ensure_configured_db_uses_cfg_for_connection_and_schema(self) -> None:
        cfg = SimpleNamespace(
            db_name="/tmp/test.db",
            sqlite_cache_mb=128,
            sqlite_mmap_mb=256,
            force_heal_fts=1,
        )
        fake_conn = object()
        fake_feats = object()

        with patch(
            "tg_harvest.storage.connection.connect_db", return_value=(fake_conn, fake_feats)
        ) as connect_mock, patch(
            "tg_harvest.storage.schema.create_schema"
        ) as create_mock:
            conn, feats = ensure_configured_db(cfg=cfg)

        self.assertIs(fake_conn, conn)
        self.assertIs(fake_feats, feats)
        connect_mock.assert_called_once_with("/tmp/test.db", cache_mb=128, mmap_mb=256)
        create_mock.assert_called_once_with(fake_conn, fake_feats, force_heal_fts=1)

    def test_create_schema_upgrades_old_bigram_only_search_terms(self) -> None:
        feats = detect_sqlite_features(self.conn)
        create_schema(self.conn, feats)
        cur = self.conn.cursor()
        cur.execute("INSERT INTO chats(chat_id, chat_title) VALUES (1, 'Chat 1')")
        cur.execute(
            """
            INSERT INTO messages(
                pk, chat_id, message_id, msg_date_text, msg_date_ts, msg_type,
                content, content_norm, has_media
            ) VALUES (1, 1, 10, '2026-01-01 00:00:00', 1, 'TEXT', '福利姬', '福利姬', 0)
            """
        )
        cur.execute("DELETE FROM message_search_terms")
        cur.executemany(
            "INSERT INTO message_search_terms(pk, term) VALUES (1, ?)",
            [("福利",), ("利姬",)],
        )
        cur.execute(
            """
            INSERT INTO message_search_terms_meta(key, value)
            VALUES ('cjk_terms_version', '1')
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """
        )
        self.conn.commit()

        create_schema(self.conn, feats)

        cur.execute(
            "SELECT value FROM message_search_terms_meta WHERE key = 'cjk_terms_backfill_mode'"
        )
        self.assertEqual("unigram", cur.fetchone()["value"])

        self.assertEqual(1, backfill_message_search_terms_upgrade_batch(self.conn))
        self.assertEqual(0, backfill_message_search_terms_upgrade_batch(self.conn))

        cur.execute("SELECT term FROM message_search_terms ORDER BY term")
        self.assertEqual(
            ["利", "利姬", "姬", "福", "福利"],
            [row["term"] for row in cur.fetchall()],
        )
        cur.execute(
            "SELECT value FROM message_search_terms_meta WHERE key = 'cjk_terms_version'"
        )
        self.assertEqual("2", cur.fetchone()["value"])


class StorageAccessFtsDetectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.feats = detect_sqlite_features(self.conn)

    def tearDown(self) -> None:
        self.conn.close()

    def test_has_fts_returns_false_when_external_content_index_is_empty_but_messages_exist(
        self,
    ) -> None:
        if not self.feats.supports_fts5:
            self.skipTest("SQLite build does not support FTS5")

        cur = self.conn.cursor()
        cur.execute("CREATE TABLE messages(pk INTEGER PRIMARY KEY, content TEXT)")
        cur.execute(
            """
            CREATE VIRTUAL TABLE messages_fts
            USING fts5(content, content='messages', content_rowid='pk', tokenize='trigram')
            """
        )
        cur.execute("INSERT INTO messages(pk, content) VALUES (1, 'hello')")
        self.conn.commit()

        self.assertFalse(has_fts(self.conn))


class SearchResultMapperTests(unittest.TestCase):
    def test_map_search_items_tolerates_missing_optional_columns(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE sample (
                pk INTEGER,
                chat_id INTEGER,
                chat_title TEXT,
                message_id INTEGER,
                msg_date_text TEXT,
                msg_type TEXT,
                content TEXT
            )
            """
        )
        cur.execute(
            """
            INSERT INTO sample(pk, chat_id, chat_title, message_id, msg_date_text, msg_type, content)
            VALUES (1, 100, 'Test', 200, '2026-01-01 00:00:00', 'TEXT', 'hello')
            """
        )
        cur.execute("SELECT * FROM sample")
        row = cur.fetchone()

        items = _map_search_items([row])
        self.assertEqual(1, len(items))
        self.assertEqual("/open/telegram?chat_id=100&message_id=200", items[0]["link"])
        self.assertEqual(0, items[0]["is_promo"])
        self.assertIsNone(items[0]["file_size"])
        conn.close()


class MessageSearchTermExtractionTests(unittest.TestCase):
    def test_extract_cjk_bigrams_keeps_distinct_adjacent_cjk_pairs(self) -> None:
        self.assertEqual(
            ["福利", "利姬"],
            extract_cjk_bigrams("福利姬"),
        )

    def test_extract_cjk_bigrams_ignores_non_cjk_pairs(self) -> None:
        self.assertEqual(
            ["福利"],
            extract_cjk_bigrams("#福利A福利"),
        )

    def test_extract_cjk_search_terms_includes_unigrams_and_bigrams(self) -> None:
        self.assertEqual(
            ["福", "利", "姬", "福利", "利姬"],
            extract_cjk_search_terms("福利姬"),
        )


class MessageSearchTermQueueTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        feats = detect_sqlite_features(self.conn)
        create_schema(self.conn, feats)
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO chats(chat_id, chat_title, message_count)
            VALUES (1, 'Chat 1', 0)
            """
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()

    def test_insert_into_messages_enqueues_rebuild(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type,
                content, content_norm, has_media
            ) VALUES (1, 10, '2026-01-01 00:00:00', 1, 'TEXT', '福利姬', '福利姬', 0)
            """
        )
        self.conn.commit()

        cur.execute("SELECT reason FROM message_search_terms_rebuild_queue")
        self.assertEqual("insert", cur.fetchone()["reason"])

        drained = drain_message_search_terms_rebuild_queue(self.conn)
        self.assertEqual(1, drained)

        cur.execute("SELECT term FROM message_search_terms ORDER BY term")
        self.assertEqual(
            ["利", "利姬", "姬", "福", "福利"],
            [row["term"] for row in cur.fetchall()],
        )

    def test_empty_message_does_not_enqueue_search_term_rebuild(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type,
                content, content_norm, has_media
            ) VALUES (1, 12, '2026-01-01 00:00:00', 1, 'TEXT', '', '', 0)
            """
        )
        self.conn.commit()

        cur.execute("SELECT COUNT(*) AS c FROM message_search_terms_rebuild_queue")
        self.assertEqual(0, int(cur.fetchone()["c"]))

    def test_update_content_norm_enqueues_rebuild(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_date_ts, msg_type,
                content, content_norm, has_media
            ) VALUES (1, 11, '2026-01-01 00:00:00', 1, 'TEXT', '普通文本', '普通文本', 0)
            """
        )
        self.conn.commit()
        drain_message_search_terms_rebuild_queue(self.conn)

        cur.execute(
            "UPDATE messages SET content_norm = '福利文本' WHERE chat_id = 1 AND message_id = 11"
        )
        self.conn.commit()

        cur.execute("SELECT reason FROM message_search_terms_rebuild_queue")
        self.assertEqual("update", cur.fetchone()["reason"])


if __name__ == "__main__":
    unittest.main()
