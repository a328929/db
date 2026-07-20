import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from tg_harvest.app.admin_payloads import (
    build_admin_stats_payload,
    build_admin_storage_health_payload,
    build_admin_sync_live_messages_payload,
    build_admin_sync_stats_payload,
)
from tg_harvest.config import _load_raw_config_values, _normalize_config_values


class AdminPayloadPerformanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(
            """
            CREATE TABLE chats(
                chat_id INTEGER PRIMARY KEY,
                chat_title TEXT NOT NULL,
                chat_username TEXT,
                chat_type TEXT,
                message_count INTEGER NOT NULL DEFAULT 0,
                last_message_created_at TEXT NOT NULL DEFAULT ''
            )
            """
        )
        self.conn.execute(
            """
            CREATE TABLE messages(
                pk INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                msg_date_text TEXT NOT NULL DEFAULT '',
                msg_type TEXT NOT NULL DEFAULT 'TEXT',
                content TEXT,
                content_norm TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        self.conn.executemany(
            """
            INSERT INTO chats(
                chat_id, chat_title, chat_username, chat_type,
                message_count, last_message_created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (1, "A", "chat_a", "channel", 3, ""),
                (2, "B", "chat_b", "channel", 5, ""),
            ],
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()

    def test_all_scope_stats_use_single_aggregate_query(self) -> None:
        statements = []
        self.conn.set_trace_callback(
            lambda sql: statements.append(" ".join(str(sql).split()))
        )
        try:
            payload, status_code = build_admin_stats_payload(self.conn, None)
        finally:
            self.conn.set_trace_callback(None)

        select_statements = [
            sql for sql in statements if sql.upper().startswith("SELECT")
        ]
        self.assertEqual(200, status_code)
        self.assertEqual(2, payload["chat_count"])
        self.assertEqual(8, payload["message_count"])
        self.assertEqual(1, len(select_statements))
        self.assertIn("COUNT(*) AS chat_count", select_statements[0])
        self.assertIn("SUM(message_count)", select_statements[0])

    def test_sync_stats_groups_message_ingest_by_time_windows(self) -> None:
        self.conn.executemany(
            "INSERT INTO messages(chat_id, message_id, created_at) VALUES (?, ?, ?)",
            [
                (1, 101, "2026-06-28 11:55:00"),
                (1, 102, "2026-06-28 11:40:00"),
                (2, 201, "2026-06-28 10:30:00"),
                (2, 202, "2026-06-27 13:00:00"),
            ],
        )
        self.conn.execute(
            "UPDATE chats SET message_count = 2, last_message_created_at = '2026-06-28 11:55:00' WHERE chat_id = 1"
        )
        self.conn.execute(
            "UPDATE chats SET message_count = 2, last_message_created_at = '2026-06-28 10:30:00' WHERE chat_id = 2"
        )
        self.conn.commit()

        with unittest.mock.patch(
            "tg_harvest.app.admin_payloads._utc_now",
            return_value=__import__("datetime").datetime(
                2026, 6, 28, 12, 0, 0, tzinfo=__import__("datetime").timezone.utc
            ),
        ):
            payload = build_admin_sync_stats_payload(self.conn)

        self.assertTrue(payload["ok"])
        self.assertEqual("live", payload["default_window_key"])
        self.assertEqual("2026-06-28 11:55:00", payload["latest_message_created_at"])
        windows_by_key = {
            str(item["window_key"]): item for item in payload["windows"]
        }
        self.assertTrue(windows_by_key["live"]["is_live"])
        self.assertEqual(4, windows_by_key["live"]["message_count"])
        self.assertEqual(2, windows_by_key["live"]["chat_count"])
        self.assertEqual(1, windows_by_key["10m"]["message_count"])
        self.assertEqual(1, windows_by_key["10m"]["chat_count"])
        self.assertEqual(2, windows_by_key["30m"]["message_count"])
        self.assertEqual(1, windows_by_key["30m"]["chat_count"])
        self.assertEqual(3, windows_by_key["2h"]["message_count"])
        self.assertEqual(2, windows_by_key["2h"]["chat_count"])
        self.assertEqual(4, windows_by_key["2d"]["message_count"])
        self.assertEqual(2, windows_by_key["2d"]["chat_count"])

    def test_sync_stats_uses_window_bounded_aggregates_without_multi_window_join(self) -> None:
        self.conn.executemany(
            "INSERT INTO messages(chat_id, message_id, created_at) VALUES (?, ?, ?)",
            [
                (1, 101, "2026-06-28 11:55:00"),
                (1, 102, "2026-06-28 11:40:00"),
                (2, 201, "2026-06-28 10:30:00"),
                (2, 202, "2026-06-27 13:00:00"),
            ],
        )
        self.conn.execute(
            "UPDATE chats SET message_count = 2, last_message_created_at = '2026-06-28 11:55:00' WHERE chat_id = 1"
        )
        self.conn.execute(
            "UPDATE chats SET message_count = 2, last_message_created_at = '2026-06-28 10:30:00' WHERE chat_id = 2"
        )
        self.conn.commit()

        statements = []
        self.conn.set_trace_callback(
            lambda sql: statements.append(" ".join(str(sql).split()))
        )
        try:
            with unittest.mock.patch(
                "tg_harvest.app.admin_payloads._utc_now",
                return_value=__import__("datetime").datetime(
                    2026, 6, 28, 12, 0, 0, tzinfo=__import__("datetime").timezone.utc
                ),
            ):
                payload = build_admin_sync_stats_payload(self.conn)
        finally:
            self.conn.set_trace_callback(None)

        select_statements = [
            sql for sql in statements if sql.upper().startswith("SELECT")
        ]
        self.assertTrue(payload["ok"])
        self.assertGreaterEqual(len(select_statements), 3)
        self.assertTrue(
            any(
                "SUM(CASE WHEN created_at >=" in sql
                and "COUNT(DISTINCT CASE WHEN created_at >=" not in sql
                for sql in select_statements
            )
        )
        self.assertTrue(
            any(
                "SUM(CASE WHEN last_message_created_at >=" in sql
                and "FROM chats" in sql
                for sql in select_statements
            )
        )
        self.assertFalse(
            any("COUNT(DISTINCT chat_id)" in sql for sql in select_statements)
        )
        self.assertFalse(
            any("GROUP BY chat_id" in sql for sql in select_statements)
        )
        self.assertFalse(
            any("LEFT JOIN messages m ON m.created_at >= w.cutoff_at" in sql for sql in select_statements)
        )

    def test_sync_stats_returns_zero_windows_when_messages_table_missing(self) -> None:
        self.conn.execute("DROP TABLE messages")
        self.conn.commit()

        payload = build_admin_sync_stats_payload(self.conn)

        self.assertTrue(payload["ok"])
        self.assertEqual("", payload["latest_message_created_at"])
        self.assertTrue(payload["windows"])
        self.assertEqual("live", payload["default_window_key"])
        self.assertTrue(
            all(int(item["message_count"]) == 0 for item in payload["windows"])
        )

    def test_sync_stats_reports_critical_health_when_recent_ingest_stalls_and_listener_down(
        self,
    ) -> None:
        self.conn.execute(
            "UPDATE chats SET message_count = 1, last_message_created_at = '2026-06-28 11:40:00' WHERE chat_id = 1"
        )
        self.conn.execute(
            "INSERT INTO messages(chat_id, message_id, created_at) VALUES (?, ?, ?)",
            (1, 101, "2026-06-28 11:40:00"),
        )
        self.conn.commit()

        health_snapshot = {
            "listener_enabled": True,
            "public_probe_enabled": True,
            "active_listener_count": 0,
            "configured_listener_count": 1,
            "worker_thread_alive": False,
            "refresh_thread_alive": True,
            "public_probe_thread_alive": True,
            "queue_size": 0,
            "tracked_chat_count": 1,
            "last_update_success_age_seconds": 1200,
            "last_update_failure_age_seconds": 30,
            "last_update_failure_message": "session invalid",
            "last_probe_status": "failed",
            "last_probe_result_age_seconds": 60,
            "accounts": [],
        }

        with unittest.mock.patch(
            "tg_harvest.app.admin_payloads._utc_now",
            return_value=__import__("datetime").datetime(
                2026, 6, 28, 12, 0, 0, tzinfo=__import__("datetime").timezone.utc
            ),
        ):
            payload = build_admin_sync_stats_payload(
                self.conn,
                health_snapshot=health_snapshot,
            )

        self.assertEqual("critical", payload["health"]["status"])
        reason_codes = {item["code"] for item in payload["health"]["reasons"]}
        self.assertIn("listener_disconnected", reason_codes)
        self.assertIn("no_recent_ingest", reason_codes)

    def test_sync_stats_reports_warning_when_queue_is_backlogged(self) -> None:
        health_snapshot = {
            "listener_enabled": True,
            "public_probe_enabled": True,
            "active_listener_count": 1,
            "configured_listener_count": 1,
            "worker_thread_alive": True,
            "refresh_thread_alive": True,
            "public_probe_thread_alive": True,
            "queue_size": 25,
            "tracked_chat_count": 2,
            "accounts": [],
        }

        payload = build_admin_sync_stats_payload(
            self.conn,
            health_snapshot=health_snapshot,
        )

        self.assertEqual("warning", payload["health"]["status"])
        self.assertTrue(
            any(
                item["code"] == "queue_backlog_warn"
                for item in payload["health"]["reasons"]
            )
        )

    def test_sync_live_messages_returns_recent_messages(self) -> None:
        self.conn.executemany(
            """
            INSERT INTO messages(
                chat_id, message_id, msg_date_text, msg_type, content, content_norm, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (1, 101, "2026-06-28 11:55:00", "TEXT", "first message", "", "2026-06-28 11:56:00"),
                (2, 201, "2026-06-28 11:57:00", "TEXT", "second message", "", "2026-06-28 11:58:00"),
            ],
        )
        self.conn.commit()

        payload = build_admin_sync_live_messages_payload(self.conn, limit=10)

        self.assertTrue(payload["ok"])
        self.assertEqual(2, len(payload["items"]))
        self.assertEqual(2, payload["items"][0]["chat_id"])
        self.assertEqual(201, payload["items"][0]["message_id"])
        self.assertEqual("second message", payload["items"][0]["content_preview"])


class AdminStorageHealthPayloadTests(unittest.TestCase):
    def _cfg(self, db_name: str, **overrides) -> SimpleNamespace:
        values = {
            "db_name": db_name,
            "db_health_size_warning_bytes": 20 * 1024 * 1024 * 1024,
            "db_health_size_critical_bytes": 50 * 1024 * 1024 * 1024,
            "db_health_wal_warning_bytes": 512 * 1024 * 1024,
            "db_health_wal_critical_bytes": 2 * 1024 * 1024 * 1024,
            "db_health_disk_free_warning_bytes": 10 * 1024 * 1024 * 1024,
            "db_health_disk_free_critical_bytes": 3 * 1024 * 1024 * 1024,
            "manticore_table": "tg_messages",
        }
        values.update(overrides)
        return SimpleNamespace(**values)

    def _open_health_db(self, db_path: Path, *, index_status: str = "ready"):
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE chats(
                chat_id INTEGER PRIMARY KEY,
                message_count INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE messages(pk INTEGER PRIMARY KEY, content TEXT);
            CREATE TABLE media_groups(id INTEGER PRIMARY KEY);
            CREATE TABLE manticore_search_outbox(pk INTEGER PRIMARY KEY);
            CREATE TABLE manticore_search_meta(key TEXT PRIMARY KEY, value TEXT);
            CREATE INDEX idx_health_media_groups ON media_groups(id);
            CREATE TABLE admin_jobs(
                job_type TEXT,
                status TEXT,
                created_at TEXT,
                updated_at TEXT
            );
            """
        )
        conn.executemany(
            "INSERT INTO chats(chat_id, message_count) VALUES (?, ?)",
            [(1, 21), (2, 21)],
        )
        conn.executemany("INSERT INTO media_groups(id) VALUES (?)", [(1,), (2,)])
        conn.execute("INSERT INTO manticore_search_outbox(pk) VALUES (1)")
        conn.execute(
            "INSERT INTO manticore_search_meta(key, value) VALUES (?, ?)",
            ("index_status:tg_messages", index_status),
        )
        conn.execute(
            """
            INSERT INTO admin_jobs(job_type, status, created_at, updated_at)
            VALUES ('cleanup', 'done', '2026-07-10 10:00:00', '2026-07-10 11:00:00')
            """
        )
        conn.commit()
        conn.execute("ANALYZE")
        conn.commit()
        return conn

    def test_storage_health_reports_lightweight_capacity_and_index_metrics(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "health.db"
            conn = self._open_health_db(db_path)
            try:
                payload = build_admin_storage_health_payload(
                    conn,
                    cfg=self._cfg(str(db_path)),
                )
            finally:
                conn.close()

        self.assertTrue(payload["ok"])
        self.assertTrue(payload["checked_at"])
        self.assertTrue(payload["database"]["path_configured"])
        self.assertGreater(payload["database"]["main_bytes"], 0)
        self.assertIn(payload["database"]["journal_mode"], {"delete", "wal"})
        self.assertEqual(42, payload["counts"]["message_count"])
        self.assertEqual(2, payload["counts"]["media_group_count"])
        self.assertEqual(1, payload["counts"]["manticore_outbox_pending"])
        self.assertTrue(payload["indexes"]["manticore_ready"])
        self.assertEqual(
            "cleanup",
            payload["maintenance"]["last_recorded_job"]["job_type"],
        )

    def test_storage_health_handles_unconfigured_path_and_missing_wal(self) -> None:
        memory_conn = sqlite3.connect(":memory:")
        memory_conn.row_factory = sqlite3.Row
        try:
            payload = build_admin_storage_health_payload(
                memory_conn,
                cfg=self._cfg(""),
            )
        finally:
            memory_conn.close()
        self.assertFalse(payload["database"]["path_configured"])
        self.assertIn(
            "database_path_unconfigured",
            {item["code"] for item in payload["reasons"]},
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "without-wal.db"
            conn = self._open_health_db(db_path)
            try:
                self.assertFalse(Path(f"{db_path}-wal").exists())
                payload = build_admin_storage_health_payload(
                    conn,
                    cfg=self._cfg(str(db_path)),
                )
            finally:
                conn.close()
        self.assertEqual(0, payload["database"]["wal_bytes"])
        self.assertEqual(0, payload["database"]["shm_bytes"])

    def test_storage_health_applies_threshold_boundaries_and_low_disk_actions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "thresholds.db"
            conn = self._open_health_db(db_path)
            try:
                db_size = db_path.stat().st_size
                warning_cfg = self._cfg(
                    str(db_path),
                    db_health_size_warning_bytes=db_size,
                    db_health_size_critical_bytes=db_size + 1,
                )
                payload = build_admin_storage_health_payload(conn, cfg=warning_cfg)
                codes = {item["code"] for item in payload["reasons"]}
                self.assertIn("database_size_warning", codes)
                self.assertNotIn("database_size_critical", codes)

                payload = build_admin_storage_health_payload(
                    conn, cfg=self._cfg(str(db_path))
                )
                self.assertIn(
                    "manticore_sync_pending",
                    {item["code"] for item in payload["reasons"]},
                )

                low_disk_cfg = self._cfg(
                    str(db_path),
                    db_health_disk_free_warning_bytes=1000,
                    db_health_disk_free_critical_bytes=500,
                )
                disk_usage = __import__("shutil").disk_usage(tmpdir)._replace(free=500)
                with patch(
                    "tg_harvest.storage.db_health.shutil.disk_usage",
                    return_value=disk_usage,
                ):
                    payload = build_admin_storage_health_payload(conn, cfg=low_disk_cfg)
            finally:
                conn.close()

        self.assertIn(
            "disk_free_critical",
            {item["code"] for item in payload["reasons"]},
        )
        self.assertIn("磁盘余量不足以安全压缩；先扩容或清理非数据库文件。", payload["actions"])

    def test_storage_health_flags_manticore_not_ready_and_avoids_heavy_queries(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "search.db"
            conn = self._open_health_db(db_path, index_status="incomplete")
            statements = []
            conn.set_trace_callback(lambda sql: statements.append(str(sql).lower()))
            try:
                payload = build_admin_storage_health_payload(
                    conn,
                    cfg=self._cfg(str(db_path)),
                )
            finally:
                conn.set_trace_callback(None)
                conn.close()

        self.assertIn(
            "manticore_not_ready", {item["code"] for item in payload["reasons"]}
        )
        self.assertFalse(any("dbstat" in statement for statement in statements))
        self.assertFalse(any("integrity_check" in statement for statement in statements))
        self.assertFalse(
            any("count(*) from messages" in statement for statement in statements)
        )

    def test_storage_health_flags_a_wal_that_keeps_growing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "wal-growth.db"
            conn = self._open_health_db(db_path)
            wal_path = Path(f"{db_path}-wal")
            cfg = self._cfg(
                str(db_path),
                db_health_wal_warning_bytes=1024 * 1024,
                db_health_wal_critical_bytes=32 * 1024 * 1024,
            )
            try:
                wal_path.write_bytes(b"x" * (8 * 1024 * 1024))
                build_admin_storage_health_payload(conn, cfg=cfg)
                wal_path.write_bytes(b"x" * (16 * 1024 * 1024))
                payload = build_admin_storage_health_payload(conn, cfg=cfg)
            finally:
                conn.close()

        self.assertIn("wal_growing", {item["code"] for item in payload["reasons"]})

    def test_config_normalizes_database_health_threshold_pairs(self) -> None:
        raw = _load_raw_config_values()
        raw.update(
            {
                "db_health_size_warning_bytes": 100,
                "db_health_size_critical_bytes": 10,
                "db_health_wal_warning_bytes": -1,
                "db_health_wal_critical_bytes": 0,
                "db_health_disk_free_warning_bytes": 100,
                "db_health_disk_free_critical_bytes": 1000,
            }
        )
        normalized = _normalize_config_values(raw)

        self.assertEqual(100, normalized["db_health_size_critical_bytes"])
        self.assertEqual(1, normalized["db_health_wal_warning_bytes"])
        self.assertEqual(1, normalized["db_health_wal_critical_bytes"])
        self.assertEqual(100, normalized["db_health_disk_free_critical_bytes"])


if __name__ == "__main__":
    unittest.main()
