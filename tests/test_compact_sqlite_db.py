import sqlite3
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tg_harvest.storage.connection import detect_sqlite_features
from tg_harvest.storage.schema import create_schema
from tools import compact_sqlite_db


class CompactSqliteDbTests(unittest.TestCase):
    def test_source_snapshot_is_stable_and_does_not_touch_source_permissions(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source_path = root / "source.db"
            snapshot_path = root / "source.snapshot.db"

            with sqlite3.connect(source_path) as source_conn:
                source_conn.execute("CREATE TABLE sample (value INTEGER PRIMARY KEY)")
                source_conn.execute("INSERT INTO sample(value) VALUES (1)")
            source_path.chmod(0o644)

            compact_sqlite_db._create_source_snapshot(source_path, snapshot_path)

            with sqlite3.connect(source_path) as source_conn:
                source_conn.execute("INSERT INTO sample(value) VALUES (2)")
            with sqlite3.connect(snapshot_path) as snapshot_conn:
                snapshot_values = [
                    row[0]
                    for row in snapshot_conn.execute(
                        "SELECT value FROM sample ORDER BY value"
                    )
                ]

            self.assertEqual([1], snapshot_values)
            self.assertEqual(0o644, stat.S_IMODE(source_path.stat().st_mode))
            self.assertEqual(0o600, stat.S_IMODE(snapshot_path.stat().st_mode))

    def test_required_free_space_accounts_for_snapshot_and_build(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source_path = Path(tmpdir) / "source.db"
            source_path.write_bytes(b"database")
            Path(f"{source_path}-wal").write_bytes(b"wal")
            Path(f"{source_path}-shm").write_bytes(b"shm")

            source_bytes = len(b"database") + len(b"wal") + len(b"shm")
            required = compact_sqlite_db._required_free_bytes(
                source_path,
                min_free_gb=0,
            )

            self.assertEqual(
                source_bytes * 2 + compact_sqlite_db._SNAPSHOT_BUILD_HEADROOM_BYTES,
                required,
            )

    def test_compaction_refuses_insufficient_snapshot_build_space(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source_path = root / "source.db"
            target_path = root / "compact.db"
            source_path.write_bytes(b"database")
            required = compact_sqlite_db._required_free_bytes(
                source_path,
                min_free_gb=0,
            )

            with patch.object(
                compact_sqlite_db,
                "_available_bytes",
                return_value=required - 1,
            ), patch.object(
                sys,
                "argv",
                [
                    "compact_sqlite_db.py",
                    "--source",
                    str(source_path),
                    "--target",
                    str(target_path),
                    "--min-free-gb",
                    "0",
                ],
            ), self.assertRaisesRegex(SystemExit, "source snapshot and compact build"):
                compact_sqlite_db.main()

    def test_open_target_restricts_work_database_without_changing_parent(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            target_dir = Path(tmpdir) / "target"
            target_dir.mkdir(mode=0o755)
            target_dir.chmod(0o755)
            work_target = target_dir / "compact.db.building"

            conn = compact_sqlite_db._open_target(work_target)
            try:
                conn.execute("CREATE TABLE sample (value INTEGER)")
                conn.commit()
                self.assertEqual(0o755, stat.S_IMODE(target_dir.stat().st_mode))
                for artifact in compact_sqlite_db._db_files(work_target):
                    if artifact.exists():
                        self.assertEqual(0o600, stat.S_IMODE(artifact.stat().st_mode))
            finally:
                conn.close()

    def test_promote_work_target_restricts_final_database_and_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            target_dir = Path(tmpdir) / "target"
            target_dir.mkdir(mode=0o755)
            target_dir.chmod(0o755)
            work_target = target_dir / "compact.db.building"
            target = target_dir / "compact.db"
            work_target.write_bytes(b"sqlite database")
            work_target.chmod(0o644)
            for suffix in ("-wal", "-shm", "-journal"):
                sidecar = Path(f"{work_target}{suffix}")
                sidecar.write_bytes(b"sidecar")
                sidecar.chmod(0o644)

            compact_sqlite_db._promote_work_target(
                work_target,
                target,
                force=False,
            )

            self.assertEqual(0o755, stat.S_IMODE(target_dir.stat().st_mode))
            target_artifacts = (
                target,
                *(Path(f"{target}{suffix}") for suffix in ("-wal", "-shm", "-journal")),
            )
            for candidate in target_artifacts:
                self.assertTrue(candidate.exists())
                self.assertEqual(0o600, stat.S_IMODE(candidate.stat().st_mode))

    def test_compaction_uses_snapshot_after_live_source_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source_path = root / "source.db"
            target_path = root / "compact.db"
            self._seed_source(source_path)
            create_snapshot = compact_sqlite_db._create_source_snapshot

            def snapshot_then_append_chat(source: Path, snapshot: Path) -> None:
                create_snapshot(source, snapshot)
                with sqlite3.connect(source) as source_conn:
                    source_conn.execute(
                        "INSERT INTO chats(chat_id, chat_title) VALUES (2, 'late chat')"
                    )

            with patch.object(
                compact_sqlite_db,
                "_create_source_snapshot",
                side_effect=snapshot_then_append_chat,
            ), patch.object(
                sys,
                "argv",
                [
                    "compact_sqlite_db.py",
                    "--source",
                    str(source_path),
                    "--target",
                    str(target_path),
                    "--min-free-gb",
                    "0",
                    "--batch-size",
                    "1",
                ],
            ):
                self.assertEqual(0, compact_sqlite_db.main())

            with sqlite3.connect(source_path) as source_conn, sqlite3.connect(
                target_path
            ) as target_conn:
                self.assertEqual(
                    2,
                    source_conn.execute("SELECT COUNT(*) FROM chats").fetchone()[0],
                )
                self.assertEqual(
                    1,
                    target_conn.execute("SELECT COUNT(*) FROM chats").fetchone()[0],
                )

    def test_compaction_cleans_source_snapshot_after_build_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source_path = root / "source.db"
            target_path = root / "compact.db"
            self._seed_source(source_path)
            snapshot_path = compact_sqlite_db._source_snapshot_path_for_work_target(
                compact_sqlite_db._work_path_for_target(target_path)
            )

            with patch.object(
                compact_sqlite_db,
                "_open_target",
                side_effect=RuntimeError("target open failed"),
            ), patch.object(
                sys,
                "argv",
                [
                    "compact_sqlite_db.py",
                    "--source",
                    str(source_path),
                    "--target",
                    str(target_path),
                    "--min-free-gb",
                    "0",
                ],
            ), self.assertRaisesRegex(RuntimeError, "target open failed"):
                compact_sqlite_db.main()

            self.assertFalse(compact_sqlite_db._any_db_file_exists(snapshot_path))

    def test_compaction_preserves_scheduler_and_model_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            source_path = Path(tmpdir) / "source.db"
            target_dir = Path(tmpdir) / "target"
            target_dir.mkdir(mode=0o755)
            target_dir.chmod(0o755)
            target_path = target_dir / "compact.db"
            work_target = compact_sqlite_db._work_path_for_target(target_path)
            source_snapshot = compact_sqlite_db._source_snapshot_path_for_work_target(
                work_target
            )
            self._seed_source(source_path)

            with patch.object(
                sys,
                "argv",
                [
                    "compact_sqlite_db.py",
                    "--source",
                    str(source_path),
                    "--target",
                    str(target_path),
                    "--min-free-gb",
                    "0",
                    "--batch-size",
                    "1",
                ],
            ):
                self.assertEqual(0, compact_sqlite_db.main())

            self.assertFalse(compact_sqlite_db._any_db_file_exists(work_target))
            self.assertFalse(compact_sqlite_db._any_db_file_exists(source_snapshot))
            self.assertEqual(0o755, stat.S_IMODE(target_dir.stat().st_mode))
            self.assertEqual(0o600, stat.S_IMODE(target_path.stat().st_mode))

            with sqlite3.connect(source_path) as source_conn, sqlite3.connect(
                target_path
            ) as target_conn:
                for table in (
                    "account_runtime_state",
                    "sync_chat_state",
                    "sync_pending_updates",
                    "sync_learning_events",
                    "sync_model_state",
                ):
                    source_count = source_conn.execute(
                        f"SELECT COUNT(*) FROM {table}"
                    ).fetchone()[0]
                    target_count = target_conn.execute(
                        f"SELECT COUNT(*) FROM {table}"
                    ).fetchone()[0]
                    self.assertEqual(source_count, target_count, table)

                pending = target_conn.execute(
                    "SELECT due_at, in_flight FROM sync_pending_updates WHERE chat_id = 1"
                ).fetchone()
                self.assertEqual(("2026-07-03 00:01:00", 1), pending)
                model = target_conn.execute(
                    "SELECT backend, artifact_path FROM sync_model_state WHERE model_key = ?",
                    ("temporal_batch_predictor",),
                ).fetchone()
                self.assertEqual(("torch", "/tmp/model.pt"), model)
                for key in (
                    "cjk_terms_rebuild_state",
                    "cjk_terms_backfill_mode",
                    "cjk_terms_backfill_last_pk",
                ):
                    self.assertIsNone(
                        target_conn.execute(
                            "SELECT 1 FROM message_search_terms_meta WHERE key = ?",
                            (key,),
                        ).fetchone(),
                        key,
                    )

    def _seed_source(self, path: Path) -> None:
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        try:
            create_schema(
                conn,
                detect_sqlite_features(conn),
                skip_fts_auto_heal=1,
            )
            conn.execute(
                "INSERT INTO chats(chat_id, chat_title) VALUES (1, 'Chat 1')"
            )
            conn.execute(
                """
                INSERT INTO account_runtime_state(
                    account_key, session_name, label, cooldown_until, in_flight_count
                ) VALUES ('primary', 'primary.session', 'Primary', '2026-07-03 01:00:00', 1)
                """
            )
            conn.execute(
                """
                INSERT INTO sync_chat_state(
                    chat_id, chat_title, status, next_update_at, is_active
                ) VALUES (1, 'Chat 1', 'updating', '2026-07-03 00:01:00', 1)
                """
            )
            conn.execute(
                """
                INSERT INTO sync_pending_updates(
                    chat_id, chat_title, due_at, generation, in_flight, in_flight_generation
                ) VALUES (1, 'Chat 1', '2026-07-03 00:01:00', 3, 1, 3)
                """
            )
            conn.execute(
                """
                INSERT INTO sync_learning_events(
                    chat_id, event_type, status, features_json
                ) VALUES (1, 'update_outcome', 'success', '{"sample": true}')
                """
            )
            conn.execute(
                """
                INSERT INTO sync_model_state(
                    model_key, backend, artifact_path, state_json
                ) VALUES ('temporal_batch_predictor', 'torch', '/tmp/model.pt', '{"epoch": 3}')
                """
            )
            conn.executemany(
                """
                INSERT INTO message_search_terms_meta(key, value)
                VALUES (?, ?)
                """,
                [
                    ("cjk_terms_rebuild_state", "full"),
                    ("cjk_terms_backfill_mode", "legacy"),
                    ("cjk_terms_backfill_last_pk", "1"),
                ],
            )
            conn.commit()
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
