import unittest
from types import SimpleNamespace
from unittest.mock import patch

from flask import Flask

import tg_harvest.web.auth as auth_module
from tg_harvest.web.auth import register_auth_routes
from tg_harvest.web.routes.clone import register_clone_routes


class _LoggerStub:
    def exception(self, _message):
        return None


class _ConnStub:
    def close(self):
        return None


class _Bundle:
    def __init__(self, app_link, web_link):
        self.app_link = app_link
        self.web_link = web_link


class CloneRoutesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.started_jobs = []
        self.logs = []
        self.status_updates = []
        self.created_jobs = []
        self.created_clone_runs = []
        self.created_clone_plans = []
        self.created_clone_migrations = []
        self.clone_message_mappings = []
        self.deleted_clone_run_ids = []
        self.started_deep_preflight_jobs = []
        self.started_timeline_migration_jobs = []
        self.started_target_delete_jobs = []
        self.started_message_delete_jobs = []
        self.clone_timeline_preview_override = None
        self.clone_run_create_error = None
        self.clone_plan_create_error = None
        self.clone_migration_create_error = None
        self.clone_structure_start_error = None
        self.job_statuses = {}
        self.clone_runs = [
            {
                "run_id": "run-existing",
                "job_id": "job-existing",
                "source_chat_id": 100,
                "source_title": "Source",
                "source_chat_username": "source",
                "source_chat_type": "Megagroup",
                "source_message_count": 10,
                "source_last_message_at": "2026-01-02 00:00:00",
                "source_last_message_ts": 2,
                "target_chat_id": 777,
                "target_access_hash": "123",
                "target_title": "Source Existing Copy",
                "target_kind": "megagroup",
                "target_username": "",
                "target_owner_session": "clone",
                "phase": "done",
                "status": "done",
                "plan_json": "",
                "error_message": "",
                "target_created_at": "2026-06-18T00:00:00+00:00",
                "completed_at": "2026-06-18T00:00:00+00:00",
                "created_at": "2026-06-18T00:00:00+00:00",
                "updated_at": "2026-06-18T00:00:00+00:00",
            }
        ]
        self.secondary_ready = True
        self.app = Flask(__name__, template_folder="/root/db/templates")
        self.app.secret_key = "test"

        def build_bundle(chat_id, chat_username=None):
            username = str(chat_username or "")
            return _Bundle(
                app_link=(
                    f"tg://resolve?domain={username}"
                    if username
                    else f"tg://openmessage?chat_id={chat_id}"
                ),
                web_link=f"https://t.me/{username}" if username else "",
            )

        register_auth_routes(self.app)
        register_clone_routes(
            self.app,
            logger=_LoggerStub(),
            get_conn_fn=lambda: _ConnStub(),
            cfg=SimpleNamespace(session_name="main", secondary_session_name="clone"),
            list_clone_source_chats_fn=lambda _conn, sort: [
                {
                    "chat_id": 100,
                    "chat_title": "Source",
                    "chat_username": "source",
                    "chat_type": "Megagroup",
                    "message_count": 10,
                    "media_rows": 3,
                    "last_seen_at": "2026-01-01 00:00:00",
                    "last_message_at": "2026-01-02 00:00:00",
                    "last_message_ts": 2,
                }
            ],
            build_clone_preflight_report_fn=self._build_report,
            create_clone_run_fn=self._create_clone_run,
            load_clone_run_fn=self._load_clone_run,
            list_clone_runs_fn=self._list_clone_runs,
            count_clone_runs_fn=self._count_clone_runs,
            load_clone_run_detail_fn=self._load_clone_run_detail,
            load_clone_run_progress_fn=self._load_clone_run_progress,
            list_clone_message_mappings_fn=self._list_clone_message_mappings,
            count_clone_message_mappings_fn=self._count_clone_message_mappings,
            delete_clone_run_fn=self._delete_clone_run,
            create_clone_plan_fn=self._create_clone_plan,
            load_latest_clone_plan_fn=self._load_latest_clone_plan,
            create_clone_migration_fn=self._create_clone_migration,
            load_latest_clone_migration_fn=self._load_latest_clone_migration,
            build_telegram_chat_link_bundle_fn=build_bundle,
            admin_try_create_exclusive_job_fn=self._create_job,
            admin_job_get_snapshot_fn=self._job_snapshot,
            admin_job_append_log_fn=lambda job_id, message: self.logs.append(
                (job_id, str(message))
            ),
            admin_job_set_status_fn=(
                lambda job_id, status: self.status_updates.append(
                    (str(job_id), str(status))
                )
                or True
            ),
            admin_start_clone_structure_job_thread_fn=self._start_clone_structure_job,
            admin_start_clone_deep_preflight_job_thread_fn=(
                lambda *args, **kwargs: self.started_deep_preflight_jobs.append(
                    (args, kwargs)
                )
            ),
            admin_start_clone_timeline_migration_job_thread_fn=(
                lambda *args, **kwargs: self.started_timeline_migration_jobs.append(
                    (args, kwargs)
                )
            ),
            admin_start_clone_target_delete_job_thread_fn=(
                lambda *args, **kwargs: self.started_target_delete_jobs.append(
                    (args, kwargs)
                )
            ),
            admin_start_clone_message_delete_job_thread_fn=(
                lambda *args, **kwargs: self.started_message_delete_jobs.append(
                    (args, kwargs)
                )
            ),
        )
        self.client = self.app.test_client()

    def _build_report(self, _conn, *, chat_id, cfg):
        if int(chat_id) != 100:
            raise ValueError("chat_id 不存在")
        return {
            "source": {
                "chat_id": 100,
                "chat_title": "Source",
                "chat_type": "Megagroup",
            },
            "target": {
                "default_title": "Source 副本",
                "supported_kinds": ["channel", "megagroup"],
            },
            "metrics": {"total_messages": 10},
            "account": {
                "secondary_session_distinct": self.secondary_ready,
            },
            "capabilities": [],
            "warnings": [],
            "recommendation": {},
            "confirm": f"CLONE:STRUCTURE:{int(chat_id)}",
        }

    def _create_job(self, job_type, *, target_chat_id=None, target_label=None):
        self.created_jobs.append(
            {
                "job_type": job_type,
                "target_chat_id": target_chat_id,
                "target_label": target_label,
            }
        )
        return {"job_id": "job-clone-1"}, None

    def _job_snapshot(self, job_id):
        job_type = self.created_jobs[-1]["job_type"] if self.created_jobs else "clone_structure"
        return {
            "job_id": job_id,
            "job_type": job_type,
            "status": self.job_statuses.get(
                str(job_id),
                "queued" if str(job_id) == "job-clone-1" else "done",
            ),
        }

    def _start_clone_structure_job(self, *args, **kwargs):
        if self.clone_structure_start_error is not None:
            raise self.clone_structure_start_error
        self.started_jobs.append((args, kwargs))

    def _create_clone_run(self, _conn, **kwargs):
        if self.clone_run_create_error is not None:
            raise self.clone_run_create_error
        self.created_clone_runs.append(kwargs)
        return {
            "run_id": kwargs["run_id"],
            "job_id": kwargs["job_id"],
            "source_chat_id": kwargs["source_chat"]["chat_id"],
            "source_title": kwargs["source_chat"]["chat_title"],
            "source_chat_username": "",
            "source_chat_type": kwargs["source_chat"]["chat_type"],
            "source_message_count": 10,
            "source_last_message_at": "",
            "source_last_message_ts": None,
            "target_chat_id": None,
            "target_access_hash": "",
            "target_title": kwargs["target_title"],
            "target_kind": kwargs["target_kind"],
            "target_username": "",
            "target_owner_session": kwargs["target_owner_session"],
            "phase": "queued",
            "status": "queued",
            "plan_json": "",
            "error_message": "",
            "target_created_at": "",
            "completed_at": "",
            "created_at": "2026-06-18T00:00:00+00:00",
            "updated_at": "2026-06-18T00:00:00+00:00",
        }

    def _load_clone_run(self, _conn, run_id):
        for run in self.clone_runs:
            if str(run["run_id"]) == str(run_id):
                return dict(run)
        return None

    def _list_clone_runs(
        self,
        _conn,
        *,
        source_chat_id=None,
        limit=20,
        offset=0,
        status="",
        q="",
        sort="",
    ):
        items = self.clone_runs
        if source_chat_id is not None:
            items = [
                item
                for item in items
                if int(item["source_chat_id"]) == int(source_chat_id)
            ]
        if status:
            items = [
                item
                for item in items
                if str(item.get("status") or "") == str(status)
            ]
        if q:
            query = str(q).lower()
            items = [
                item
                for item in items
                if query in str(item.get("source_title") or "").lower()
                or query in str(item.get("target_title") or "").lower()
                or query in str(item.get("run_id") or "").lower()
            ]
        return items[int(offset) : int(offset) + int(limit)]

    def _count_clone_runs(self, _conn, *, source_chat_id=None, status="", q=""):
        return len(
            self._list_clone_runs(
                _conn,
                source_chat_id=source_chat_id,
                limit=100,
                offset=0,
                status=status,
                q=q,
            )
        )

    def _load_clone_run_detail(self, _conn, run_id):
        run = self._load_clone_run(_conn, run_id)
        if run is None:
            return None
        return {
            "run": run,
            "plan": self._load_latest_clone_plan(_conn, run_id),
            "migration": self._load_latest_clone_migration(
                _conn,
                run_id,
                mode="timeline_replay",
            ),
            "timeline_preview": self._build_clone_timeline_replay_preview(
                _conn,
                run_id=run_id,
                source_chat_id=int(run["source_chat_id"]),
            ),
            "mapping_summary": {
                "total": len(self.clone_message_mappings),
                "done": sum(
                    1
                    for item in self.clone_message_mappings
                    if item.get("status") == "done"
                ),
                "error": sum(
                    1
                    for item in self.clone_message_mappings
                    if item.get("status") == "error"
                ),
            },
            "recent_mappings": list(self.clone_message_mappings[:100]),
            "failure_items": [
                item
                for item in self.clone_message_mappings
                if item.get("status") == "error"
            ][:100],
        }

    def _list_clone_message_mappings(
        self,
        _conn,
        *,
        run_id,
        status="",
        mode="",
        limit=100,
        offset=0,
    ):
        items = [
            item
            for item in self.clone_message_mappings
            if str(item.get("run_id") or "") == str(run_id)
        ]
        if status:
            items = [item for item in items if str(item.get("status") or "") == status]
        if mode:
            items = [item for item in items if str(item.get("mode") or "") == mode]
        return items[int(offset) : int(offset) + int(limit)]

    def _count_clone_message_mappings(
        self,
        _conn,
        *,
        run_id,
        status="",
        mode="",
    ):
        return len(
            self._list_clone_message_mappings(
                _conn,
                run_id=run_id,
                status=status,
                mode=mode,
                limit=100000,
                offset=0,
            )
        )

    def _delete_clone_run(self, _conn, *, run_id):
        before = len(self.clone_runs)
        self.clone_runs = [
            item for item in self.clone_runs if str(item.get("run_id") or "") != str(run_id)
        ]
        deleted = len(self.clone_runs) < before
        if deleted:
            self.deleted_clone_run_ids.append(str(run_id))
        return deleted

    def _create_clone_plan(self, _conn, **kwargs):
        if self.clone_plan_create_error is not None:
            raise self.clone_plan_create_error
        plan = {
            "plan_id": kwargs["plan_id"],
            "run_id": kwargs["run_id"],
            "job_id": kwargs.get("job_id", ""),
            "status": kwargs.get("status", "queued"),
            "source_access": "unknown",
            "target_access": "unknown",
            "primary_session_status": "unknown",
            "secondary_session_status": "unknown",
            "migration_account": "",
            "text_strategy": "",
            "media_strategy": "",
            "media_group_strategy": "",
            "avatar_strategy": "",
            "blocking_issues": [],
            "warnings": [],
            "capabilities": {},
            "plan": kwargs.get("plan", {}),
            "error_message": "",
            "created_at": "2026-06-18T00:00:00+00:00",
            "updated_at": "2026-06-18T00:00:00+00:00",
            "completed_at": "",
        }
        self.created_clone_plans.append(plan)
        return dict(plan)

    def _load_latest_clone_plan(self, _conn, run_id):
        for plan in reversed(self.created_clone_plans):
            if str(plan["run_id"]) == str(run_id):
                return dict(plan)
        return None

    def _create_done_plan(self):
        plan = {
            "plan_id": "plan-ready",
            "run_id": "run-existing",
            "job_id": "job-deep",
            "status": "done",
            "source_access": "ok",
            "target_access": "ok",
            "primary_session_status": "ok",
            "secondary_session_status": "ok",
            "migration_account": "primary",
            "text_strategy": "database_replay",
            "media_strategy": "source_copy_without_attribution",
            "media_group_strategy": "strict_skip_incomplete",
            "avatar_strategy": "skip_not_implemented",
            "blocking_issues": [],
            "warnings": [],
            "capabilities": {"target_write_account": "primary"},
            "plan": {"target_write_account": "primary"},
            "error_message": "",
            "created_at": "2026-06-18T00:00:00+00:00",
            "updated_at": "2026-06-18T00:00:00+00:00",
            "completed_at": "2026-06-18T00:00:00+00:00",
        }
        self.created_clone_plans.append(plan)
        return plan

    def _create_relay_done_plan(self):
        media_relay = {
            "enabled": True,
            "chat_id": 999,
            "username": "",
            "source_account": "primary",
            "target_account": "secondary",
            "privacy": "without_source_attribution",
            "requires_drop_author_each_hop": True,
            "keeps_source_link": False,
            "keeps_relay_link": False,
        }
        plan = {
            "plan_id": "plan-relay-ready",
            "run_id": "run-existing",
            "job_id": "job-deep",
            "status": "done",
            "source_access": "ok",
            "target_access": "ok",
            "primary_session_status": "ok",
            "secondary_session_status": "ok",
            "migration_account": "unavailable",
            "text_strategy": "database_replay",
            "media_strategy": "relay_copy_without_attribution",
            "media_group_strategy": "relay_api_rebuild",
            "avatar_strategy": "skip_not_implemented",
            "blocking_issues": [],
            "warnings": [],
            "capabilities": {
                "target_write_account": "secondary",
                "media_relay": media_relay,
            },
            "plan": {
                "target_write_account": "secondary",
                "media_relay": media_relay,
            },
            "error_message": "",
            "created_at": "2026-06-18T00:00:00+00:00",
            "updated_at": "2026-06-18T00:00:00+00:00",
            "completed_at": "2026-06-18T00:00:00+00:00",
        }
        self.created_clone_plans.append(plan)
        return plan

    def _create_clone_migration(self, _conn, **kwargs):
        if self.clone_migration_create_error is not None:
            raise self.clone_migration_create_error
        migration = {
            "migration_id": kwargs["migration_id"],
            "run_id": kwargs["run_id"],
            "plan_id": kwargs.get("plan_id", ""),
            "job_id": kwargs.get("job_id", ""),
            "mode": kwargs.get("mode", "text_replay"),
            "status": kwargs.get("status", "queued"),
            "phase": kwargs.get("phase", "queued"),
            "target_chat_id": kwargs.get("target_chat_id"),
            "target_title": kwargs.get("target_title", ""),
            "target_write_account": kwargs.get("target_write_account", ""),
            "requested_limit": kwargs.get("requested_limit", 0),
            "send_delay_ms": kwargs.get("send_delay_ms", 0),
            "text_total": kwargs.get("text_total", 0),
            "text_sent": 0,
            "text_skipped": 0,
            "text_failed": 0,
            "media_total": kwargs.get("media_total", 0),
            "media_sent": 0,
            "media_skipped": kwargs.get("media_skipped", 0),
            "media_failed": 0,
            "media_group_total": kwargs.get("media_group_total", 0),
            "media_group_sent": 0,
            "media_group_skipped": kwargs.get("media_group_skipped", 0),
            "media_group_failed": 0,
            "plan": kwargs.get("plan", {}),
            "error_message": "",
            "created_at": "2026-06-18T00:00:00+00:00",
            "updated_at": "2026-06-18T00:00:00+00:00",
            "completed_at": "",
        }
        self.created_clone_migrations.append(migration)
        return dict(migration)

    def _load_latest_clone_migration(self, _conn, run_id, mode=None):
        for migration in reversed(self.created_clone_migrations):
            if str(migration["run_id"]) == str(run_id):
                if mode and str(migration.get("mode") or "") != str(mode):
                    continue
                return dict(migration)
        return None

    def _load_clone_run_progress(self, _conn, run_id):
        snapshot = next(
            (
                item
                for item in reversed(self.created_clone_migrations)
                if str(item.get("run_id") or "") == str(run_id)
                and str(item.get("mode") or "") == "timeline_replay"
                and (
                    int(item.get("text_total") or 0) > 0
                    or int(item.get("media_total") or 0) > 0
                    or int(item.get("media_group_total") or 0) > 0
                )
            ),
            None,
        )
        text_done = {
            int(item.get("source_message_id") or 0)
            for item in self.clone_message_mappings
            if str(item.get("run_id") or "") == str(run_id)
            and str(item.get("mode") or "") == "text_replay"
            and str(item.get("status") or "") == "done"
        }
        media_done = {
            int(item.get("source_message_id") or 0)
            for item in self.clone_message_mappings
            if str(item.get("run_id") or "") == str(run_id)
            and str(item.get("mode") or "")
            in {"media_copy", "media_group_copy"}
            and str(item.get("status") or "") == "done"
        }
        if snapshot is None:
            return {
                "assessment_state": "unverified",
                "snapshot_migration_id": "",
                "verified_at": "",
                "messages_total": 0,
                "messages_done": len(text_done) + len(media_done),
                "messages_error": 0,
                "messages_remaining": 0,
                "text_total": 0,
                "text_done": len(text_done),
                "text_error": 0,
                "text_remaining": 0,
                "media_total": 0,
                "media_done": len(media_done),
                "media_error": 0,
                "media_remaining": 0,
                "media_group_total": 0,
                "media_group_items_done": 0,
            }
        text_total = int(snapshot.get("text_total") or 0)
        media_total = int(snapshot.get("media_total") or 0)
        message_total = text_total + media_total
        message_done = min(message_total, len(text_done) + len(media_done))
        return {
            "assessment_state": "verified",
            "snapshot_migration_id": str(snapshot.get("migration_id") or ""),
            "verified_at": str(snapshot.get("updated_at") or ""),
            "messages_total": message_total,
            "messages_done": message_done,
            "messages_error": 0,
            "messages_remaining": message_total - message_done,
            "text_total": text_total,
            "text_done": min(text_total, len(text_done)),
            "text_error": 0,
            "text_remaining": max(0, text_total - len(text_done)),
            "media_total": media_total,
            "media_done": min(media_total, len(media_done)),
            "media_error": 0,
            "media_remaining": max(0, media_total - len(media_done)),
            "media_group_total": int(snapshot.get("media_group_total") or 0),
            "media_group_items_done": 0,
        }

    def _build_clone_timeline_replay_preview(self, _conn, *, run_id, source_chat_id):
        preview = {
            "run_id": str(run_id),
            "source_chat_id": int(source_chat_id),
            "mode": "timeline_replay",
            "timeline_items_total": 11,
            "timeline_source_messages_total": 14,
            "timeline_remaining": 11,
            "text_total": 8,
            "text_completed": 2,
            "text_remaining": 6,
            "media_total": 6,
            "media_completed": 1,
            "media_remaining": 5,
            "media_group_total": 3,
            "media_group_candidate_items": 5,
            "db_self_check_risk_group_total": 2,
            "db_self_check_risk_group_items": 3,
        }
        if self.clone_timeline_preview_override:
            preview.update(self.clone_timeline_preview_override)
        return preview

    def _auth_config_patch(self):
        return patch(
            "tg_harvest.web.auth._get_auth_config",
            return_value=SimpleNamespace(
                admin_password="secret",
                admin_session_expiry=60,
            ),
        )

    def _login_admin(self) -> str:
        response = self.client.post(
            "/api/admin/auth/login",
            json={"password": "secret"},
        )
        self.assertEqual(200, response.status_code)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        return str(payload["csrf_token"])

    def test_clone_page_redirects_to_login_when_unauthenticated(self) -> None:
        with self._auth_config_patch():
            response = self.client.get("/admin/clone")

        self.assertEqual(302, response.status_code)
        self.assertEqual("/admin/login?next=%2Fadmin%2Fclone", response.location)

    def test_clone_runs_manage_page_redirects_to_login_when_unauthenticated(self) -> None:
        with self._auth_config_patch():
            response = self.client.get("/admin/clone/runs/manage")

        self.assertEqual(302, response.status_code)
        self.assertEqual(
            "/admin/login?next=%2Fadmin%2Fclone%2Fruns%2Fmanage",
            response.location,
        )

    def test_clone_run_detail_page_redirects_to_login_when_unauthenticated(self) -> None:
        with self._auth_config_patch():
            response = self.client.get("/admin/clone/runs/detail?run_id=run-existing")

        self.assertEqual(302, response.status_code)
        self.assertEqual(
            "/admin/login?next=%2Fadmin%2Fclone%2Fruns%2Fdetail%3Frun_id%3Drun-existing",
            response.location,
        )

    def test_clone_create_page_redirects_to_login_when_unauthenticated(self) -> None:
        with self._auth_config_patch():
            response = self.client.get("/admin/clone/create")

        self.assertEqual(302, response.status_code)
        self.assertEqual("/admin/login?next=%2Fadmin%2Fclone%2Fcreate", response.location)

    def test_clone_migrate_page_redirects_to_login_when_unauthenticated(self) -> None:
        with self._auth_config_patch():
            response = self.client.get("/admin/clone/migrate")

        self.assertEqual(302, response.status_code)
        self.assertEqual("/admin/login?next=%2Fadmin%2Fclone%2Fmigrate", response.location)

    def test_clone_hub_page_renders_when_authenticated(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/admin/clone")

        self.assertEqual(200, response.status_code)
        body = response.get_data(as_text=True)
        self.assertIn("克隆工作台", body)
        self.assertIn("创建副本", body)
        self.assertIn("迁移消息", body)
        self.assertIn("管理克隆记录", body)

    def test_clone_create_page_renders_when_authenticated(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/admin/clone/create")

        self.assertEqual(200, response.status_code)
        body = response.get_data(as_text=True)
        self.assertIn("创建副本", body)
        self.assertIn('data-clone-mode="create"', body)
        self.assertIn("最近创建记录", body)
        self.assertIn("第 1 步 / 共 3 步", body)

    def test_clone_migrate_page_renders_when_authenticated(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/admin/clone/migrate")

        self.assertEqual(200, response.status_code)
        body = response.get_data(as_text=True)
        self.assertIn("迁移消息", body)
        self.assertIn('data-clone-mode="migrate"', body)
        self.assertIn("生成迁移方案", body)
        self.assertIn("第 2 步 / 共 3 步", body)

    def test_clone_runs_manage_page_renders_when_authenticated(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/admin/clone/runs/manage")

        self.assertEqual(200, response.status_code)
        body = response.get_data(as_text=True)
        self.assertIn("已克隆群管理", body)
        self.assertIn("第 3 步 / 共 3 步", body)

    def test_clone_run_detail_page_renders_when_authenticated(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/admin/clone/runs/detail?run_id=run-existing")

        self.assertEqual(200, response.status_code)
        body = response.get_data(as_text=True)
        self.assertIn("已克隆群详情", body)
        self.assertIn("消息映射与排错", body)
        self.assertIn("第 3 步 / 共 3 步", body)

    def test_clone_workbench_api_returns_next_action_and_summary(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/api/admin/clone/workbench")

        self.assertEqual(200, response.status_code)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(
            {"total": 1, "creating": 0, "created": 1, "failed": 0},
            payload["summary"],
        )
        self.assertEqual("migrate", payload["focus"]["step"])
        self.assertEqual("ready", payload["focus"]["state"])
        self.assertEqual("run-existing", payload["focus"]["run"]["run_id"])
        self.assertEqual(
            "/admin/clone/migrate?run_id=run-existing",
            payload["focus"]["action"]["href"],
        )

    def test_clone_workbench_api_prioritizes_running_message_migration(self) -> None:
        self._create_done_plan()
        self.created_clone_migrations.append(
            {
                "migration_id": "migration-active",
                "run_id": "run-existing",
                "job_id": "job-migration-active",
                "mode": "timeline_replay",
                "status": "running",
                "phase": "replaying_timeline",
            }
        )

        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/api/admin/clone/workbench")

        self.assertEqual(200, response.status_code)
        focus = response.get_json()["focus"]
        self.assertEqual("migrate", focus["step"])
        self.assertEqual("active", focus["state"])
        self.assertEqual("查看迁移进度", focus["action"]["label"])

    def test_clone_chats_api_includes_telegram_links(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/api/admin/clone/chats")

        self.assertEqual(200, response.status_code)
        payload = response.get_json()
        self.assertEqual(1, payload["count"])
        item = payload["items"][0]
        self.assertEqual("Source", item["chat_title"])
        self.assertEqual("tg://resolve?domain=source", item["telegram_app_link"])
        self.assertEqual("https://t.me/source", item["telegram_web_link"])
        self.assertTrue(item["has_public_link"])

    def test_clone_runs_api_includes_source_and_target_links(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/api/admin/clone/runs?source_chat_id=100")

        self.assertEqual(200, response.status_code)
        payload = response.get_json()
        self.assertEqual(1, payload["total"])
        self.assertNotIn("count", payload)
        item = payload["items"][0]
        self.assertEqual("run-existing", item["run_id"])
        self.assertEqual("Source Existing Copy", item["target_title"])
        self.assertEqual("tg://resolve?domain=source", item["source_telegram_app_link"])
        self.assertEqual("tg://openmessage?chat_id=777", item["target_telegram_app_link"])

    def test_clone_runs_api_returns_recent_runs_without_source_filter(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/api/admin/clone/runs?limit=20")

        self.assertEqual(200, response.status_code)
        payload = response.get_json()
        self.assertEqual("run-existing", payload["items"][0]["run_id"])
        self.assertEqual(1, payload["total"])
        self.assertNotIn("count", payload)
        self.assertEqual(20, payload["limit"])
        self.assertEqual(0, payload["offset"])

    def test_clone_runs_api_can_skip_unused_total_count(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/api/admin/clone/runs?limit=20&include_total=0")

        self.assertEqual(200, response.status_code)
        payload = response.get_json()
        self.assertEqual("run-existing", payload["items"][0]["run_id"])
        self.assertNotIn("total", payload)

    def test_clone_runs_api_supports_manage_filters(self) -> None:
        self.clone_runs.append(
            {
                **self.clone_runs[0],
                "run_id": "run-error",
                "job_id": "job-error",
                "target_title": "Broken Copy",
                "status": "error",
                "updated_at": "2026-06-18T00:01:00+00:00",
            }
        )

        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get(
                "/api/admin/clone/runs?status=error&q=broken&limit=20&offset=0"
            )

        self.assertEqual(200, response.status_code)
        payload = response.get_json()
        self.assertEqual(1, payload["total"])
        self.assertEqual("run-error", payload["items"][0]["run_id"])

    def test_clone_run_detail_api_returns_manage_payload(self) -> None:
        self.created_clone_migrations.append(
            {
                "migration_id": "migration-timeline",
                "run_id": "run-existing",
                "mode": "timeline_replay",
                "status": "done",
                "phase": "done",
                "text_sent": 2,
                "text_total": 8,
                "media_sent": 1,
                "media_total": 6,
                "updated_at": "2026-06-18T00:01:00+00:00",
            }
        )
        self.clone_message_mappings.append(
            {
                "run_id": "run-existing",
                "source_message_id": 1,
                "target_message_id": 9001,
                "mode": "text_replay",
                "status": "done",
                "updated_at": "2026-06-18T00:01:00+00:00",
            }
        )

        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/api/admin/clone/runs/run-existing/detail")

        self.assertEqual(200, response.status_code)
        payload = response.get_json()
        self.assertEqual("run-existing", payload["run"]["run_id"])
        self.assertEqual("migration-timeline", payload["migration"]["migration_id"])
        self.assertEqual("DELETE-CLONE-RUN:run-existing", payload["delete_confirm"])
        self.assertEqual(1, payload["mapping_summary"]["total"])
        self.assertEqual(1, len(payload["recent_mappings"]))

    def test_clone_run_messages_api_filters_errors(self) -> None:
        self.clone_message_mappings.extend(
            [
                {
                    "run_id": "run-existing",
                    "source_message_id": 1,
                    "target_message_id": 9001,
                    "mode": "text_replay",
                    "status": "done",
                },
                {
                    "run_id": "run-existing",
                    "source_message_id": 2,
                    "target_message_id": None,
                    "mode": "media_copy",
                    "status": "error",
                },
            ]
        )

        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get(
                "/api/admin/clone/runs/run-existing/messages?status=error"
            )

        self.assertEqual(200, response.status_code)
        payload = response.get_json()
        self.assertEqual(1, payload["total"])
        self.assertNotIn("count", payload)
        self.assertEqual(2, payload["items"][0]["source_message_id"])

    def test_clone_run_delete_requires_confirm(self) -> None:
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.delete(
                "/api/admin/clone/runs/run-existing",
                json={"confirm": "wrong"},
                headers={"X-CSRF-Token": csrf_token},
            )

        self.assertEqual(400, response.status_code)
        self.assertEqual("confirm 参数不匹配", response.get_json()["error"])
        self.assertEqual([], self.deleted_clone_run_ids)

    def test_clone_run_delete_starts_target_and_local_cleanup_job(self) -> None:
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.delete(
                "/api/admin/clone/runs/run-existing",
                json={"confirm": "DELETE-CLONE-RUN:run-existing"},
                headers={"X-CSRF-Token": csrf_token},
            )

        self.assertEqual(200, response.status_code)
        payload = response.get_json()
        self.assertEqual("clone_target_delete", payload["job"]["job_type"])
        self.assertTrue(payload["deletion"]["target_delete_requested"])
        self.assertEqual("run-existing", payload["deletion"]["run_id"])
        self.assertEqual([], self.deleted_clone_run_ids)
        self.assertEqual(1, len(self.started_target_delete_jobs))
        args, kwargs = self.started_target_delete_jobs[0]
        self.assertEqual(("job-clone-1",), args)
        self.assertEqual("run-existing", kwargs["clone_run"]["run_id"])
        self.assertEqual(777, kwargs["clone_run"]["target_chat_id"])

    def test_clone_run_delete_rejects_related_active_migration(self) -> None:
        self.created_clone_migrations.append(
            {
                "migration_id": "migration-active",
                "run_id": "run-existing",
                "job_id": "job-migration-active",
                "mode": "timeline_replay",
                "status": "running",
            }
        )
        self.job_statuses["job-migration-active"] = "running"

        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.delete(
                "/api/admin/clone/runs/run-existing",
                json={"confirm": "DELETE-CLONE-RUN:run-existing"},
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(409, response.status_code)
        payload = response.get_json()
        self.assertEqual("关联克隆任务仍在执行，不能删除本地记录", payload["error"])
        self.assertEqual("job-migration-active", payload["active_job"]["job_id"])
        self.assertEqual([], self.deleted_clone_run_ids)
        self.assertEqual([], self.started_target_delete_jobs)

    def test_clone_run_message_delete_starts_second_account_job(self) -> None:
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.post(
                "/api/admin/clone/runs/run-existing/delete-messages",
                json={"selection": "200-1000", "delete_delay_ms": 50},
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(200, response.status_code)
        payload = response.get_json()
        self.assertEqual("clone_message_delete", payload["job"]["job_type"])
        self.assertEqual("range", payload["deletion"]["mode"])
        self.assertEqual(801, payload["deletion"]["requested_count"])
        self.assertEqual(50, payload["deletion"]["delete_delay_ms"])
        self.assertEqual(1, len(self.started_message_delete_jobs))
        args, kwargs = self.started_message_delete_jobs[0]
        self.assertEqual(("job-clone-1",), args)
        self.assertEqual("run-existing", kwargs["clone_run"]["run_id"])
        self.assertEqual("range", kwargs["selection"].mode)
        self.assertEqual(200, kwargs["selection"].first_message_id)
        self.assertEqual(1000, kwargs["selection"].last_message_id)
        self.assertEqual(50, kwargs["delete_delay_ms"])

    def test_clone_run_message_delete_rejects_invalid_selection_before_job_creation(self) -> None:
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.post(
                "/api/admin/clone/runs/run-existing/delete-messages",
                json={"selection": "1000-200", "delete_delay_ms": 0},
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(400, response.status_code)
        self.assertIn("起始消息 ID", response.get_json()["error"])
        self.assertEqual([], self.started_message_delete_jobs)

    def test_clone_message_delete_page_accepts_authenticated_record_link(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get(
                "/admin/clone/runs/messages/delete?run_id=run-existing"
            )

        self.assertEqual(200, response.status_code)
        self.assertIn("删除局部克隆消息", response.get_data(as_text=True))

    def test_clone_run_target_message_count_reads_remote_snapshot(self) -> None:
        with (
            self._auth_config_patch(),
            patch(
                "tg_harvest.web.routes.clone.load_clone_target_message_count",
                return_value=4321,
            ) as load_count,
        ):
            self._login_admin()
            response = self.client.get(
                "/api/admin/clone/runs/run-existing/target-message-count"
            )

        self.assertEqual(200, response.status_code)
        self.assertEqual(4321, response.get_json()["message_count"])
        load_count.assert_called_once()
        clone_run = load_count.call_args.args[0]
        self.assertEqual(777, clone_run["target_chat_id"])

    def test_clone_run_plan_returns_null_when_absent(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/api/admin/clone/runs/run-existing/plan")

        self.assertEqual(200, response.status_code)
        self.assertIsNone(response.get_json()["plan"])

    def test_clone_run_plan_rejects_missing_run(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/api/admin/clone/runs/missing-run/plan")

        self.assertEqual(404, response.status_code)
        self.assertEqual("克隆运行记录不存在", response.get_json()["error"])

    def test_clone_run_api_returns_lightweight_record(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/api/admin/clone/runs/run-existing")

        self.assertEqual(200, response.status_code)
        payload = response.get_json()
        self.assertEqual("run-existing", payload["run"]["run_id"])
        self.assertEqual(
            "tg://resolve?domain=source",
            payload["run"]["source_telegram_app_link"],
        )

    def test_clone_run_migration_returns_null_when_absent(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/api/admin/clone/runs/run-existing/migration")

        self.assertEqual(200, response.status_code)
        payload = response.get_json()
        self.assertIsNone(payload["migration"])
        self.assertIsNone(payload["timeline_migration"])
        self.assertNotIn("text_migration", payload)
        self.assertNotIn("media_resolve_preflight", payload)
        self.assertNotIn("media_migration", payload)
        self.assertNotIn("preview", payload)
        self.assertNotIn("media_preview", payload)
        self.assertEqual("deferred", payload["timeline_preview"]["assessment_state"])
        self.assertFalse(payload["timeline_preview"]["can_migrate_timeline"])
        self.assertEqual(
            ["请先执行在线深度预检并生成迁移计划"],
            payload["timeline_preview"]["readiness_reasons"],
        )

    def test_clone_deep_preflight_rejects_missing_csrf_token(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.post(
                "/api/admin/clone/runs/run-existing/deep-preflight",
                json={},
            )

        self.assertEqual(403, response.status_code)
        self.assertTrue(response.get_json()["csrf_required"])

    def test_clone_deep_preflight_rejects_missing_run(self) -> None:
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.post(
                "/api/admin/clone/runs/missing-run/deep-preflight",
                json={},
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(404, response.status_code)
        self.assertEqual("克隆运行记录不存在", response.get_json()["error"])
        self.assertEqual([], self.started_deep_preflight_jobs)

    def test_clone_deep_preflight_creates_plan_and_starts_job(self) -> None:
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.post(
                "/api/admin/clone/runs/run-existing/deep-preflight",
                json={},
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(200, response.status_code)
        payload = response.get_json()
        self.assertEqual(
            {
                "job_id": "job-clone-1",
                "job_type": "clone_deep_preflight",
                "status": "queued",
            },
            payload["job"],
        )
        self.assertEqual(
            {
                "job_type": "clone_deep_preflight",
                "target_chat_id": 100,
                "target_label": "Source -> Source Existing Copy",
            },
            self.created_jobs[0],
        )
        self.assertEqual(1, len(self.created_clone_plans))
        self.assertEqual("job-clone-1", self.created_clone_plans[0]["plan_id"])
        self.assertEqual("run-existing", self.created_clone_plans[0]["run_id"])
        self.assertEqual(1, len(self.started_deep_preflight_jobs))
        args, kwargs = self.started_deep_preflight_jobs[0]
        self.assertEqual(("job-clone-1",), args)
        self.assertEqual("run-existing", kwargs["run_id"])
        self.assertEqual("job-clone-1", kwargs["plan_id"])
        self.assertIn(("job-clone-1", "已接收克隆深度预检请求"), self.logs)

    def test_clone_deep_preflight_marks_job_error_when_plan_creation_fails(self) -> None:
        self.clone_plan_create_error = RuntimeError("plan boom")
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.post(
                "/api/admin/clone/runs/run-existing/deep-preflight",
                json={},
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(400, response.status_code)
        self.assertEqual("plan boom", response.get_json()["error"])
        self.assertEqual([], self.started_deep_preflight_jobs)
        self.assertIn(("job-clone-1", "error"), self.status_updates)
        self.assertIn(
            ("job-clone-1", "克隆迁移计划创建失败，任务未启动"),
            self.logs,
        )

    def test_clone_timeline_migration_creates_migration_and_starts_job(self) -> None:
        self._create_done_plan()
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.post(
                "/api/admin/clone/runs/run-existing/migrate-timeline",
                json={"message_limit": 20, "send_delay_ms": 1000},
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(200, response.status_code)
        payload = response.get_json()
        self.assertEqual("deferred", payload["timeline_preview"]["assessment_state"])
        self.assertEqual(
            {
                "job_id": "job-clone-1",
                "job_type": "clone_timeline_migration",
                "status": "queued",
            },
            payload["job"],
        )
        self.assertEqual(1, len(self.created_clone_migrations))
        migration = self.created_clone_migrations[0]
        self.assertEqual("timeline_replay", migration["mode"])
        self.assertEqual("text:primary; media:primary", migration["target_write_account"])
        self.assertEqual(20, migration["requested_limit"])
        self.assertEqual(1000, migration["send_delay_ms"])
        self.assertEqual(0, migration["text_total"])
        self.assertEqual(0, migration["media_total"])
        self.assertEqual(0, migration["media_group_total"])
        self.assertEqual(1, len(self.started_timeline_migration_jobs))
        args, kwargs = self.started_timeline_migration_jobs[0]
        self.assertEqual(("job-clone-1",), args)
        self.assertEqual("run-existing", kwargs["run_id"])
        self.assertEqual("plan-ready", kwargs["plan_id"])
        self.assertEqual("job-clone-1", kwargs["migration_id"])
        self.assertEqual(20, kwargs["message_limit"])
        self.assertEqual(1000, kwargs["send_delay_ms"])

    def test_clone_timeline_migration_marks_job_error_when_record_creation_fails(
        self,
    ) -> None:
        self._create_done_plan()
        self.clone_migration_create_error = RuntimeError("migration boom")
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.post(
                "/api/admin/clone/runs/run-existing/migrate-timeline",
                json={},
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(400, response.status_code)
        self.assertEqual("migration boom", response.get_json()["error"])
        self.assertEqual([], self.started_timeline_migration_jobs)
        self.assertIn(("job-clone-1", "error"), self.status_updates)
        self.assertIn(
            ("job-clone-1", "完整时间线迁移记录创建失败，任务未启动"),
            self.logs,
        )

    def test_clone_timeline_migration_allows_ready_relay_plan(self) -> None:
        self._create_relay_done_plan()
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.post(
                "/api/admin/clone/runs/run-existing/migrate-timeline",
                json={},
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(200, response.status_code)
        self.assertEqual(1, len(self.created_clone_migrations))
        migration = self.created_clone_migrations[0]
        self.assertEqual("timeline_replay", migration["mode"])
        self.assertEqual(
            "text:secondary; media:primary->relay->secondary",
            migration["target_write_account"],
        )
        self.assertEqual(1, len(self.started_timeline_migration_jobs))

    def test_clone_timeline_migration_defers_incomplete_relay_check_to_job(self) -> None:
        plan = self._create_relay_done_plan()
        plan["capabilities"]["media_relay"]["target_account"] = ""
        plan["plan"]["media_relay"]["target_account"] = ""
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.post(
                "/api/admin/clone/runs/run-existing/migrate-timeline",
                json={},
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(200, response.status_code)
        self.assertEqual(1, len(self.created_jobs))
        self.assertEqual(1, len(self.created_clone_migrations))
        self.assertEqual(1, len(self.started_timeline_migration_jobs))

    def test_clone_timeline_migration_defers_no_remaining_check_to_job(self) -> None:
        self._create_done_plan()
        self.clone_timeline_preview_override = {
            "timeline_remaining": 0,
            "text_remaining": 0,
            "media_remaining": 0,
        }
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.post(
                "/api/admin/clone/runs/run-existing/migrate-timeline",
                json={},
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(200, response.status_code)
        self.assertEqual(1, len(self.created_jobs))
        self.assertEqual(1, len(self.created_clone_migrations))
        self.assertEqual(1, len(self.started_timeline_migration_jobs))

    def test_clone_migration_payload_includes_timeline_preview(self) -> None:
        self._create_done_plan()
        self.created_clone_migrations.append(
            {
                "migration_id": "migration-timeline",
                "run_id": "run-existing",
                "plan_id": "plan-ready",
                "job_id": "job-timeline",
                "mode": "timeline_replay",
                "status": "done",
                "phase": "done",
                "target_chat_id": 777,
                "target_title": "Source Backup",
                "target_write_account": "text:primary; media:primary",
                "requested_limit": 0,
                "send_delay_ms": 0,
                "text_total": 8,
                "text_sent": 6,
                "text_skipped": 0,
                "text_failed": 0,
                "media_total": 6,
                "media_sent": 5,
                "media_skipped": 0,
                "media_failed": 0,
                "media_group_total": 3,
                "media_group_sent": 1,
                "media_group_skipped": 0,
                "media_group_failed": 0,
                "plan": {"plan_id": "plan-ready"},
                "error_message": "",
                "created_at": "2026-06-18T00:00:00+00:00",
                "updated_at": "2026-06-18T00:00:00+00:00",
                "completed_at": "2026-06-18T00:00:00+00:00",
            }
        )
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.get(
                "/api/admin/clone/runs/run-existing/migration",
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(200, response.status_code)
        payload = response.get_json()
        self.assertIn("timeline_preview", payload)
        self.assertIn("timeline_migration", payload)
        self.assertIn("task_report", payload)
        self.assertIn("group_progress", payload)
        self.assertEqual("timeline_replay", payload["timeline_migration"]["mode"])
        self.assertEqual("deferred", payload["timeline_preview"]["assessment_state"])

    def test_clone_migration_payload_keeps_task_report_and_group_progress_separate(
        self,
    ) -> None:
        self._create_done_plan()
        self.created_clone_migrations.append(
            {
                "migration_id": "migration-latest",
                "run_id": "run-existing",
                "plan_id": "plan-ready",
                "job_id": "job-latest",
                "mode": "timeline_replay",
                "status": "done",
                "phase": "limited_done",
                "requested_limit": 200,
                "text_total": 1,
                "text_sent": 0,
                "text_skipped": 0,
                "text_failed": 0,
                "media_total": 14129,
                "media_sent": 200,
                "media_skipped": 0,
                "media_failed": 0,
                "media_group_total": 0,
                "media_group_sent": 0,
                "media_group_skipped": 0,
                "media_group_failed": 0,
                "updated_at": "2026-06-18T00:00:00+00:00",
            }
        )

        with self._auth_config_patch():
            self._login_admin()
            response = self.client.get("/api/admin/clone/runs/run-existing/migration")

        self.assertEqual(200, response.status_code)
        payload = response.get_json()
        self.assertEqual(200, payload["task_report"]["processed"])
        self.assertEqual(200, payload["task_report"]["media"]["sent"])
        self.assertEqual(200, payload["task_report"]["requested_limit"])
        self.assertEqual(14130, payload["group_progress"]["messages_total"])
        self.assertEqual("verified", payload["group_progress"]["assessment_state"])

    def test_clone_preflight_rejects_missing_csrf_token(self) -> None:
        with self._auth_config_patch():
            self._login_admin()
            response = self.client.post(
                "/api/admin/clone/preflight",
                json={"chat_id": 100},
            )

        self.assertEqual(403, response.status_code)
        self.assertTrue(response.get_json()["csrf_required"])

    def test_clone_preflight_returns_report(self) -> None:
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.post(
                "/api/admin/clone/preflight",
                json={"chat_id": 100},
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(200, response.status_code)
        report = response.get_json()["report"]
        self.assertEqual("CLONE:STRUCTURE:100", report["confirm"])
        self.assertEqual("Source 副本", report["target"]["default_title"])

    def test_clone_job_requires_matching_confirmation(self) -> None:
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.post(
                "/api/admin/clone/jobs",
                json={"chat_id": 100, "confirm": "bad"},
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(400, response.status_code)
        self.assertEqual("confirm 参数不匹配", response.get_json()["error"])
        self.assertEqual([], self.started_jobs)

    def test_clone_job_rejects_blocked_second_account(self) -> None:
        self.secondary_ready = False
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.post(
                "/api/admin/clone/jobs",
                json={"chat_id": 100, "confirm": "CLONE:STRUCTURE:100"},
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(400, response.status_code)
        self.assertEqual("第二账号未就绪，不能开始结构克隆", response.get_json()["error"])
        self.assertEqual([], self.started_jobs)

    def test_clone_job_starts_structure_clone_thread(self) -> None:
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.post(
                "/api/admin/clone/jobs",
                json={
                    "chat_id": 100,
                    "target_title": "Source Backup",
                    "target_kind": "megagroup",
                    "confirm": "CLONE:STRUCTURE:100",
                },
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(200, response.status_code)
        self.assertEqual(
            {
                "job_id": "job-clone-1",
                "job_type": "clone_structure",
                "status": "queued",
            },
            response.get_json()["job"],
        )
        self.assertEqual(
            {
                "job_type": "clone_structure",
                "target_chat_id": 100,
                "target_label": "Source Backup",
            },
            self.created_jobs[0],
        )
        self.assertEqual(1, len(self.started_jobs))
        self.assertEqual(1, len(self.created_clone_runs))
        self.assertEqual("job-clone-1", self.created_clone_runs[0]["run_id"])
        self.assertEqual("Source Backup", self.created_clone_runs[0]["target_title"])
        payload = response.get_json()
        self.assertEqual("job-clone-1", payload["clone_run"]["run_id"])
        self.assertEqual("queued", payload["clone_run"]["status"])
        args, kwargs = self.started_jobs[0]
        self.assertEqual(("job-clone-1",), args)
        self.assertEqual("job-clone-1", kwargs["clone_run_id"])
        self.assertEqual(100, kwargs["source_chat_id"])
        self.assertEqual("Source Backup", kwargs["target_title"])
        self.assertEqual("megagroup", kwargs["target_kind"])
        self.assertIn(("job-clone-1", "已接收结构克隆请求"), self.logs)

    def test_clone_job_marks_job_error_when_thread_start_fails(self) -> None:
        self.clone_structure_start_error = RuntimeError("thread unavailable")
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.post(
                "/api/admin/clone/jobs",
                json={
                    "chat_id": 100,
                    "target_title": "Source Backup",
                    "target_kind": "megagroup",
                    "confirm": "CLONE:STRUCTURE:100",
                },
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(500, response.status_code)
        self.assertEqual("克隆任务启动失败", response.get_json()["error"])
        self.assertEqual([], self.started_jobs)
        self.assertEqual(1, len(self.created_clone_runs))
        self.assertIn(("job-clone-1", "error"), self.status_updates)
        self.assertIn(
            ("job-clone-1", "克隆任务启动失败，任务未启动"),
            self.logs,
        )

    def test_clone_job_marks_job_error_when_run_creation_fails(self) -> None:
        self.clone_run_create_error = RuntimeError("run boom")
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.post(
                "/api/admin/clone/jobs",
                json={
                    "chat_id": 100,
                    "target_title": "Source Backup",
                    "target_kind": "megagroup",
                    "confirm": "CLONE:STRUCTURE:100",
                },
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(400, response.status_code)
        self.assertEqual("run boom", response.get_json()["error"])
        self.assertEqual([], self.started_jobs)
        self.assertIn(("job-clone-1", "error"), self.status_updates)
        self.assertIn(
            ("job-clone-1", "克隆运行记录创建失败，任务未启动"),
            self.logs,
        )

    def test_clone_job_rejects_invalid_target_kind(self) -> None:
        with self._auth_config_patch():
            csrf_token = self._login_admin()
            response = self.client.post(
                "/api/admin/clone/jobs",
                json={
                    "chat_id": 100,
                    "target_kind": "invalid",
                    "confirm": "CLONE:STRUCTURE:100",
                },
                headers={auth_module.ADMIN_CSRF_HEADER: csrf_token},
            )

        self.assertEqual(400, response.status_code)
        self.assertEqual("target_kind 参数必须为 channel 或 megagroup", response.get_json()["error"])
        self.assertEqual([], self.started_jobs)


if __name__ == "__main__":
    unittest.main()
