import logging
import os
import sqlite3
import threading
from datetime import timedelta
from pathlib import Path

from flask import Flask, request

from tg_harvest.admin_jobs.channel_inventory import (
    _admin_start_absent_chats_scan_job_thread,
    _admin_start_missing_chats_scan_job_thread,
    _admin_start_restricted_chats_scan_job_thread,
)
from tg_harvest.admin_jobs.clone import _admin_start_clone_structure_job_thread
from tg_harvest.admin_jobs.clone_preflight import (
    _admin_start_clone_deep_preflight_job_thread,
)
from tg_harvest.admin_jobs.clone_timeline_migration import (
    _admin_start_clone_timeline_migration_job_thread,
)
from tg_harvest.admin_jobs.core import (
    _admin_create_chat_job_if_absent,
    _admin_get_active_job,
    _admin_has_any_active_job,
    _admin_job_append_log,
    _admin_job_create,
    _admin_job_get_logs,
    _admin_job_get_snapshot,
    _admin_job_set_status,
    _admin_make_job_log_handler,
    _admin_recover_interrupted_jobs,
    _admin_request_job_stop,
    _admin_try_create_exclusive_job,
)
from tg_harvest.admin_jobs.recovery import (
    _admin_start_recovery_restore_job_thread,
    _admin_start_recovery_scan_job_thread,
)
from tg_harvest.admin_jobs.runners import (
    _admin_start_cleanup_empty_job_thread,
    _admin_start_cleanup_job_thread,
    _admin_start_delete_empty_chats_job_thread,
    _admin_start_delete_job_thread,
    _admin_start_harvest_job_thread,
    _admin_start_update_job_thread,
)
from tg_harvest.admin_jobs.runtime import configure_admin_job_runtime
from tg_harvest.app.admin_payloads import (
    build_admin_chats_payload,
    build_admin_stats_payload,
    get_admin_chat_brief,
    parse_admin_chat_id,
)
from tg_harvest.app.routes_registry import register_all_routes
from tg_harvest.app.services import (
    AdminRouteServices,
    ChannelRouteServices,
    CloneRouteServices,
    RecoveryRouteServices,
    RouteRegistryServices,
)
from tg_harvest.config import CFG
from tg_harvest.domain.meta_payload import _build_meta_payload
from tg_harvest.ingest.parse import setup_logging
from tg_harvest.search.maintenance import (
    configure_message_search_maintenance,
    schedule_message_search_maintenance,
)
from tg_harvest.search.params import _parse_search_params
from tg_harvest.search.result_mapper import _map_search_items
from tg_harvest.search.service import _search_payload_service
from tg_harvest.storage.access import FROM_SQL, has_fts
from tg_harvest.storage.access import get_conn as runtime_get_conn
from tg_harvest.storage.channel_management import (
    list_absent_chat_scan_results,
    list_database_channels,
    list_missing_chat_scan_results,
    list_restricted_chat_scan_results,
)
from tg_harvest.storage.clone import (
    build_clone_preflight_report,
    build_clone_timeline_replay_preview,
    count_clone_message_mappings,
    count_clone_runs,
    create_clone_migration,
    create_clone_plan,
    create_clone_run,
    delete_clone_run,
    list_clone_message_mappings,
    list_clone_runs,
    list_clone_source_chats,
    load_clone_run,
    load_clone_run_detail,
    load_latest_clone_migration,
    load_latest_clone_plan,
)
from tg_harvest.storage.connection import connect_db, ensure_configured_db
from tg_harvest.storage.recovery import (
    build_recovery_overview,
    list_recovery_chat_candidates,
)
from tg_harvest.web.telegram_links import build_telegram_chat_link_bundle

logger = logging.getLogger(__name__)
PROJECT_ROOT = Path(__file__).resolve().parents[2]


setup_logging()

DB_PATH = Path(CFG.db_name)
PAGE_SIZE = 100
MAX_COUNT = 50000000

ADMIN_HARVEST_TARGET_MAX_LEN = 300
ADMIN_CLEANUP_KEYWORD_MAX_LEN = 120
_TRUE_ENV_VALUES = {"1", "true", "yes", "on"}
_FALSE_ENV_VALUES = {"0", "false", "no", "off"}
_PRODUCTION_ENV_VALUES = {"prod", "production"}
_DB_FREE_ENDPOINTS = frozenset(
    {
        None,
        "static",
        "index",
        "admin_login_page",
        "admin_manage_page",
        "admin_channels_page",
        "admin_clone_page",
        "admin_clone_runs_manage_page",
        "admin_recovery_page",
        "chat_context_page",
        "api_auth_check",
        "api_auth_login",
        "api_auth_logout",
    }
)


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    normalized = raw.strip().lower()
    if normalized in _TRUE_ENV_VALUES:
        return True
    if normalized in _FALSE_ENV_VALUES:
        return False
    return bool(default)


def _is_production_runtime() -> bool:
    if _env_flag("TG_REQUIRE_SECURE_CONFIG", False):
        return True
    for name in ("TG_ENV", "APP_ENV", "FLASK_ENV", "ENV"):
        if os.getenv(name, "").strip().lower() in _PRODUCTION_ENV_VALUES:
            return True
    return False


def _configured_flask_secret_key() -> str:
    return os.getenv("FLASK_SECRET_KEY", "").strip()


def _validate_secure_runtime_config(*, production: bool) -> None:
    if not production:
        return

    missing = []
    if not _configured_flask_secret_key():
        missing.append("FLASK_SECRET_KEY")
    if not str(CFG.admin_password or "").strip():
        missing.append("TG_ADMIN_PASSWORD")
    if missing:
        raise RuntimeError(
            "生产环境缺少必需安全配置: " + ", ".join(sorted(missing))
        )


def _build_flask_secret_key(*, production: bool) -> str:
    configured = _configured_flask_secret_key()
    if configured:
        return configured
    _validate_secure_runtime_config(production=production)
    logger.warning(
        "FLASK_SECRET_KEY 未配置，当前进程将使用临时随机密钥；"
        "生产环境请显式设置以保证登录态稳定"
    )
    return os.urandom(32).hex()


def _should_use_secure_session_cookie(*, production: bool) -> bool:
    raw = os.getenv("TG_SESSION_COOKIE_SECURE")
    if raw is not None:
        return _env_flag("TG_SESSION_COOKIE_SECURE", production)
    return bool(production)


def _connect_runtime_db(
    db_path: str, *, cache_mb: int, mmap_mb: int
) -> tuple[sqlite3.Connection, object]:
    return connect_db(
        db_path,
        cache_mb=cache_mb,
        mmap_mb=mmap_mb,
        set_journal_mode=False,
    )


def get_conn() -> sqlite3.Connection:
    return runtime_get_conn(
        db_path=DB_PATH,
        connect_db_fn=_connect_runtime_db,
        cache_mb=CFG.sqlite_cache_mb,
        mmap_mb=CFG.sqlite_mmap_mb,
    )


def _ensure_db() -> None:
    conn, _ = ensure_configured_db(cfg=CFG)
    conn.close()
    recovered_jobs = _admin_recover_interrupted_jobs()
    if recovered_jobs > 0:
        logger.warning(
            "检测到 %s 个后台任务因进程重启中断，已统一标记为失败",
            recovered_jobs,
        )
    configure_message_search_maintenance(get_conn)
    schedule_message_search_maintenance()


def _ensure_runtime_db(app: Flask) -> None:
    if app.extensions.get("tg_db_ready"):
        return

    lock = app.extensions["tg_db_ready_lock"]
    with lock:
        if app.extensions.get("tg_db_ready"):
            return
        _ensure_db()
        app.extensions["tg_db_ready"] = True


def _request_requires_runtime_db() -> bool:
    if str(request.path or "").startswith("/api/admin/"):
        from tg_harvest.web.auth import is_authenticated

        return is_authenticated()
    return request.endpoint not in _DB_FREE_ENDPOINTS


def _build_admin_route_services() -> AdminRouteServices:
    return AdminRouteServices(
        logger=logger,
        cfg=CFG,
        get_conn_fn=get_conn,
        parse_admin_chat_id_fn=parse_admin_chat_id,
        build_admin_chats_payload_fn=build_admin_chats_payload,
        build_admin_stats_payload_fn=build_admin_stats_payload,
        admin_get_chat_brief_fn=get_admin_chat_brief,
        admin_job_get_snapshot_fn=_admin_job_get_snapshot,
        admin_job_get_logs_fn=_admin_job_get_logs,
        admin_get_active_job_fn=_admin_get_active_job,
        admin_request_job_stop_fn=_admin_request_job_stop,
        admin_has_any_active_job_fn=_admin_has_any_active_job,
        admin_try_create_exclusive_job_fn=_admin_try_create_exclusive_job,
        admin_create_chat_job_if_absent_fn=_admin_create_chat_job_if_absent,
        admin_job_create_fn=_admin_job_create,
        admin_job_append_log_fn=_admin_job_append_log,
        admin_start_harvest_job_thread_fn=_admin_start_harvest_job_thread,
        admin_start_update_job_thread_fn=_admin_start_update_job_thread,
        admin_start_delete_job_thread_fn=_admin_start_delete_job_thread,
        admin_start_delete_empty_chats_job_thread_fn=(
            _admin_start_delete_empty_chats_job_thread
        ),
        admin_start_cleanup_job_thread_fn=_admin_start_cleanup_job_thread,
        admin_start_cleanup_empty_job_thread_fn=_admin_start_cleanup_empty_job_thread,
        admin_make_job_log_handler_fn=_admin_make_job_log_handler,
        admin_job_set_status_fn=_admin_job_set_status,
        admin_harvest_target_max_len=ADMIN_HARVEST_TARGET_MAX_LEN,
        admin_cleanup_keyword_max_len=ADMIN_CLEANUP_KEYWORD_MAX_LEN,
    )


def _shared_route_service_kwargs(admin_services: AdminRouteServices) -> dict[str, object]:
    return {
        "logger": logger,
        "get_conn_fn": get_conn,
        "cfg": admin_services.cfg,
    }


def _shared_admin_job_route_kwargs(admin_services: AdminRouteServices) -> dict[str, object]:
    return {
        "admin_try_create_exclusive_job_fn": (
            admin_services.admin_try_create_exclusive_job_fn
        ),
        "admin_job_get_snapshot_fn": admin_services.admin_job_get_snapshot_fn,
        "admin_job_append_log_fn": admin_services.admin_job_append_log_fn,
        "admin_job_set_status_fn": admin_services.admin_job_set_status_fn,
    }


def _shared_admin_harvest_route_kwargs(
    admin_services: AdminRouteServices,
) -> dict[str, object]:
    return {
        "admin_start_harvest_job_thread_fn": (
            admin_services.admin_start_harvest_job_thread_fn
        ),
        "admin_make_job_log_handler_fn": (
            admin_services.admin_make_job_log_handler_fn
        ),
        "admin_harvest_target_max_len": admin_services.admin_harvest_target_max_len,
    }


def _build_route_services() -> RouteRegistryServices:
    admin_services = _build_admin_route_services()
    clone_services = CloneRouteServices(
        **_shared_route_service_kwargs(admin_services),
        list_clone_source_chats_fn=list_clone_source_chats,
        build_clone_preflight_report_fn=build_clone_preflight_report,
        create_clone_run_fn=create_clone_run,
        load_clone_run_fn=load_clone_run,
        list_clone_runs_fn=list_clone_runs,
        count_clone_runs_fn=count_clone_runs,
        load_clone_run_detail_fn=load_clone_run_detail,
        list_clone_message_mappings_fn=list_clone_message_mappings,
        count_clone_message_mappings_fn=count_clone_message_mappings,
        delete_clone_run_fn=delete_clone_run,
        create_clone_plan_fn=create_clone_plan,
        load_latest_clone_plan_fn=load_latest_clone_plan,
        create_clone_migration_fn=create_clone_migration,
        load_latest_clone_migration_fn=load_latest_clone_migration,
        build_clone_timeline_replay_preview_fn=build_clone_timeline_replay_preview,
        build_telegram_chat_link_bundle_fn=build_telegram_chat_link_bundle,
        **_shared_admin_job_route_kwargs(admin_services),
        admin_start_clone_structure_job_thread_fn=_admin_start_clone_structure_job_thread,
        admin_start_clone_deep_preflight_job_thread_fn=(
            _admin_start_clone_deep_preflight_job_thread
        ),
        admin_start_clone_timeline_migration_job_thread_fn=(
            _admin_start_clone_timeline_migration_job_thread
        ),
    )
    channel_services = ChannelRouteServices(
        **_shared_route_service_kwargs(admin_services),
        list_database_channels_fn=list_database_channels,
        list_missing_chat_scan_results_fn=list_missing_chat_scan_results,
        list_absent_chat_scan_results_fn=list_absent_chat_scan_results,
        list_restricted_chat_scan_results_fn=list_restricted_chat_scan_results,
        build_telegram_chat_link_bundle_fn=build_telegram_chat_link_bundle,
        **_shared_admin_job_route_kwargs(admin_services),
        admin_start_missing_chats_scan_job_thread_fn=(
            _admin_start_missing_chats_scan_job_thread
        ),
        admin_start_absent_chats_scan_job_thread_fn=(
            _admin_start_absent_chats_scan_job_thread
        ),
        admin_start_restricted_chats_scan_job_thread_fn=(
            _admin_start_restricted_chats_scan_job_thread
        ),
    )
    recovery_services = RecoveryRouteServices(
        **_shared_route_service_kwargs(admin_services),
        list_recovery_chat_candidates_fn=list_recovery_chat_candidates,
        build_recovery_overview_fn=build_recovery_overview,
        build_telegram_chat_link_bundle_fn=build_telegram_chat_link_bundle,
        **_shared_admin_job_route_kwargs(admin_services),
        **_shared_admin_harvest_route_kwargs(admin_services),
        admin_start_recovery_scan_job_thread_fn=_admin_start_recovery_scan_job_thread,
        admin_start_recovery_restore_job_thread_fn=(
            _admin_start_recovery_restore_job_thread
        ),
    )
    return RouteRegistryServices(
        page_size=PAGE_SIZE,
        logger=logger,
        get_conn_fn=get_conn,
        build_meta_payload_fn=_build_meta_payload,
        has_fts_fn=has_fts,
        from_sql=FROM_SQL,
        max_count=MAX_COUNT,
        map_search_items_fn=_map_search_items,
        parse_search_params_fn=_parse_search_params,
        search_payload_service_fn=_search_payload_service,
        admin=admin_services,
        channels=channel_services,
        recovery=recovery_services,
        clone=clone_services,
    )


def create_app(*, init_db: bool = False) -> Flask:
    production = _is_production_runtime()
    _validate_secure_runtime_config(production=production)
    configure_admin_job_runtime()
    if init_db:
        _ensure_db()
    app = Flask(
        "tg_harvest",
        root_path=str(PROJECT_ROOT),
        template_folder=str(PROJECT_ROOT / "templates"),
        static_folder=str(PROJECT_ROOT / "static"),
    )
    app.secret_key = _build_flask_secret_key(production=production)
    app.extensions["tg_db_ready"] = bool(init_db)
    app.extensions["tg_db_ready_lock"] = threading.Lock()

    # 映射：将配置文件中的秒数注入为 Flask Session 的持久生命周期
    app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(
        seconds=CFG.admin_session_expiry
    )
    app.config["TG_DB_PATH"] = str(DB_PATH)
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = _should_use_secure_session_cookie(
        production=production
    )

    @app.before_request
    def _before_request_ensure_db() -> None:
        if not _request_requires_runtime_db():
            return
        _ensure_runtime_db(app)

    @app.after_request
    def _apply_security_headers(response):
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self'; "
            "style-src 'self'; "
            "img-src 'self' data:; "
            "object-src 'none'; "
            "base-uri 'self'; "
            "frame-ancestors 'none'"
        )
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        return response

    register_all_routes(app, services=_build_route_services())
    return app

def run_web_server(*, host: str = "0.0.0.0", port: int = 8890, debug: bool = False) -> None:
    app = create_app(init_db=True)
    app.extensions["tg_db_ready"] = True
    app.run(host=host, port=port, debug=debug)
