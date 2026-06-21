from functools import partial

from tg_harvest.app.services import RouteRegistryServices
from tg_harvest.web.auth import register_auth_routes
from tg_harvest.web.routes.admin import register_admin_routes
from tg_harvest.web.routes.channels import register_channel_routes
from tg_harvest.web.routes.clone import register_clone_routes
from tg_harvest.web.routes.context import register_context_routes
from tg_harvest.web.routes.meta import register_meta_routes
from tg_harvest.web.routes.open_telegram import register_open_telegram_routes
from tg_harvest.web.routes.pages import register_page_routes
from tg_harvest.web.routes.recovery import register_recovery_routes
from tg_harvest.web.routes.search import register_search_routes


def register_all_routes(
    app,
    *,
    services: RouteRegistryServices,
) -> None:
    register_auth_routes(app)

    register_page_routes(
        app,
        page_size=services.page_size,
    )

    register_open_telegram_routes(
        app,
        logger=services.logger,
        get_conn_fn=services.get_conn_fn,
    )

    register_context_routes(
        app,
        logger=services.logger,
        get_conn_fn=services.get_conn_fn,
        from_sql=services.from_sql,
        map_search_items_fn=services.map_search_items_fn,
    )

    register_meta_routes(
        app,
        logger=services.logger,
        get_conn_fn=services.get_conn_fn,
        build_meta_payload_fn=partial(
            services.build_meta_payload_fn,
            page_size=services.page_size,
        ),
    )

    register_search_routes(
        app,
        logger=services.logger,
        get_conn_fn=services.get_conn_fn,
        has_fts_fn=services.has_fts_fn,
        from_sql=services.from_sql,
        page_size=services.page_size,
        max_count=services.max_count,
        map_search_items_fn=services.map_search_items_fn,
        parse_search_params_fn=services.parse_search_params_fn,
        search_payload_service_fn=services.search_payload_service_fn,
    )

    register_admin_routes(
        app,
        services=services.admin,
    )

    register_channel_routes(
        app,
        logger=services.logger,
        get_conn_fn=services.get_conn_fn,
        cfg=services.admin.cfg,
        list_database_channels_fn=services.list_database_channels_fn,
        list_missing_chat_scan_results_fn=services.list_missing_chat_scan_results_fn,
        list_absent_chat_scan_results_fn=services.list_absent_chat_scan_results_fn,
        list_restricted_chat_scan_results_fn=(
            services.list_restricted_chat_scan_results_fn
        ),
        build_telegram_chat_link_bundle_fn=services.build_telegram_chat_link_bundle_fn,
        admin_try_create_exclusive_job_fn=services.admin.admin_try_create_exclusive_job_fn,
        admin_job_get_snapshot_fn=services.admin.admin_job_get_snapshot_fn,
        admin_job_append_log_fn=services.admin.admin_job_append_log_fn,
        admin_job_set_status_fn=services.admin.admin_job_set_status_fn,
        admin_start_missing_chats_scan_job_thread_fn=(
            services.admin_start_missing_chats_scan_job_thread_fn
        ),
        admin_start_absent_chats_scan_job_thread_fn=(
            services.admin_start_absent_chats_scan_job_thread_fn
        ),
        admin_start_restricted_chats_scan_job_thread_fn=(
            services.admin_start_restricted_chats_scan_job_thread_fn
        ),
    )

    register_recovery_routes(
        app,
        logger=services.logger,
        get_conn_fn=services.get_conn_fn,
        cfg=services.admin.cfg,
        list_recovery_chat_candidates_fn=services.list_recovery_chat_candidates_fn,
        build_recovery_overview_fn=services.build_recovery_overview_fn,
        build_telegram_chat_link_bundle_fn=services.build_telegram_chat_link_bundle_fn,
        admin_try_create_exclusive_job_fn=services.admin.admin_try_create_exclusive_job_fn,
        admin_job_get_snapshot_fn=services.admin.admin_job_get_snapshot_fn,
        admin_job_append_log_fn=services.admin.admin_job_append_log_fn,
        admin_job_set_status_fn=services.admin.admin_job_set_status_fn,
        admin_start_recovery_scan_job_thread_fn=(
            services.admin_start_recovery_scan_job_thread_fn
        ),
        admin_start_recovery_restore_job_thread_fn=(
            services.admin_start_recovery_restore_job_thread_fn
        ),
    )

    register_clone_routes(
        app,
        logger=services.logger,
        get_conn_fn=services.get_conn_fn,
        cfg=services.admin.cfg,
        list_clone_source_chats_fn=services.list_clone_source_chats_fn,
        build_clone_preflight_report_fn=services.build_clone_preflight_report_fn,
        create_clone_run_fn=services.create_clone_run_fn,
        load_clone_run_fn=services.load_clone_run_fn,
        list_clone_runs_fn=services.list_clone_runs_fn,
        count_clone_runs_fn=services.count_clone_runs_fn,
        load_clone_run_detail_fn=services.load_clone_run_detail_fn,
        list_clone_message_mappings_fn=services.list_clone_message_mappings_fn,
        count_clone_message_mappings_fn=(
            services.count_clone_message_mappings_fn
        ),
        delete_clone_run_fn=services.delete_clone_run_fn,
        create_clone_plan_fn=services.create_clone_plan_fn,
        load_latest_clone_plan_fn=services.load_latest_clone_plan_fn,
        create_clone_migration_fn=services.create_clone_migration_fn,
        load_latest_clone_migration_fn=services.load_latest_clone_migration_fn,
        build_clone_timeline_replay_preview_fn=(
            services.build_clone_timeline_replay_preview_fn
        ),
        build_telegram_chat_link_bundle_fn=services.build_telegram_chat_link_bundle_fn,
        admin_try_create_exclusive_job_fn=services.admin.admin_try_create_exclusive_job_fn,
        admin_job_get_snapshot_fn=services.admin.admin_job_get_snapshot_fn,
        admin_job_append_log_fn=services.admin.admin_job_append_log_fn,
        admin_job_set_status_fn=services.admin.admin_job_set_status_fn,
        admin_start_clone_structure_job_thread_fn=(
            services.admin_start_clone_structure_job_thread_fn
        ),
        admin_start_clone_deep_preflight_job_thread_fn=(
            services.admin_start_clone_deep_preflight_job_thread_fn
        ),
        admin_start_clone_timeline_migration_job_thread_fn=(
            services.admin_start_clone_timeline_migration_job_thread_fn
        ),
    )
