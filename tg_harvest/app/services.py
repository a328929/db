from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CloneRouteServices:
    logger: Any
    get_conn_fn: Any
    cfg: Any
    list_clone_source_chats_fn: Any
    build_clone_preflight_report_fn: Any
    create_clone_run_fn: Any
    load_clone_run_fn: Any
    list_clone_runs_fn: Any
    count_clone_runs_fn: Any
    load_clone_run_detail_fn: Any
    list_clone_message_mappings_fn: Any
    count_clone_message_mappings_fn: Any
    delete_clone_run_fn: Any
    create_clone_plan_fn: Any
    load_latest_clone_plan_fn: Any
    create_clone_migration_fn: Any
    load_latest_clone_migration_fn: Any
    build_clone_timeline_replay_preview_fn: Any
    build_telegram_chat_link_bundle_fn: Any
    admin_try_create_exclusive_job_fn: Any
    admin_job_get_snapshot_fn: Any
    admin_job_append_log_fn: Any
    admin_job_set_status_fn: Any
    admin_start_clone_structure_job_thread_fn: Any
    admin_start_clone_deep_preflight_job_thread_fn: Any
    admin_start_clone_timeline_migration_job_thread_fn: Any


@dataclass(frozen=True)
class ChannelRouteServices:
    logger: Any
    get_conn_fn: Any
    cfg: Any
    list_database_channels_fn: Any
    list_missing_chat_scan_results_fn: Any
    list_absent_chat_scan_results_fn: Any
    list_restricted_chat_scan_results_fn: Any
    build_telegram_chat_link_bundle_fn: Any
    admin_try_create_exclusive_job_fn: Any
    admin_job_get_snapshot_fn: Any
    admin_job_append_log_fn: Any
    admin_job_set_status_fn: Any
    admin_start_missing_chats_scan_job_thread_fn: Any
    admin_start_absent_chats_scan_job_thread_fn: Any
    admin_start_restricted_chats_scan_job_thread_fn: Any


@dataclass(frozen=True)
class RecoveryRouteServices:
    logger: Any
    get_conn_fn: Any
    cfg: Any
    list_recovery_chat_candidates_fn: Any
    build_recovery_overview_fn: Any
    build_telegram_chat_link_bundle_fn: Any
    admin_try_create_exclusive_job_fn: Any
    admin_job_get_snapshot_fn: Any
    admin_job_append_log_fn: Any
    admin_job_set_status_fn: Any
    admin_start_harvest_job_thread_fn: Any
    admin_make_job_log_handler_fn: Any
    admin_harvest_target_max_len: int
    admin_start_recovery_scan_job_thread_fn: Any
    admin_start_recovery_restore_job_thread_fn: Any


@dataclass(frozen=True)
class AdminRouteServices:
    logger: Any
    cfg: Any
    get_conn_fn: Any
    parse_admin_chat_id_fn: Any
    build_admin_chats_payload_fn: Any
    build_admin_stats_payload_fn: Any
    build_admin_sync_stats_payload_fn: Any
    build_admin_sync_live_messages_payload_fn: Any
    get_sync_health_snapshot_fn: Any
    trigger_sync_remediation_fn: Any
    admin_get_chat_brief_fn: Any
    admin_job_get_snapshot_fn: Any
    admin_job_get_logs_fn: Any
    admin_get_active_job_fn: Any
    admin_request_job_stop_fn: Any
    admin_has_any_active_job_fn: Any
    admin_try_create_exclusive_job_fn: Any
    admin_create_chat_job_if_absent_fn: Any
    admin_job_create_fn: Any
    admin_job_append_log_fn: Any
    admin_start_harvest_job_thread_fn: Any
    admin_start_update_job_thread_fn: Any
    admin_start_delete_job_thread_fn: Any
    admin_start_delete_empty_chats_job_thread_fn: Any
    admin_start_cleanup_job_thread_fn: Any
    admin_start_cleanup_empty_job_thread_fn: Any
    admin_make_job_log_handler_fn: Any
    admin_job_set_status_fn: Any
    admin_harvest_target_max_len: int
    admin_cleanup_keyword_max_len: int


@dataclass(frozen=True)
class RouteRegistryServices:
    page_size: int
    logger: Any
    get_conn_fn: Any
    build_meta_payload_fn: Any
    has_fts_fn: Any
    from_sql: str
    max_count: int
    map_search_items_fn: Any
    parse_search_params_fn: Any
    search_payload_service_fn: Any
    admin: AdminRouteServices
    channels: ChannelRouteServices
    recovery: RecoveryRouteServices
    clone: CloneRouteServices
