from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AdminRouteServices:
    logger: Any
    cfg: Any
    get_conn_fn: Any
    parse_admin_chat_id_fn: Any
    build_admin_chats_payload_fn: Any
    build_admin_stats_payload_fn: Any
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
    list_database_channels_fn: Any
    list_missing_chat_scan_results_fn: Any
    list_absent_chat_scan_results_fn: Any
    list_restricted_chat_scan_results_fn: Any
    list_recovery_chat_candidates_fn: Any
    build_recovery_overview_fn: Any
    build_telegram_chat_link_bundle_fn: Any
    admin_start_missing_chats_scan_job_thread_fn: Any
    admin_start_absent_chats_scan_job_thread_fn: Any
    admin_start_restricted_chats_scan_job_thread_fn: Any
    admin_start_recovery_scan_job_thread_fn: Any
    admin_start_recovery_restore_job_thread_fn: Any
