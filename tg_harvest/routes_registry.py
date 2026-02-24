from functools import partial

from tg_harvest.routes_admin import register_admin_routes
from tg_harvest.routes_meta import register_meta_routes
from tg_harvest.routes_pages import register_page_routes
from tg_harvest.routes_search import register_search_routes


def register_all_routes(
    app,
    *,
    page_size,
    logger,
    get_conn_fn,
    build_meta_payload_fn,
    has_fts_fn,
    from_sql,
    max_count,
    tokenize_query_fn,
    to_fts_match_fn,
    map_search_items_fn,
    parse_search_params_fn,
    search_payload_service_fn,
    cfg,
    parse_admin_chat_id_fn,
    build_admin_chats_payload_fn,
    build_admin_stats_payload_fn,
    admin_get_chat_brief_fn,
    admin_job_get_snapshot_fn,
    admin_job_get_logs_fn,
    admin_has_any_active_job_fn,
    admin_create_chat_job_if_absent_fn,
    admin_job_create_fn,
    admin_job_append_log_fn,
    admin_start_harvest_job_thread_fn,
    admin_start_update_job_thread_fn,
    admin_start_delete_job_thread_fn,
    admin_start_cleanup_job_thread_fn,
    admin_make_job_log_handler_fn,
    admin_job_set_status_fn,
    admin_harvest_target_max_len,
    admin_cleanup_keyword_max_len,
) -> None:
    register_page_routes(
        app,
        page_size=page_size,
    )

    register_meta_routes(
        app,
        logger=logger,
        get_conn_fn=get_conn_fn,
        build_meta_payload_fn=partial(build_meta_payload_fn, page_size=page_size),
    )

    register_search_routes(
        app,
        logger=logger,
        get_conn_fn=get_conn_fn,
        has_fts_fn=has_fts_fn,
        from_sql=from_sql,
        page_size=page_size,
        max_count=max_count,
        tokenize_query_fn=tokenize_query_fn,
        to_fts_match_fn=to_fts_match_fn,
        map_search_items_fn=map_search_items_fn,
        parse_search_params_fn=parse_search_params_fn,
        search_payload_service_fn=search_payload_service_fn,
    )

    register_admin_routes(
        app,
        logger=logger,
        cfg=cfg,
        get_conn_fn=get_conn_fn,
        parse_admin_chat_id_fn=parse_admin_chat_id_fn,
        build_admin_chats_payload_fn=build_admin_chats_payload_fn,
        build_admin_stats_payload_fn=build_admin_stats_payload_fn,
        admin_get_chat_brief_fn=admin_get_chat_brief_fn,
        admin_job_get_snapshot_fn=admin_job_get_snapshot_fn,
        admin_job_get_logs_fn=admin_job_get_logs_fn,
        admin_has_any_active_job_fn=admin_has_any_active_job_fn,
        admin_create_chat_job_if_absent_fn=admin_create_chat_job_if_absent_fn,
        admin_job_create_fn=admin_job_create_fn,
        admin_job_append_log_fn=admin_job_append_log_fn,
        admin_start_harvest_job_thread_fn=admin_start_harvest_job_thread_fn,
        admin_start_update_job_thread_fn=admin_start_update_job_thread_fn,
        admin_start_delete_job_thread_fn=admin_start_delete_job_thread_fn,
        admin_start_cleanup_job_thread_fn=admin_start_cleanup_job_thread_fn,
        admin_make_job_log_handler_fn=admin_make_job_log_handler_fn,
        admin_job_set_status_fn=admin_job_set_status_fn,
        admin_harvest_target_max_len=admin_harvest_target_max_len,
        admin_cleanup_keyword_max_len=admin_cleanup_keyword_max_len,
        has_fts_fn=has_fts_fn,
    )
