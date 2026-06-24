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
        services=services.channels,
    )

    register_recovery_routes(
        app,
        services=services.recovery,
    )

    register_clone_routes(
        app,
        services=services.clone,
    )
