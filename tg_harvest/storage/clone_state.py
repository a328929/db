from tg_harvest.storage.clone_state_common import (
    _append_optional_fields,
    _build_clone_message_mapping_filters,
    _build_clone_run_filters,
    _clone_message_mapping_from_row,
    _clone_message_mapping_summary_from_row,
    _clone_migration_from_row,
    _clone_plan_from_row,
    _clone_run_from_row,
    _commit_and_load_required,
    _commit_and_reload,
    _execute_update_and_reload,
    _normalize_offset,
    _query_all,
    _query_count,
    _query_one,
)
from tg_harvest.storage.clone_state_delete import delete_clone_run
from tg_harvest.storage.clone_state_mappings import (
    count_clone_message_mappings,
    list_clone_message_mappings,
    load_clone_message_mapping,
    load_clone_message_mapping_summary,
    record_clone_message_mapping,
)
from tg_harvest.storage.clone_state_migrations import (
    create_clone_migration,
    load_clone_migration,
    load_latest_clone_migration,
    update_clone_migration,
)
from tg_harvest.storage.clone_state_plans import (
    create_clone_plan,
    list_clone_plans,
    load_clone_plan,
    load_latest_clone_plan,
    update_clone_plan,
)
from tg_harvest.storage.clone_state_runs import (
    count_clone_runs,
    create_clone_run,
    list_clone_runs,
    load_clone_run,
    load_clone_run_detail as _load_clone_run_detail_impl,
    update_clone_run,
)


def load_clone_run_detail(conn, run_id: str):
    return _load_clone_run_detail_impl(
        conn,
        run_id,
        load_latest_clone_plan_fn=load_latest_clone_plan,
        load_latest_clone_migration_fn=load_latest_clone_migration,
        load_clone_message_mapping_summary_fn=load_clone_message_mapping_summary,
        list_clone_message_mappings_fn=list_clone_message_mappings,
    )
