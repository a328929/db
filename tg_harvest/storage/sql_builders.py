"""SQL query builder utilities for complex UPDATE operations.

This module provides reusable SQL builders for common patterns found in the storage layer:
- Batch CASE-based updates (match multiple rows, set different values per row)
- Status transition updates with related field changes
- Conditional SET clauses using CASE WHEN logic
- In-flight state tracking updates

All builders return (sql_string, params_tuple) for use with cursor.execute().
"""

from typing import Any


def build_batch_case_update(
    table: str,
    match_field: str,
    case_updates: dict[Any, dict[str, Any]],
    set_fields: dict[str, Any] | None = None,
    where_conditions: dict[str, Any] | None = None,
    where_clauses: list[str] | None = None,
) -> tuple[str, tuple]:
    """Build batch UPDATE with CASE statements for different values per row.

    This builder creates UPDATE queries that set different values for different rows
    based on a match field, using CASE WHEN logic. It's useful when you need to update
    multiple rows with different values in a single query.

    Args:
        table: Table name to update
        match_field: Field used to match rows (e.g., 'id', 'message_id')
        case_updates: Dict mapping match_value -> {field: new_value}
                     Example: {123: {'status': 'sent', 'hop': 2},
                              456: {'status': 'sent', 'hop': 2}}
        set_fields: Optional dict of fields to set to same value for all matched rows
                   Example: {'updated_at': time.time()}
        where_conditions: Optional dict of field=value conditions for WHERE clause
                         Example: {'status': 'pending'}
        where_clauses: Optional list of raw SQL WHERE clauses
                      Example: ['hop < 3', 'error_count = 0']

    Returns:
        Tuple of (sql_string, params_tuple) ready for cursor.execute()

    Example:
        >>> sql, params = build_batch_case_update(
        ...     table='clone_media_transfers',
        ...     match_field='message_id',
        ...     case_updates={
        ...         123: {'status': 'sent', 'hop_count': 2},
        ...         456: {'status': 'sent', 'hop_count': 3}
        ...     },
        ...     set_fields={'updated_at': 1234567890},
        ...     where_conditions={'clone_run_id': 42}
        ... )
        >>> # SQL: UPDATE clone_media_transfers SET
        >>> #      status = CASE message_id WHEN ? THEN ? WHEN ? THEN ? END,
        >>> #      hop_count = CASE message_id WHEN ? THEN ? WHEN ? THEN ? END,
        >>> #      updated_at = ?
        >>> #      WHERE message_id IN (?, ?) AND clone_run_id = ?
    """
    if not case_updates:
        raise ValueError("case_updates cannot be empty")

    # Collect all fields that need CASE statements
    all_case_fields: set[str] = set()
    for field_dict in case_updates.values():
        all_case_fields.update(field_dict.keys())

    if not all_case_fields:
        raise ValueError("No fields to update in case_updates")

    # Build SET clauses with CASE statements
    set_clauses: list[str] = []
    params: list[Any] = []

    for field in sorted(all_case_fields):  # Sort for deterministic output
        case_parts = [f"{field} = CASE {match_field}"]

        for match_value, field_dict in case_updates.items():
            if field in field_dict:
                case_parts.append("WHEN ? THEN ?")
                params.append(match_value)
                params.append(field_dict[field])

        case_parts.append("END")
        set_clauses.append(" ".join(case_parts))

    # Add constant SET fields
    if set_fields:
        for field, value in set_fields.items():
            set_clauses.append(f"{field} = ?")
            params.append(value)

    # Build WHERE clause
    where_parts: list[str] = []

    # Add IN clause for match_field
    match_values = list(case_updates.keys())
    placeholders = ", ".join("?" * len(match_values))
    where_parts.append(f"{match_field} IN ({placeholders})")
    params.extend(match_values)

    # Add equality conditions
    if where_conditions:
        for field, value in where_conditions.items():
            where_parts.append(f"{field} = ?")
            params.append(value)

    # Add raw WHERE clauses
    if where_clauses:
        where_parts.extend(where_clauses)

    # Assemble final SQL
    sql = f"UPDATE {table} SET {', '.join(set_clauses)} WHERE {' AND '.join(where_parts)}"

    return (sql, tuple(params))


def build_status_transition_update(
    table: str,
    status_field: str,
    new_status: str,
    set_fields: dict[str, Any] | None = None,
    increment_fields: list[str] | None = None,
    where_conditions: dict[str, Any] | None = None,
) -> tuple[str, tuple]:
    """Build UPDATE for status transitions with related field changes.

    This builder creates UPDATE queries for status transitions that may also update
    related fields (like timestamps, counters, ownership) and increment counters.

    Args:
        table: Table name to update
        status_field: Name of the status field (e.g., 'status', 'state')
        new_status: New status value
        set_fields: Optional dict of additional fields to set
                   Example: {'started_at': time.time(), 'owner_pid': 12345}
        increment_fields: Optional list of fields to increment by 1
                         Example: ['attempt_count', 'retry_count']
        where_conditions: Optional dict of field=value conditions for WHERE clause
                         Example: {'id': 42, 'status': 'pending'}

    Returns:
        Tuple of (sql_string, params_tuple) ready for cursor.execute()

    Example:
        >>> sql, params = build_status_transition_update(
        ...     table='sync_jobs',
        ...     status_field='status',
        ...     new_status='running',
        ...     set_fields={'started_at': 1234567890, 'owner_pid': 12345},
        ...     increment_fields=['attempt_count'],
        ...     where_conditions={'id': 42, 'status': 'pending'}
        ... )
        >>> # SQL: UPDATE sync_jobs SET status = ?, started_at = ?, owner_pid = ?,
        >>> #      attempt_count = attempt_count + 1
        >>> #      WHERE id = ? AND status = ?
    """
    set_clauses: list[str] = []
    params: list[Any] = []

    # Set status field
    set_clauses.append(f"{status_field} = ?")
    params.append(new_status)

    # Add additional set fields
    if set_fields:
        for field, value in set_fields.items():
            set_clauses.append(f"{field} = ?")
            params.append(value)

    # Add increment fields
    if increment_fields:
        for field in increment_fields:
            set_clauses.append(f"{field} = {field} + 1")

    # Build WHERE clause
    where_parts: list[str] = []
    if where_conditions:
        for field, value in where_conditions.items():
            where_parts.append(f"{field} = ?")
            params.append(value)

    # Assemble final SQL
    sql = f"UPDATE {table} SET {', '.join(set_clauses)}"
    if where_parts:
        sql += f" WHERE {' AND '.join(where_parts)}"

    return (sql, tuple(params))


def build_conditional_set_clauses(
    conditional_fields: dict[str, tuple[str, Any, Any]],
    table_prefix: str = "",
    fallback_prefix: str = "",
) -> list[str]:
    """Build CASE WHEN clauses for conditional field updates in SET.

    This builder creates SET clauses with CASE WHEN logic for conditional updates,
    where a field is set to one value if a condition is met, otherwise another value.

    Args:
        conditional_fields: Dict mapping field_name -> (condition_sql, true_value, false_value)
                          Example: {
                              'status': ('new_status IS NOT NULL', 'new.status', 'old.status'),
                              'updated_at': ('new_status IS NOT NULL', 'new.updated_at', 'old.updated_at')
                          }
        table_prefix: Optional prefix for field names in SET clause (e.g., 'table.')
        fallback_prefix: Optional prefix for fallback values (e.g., 'excluded.')

    Returns:
        List of SQL SET clauses as strings

    Example:
        >>> clauses = build_conditional_set_clauses(
        ...     conditional_fields={
        ...         'status': ('new_status IS NOT NULL', 'new.status', 'old.status'),
        ...         'updated_at': ('new_updated_at IS NOT NULL', '?', 'old.updated_at')
        ...     },
        ...     table_prefix='channels.',
        ...     fallback_prefix='excluded.'
        ... )
        >>> # Returns: [
        >>> #   'channels.status = CASE WHEN new_status IS NOT NULL THEN new.status ELSE old.status END',
        >>> #   'channels.updated_at = CASE WHEN new_updated_at IS NOT NULL THEN ? ELSE old.updated_at END'
        >>> # ]
    """
    clauses: list[str] = []

    for field, (condition, true_value, false_value) in conditional_fields.items():
        field_name = f"{table_prefix}{field}" if table_prefix else field
        clause = f"{field_name} = CASE WHEN {condition} THEN {true_value} ELSE {false_value} END"
        clauses.append(clause)

    return clauses


def build_in_flight_state_update(
    table: str,
    in_flight: bool,
    generation: int | None = None,
    owner_instance_id: str | None = None,
    owner_pid: int | None = None,
    owner_host: str | None = None,
    where_conditions: dict[str, Any] | None = None,
) -> tuple[str, tuple]:
    """Build UPDATE for in-flight tracking state changes.

    This builder creates UPDATE queries for managing in-flight state tracking,
    which is used to coordinate distributed work processing. When setting in_flight=True,
    it captures ownership information. When setting in_flight=False, it clears it.

    Args:
        table: Table name to update
        in_flight: Whether to mark as in-flight (True) or clear (False)
        generation: Optional generation number for optimistic locking
        owner_instance_id: Optional instance ID claiming/releasing the work
        owner_pid: Optional process ID claiming/releasing the work
        owner_host: Optional hostname claiming/releasing the work
        where_conditions: Optional dict of field=value conditions for WHERE clause
                         Example: {'id': 42, 'in_flight': False}

    Returns:
        Tuple of (sql_string, params_tuple) ready for cursor.execute()

    Example:
        >>> # Claim work
        >>> sql, params = build_in_flight_state_update(
        ...     table='sync_jobs',
        ...     in_flight=True,
        ...     generation=5,
        ...     owner_instance_id='worker-01',
        ...     owner_pid=12345,
        ...     owner_host='server1.example.com',
        ...     where_conditions={'id': 42, 'in_flight': False}
        ... )
        >>>
        >>> # Release work
        >>> sql, params = build_in_flight_state_update(
        ...     table='sync_jobs',
        ...     in_flight=False,
        ...     where_conditions={'id': 42}
        ... )
    """
    set_clauses: list[str] = []
    params: list[Any] = []

    # Set in_flight state
    set_clauses.append("in_flight = ?")
    params.append(1 if in_flight else 0)

    # If marking in-flight, set ownership fields
    if in_flight:
        if generation is not None:
            set_clauses.append("generation = ?")
            params.append(generation)
        if owner_instance_id is not None:
            set_clauses.append("owner_instance_id = ?")
            params.append(owner_instance_id)
        if owner_pid is not None:
            set_clauses.append("owner_pid = ?")
            params.append(owner_pid)
        if owner_host is not None:
            set_clauses.append("owner_host = ?")
            params.append(owner_host)
    else:
        # Clear ownership fields when releasing
        set_clauses.extend([
            "owner_instance_id = NULL",
            "owner_pid = NULL",
            "owner_host = NULL"
        ])

    # Build WHERE clause
    where_parts: list[str] = []
    if where_conditions:
        for field, value in where_conditions.items():
            where_parts.append(f"{field} = ?")
            params.append(value)

    # Assemble final SQL
    sql = f"UPDATE {table} SET {', '.join(set_clauses)}"
    if where_parts:
        sql += f" WHERE {' AND '.join(where_parts)}"

    return (sql, tuple(params))
