"""SQL builder for applying different values to multiple rows in one UPDATE."""

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
