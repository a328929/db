import sqlite3

SEARCH_TEXT_PRESENT_COLUMN = "search_text_present"


def search_text_present_expression(alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return (
        "CASE WHEN "
        f"COALESCE(NULLIF(TRIM({prefix}content_norm), ''), "
        f"NULLIF(TRIM({prefix}content), ''), '') <> '' "
        "THEN 1 ELSE 0 END"
    )


def search_text_present_column_sql() -> str:
    return (
        f"{SEARCH_TEXT_PRESENT_COLUMN} INTEGER GENERATED ALWAYS AS "
        f"({search_text_present_expression()}) VIRTUAL"
    )


def table_has_column(cur: sqlite3.Cursor, table_name: str, column_name: str) -> bool:
    try:
        cur.execute(f"PRAGMA table_xinfo({table_name})")
    except sqlite3.Error:
        cur.execute(f"PRAGMA table_info({table_name})")
    return any(str(row[1]) == column_name for row in cur.fetchall())


def table_has_index(cur: sqlite3.Cursor, table_name: str, index_name: str) -> bool:
    cur.execute(f"PRAGMA index_list({table_name})")
    return any(str(row[1]) == index_name for row in cur.fetchall())


def unsearchable_message_predicate(alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return f"COALESCE(NULLIF(TRIM({prefix}content_norm), ''), NULLIF(TRIM({prefix}content), ''), '') = ''"


def indexed_unsearchable_message_predicate(
    cur: sqlite3.Cursor, *, alias: str = "m"
) -> str:
    if table_has_column(cur, "messages", SEARCH_TEXT_PRESENT_COLUMN):
        prefix = f"{alias}." if alias else ""
        return f"{prefix}{SEARCH_TEXT_PRESENT_COLUMN} = 0"
    return unsearchable_message_predicate(alias)


def indexed_messages_from_clause(
    cur: sqlite3.Cursor,
    *,
    alias: str = "m",
    chat_scoped: bool = False,
) -> str:
    alias_sql = f" AS {alias}" if alias else ""
    if not table_has_column(cur, "messages", SEARCH_TEXT_PRESENT_COLUMN):
        return f"messages{alias_sql}"

    preferred_index = (
        "idx_messages_unsearchable_chat"
        if chat_scoped
        else "idx_messages_unsearchable_pk"
    )
    fallback_index = (
        "idx_messages_unsearchable_pk"
        if chat_scoped
        else "idx_messages_unsearchable_chat"
    )
    if table_has_index(cur, "messages", preferred_index):
        return f"messages{alias_sql} INDEXED BY {preferred_index}"
    if table_has_index(cur, "messages", fallback_index):
        return f"messages{alias_sql} INDEXED BY {fallback_index}"
    return f"messages{alias_sql}"
