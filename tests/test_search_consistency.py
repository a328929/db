import sqlite3

import pytest

from tg_harvest.search.browse_service import sqlite_browse_payload_service
from tg_harvest.search.expression import parse_query
from tg_harvest.search.manticore_sync import (
    _load_validation_sample,
    validate_manticore_state,
)
from tg_harvest.search.params import _parse_search_params
from tg_harvest.storage.connection import detect_sqlite_features
from tg_harvest.storage.manticore_outbox import (
    get_manticore_index_status,
    set_manticore_index_status,
)
from tg_harvest.storage.schema import create_schema


@pytest.fixture
def search_db(tmp_path):
    path = tmp_path / "search.db"

    def connect():
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        return conn

    with connect() as conn:
        create_schema(conn, detect_sqlite_features(conn))
        conn.execute("INSERT INTO chats(chat_id, chat_title) VALUES (1, 'Example')")
        for message_id in range(1, 7):
            conn.execute(
                "INSERT INTO messages(chat_id,message_id,msg_date_text,msg_date_ts,msg_type) "
                "VALUES (1, ?, 'date', ?, 'VIDEO')",
                (message_id, message_id),
            )
        for message_id, value in ((1, 20), (2, 0), (3, None), (5, 10), (6, -1)):
            conn.execute(
                "INSERT INTO message_media(chat_id,message_id,file_size,duration_sec) VALUES (1,?,?,?)",
                (message_id, value, value),
            )
    conn.close()
    return connect


@pytest.mark.parametrize(
    "query", ["(" * 300 + "word" + ")" * 300, "-" * 300 + "word", "a " * 400]
)
def test_parser_rejects_complex_input_without_recursion_error(query):
    with pytest.raises(ValueError):
        parse_query(query)


@pytest.mark.parametrize("sort", ["size", "duration"])
@pytest.mark.parametrize(
    "order,expected", [("asc", [6, 2, 3, 4, 5, 1]), ("desc", [1, 5, 4, 3, 2, 6])]
)
def test_media_sort_preserves_missing_and_null_metadata_across_pages(
    search_db, sort, order, expected
):
    with search_db() as conn:
        seen = []
        for page in (1, 2, 3):
            result = sqlite_browse_payload_service(
                conn,
                _parse_search_params(
                    {
                        "search_type": "video",
                        "sort_by": sort,
                        "order": order,
                        "page": page,
                    }
                ),
                page_size=2,
                max_count=100,
                map_search_items_fn=lambda rows: list(rows),
            )
            assert result["total"] == 6
            seen.extend(row["message_id"] for row in result["items"])
        assert seen == expected
    conn.close()


def test_media_sort_duration_filter_and_result_cap(search_db):
    with search_db() as conn:
        result = sqlite_browse_payload_service(
            conn,
            _parse_search_params(
                {
                    "query": "【00:00】",
                    "search_type": "video",
                    "sort_by": "size",
                    "page": 2,
                }
            ),
            page_size=2,
            max_count=3,
            map_search_items_fn=lambda rows: list(rows),
        )
        assert result["total"] == 3
        assert [row["message_id"] for row in result["items"]] == [2]
    conn.close()


def test_validation_does_not_invalidate_index_when_source_changes(search_db):
    with search_db() as conn:
        conn.execute("DELETE FROM manticore_search_outbox")
        set_manticore_index_status(conn, "tg_messages", "ready")
    conn.close()

    class Client:
        table = "tg_messages"

        def ensure_table(self):
            pass

        def document_count(self):
            with search_db() as conn:
                conn.execute(
                    "INSERT INTO messages(chat_id,message_id,msg_date_text,msg_date_ts,msg_type) VALUES (1,7,'date',7,'TEXT')"
                )
                conn.execute("DELETE FROM manticore_search_outbox")
            conn.close()
            return 7

        def sample_documents(self, ids):
            with search_db() as conn:
                result = _load_validation_sample(conn)
            conn.close()
            return result

    assert validate_manticore_state(search_db, Client())
    with search_db() as conn:
        assert get_manticore_index_status(conn, "tg_messages") == "ready"
    conn.close()
