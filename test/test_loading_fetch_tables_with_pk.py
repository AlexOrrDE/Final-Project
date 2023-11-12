from unittest.mock import patch
from src.loading.fetch_tables_with_pk import fetch_tables_with_pk


@patch("pg8000.connect")
def test_fetch_tables_with_pk(mock_connect):
    mock_conn = mock_connect.return_value
    mock_cursor = mock_conn.cursor.return_value

    mock_cursor.fetchall.return_value = [("table_1", "id_1"), ("table_2", "id_2")]

    result = fetch_tables_with_pk(mock_conn)

    expected = [
        {"table_name": "table_1", "primary_key_column": "id_1"},
        {"table_name": "table_2", "primary_key_column": "id_2"},
    ]
    assert result == expected
