import pytest
from src.ingestion.get_table_names import fetch_tables


@pytest.fixture(scope="function")
def mock_db(postgresql):
    """Fixture that provides a mock PostgreSQL database for testing."""
    return postgresql


def test_fetch_tables_with_tables(mock_db):
    """Test the fetch_tables function with tables created in the mock database.

    This test checks whether the fetch_tables function retrieves the correct
    table names when tables are created in the mock database.

    Args:
        mock_db (fixture): A fixture providing a mock PostgreSQL database.
    """
    with mock_db.cursor() as cursor:
        cursor.execute(
            'CREATE TABLE table1 (id serial PRIMARY KEY, name TEXT)')
        cursor.execute(
            'CREATE TABLE table2 (id serial PRIMARY KEY, description TEXT)')
        cursor.execute(
            'CREATE TABLE table3 (id serial PRIMARY KEY, value REAL)')

    expected_table_names = ["table1", "table2", "table3"]
    assert sorted(fetch_tables(mock_db)) == expected_table_names
