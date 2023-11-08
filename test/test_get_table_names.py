import pytest
from src.ingestion.get_table_names import fetch_tables
import pg8000


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


def test_fetch_tables_removes_prisma_table(mock_db):
    """Test that the fetch_tables function removes _prisma_migrations
      table created in the mock database.
    Args:
        mock_db (fixture): A fixture providing a mock PostgreSQL database.
    """
    with mock_db.cursor() as cursor:
        cursor.execute(
            'CREATE TABLE table1 (id serial PRIMARY KEY, name TEXT)')
        cursor.execute(
            'CREATE TABLE _prisma_migrations (id serial PRIMARY KEY)')

    expected_table_names = ["table1"]
    assert sorted(fetch_tables(mock_db)) == expected_table_names


def test_fetch_tables_throws_pg8000_error_when_database_has_no_schema():
    """check that totesys_fetch_tables returns pg8000.Error
        when it fails to find any tables in the database.
    """
    class MockConn:
        class cursor:
            def execute(self, *args):
                return

            def fetchall(self):
                raise pg8000.Error
    with pytest.raises(pg8000.Error):
        fetch_tables(MockConn)


def test_totesys_fetch_tables_returns_a_list_of_table_names_from_schema():
    """check that totesys_fetch_tables returns table names in list
    from the schema and disregards table "_prisma_migrations".
    """
    class MockConn:
        class cursor:
            def execute(self, *args):
                return

            def fetchall(self):
                return [
                    ['table1'],
                    ['table2'],
                    ["_prisma_migrations"],
                    ['table3']]
    assert fetch_tables(MockConn) == ['table1', 'table2', 'table3']
