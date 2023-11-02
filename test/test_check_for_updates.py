from src.ingestion.check_for_updates import check_for_updates
from src.ingestion.connection import connect_to_database
from unittest.mock import patch
from pytest import raises
import pg8000


def test_should_return_None_when_no_updates_are_found():
    class MockConn:
        class cursor:
            def execute(self, *args):
                return 
            def fetchall(self):
                return []
    assert check_for_updates(MockConn, "staff", '2023-11-01 11:18:34.305980') == None


def test_should_return_True_when_updates_are_found():
    class MockConn:
        class cursor:
            def execute(self, *args):
                return 
            def fetchall(self):
                return [1, 2]
    assert check_for_updates(MockConn, "staff", '2023-11-01 11:18:34.305980') == True    


def test_should_raise_database_errors_to_be_handled_in_the_handler():
    with raises(pg8000.DatabaseError):
        check_for_updates(connect_to_database(), "error", '2023-11-01 12:06:35.943017')


def test_should_raise_attribute_error_if_connection_to_database_is_not_valid():
    with raises(AttributeError):
        check_for_updates(connect_to_database, "staff", '2023-11-01 12:06:35.943017')