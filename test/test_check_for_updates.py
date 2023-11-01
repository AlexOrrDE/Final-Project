from src.ingestion.check_for_updates import check_for_updates
from unittest.mock import patch


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
    assert check_for_updates(MockConn, "staff", '2023-11-01 11:18:34.305980') != False
    

