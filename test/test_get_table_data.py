from src.ingestion.get_table_data import fetch_data_from_tables
import os
from moto import mock_s3
import pytest 
from pandas import Timestamp
import datetime
from pg8000 import DatabaseError

class FullMock:
        class cursor:
            def execute(self, *args):
                return

            def fetchall(self):
                return ([1, 'Jeremie', 'Franey', 2, 'jeremie.franey@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)], [2, 'Deron', 'Beier', 6, 'deron.beier@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)], [3, 'Jeanette', 'Erdman', 6, 'jeanette.erdman@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)], [4, 'Ana', 'Glover', 3, 'ana.glover@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)], [5, 'Magdalena', 'Zieme', 8, 'magdalena.zieme@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)], [6, 'Korey', 'Kreiger', 3, 'korey.kreiger@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)], [7, 'Raphael', 'Rippin', 2, 'raphael.rippin@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)], [8, 'Oswaldo', 'Bergstrom', 7, 'oswaldo.bergstrom@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)], [9, 'Brody', 'Ratke', 2, 'brody.ratke@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)], [10, 'Jazmyn', 'Kuhn', 2, 'jazmyn.kuhn@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)], [11, 'Meda', 'Cremin', 5, 'meda.cremin@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)], [12, 'Imani', 'Walker', 5, 'imani.walker@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)], [13, 'Stan', 'Lehner', 4, 'stan.lehner@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)], [14, 'Rigoberto', 'VonRueden', 7, 'rigoberto.vonrueden@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)], [15, 'Tom', 'Gutkowski', 3, 'tom.gutkowski@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)], [16, 'Jett', 'Parisian', 6, 'jett.parisian@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)], [17, 'Irving', "O'Keefe", 3, "irving.o'keefe@terrifictotes.com", datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)], [18, 'Tomasa', 'Moore', 8, 'tomasa.moore@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)], [19, 'Pierre', 'Sauer', 2, 'pierre.sauer@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)], [20, 'Flavio', 'Kulas', 3, 'flavio.kulas@terrifictotes.com', datetime.datetime(2024, 11, 3, 14, 20, 51, 563000), datetime.datetime(2024, 11, 3, 14, 20, 51, 563000)])
            description = [('staff_id', 23, None, None, None, None, None), ('first_name', 25, None, None, None, None, None), ('last_name', 25, None, None, None, None, None), ('department_id', 23, None, None, None, None, None), ('email_address', 25, None, None, None, None, None), ('created_at', 1114, None, None, None, None, None), ('last_updated', 1114, None, None, None, None, None)]



def test_should_return_False_when_no_updates_are_found():
    class MockConn:
        class cursor:
            def execute(self, *args):
                return

            def fetchall(self):
                return []
    assert fetch_data_from_tables(
        MockConn,
        "staff",
        '2023-11-01 11:18:34.305980') is False

def test_should_return_all_data_from_database():
    """if not given a date to query, should get all data from given table"""
            
    expected = {'table_name': 'staff', 'data': [{'staff_id': 1, 'first_name': 'Jeremie', 'last_name': 'Franey', 'department_id': 2, 'email_address': 'jeremie.franey@terrifictotes.com', 'created_at': Timestamp('2022-11-03 14:20:51.563000'), 'last_updated': Timestamp('2022-11-03 14:20:51.563000')}, {'staff_id': 2, 'first_name': 'Deron', 'last_name': 'Beier', 'department_id': 6, 'email_address': 'deron.beier@terrifictotes.com', 'created_at': Timestamp('2022-11-03 14:20:51.563000'), 'last_updated': Timestamp('2022-11-03 14:20:51.563000')}, {'staff_id': 3, 'first_name': 'Jeanette', 'last_name': 'Erdman', 'department_id': 6, 'email_address': 'jeanette.erdman@terrifictotes.com', 'created_at': Timestamp('2022-11-03 14:20:51.563000'), 'last_updated': Timestamp('2022-11-03 14:20:51.563000')}, {'staff_id': 4, 'first_name': 'Ana', 'last_name': 'Glover', 'department_id': 3, 'email_address': 'ana.glover@terrifictotes.com', 'created_at': Timestamp('2022-11-03 14:20:51.563000'), 'last_updated': Timestamp('2022-11-03 14:20:51.563000')}, {'staff_id': 5, 'first_name': 'Magdalena', 'last_name': 'Zieme', 'department_id': 8, 'email_address': 'magdalena.zieme@terrifictotes.com', 'created_at': Timestamp('2022-11-03 14:20:51.563000'), 'last_updated': Timestamp('2022-11-03 14:20:51.563000')}, {'staff_id': 6, 'first_name': 'Korey', 'last_name': 'Kreiger', 'department_id': 3, 'email_address': 'korey.kreiger@terrifictotes.com', 'created_at': Timestamp('2022-11-03 14:20:51.563000'), 'last_updated': Timestamp('2022-11-03 14:20:51.563000')}, {'staff_id': 7, 'first_name': 'Raphael', 'last_name': 'Rippin', 'department_id': 2, 'email_address': 'raphael.rippin@terrifictotes.com', 'created_at': Timestamp('2022-11-03 14:20:51.563000'), 'last_updated': Timestamp('2022-11-03 14:20:51.563000')}, {'staff_id': 8, 'first_name': 'Oswaldo', 'last_name': 'Bergstrom', 'department_id': 7, 'email_address': 'oswaldo.bergstrom@terrifictotes.com', 'created_at': Timestamp('2022-11-03 14:20:51.563000'), 'last_updated': Timestamp('2022-11-03 14:20:51.563000')}, {'staff_id': 9, 'first_name': 'Brody', 'last_name': 'Ratke', 'department_id': 2, 'email_address': 'brody.ratke@terrifictotes.com', 'created_at': Timestamp('2022-11-03 14:20:51.563000'), 'last_updated': Timestamp('2022-11-03 14:20:51.563000')}, {'staff_id': 10, 'first_name': 'Jazmyn', 'last_name': 'Kuhn', 'department_id': 2, 'email_address': 'jazmyn.kuhn@terrifictotes.com', 'created_at': Timestamp('2022-11-03 14:20:51.563000'), 'last_updated': Timestamp('2022-11-03 14:20:51.563000')}, {'staff_id': 11, 'first_name': 'Meda', 'last_name': 'Cremin', 'department_id': 5, 'email_address': 'meda.cremin@terrifictotes.com', 'created_at': Timestamp('2022-11-03 14:20:51.563000'), 'last_updated': Timestamp('2022-11-03 14:20:51.563000')}, {'staff_id': 12, 'first_name': 'Imani', 'last_name': 'Walker', 'department_id': 5, 'email_address': 'imani.walker@terrifictotes.com', 'created_at': Timestamp('2022-11-03 14:20:51.563000'), 'last_updated': Timestamp('2022-11-03 14:20:51.563000')}, {'staff_id': 13, 'first_name': 'Stan', 'last_name': 'Lehner', 'department_id': 4, 'email_address': 'stan.lehner@terrifictotes.com', 'created_at': Timestamp('2022-11-03 14:20:51.563000'), 'last_updated': Timestamp('2022-11-03 14:20:51.563000')}, {'staff_id': 14, 'first_name': 'Rigoberto', 'last_name': 'VonRueden', 'department_id': 7, 'email_address': 'rigoberto.vonrueden@terrifictotes.com', 'created_at': Timestamp('2022-11-03 14:20:51.563000'), 'last_updated': Timestamp('2022-11-03 14:20:51.563000')}, {'staff_id': 15, 'first_name': 'Tom', 'last_name': 'Gutkowski', 'department_id': 3, 'email_address': 'tom.gutkowski@terrifictotes.com', 'created_at': Timestamp('2022-11-03 14:20:51.563000'), 'last_updated': Timestamp('2022-11-03 14:20:51.563000')}, {'staff_id': 16, 'first_name': 'Jett', 'last_name': 'Parisian', 'department_id': 6, 'email_address': 'jett.parisian@terrifictotes.com', 'created_at': Timestamp('2022-11-03 14:20:51.563000'), 'last_updated': Timestamp('2022-11-03 14:20:51.563000')}, {'staff_id': 17, 'first_name': 'Irving', 'last_name': "O'Keefe", 'department_id': 3, 'email_address': "irving.o'keefe@terrifictotes.com", 'created_at': Timestamp('2022-11-03 14:20:51.563000'), 'last_updated': Timestamp('2022-11-03 14:20:51.563000')}, {'staff_id': 18, 'first_name': 'Tomasa', 'last_name': 'Moore', 'department_id': 8, 'email_address': 'tomasa.moore@terrifictotes.com', 'created_at': Timestamp('2022-11-03 14:20:51.563000'), 'last_updated': Timestamp('2022-11-03 14:20:51.563000')}, {'staff_id': 19, 'first_name': 'Pierre', 'last_name': 'Sauer', 'department_id': 2, 'email_address': 'pierre.sauer@terrifictotes.com', 'created_at': Timestamp('2022-11-03 14:20:51.563000'), 'last_updated': Timestamp('2022-11-03 14:20:51.563000')}, {'staff_id': 20, 'first_name': 'Flavio', 'last_name': 'Kulas', 'department_id': 3, 'email_address': 'flavio.kulas@terrifictotes.com', 'created_at': Timestamp('2024-11-03 14:20:51.563000'), 'last_updated': Timestamp('2024-11-03 14:20:51.563000')}]}

    assert fetch_data_from_tables(
        FullMock,
        "staff") == expected

def test_should_return_updates_if_table_has_been_updated():
    """queries database with given date and returns all changes"""
    class MockConn:
        class cursor:
            def execute(self, *args):
                return

            def fetchall(self):
                return ([20, 'Flavio', 'Kulas', 3, 'flavio.kulas@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2023, 11, 6, 14, 20, 51, 563000)],[1, 'Jeremie', 'Franey', 2, 'jeremie.franey@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2023, 11, 6, 14, 20, 51, 563000)])
            
            description = [('staff_id', 23, None, None, None, None, None), ('first_name', 25, None, None, None, None, None), ('last_name', 25, None, None, None, None, None), ('department_id', 23, None, None, None, None, None), ('email_address', 25, None, None, None, None, None), ('created_at', 1114, None, None, None, None, None), ('last_updated', 1114, None, None, None, None, None)]
            
    expected = {'table_name': 'staff', 'data': [{'staff_id': 20, 'first_name': 'Flavio', 'last_name': 'Kulas', 'department_id': 3, 'email_address': 'flavio.kulas@terrifictotes.com', 'created_at': Timestamp('2022-11-03 14:20:51.563000'), 'last_updated': Timestamp('2023-11-06 14:20:51.563000')}, {'staff_id': 1, 'first_name': 'Jeremie', 'last_name': 'Franey', 'department_id': 2, 'email_address': 'jeremie.franey@terrifictotes.com', 'created_at': Timestamp('2022-11-03 14:20:51.563000'), 'last_updated': Timestamp('2023-11-06 14:20:51.563000')}]}


    assert fetch_data_from_tables(MockConn, "staff", "2023-11-06 12:00:00") == expected


def test_should_raise_database_error_to_be_handled_in_handler(caplog):
    """should reference the specific table in an error message"""
    with pytest.raises(DatabaseError):
        class MockConn:
            class cursor:
                def execute(self, *args):
                    raise DatabaseError

                def fetchall(self):
                    return []
        fetch_data_from_tables(MockConn, "staff")
        assert "Error occured in fetch_data_from_tables, calling table staff" in caplog.text