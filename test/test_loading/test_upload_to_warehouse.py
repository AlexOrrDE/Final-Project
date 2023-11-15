from src.loading.upload_to_warehouse import upload_to_warehouse
from src.loading.psycopg2_conn import connect_to_warehouse
import pytest
from psycopg2 import OperationalError
import requests
from unittest.mock import Mock
import pandas as pd

class MockConn:
    class cursor:
        def commit(self):
            return

        def close(self):
            return 
        
        def connection(self):
            return 

df =  pd.read_parquet('test/test_loading/parquet_files/design.parquet')   
# print(df) 
# @pytest.fixture
# def mock_conn(mocker):
#     mock = Mock()
#     mocker.patch('conn', return_value=mock)
#     return mock
# def test_mock_conn(mock_conn):
#     mock_conn.cursor.return_value = True
#     mock_conn.commit.return_value = True
#     mock_conn.close.return_value = True

#     response = upload_to_warehouse(mock_conn, "", "", "")
#     assert response.status_code == 200
#     assert response.json() == {'key': 'value'}

def test_raises_attribute_error_if_given_incorrect_table_or_primary_key():
    with pytest.raises(AttributeError):
        upload_to_warehouse(connect_to_warehouse, "dim_", "design_id", df)
    with pytest.raises(AttributeError):
        upload_to_warehouse(connect_to_warehouse, "dim_design", "des_id", df)

# def test_should_log_successful_statement_if_upload_completes(caplog):
#     upload_to_warehouse(MockConn, "dim_design", "design_id", df)
#     assert "Data uploaded to dim_design successfully." in caplog.text
