from get_table_names import fetch_tables
from moto import mock_rds
# from sqlalchemy import create_engine
import unittest
import boto3
import os
import pytest
import pg8000


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

@pytest.fixture(scope="function")
def rds_client(aws_credentials):
    with mock_rds():
        yield boto3.client("rds", region_name="eu-west-2")



def test_totesys_fetch_tables_throws_pg8000_error_when_the_database_has_no_schema(rds_client):
    """chech that totesys_fetch_tables returns pg800.Error when it fails to find any tables in the datbase."""

    class MockConn:
            class cursor:
                def execute(self, *args):
                    return

                def fetchall(self):
                    raise pg8000.Error
                
    with pytest.raises(pg8000.Error):
        fetch_tables(MockConn)


def test_totesys_fetch_tables_returns_a_list_of_table_names_from_schema(rds_client):
    """chech that totesys_fetch_tables rturns table names in list from the schema and disregards table "_prisma_migrations"."""
    # arrange
    class MockConn:
        class cursor:
            def execute(self, *args):
                return

            def fetchall(self):
                return [['table1'], ['table2'], ["_prisma_migrations"], ['table3']]
                    
    # assert
    assert fetch_tables(MockConn) == ['table1', 'table2', 'table3']
