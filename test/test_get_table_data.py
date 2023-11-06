import boto3
import os
from moto import mock_s3
import pytest


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

def test_totesys_fetch_data_from_tables_returns_a_list_of_table_names_from_schema(rds_client):
    """chech that totesys_fetch_data_from_tables returns all of table contents from 
    a given table name."""
    # arrange
    staff_table_contents = {'table_name': 'staff',
             'data': [{'staff_id': 1,
                       'first_name': 'Jeremie',
                       'last_name': 'Franey',
                       'department_id': 2,
                       'email_address': 'jeremie.franey@terrifictotes.com',
                       'created_at': Timestamp('2022-11-03 14:20:51.563000'),
                       'last_updated': Timestamp('2022-11-03 14:20:51.563000')
                       }], [{'staff_id': 2,
                       'first_name': 'Jeremie',
                       'last_name': 'Franey',
                       'department_id': 2,
                       'email_address': 'jeremie.franey@terrifictotes.com',
                       'created_at': Timestamp('2022-11-03 14:20:51.563000'),
                       'last_updated': Timestamp('2022-11-03 14:20:51.563000')
                       }], [{'staff_id': 3,
                       'first_name': 'Jeremie',
                       'last_name': 'Franey',
                       'department_id': 2,
                       'email_address': 'jeremie.franey@terrifictotes.com',
                       'created_at': Timestamp('2022-11-03 14:20:51.563000'),
                       'last_updated': Timestamp('2022-11-03 14:20:51.563000')
                       }]
             }
    
    class MockConn:
        class cursor:
            def execute(self, *args):
                return

            def fetchall(self):
                return 
                    
    # assert
    assert fetch_data_from_tables(MockConn) == ['table1', 'table2', 'table3']
