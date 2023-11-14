from src.processing.handler import handler
from src.processing.table_merge import table_merge
from unittest.mock import patch
from moto import mock_s3
import boto3
import os
import pytest

import sys
sys.path.append('processing')

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    """Mocks the call to the AWS S3 client."""
    with mock_s3():
        yield boto3.client("s3", region_name="eu-west-2")

@pytest.fixture(scope='function')
def mock_lambda(s3_client):
    s3_client.create_bucket(
        Bucket="ingestion-data-bucket-marble",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    s3_client.create_bucket(
        Bucket="processed-data-bucket-marble",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    with (open('./test/test_processing/csv_files/counterparty.csv', 'rb') as a, open('./test/test_processing/csv_files/currency.csv', 'rb') as b, open('./test/test_processing/csv_files/department.csv', 'rb') as c, open('./test/test_processing/csv_files/design.csv', 'rb') as d, open('./test/test_processing/csv_files/sales_order.csv', 'rb') as e, open('./test/test_processing/csv_files/staff.csv', 'rb') as f, open('./test/test_processing/csv_files/address.csv', 'rb') as g):
        files = {
            'counterparty': a, 
            'currency': b, 
            'department': c,
            'design': d,
            'sales_order': e,
            'staff': f,
            'address': g
        }
        
        for file in files:
            s3_client.put_object(
                Bucket="ingestion-data-bucket-marble",
                Key=f"{file}",
                Body=files[file])


event_dict = {
    "counterparty": 'counterparty',
    "currency": 'currency',
    "design": 'design',
    "staff": 'staff',
    "sales_order": 'sales_order',
    "address": 'address',
    "department": 'department'
}


def test_should_put_to_processing_bucket(s3_client, mock_lambda):
    handler(event_dict, "")
    response = s3_client.list_objects(Bucket="processed-data-bucket-marble")
    assert len(response['Contents']) == 7


def test_should_convert_files_to_parquet(s3_client, mock_lambda):
    handler(event_dict, "")
    response = s3_client.list_objects(Bucket="processed-data-bucket-marble")
    for key in response['Contents']:
        assert key['Key'][-8:] == ".parquet"


def test_table_merge_should_be_called_for_tables_in_function_dict(s3_client, mock_lambda):
    with patch('src.processing.handler.table_merge', side_effect=table_merge) as tm:
        handler(event_dict, "")
        assert tm.call_count == 6
