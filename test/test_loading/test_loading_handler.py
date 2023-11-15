import os
from moto import mock_s3, mock_secretsmanager
from unittest.mock import patch
import pytest
import boto3
from src.loading.handler import handler


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


@pytest.fixture(scope="function")
def secrets_client(aws_credentials):
    """Mocks the call to the AWS Secrets Manager"""
    with mock_secretsmanager():
        yield boto3.client("secretsmanager", region_name="eu-west-2")


@pytest.fixture(scope="function")
def create_bucket(s3_client):
    """Creates mock S3 buckets"""
    s3_client.create_bucket(
        Bucket="ingest-data-bucket-marble",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    s3_client.create_bucket(
        Bucket="processed-data-bucket-marble",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    parquets = [
        "dim_design",
        "dim_counterparty",
        "dim_currency",
        "dim_date",
        "dim_location",
        "dim_staff",
        "fact_sales_order",
    ]
    for file in parquets:
        file_path = os.path.abspath(f"test/test_loading/parquet_files/{file}.parquet")
        s3_client.upload_file(
            file_path, "processed-data-bucket-marble", f"{file}.parquet"
        )


def test_handler_calls_connect_to_warehouse(s3_client, secrets_client):
    with patch("src.loading.handler.connect_to_warehouse") as mock_conn:
        mock_conn.assert_called


def test_handler_calls_fetch_tables_with_pk(create_bucket, s3_client, secrets_client):
    with (
        patch("src.loading.handler.connect_to_warehouse"),
        patch("src.loading.handler.fetch_tables_with_pk") as fetch_pk,
    ):
        handler("event", "context")
        fetch_pk.assert_called


def test_handler_calls_fetch_data_from_s3(create_bucket, s3_client, secrets_client):
    table_dict = [{"table_name": "test_table", "primary_key": "test_key"}]
    with (
        patch("src.loading.handler.connect_to_warehouse"),
        patch("src.loading.handler.fetch_tables_with_pk", return_value=table_dict),
        patch("src.loading.handler.fetch_data_from_s3") as fetch_s3,
    ):
        handler("event", "context")
        fetch_s3.assert_called


def test_handler_calls_upload_to_warehouse(create_bucket, s3_client, secrets_client):
    table_dict = [{"table_name": "test_table", "primary_key": "test_key"}]
    with (
        patch("src.loading.handler.connect_to_warehouse"),
        patch("src.loading.handler.fetch_tables_with_pk", return_value=table_dict),
        patch("src.loading.handler.upload_to_warehouse") as warehouse_up,
    ):
        handler("event", "context")
        warehouse_up.assert_called
