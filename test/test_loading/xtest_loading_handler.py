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


def xtest_handler_calls_x(s3_client, secrets_client):
    with patch('src.loading.handler.connect_to_database') as mock_conn:
        mock_conn.cursor = True
        mock_conn.execute = True


def xtest_handler_calls_fetch_tables_with_pk(s3_client, secrets_client):
    with (patch('src.loading.handler.connect_to_database') as mock_conn,
          patch('src.loading.handler.fetch_tables_with_pk') as fetch_pk):
        mock_conn.cursor = True
        mock_conn.execute = True
        handler('event', 'context')
        fetch_pk.assert_called