from moto import mock_s3
import boto3
import os
import pytest
from src.loading.fetch_s3_data import fetch_data_from_s3


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'test'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
    os.environ['AWS_SECURITY_TOKEN'] = 'test'
    os.environ['AWS_SESSION_TOKEN'] = 'test'
    os.environ['AWS_DEFAULT_REGION'] = 'eu-west-2'


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    """Mocks the call to the AWS S3 client."""
    with mock_s3():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.fixture(scope="function")
def create_bucket(s3_client):
    s3_client.create_bucket(
        Bucket="processed-bucket-marble",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )


def test_x(s3_client, create_bucket):
    pass