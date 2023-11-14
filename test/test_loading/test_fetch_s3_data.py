from moto import mock_s3
import boto3
import os
import pytest
import pandas as pd
from botocore.exceptions import ClientError
from src.loading.fetch_s3_data import fetch_data_from_s3


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    """Mocks the call to the AWS S3 client."""
    with mock_s3():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.fixture(scope="function")
def create_bucket(s3_client):
    s3_client.create_bucket(
        Bucket="processed-data-bucket-marble",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )


def test_should_pull_parquet_file_from_bucket(s3_client, create_bucket):
    file_path = os.path.abspath("test/test_loading/parquet_files/design.parquet")
    s3_client.upload_file(file_path, "processed-data-bucket-marble", "design.parquet")
    result = fetch_data_from_s3(
        s3_client, "processed-data-bucket-marble", "design.parquet"
    )
    expected = pd.read_parquet(file_path)
    pd.testing.assert_frame_equal(result, expected)


def test_should_return_none_if_key_doesnt_exist(s3_client, create_bucket, caplog):
    result = fetch_data_from_s3(
        s3_client, "processed-data-bucket-marble", "badfile.parquet"
    )
    assert result is None
    assert (
        "The specified key 'badfile.parquet' does not exist in the S3 bucket."
        in caplog.text
    )
