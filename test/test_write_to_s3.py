from src.ingestion.write_to_s3 import write_to_s3
from moto import mock_s3
from freezegun import freeze_time
import pytest
import boto3
import os
from pytest import raises
from botocore.exceptions import ClientError


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    with mock_s3():
        yield boto3.client("s3", region_name="eu-west-2")


@freeze_time("2023-01-01")
def test_write_to_s3_adds_file_to_test_bucket(s3_client):
    """Test to check the write_to_s3 function is able
    to add the correct item to a bucket"""

    s3_client.create_bucket(
        Bucket="ingestion-data-bucket-marble",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    assert "Contents" not in s3_client.list_objects(
        Bucket="ingestion-data-bucket-marble")

    write_to_s3("test-table", "test-data")

    assert (len(s3_client.list_objects(
        Bucket="ingestion-data-bucket-marble")["Contents"]) == 1)


@freeze_time("2023-01-01")
def test_write_to_s3_adds_correct_prefix_and_suffix_to_filename_on_upload(
        s3_client):
    s3_client.create_bucket(
        Bucket="ingestion-data-bucket-marble",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    write_to_s3("test-table", "test-data")
    assert (s3_client.list_objects(Bucket="ingestion-data-bucket-marble")
            ["Contents"][0]["Key"] == "2023-01-01 00:00:00-test-table.csv")


def test_should_raise_client_error_if_bucket_does_not_exist(s3_client):
    with raises(ClientError):
        s3_client.create_bucket(
            Bucket='not-ingestion-data-bucket-marble',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            }
        )

        write_to_s3("test-table", "csv-data")


def test_should_raise_type_error_if_called_with_incorrect_parameters(
        s3_client):
    with raises(TypeError):
        s3_client.create_bucket(
            Bucket='not-ingestion-data-bucket-marble',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            }
        )

        write_to_s3()
