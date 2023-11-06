from src.ingestion.check_objects import check_objects
import boto3
from botocore.exceptions import ClientError
from moto import mock_s3
import os
import pytest
from pytest import raises


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
    with mock_s3():
        yield boto3.client("s3")


def test_should_return_False_if_no_objects_in_the_bucket(s3_client):
    s3_client.create_bucket(
        Bucket='ingestion-data-bucket-marble',
        CreateBucketConfiguration={
            'LocationConstraint': 'eu-west-2'
        }
    )

    assert not check_objects()


def test_should_return_True_if_one_object_in_the_bucket(s3_client):
    s3_client.create_bucket(
        Bucket='ingestion-data-bucket-marble',
        CreateBucketConfiguration={
            'LocationConstraint': 'eu-west-2'
        }
    )

    s3_client.put_object(
        Bucket="ingestion-data-bucket-marble",
        Key="2023-01-01 00:00:00-test-table.csv")

    assert check_objects()


def test_should_return_True_if_more_than_one_object_is_in_the_bucket(
        s3_client):
    s3_client.create_bucket(
        Bucket='ingestion-data-bucket-marble',
        CreateBucketConfiguration={
            'LocationConstraint': 'eu-west-2'
        }
    )

    s3_client.put_object(
        Bucket="ingestion-data-bucket-marble",
        Key="2023-01-01 00:00:00-test-table.csv")
    s3_client.put_object(
        Bucket="ingestion-data-bucket-marble",
        Key="2023-01-01 01:00:00-test-table.csv")

    assert check_objects()


def test_raises_client_error_in_handler_if_target_bucket_does_not_exist(
    s3_client,
):
    with raises(ClientError):
        s3_client.put_object(
            Bucket="no-bucket", Key="2023-01-01 00:00:00-test-table.csv"
        )
