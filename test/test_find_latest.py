from src.ingestion.find_latest import (get_previous_update_dt)
import boto3
from botocore.exceptions import ClientError
from moto import mock_s3
import os
import pytest
from pytest import raises


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    with mock_s3():
        yield boto3.client("s3", region_name="eu-west-2")


def test_extracts_datetime_from_keys_when_given_tablename_to_search(
    s3_client,
):
    s3_client.create_bucket(
        Bucket="ingestion-data-bucket-marble",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    s3_client.put_object(
        Bucket="ingestion-data-bucket-marble",
        Key="2023-01-01 00:00:00-test-table.csv")

    assert get_previous_update_dt("test-table") == "2023-01-01 00:00:00"


def test_should_extract_most_recent_datetime_from_keys(s3_client):
    s3_client.create_bucket(
        Bucket="ingestion-data-bucket-marble",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    s3_client.put_object(
        Bucket="ingestion-data-bucket-marble",
        Key="2021-01-01 00:00:00-test-table.csv")
    s3_client.put_object(
        Bucket="ingestion-data-bucket-marble",
        Key="2023-01-01 00:00:00-test-table.csv")
    s3_client.put_object(
        Bucket="ingestion-data-bucket-marble",
        Key="2022-01-01 00:00:00-test-table.csv")

    assert get_previous_update_dt("test-table") == "2023-01-01 00:00:00"


def test_should_extract_datetime_from_correct_table(s3_client):
    s3_client.create_bucket(
        Bucket='ingestion-data-bucket-marble',
        CreateBucketConfiguration={
            'LocationConstraint': 'eu-west-2'
        }
    )
    s3_client.put_object(
        Bucket="ingestion-data-bucket-marble",
        Key="2021-01-01 00:00:00-test-not-this-table.csv",
    )
    s3_client.put_object(
        Bucket="ingestion-data-bucket-marble",
        Key="2023-01-01 00:00:00-test-not-this-table-either.csv",
    )
    s3_client.put_object(
        Bucket="ingestion-data-bucket-marble",
        Key="2022-01-01 00:00:00-test-table.csv")
    s3_client.put_object(
        Bucket="ingestion-data-bucket-marble",
        Key="2023-01-01 00:00:00-test-table-with-same-date.csv",
    )

    assert get_previous_update_dt("test-table") == "2022-01-01 00:00:00"


def test_should_return_false_if_no_matches_are_found_in_the_bucket(s3_client):
    s3_client.create_bucket(
        Bucket='ingestion-data-bucket-marble',
        CreateBucketConfiguration={
            'LocationConstraint': 'eu-west-2'
        }
    )
    assert get_previous_update_dt("test-table") is False


def test_raises_client_errors_to_be_handled_if_target_bucket_does_not_exist(
    s3_client,
):
    with raises(ClientError):
        s3_client.create_bucket(
            Bucket='ingestion-data-bucket-marble',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            }
        )
        s3_client.put_object(
            Bucket="no-bucket",
            Key="2023-01-01 00:00:00-test-table-with-same-date.csv",
        )


#
