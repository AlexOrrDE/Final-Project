from src.ingestion.find_latest import get_previous_update_dt, NoPreviousInstanceError
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


def test_should_extract_datetime_from_keys_when_given_a_tablename_to_search_the_s3_bucket_for(
    s3_client,
):
    s3_client.create_bucket(
        Bucket="marble-test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    s3_client.put_object(
        Bucket="marble-test-bucket", Key="2023-01-01 00:00:00.000000-test-table.csv"
    )

    assert get_previous_update_dt("test-table") == "2023-01-01 00:00:00.000000"


def test_should_extract_most_recent_datetime_from_keys(s3_client):
    s3_client.create_bucket(
        Bucket="marble-test-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    s3_client.put_object(
        Bucket="marble-test-bucket", Key="2021-01-01 00:00:00.000000-test-table.csv"
    )
    s3_client.put_object(
        Bucket="marble-test-bucket", Key="2023-01-01 00:00:00.000000-test-table.csv"
    )
    s3_client.put_object(
        Bucket="marble-test-bucket", Key="2022-01-01 00:00:00.000000-test-table.csv"
    )

    assert get_previous_update_dt("test-table") == "2023-01-01 00:00:00.000000"


def test_should_extract_datetime_from_correct_table(s3_client):
    s3_client.create_bucket(
            Bucket='marble-test-bucket',
            CreateBucketConfiguration={
            'LocationConstraint': 'eu-west-2'
        }
    )
    s3_client.put_object(
        Bucket="marble-test-bucket",
        Key="2021-01-01 00:00:00.000000-test-not-this-table.csv",
    )
    s3_client.put_object(
        Bucket="marble-test-bucket",
        Key="2023-01-01 00:00:00.000000-test-not-this-table-either.csv",
    )
    s3_client.put_object(
        Bucket="marble-test-bucket", Key="2022-01-01 00:00:00.000000-test-table.csv"
    )
    s3_client.put_object(
        Bucket="marble-test-bucket",
        Key="2023-01-01 00:00:00.000000-test-table-with-same-date.csv",
    )

    assert get_previous_update_dt("test-table") == "2022-01-01 00:00:00.000000"


def test_should_raise_error_if_no_matches_are_found_in_the_bucket(s3_client):
    with raises(NoPreviousInstanceError):
        s3_client.create_bucket(
            Bucket='marble-test-bucket',
            CreateBucketConfiguration={
            'LocationConstraint': 'eu-west-2'
            }
        )
        get_previous_update_dt("test-table")


def test_should_raise_client_errors_to_be_handled_in_the_handler_if_target_bucket_does_not_exist(
    s3_client,
):
    with raises(ClientError):
        s3_client.create_bucket(
            Bucket='marble-test-bucket',
            CreateBucketConfiguration={
            'LocationConstraint': 'eu-west-2'
            }
        )
        s3_client.put_object(
            Bucket="no-bucket",
            Key="2023-01-01 00:00:00.000000-test-table-with-same-date.csv",
        )


#
