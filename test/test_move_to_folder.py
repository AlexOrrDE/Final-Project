from src.ingestion.move_to_folder import move_files_to_folder
import boto3
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


def test_should_return_take_2_parameters(s3_client):
    """raises type error when invoked with incorrect parameters"""
    with raises(TypeError):
        s3_client.create_bucket(
            Bucket="ingestion-data-bucket-marble",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        move_files_to_folder()


def test_should_create_new_folder_named_with_timestamp_argument(s3_client):
    s3_client.create_bucket(
        Bucket="ingestion-data-bucket-marble",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    s3_client.put_object(
        Bucket="ingestion-data-bucket-marble",
        Key="2023-01-01 00:00:00-test-table.csv")

    timestamp = "2023-01-01 00:00:00"

    move_files_to_folder(timestamp)

    assert (f"{timestamp}/" in s3_client.list_objects(
        Bucket="ingestion-data-bucket-marble")["Contents"][0]["Key"])


def test_should_group_files_with_same_prefix_timestamp_in_same_folder(
        s3_client):
    s3_client.create_bucket(
        Bucket="ingestion-data-bucket-marble",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    s3_client.put_object(
        Bucket="ingestion-data-bucket-marble",
        Key="2023-01-01 00:00:00-test-table.csv")
    s3_client.put_object(
        Bucket="ingestion-data-bucket-marble",
        Key="2023-01-01 00:00:00-staff-table.csv")

    timestamp = "2023-01-01 00:00:00.000000"
    key1 = "2023-01-01 00:00:00-test-table.csv"
    key2 = "2023-01-01 00:00:00-staff-table.csv"

    move_files_to_folder(timestamp)

    assert (f"{timestamp}/{key1}" in s3_client.list_objects(
        Bucket="ingestion-data-bucket-marble")["Contents"][1]["Key"])
    assert (f"{timestamp}/{key2}" in s3_client.list_objects(
        Bucket="ingestion-data-bucket-marble")["Contents"][0]["Key"])


def test_should_delete_previous_files_in_root_of_bucket(s3_client):
    s3_client.create_bucket(
        Bucket="ingestion-data-bucket-marble",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    s3_client.put_object(
        Bucket="ingestion-data-bucket-marble",
        Key="2023-01-01 00:00:00-test-table.csv")
    s3_client.put_object(
        Bucket="ingestion-data-bucket-marble",
        Key="2023-01-01 00:00:00-staff-table.csv")

    timestamp = "2023-01-01 00:00:00.000000"
    key1 = "2023-01-01 00:00:00-test-table.csv"
    key2 = "2023-01-01 00:00:00-staff-table.csv"

    move_files_to_folder(timestamp)

    result = s3_client.list_objects(
        Bucket="ingestion-data-bucket-marble")["Contents"]

    assert (s3_client.list_objects(Bucket="ingestion-data-bucket-marble")
            ["Contents"][1]["Key"] is not key1)
    assert (s3_client.list_objects(Bucket="ingestion-data-bucket-marble")
            ["Contents"][1]["Key"] is not key2)
    for object in result:
        assert "/" in object["Key"]
