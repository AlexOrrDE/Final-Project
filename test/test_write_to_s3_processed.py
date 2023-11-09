from src.processing.write_to_s3_processed import write_to_s3_processed
from moto import mock_s3
import pytest
import boto3
import os
import pandas as pd
import datetime as datetime
from freezegun import freeze_time


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


@freeze_time("2023-01-01")
def get_date():
    return datetime.date.today()

test_date = get_date()
df = pd.DataFrame({
    "date_id": [test_date.strftime("%Y-%m-%d")],
    "date": [test_date.strftime("%Y-%m-%d")],
    "year": [test_date.year],
    "month": [test_date.month],
    "day": [test_date.day],
    "day_of_week": [test_date.weekday()],
    "day_name": [test_date.strftime("%A")],
    "month_name": [test_date.strftime("%B")],
    "quarter": [(test_date.month-1)//3 + 1]
})

def test_write_to_s3_adds_file_to_test_bucket(aws_credentials, s3_client):
    """Test to check the write_to_s3 function is able
    to add the correct item to a bucket"""

    s3_client.create_bucket(
        Bucket="processed-data-bucket-marble",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    assert "Contents" not in s3_client.list_objects(
        Bucket="processed-data-bucket-marble")
    
    write_to_s3_processed(df, "processed-data-bucket-marble", "dim_date")

    assert (len(s3_client.list_objects(
        Bucket="processed-data-bucket-marble")["Contents"]) == 1)


def xtest_write_to_s3_processed_adds_correct_prefix_and_suffix_to_filename_on_upload(
        s3_client):
    s3_client.create_bucket(
        Bucket="processed-data-bucket-marble",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    write_to_s3_processed("test-table", "test-data")
    assert (s3_client.list_objects(Bucket="processed-data-bucket-marble")
            ["Contents"][0]["Key"] == "2023/01/01/test-table/00:00.csv")


def xtest_should_raise_client_error_if_bucket_does_not_exist(s3_client):
    with raises(ClientError):
        s3_client.create_bucket(
            Bucket='not-processed-data-bucket-marble',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            }
        )

        write_to_s3_processed("test-table", "csv-data")


def xtest_should_raise_type_error_if_called_with_incorrect_parameters(
        s3_client):
    with raises(TypeError):
        s3_client.create_bucket(
            Bucket='not-processed-data-bucket-marble',
            CreateBucketConfiguration={
                'LocationConstraint': 'eu-west-2'
            }
        )

        write_to_s3_processed()
