import json
import os
from datetime import datetime
import boto3
import pytest
from moto import mock_s3, mock_secretsmanager
from dotenv import load_dotenv, find_dotenv
from src.ingestion.handler import handler


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def db_credentials():
    """Actual credentials for db, create a .env file in root
    directory to store locally."""
    load_dotenv(find_dotenv())
    cred_dict = {
        "host": os.environ.get("HOST"),
        "port": os.environ.get("PORT"),
        "database": os.environ.get("DATABASE"),
        "user": os.environ.get("USERNAME"),
        "password": os.environ.get("PASSWORD"),
    }
    yield json.dumps(cred_dict)


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    """Mocks the call to the AWS S3 client."""
    with mock_s3():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.fixture(scope="function")
def secrets_client(aws_credentials):
    """Mocks the call to the AWS SecretsManager client."""
    with mock_secretsmanager():
        yield boto3.client("secretsmanager", region_name="eu-west-2")


def test_handler_logs_bucket_empty_and_pulling_dataset_when_needed(
    s3_client, secrets_client, caplog, db_credentials
):
    """Tests the handler produces logs for pulling data when a bucket is empty.
    Tests for incorrect logs being sent."""

    secrets_client.create_secret(
        Name="Totesys-Credentials", SecretString=db_credentials
    )
    s3_client.create_bucket(
        Bucket="ingestion-data-bucket-marble",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    handler("event", "context")

    assert "Bucket is empty." in caplog.text
    assert "Pulling dataset." in caplog.text
    assert "No need to update." not in caplog.text


def test_handler_logs_no_need_to_update_if_bucket_has_file(
    s3_client, secrets_client, caplog, db_credentials
):
    """Tests the handler produces logs for not pulling data
    when most recent file is up to date. Tests for incorrect
    logs being sent."""
    secrets_client.create_secret(
        Name="Totesys-Credentials", SecretString=db_credentials
    )
    s3_client.create_bucket(
        Bucket="ingestion-data-bucket-marble",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    file_names = [
        "currency",
        "payment",
        "department",
        "design",
        "counterparty",
        "purchase_order",
        "payment_type",
        "sales_order",
        "address",
        "staff",
        "transaction",
    ]
    prefix = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for name in file_names:
        s3_client.put_object(
            Bucket="ingestion-data-bucket-marble", Key=f"{prefix}-{name}.csv"
        )

    handler("event", "context")

    assert "Bucket is empty." not in caplog.text
    assert "Pulling dataset." not in caplog.text
    assert "No need to update." in caplog.text
