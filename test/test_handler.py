from src.ingestion.handler import handler
import boto3
import os
from moto import mock_s3
import pytest
from unittest.mock import patch


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_s3:
        yield boto3.client("s3", region_name="eu-west-2")


def test_handler_calls_connect_to_database():
        pass


# def test_handler_invokes_insert_secret_with_user_input_e(self):
#         with patch("src.password_manager.insert_secret", return_value=True) as mock:
#             handler("e")
#             assert mock.call_count == 1
