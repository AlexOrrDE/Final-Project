import os
import json
import pytest
from moto import mock_secretsmanager
import boto3
from pg8000.native import InterfaceError
from src.ingestion.connection import (
    retrieve_credentials,
    InvalidStoredCredentials,
    connect_to_database,
)


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'test'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
    os.environ['AWS_SECURITY_TOKEN'] = 'test'
    os.environ['AWS_SESSION_TOKEN'] = 'test'
    os.environ['AWS_DEFAULT_REGION'] = 'eu-west-2'


@pytest.fixture(scope="function")
def secrets_client(aws_credentials):
    """Mocks the call to the AWS SecretsManager client."""
    with mock_secretsmanager():
        yield boto3.client("secretsmanager")


def test_retrieve_credentials_returns_dictionary(secrets_client):
    """check retrieve_totesys_credentials function always return
    a dictionary when secret stored in a valid json format"""

    secrets_client.create_secret(
        Name="Totesys-Credentials",
        SecretString="""
                        {
                            "host": "x",
                            "port": "x",
                            "database": "x",
                            "user": "x",
                            "password" : "x"
                        }
                        """,
    )
    output = retrieve_credentials("Totesys-Credentials")
    assert isinstance(output, dict)
    assert (sorted(list(output))) == [
        "database", "host", "password", "port", "user"
    ]


def test_retrieve_credentials_returns_error_when_json_invalid(
        create_invalid_secret):
    """check retrieve_totesys_credentials function returns
    a json.JSONDecodeError when secret stored with invalid json format.
    Example no key-value format"""
    with pytest.raises(json.JSONDecodeError):
        retrieve_credentials("Totesys-Credentials")


def test_retrieve_credentials_throws_InvalidCredentials_error(
        create_wrong_secret):
    """check retrieve_totesys_credentials function returns
    InvalidStoredCredentials error when secret
    stored doesn't have all the required to connect.
    Example when the json is missing a required keys
    like 'database' for example"""
    with pytest.raises(InvalidStoredCredentials):
        retrieve_credentials("Totesys-Credentials")


def test_connection_throws_InterfaceError_when_cannot_connect_to_database(
    create_no_connection_secret
):
    """chech that totesys_connection returns an InterfaceError when
    it fails to connect to the datbase, when aws secret has all
    the required keys but the credentials are wrong."""
    with pytest.raises(InterfaceError):
        connect_to_database()
