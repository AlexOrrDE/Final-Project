import json
import boto3
import logging
from pg8000.native import Connection, InterfaceError


class InvalidStoredCredentials(Exception):
    pass


def connect_to_database():
    """Starts connection with totesys database.

    Typical usage example:

      conn = connect_to_database()
      data = get_data(conn)
    """

    try:
        totesys_credentials = retrieve_totesys_credentials("Totesys-Credentials")
        return Connection(
            host=totesys_credentials["host"],
            port=totesys_credentials["port"],
            database=totesys_credentials["database"],
            user=totesys_credentials["user"],
            password=totesys_credentials["password"],
        )

    except InterfaceError as db_connection_error:
        logging.info("Error: %s", db_connection_error)
        raise db_connection_error


def retrieve_totesys_credentials(secret_name):
    """
    Retrieve database credentials from AWS Secrets Manager.

    This function retrieves database connection credentials from AWS Secrets Manager
    based on the provided `secret_name`.

    Args:
        secret_name (str): The name of the secret containing the credentials in AWS Secrets Manager.

    Returns:
        A dictionary containing the database connection credentials, including:
            - 'host': The hostname of the database server.
            - 'port': The port number for the database server.
            - 'database': The name of the database.
            - 'user': The username for database authentication.
            - 'password': The password for database authentication.

    Raises:
        - InvalidStoredCredentials: If the retrieved credentials do not contain the expected keys
        ('host', 'port', 'database', 'user', 'password').
        - JSONDecodeError: If there is an issue with decoding the JSON content.
    """

    secrets_manager = boto3.client("secretsmanager")
    response = secrets_manager.get_secret_value(SecretId=secret_name)

    try:
        credentials = json.loads(response["SecretString"])

    except json.JSONDecodeError as decode_error:
        logging.info("Error: %s", decode_error)
        raise decode_error

    valid_keys = ["host", "port", "database", "user", "password"]
    credentials_keys = list(credentials.keys())

    if sorted(valid_keys) != sorted(credentials_keys):
        raise InvalidStoredCredentials()

    return credentials
