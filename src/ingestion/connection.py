import json
import boto3
import logging
from pg8000.dbapi import Connection, InterfaceError


class InvalidStoredCredentials(Exception):
    def __init__(self):
        self.message = "Incorrect credentials"


def connect_to_database(db='Totesys'):
    """Starts a connection with the specified database.

    Args:
        db (str, optional): The name of the database to connect to.
            Defaults to 'Totesys'. If set to 'Warehouse', it connects to
            the data warehouse.

    Returns:
        pg8000.dbapi.Connection: A connection object for the specified database.

    Raises:
        InterfaceError: If there is an error in connecting to the database.
    """

    try:
        credentials = retrieve_credentials(
            f"{db.capitalize()}-Credentials")
        conn = Connection(
            host = credentials["host"],
            port = credentials["port"],
            database = credentials["database"],
            user = credentials["user"],
            password = credentials["password"],
        )
        return conn

    except InterfaceError as db_connection_error:
        logging.error("Error occured in connect_to_database")
        raise db_connection_error


def retrieve_credentials(secret_name):
    """
    Retrieve database credentials from AWS Secrets Manager.

    This function retrieves database connection credentials
    from AWS Secrets Manager based on the provided `secret_name`.

    Args:
        secret_name (str): The name of the secret containing
        the credentials in AWS Secrets Manager.

    Returns:
        A dictionary containing the database connection credentials, including:
            - 'host': The hostname of the database server.
            - 'port': The port number for the database server.
            - 'database': The name of the database.
            - 'user': The username for database authentication.
            - 'password': The password for database authentication.

    Raises:
        - InvalidStoredCredentials: If the retrieved credentials
        do not contain the expected keys
        ('host', 'port', 'database', 'user', 'password').
        - JSONDecodeError: If there is an issue with decoding the JSON content.
    """

    secrets_manager = boto3.client("secretsmanager")
    response = secrets_manager.get_secret_value(SecretId=secret_name)

    try:
        credentials = json.loads(response["SecretString"])

    except json.JSONDecodeError as de:
        logging.info("Error occured in retrieve_credentials")
        raise de

    valid_keys = ["host", "port", "database", "user", "password"]
    credentials_keys = list(credentials.keys())

    if sorted(valid_keys) != sorted(credentials_keys):
        raise InvalidStoredCredentials()

    return credentials


