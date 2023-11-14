from src.ingestion.connection import retrieve_credentials
import psycopg2
import logging


def connect_to_warehouse(db="warehouse"):
    """Starts a connection with the specified database.

    Args:
        db (str, optional): The name of the database to connect to.
            Defaults to 'Totesys'. If set to 'Warehouse', it connects to
            the data warehouse.

    Returns:
        pg8000.dbapi.Connection: A connection object for specified database.

    Raises:
        InterfaceError: If there is an error in connecting to the database.
    """

    try:
        credentials = retrieve_credentials(f"{db.capitalize()}-Credentials")
        conn = psycopg2.connect(
            host=credentials["host"],
            port=credentials["port"],
            database=credentials["database"],
            user=credentials["user"],
            password=credentials["password"],
        )
        return conn

    except psycopg2.InterfaceError as db_connection_error:
        logging.error("Error occured in connect_to_database")
        raise db_connection_error
