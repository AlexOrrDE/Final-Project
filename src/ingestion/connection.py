import json
import boto3
import logging
import pg8000


class InvalidStoredCredentials(Exception):
    pass


def connect_to_database():
    return pg8000.dbapi.Connection(
        user="project_user_1",
        host="nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com",
        database="totesys",
        port=5432,
        password="WfAsWSh4nvEUEOw6",
    )



#     """Starts connection with totesys database.

#     Typical usage example:

#       conn = connect_to_database()
#       data = get_data(conn)
#     """

#     try:
#         totesys_credentials = retrieve_totesys_credentials("Totesys-Credentials")
#         return Connection(
#             host=totesys_credentials["host"],
#             port=totesys_credentials["port"],
#             database=totesys_credentials["database"],
#             user=totesys_credentials["user"],
#             password=totesys_credentials["password"],
#         )

#     except InterfaceError as db_connection_error:
#         logging.info("Error: %s", db_connection_error)


# def retrieve_totesys_credentials(secret_name):
#     """INSERT DOC STRING"""

#     secrets_manager = boto3.client("secretsmanager")
#     response = secrets_manager.get_secret_value(SecretId=secret_name)

#     try:
#         credentials = json.loads(response["SecretString"])

#     except json.JSONDecodeError as decode_error:
#         logging.info("Error: %s", decode_error)

#     valid_keys = ["host", "port", "database", "user", "password"]
#     credentials_keys = list(credentials.keys())

#     if sorted(valid_keys) != sorted(credentials_keys):
#         raise InvalidStoredCredentials()

#     return credentials
