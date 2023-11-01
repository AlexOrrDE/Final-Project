import json
import boto3
from pg8000.native import Connection, InterfaceError


class InvalidStoredCredentials(Exception):
    pass
    

def retrieve_totesys_credentials(secret_name):
    secrets_manager = boto3.client('secretsmanager')
    response = secrets_manager.get_secret_value(SecretId=secret_name)
    try:
        credentials = json.loads(response['SecretString'])
    except json.JSONDecodeError as e:
        raise e
    valid_keys = ['host','port','database','user','password']
    credentials_keys = list(credentials.keys())
    if sorted(valid_keys) != sorted(credentials_keys):
        raise InvalidStoredCredentials()
    return credentials

def totesys_connection():
    '''Returns a connection to the source database totesys, using credentials stored on aws secrets manager'''
    try:
        totesys_credentials = retrieve_totesys_credentials('Totesys-Credentials')
        return Connection(
            host=totesys_credentials['host'],
            port=totesys_credentials['port'],
            database=totesys_credentials['database'],
            user=totesys_credentials['user'],
            password=totesys_credentials['password']
        )
    except InterfaceError as e:
        raise e

