import pytest
from src.totesys_connection import retrieve_totesys_credentials, InvalidStoredCredentials, totesys_connection
from moto import mock_secretsmanager
import boto3
from pg8000.native import InterfaceError
import json



@mock_secretsmanager
def test_retrieve_totesys_credentials_returns_dictionary():
    '''check retrieve_totesys_credentials function always return a dictionary when secret stored in a valid json format'''
    client = boto3.client('secretsmanager')
    client.create_secret(Name='Totesys-Credentials',
                         SecretString='''
                        {
                            "host": "x",
                            "port": "x",
                            "database": "x",
                            "user": "x",
                            "password" : "x"
                        }
                        '''
                         )
    output = retrieve_totesys_credentials('Totesys-Credentials')
    assert type(output) == dict
    assert (sorted(list(output))) == ['database', 'host', 'password', 'port', 'user']

@mock_secretsmanager
def test_retrieve_totesys_credentials_returns_error_when_json_invalid():
    '''check retrieve_totesys_credentials function returns a json.JSONDecodeError when secret stored with invalid json format. Example no key-value format'''
    client = boto3.client('secretsmanager')
    client.create_secret(Name='Totesys-Credentials',
                         SecretString='''
                        {   
                            Invalid Json format
                        }
                        '''
                         )
    with pytest.raises(json.JSONDecodeError):
        retrieve_totesys_credentials('Totesys-Credentials')
    


@mock_secretsmanager
def test_retrieve_totesys_credentials_throws_InvalidCredentials_error():
    '''check retrieve_totesys_credentials function returns InvalidStoredCredentials error when secret stored doesn't have all the credentials required to connect. Example when the json is missing a required keys like 'database' for example '''
    client = boto3.client('secretsmanager')
    client.create_secret(Name='Totesys-Credentials',
                         SecretString='''
                        {
                            "host": "x",
                            "port": "x"
                        }
                        '''
                         )

    with pytest.raises(InvalidStoredCredentials):
        retrieve_totesys_credentials('Totesys-Credentials')


@mock_secretsmanager
def test_totesys_connection_throws_InterfaceError_when_cannot_connect_to_database():
    '''chech that totesys_connection returns an InterfaceError when it fails to connect to the datbase, when aws secret has all the required keys but the credentials are wrong.'''
    client = boto3.client('secretsmanager')
    client.create_secret(Name='Totesys-Credentials',
                         SecretString='''
                        {
                            "host": "wrong",
                            "port": "wrong",
                            "database":"wrong",
                            "user": "wrong",
                            "password" : "wrong"
                        }
                        '''
                         )

    with pytest.raises(InterfaceError):
        totesys_connection()