from get_table_data import fetch_data_from_tables
import pytest
from pg8000.dbapi import Connection, InterfaceError
import boto3
import time

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""

    AWS_ACCESS_KEY_ID = "testing"
    AWS_SECRET_ACCESS_KEY = "testing"
    AWS_SECURITY_TOKEN = "testing"
    AWS_SESSION_TOKEN = "testing"
    # AWS_DEFAULT_REGION = "eu-west-2"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"
    RDS_INSTANCE_IDENTIFIER = 'test-rds-instance'
    RDS_DB_NAME = 'mydb'
    RDS_USERNAME = 'myuser'
    RDS_PASSWORD = 'mypassword'
    RDS_DB_PORT = 5432

# Initialize a connection to AWS RDS and create the instance
def create_aws_rds_instance(aws_credentials):
    rds_client = boto3.client('rds', region_name = AWS_DEFAULT_REGION, aws_access_key_id = AWS_ACCESS_KEY_ID,                   aws_secret_access_key = AWS_SECRET_KEY_ID)
    
    rds_client.create_db_instance(
        DBInstanceIdentifier=RDS_INSTANCE_IDENTIFIER,
        AllocatedStorage=20,
        DBInstanceClass='db.t2.micro',
        Engine='postgres',
        MasterUsername=RDS_USERNAME,
        MasterUserPassword=RDS_PASSWORD,
        PubliclyAccessible=True
    )

    # Wait for the RDS instance to be available (takes a few minutes)
    while True:
        response = rds_client.describe_db_instances(DBInstanceIdentifier=RDS_INSTANCE_IDENTIFIER)
        status = response['DBInstances'][0]['DBInstanceStatus']
        if status == 'available':
            break
        time.sleep(10)

    # Return the endpoint for the RDS instance
    return response['DBInstances'][0]['Endpoint']['Address']

# The test environment setup
def setup_database(aws_credentials):
    # Create the AWS RDS instance and get its endpoint
    rds_endpoint = create_aws_rds_instance(aws_credentials)

    # Initialize a connection to the AWS RDS
    conn = Connection(
        database=RDS_DB_NAME,
        user=RDS_USERNAME,
        password=RDS_PASSWORD,
        host=rds_endpoint,
        port=RDS_DB_PORT,
    )
    return conn


# This ensures that the setup_database function is called once before any test cases
# This is where we set up AWS RDS instance and tables
@pytest.fixture(autouse=True)
def setup_test_environment(aws_credentials):
    setup_database(aws_credentials)


def test_totesys_fetch_data_from_tables_returns_all_data_from_a_supplied_table(conn):
    """chech that totesys_fetch_data_from_tables returns all of table contents from 

    # Create tables and perform setup operations here
    cursor = conn.cursor()
    query = f"CREATE TABLE IF NOT EXISTS staff
        staff_id SERIAL PRIMARY KEY,
        first_name text NOT NULL,
        last_name test NOT NULL,
        department int NOT NULL,
        email_address text NOT NULL,
        created_at timestamp DEFAULT CURRENT_TIMESTAMP,
        last_updated timestamp DEFAULT CURRENT_TIMESTAMP;"
    cursor.execute(query)
    query = f"INSERT INTO staff (first_name, last_name, department, email_address) 
        VALUES ('Jeremie', 'Franey',2,	'jeremie.franey@terrifictotes.com'),
                ('Daron', 'Beier',6, 'deron.beier@terrifictotes.com'),
                ('Jeanette', 'Erdman', 6,'jeanette.erdman@terrifictotes.com');

    # Call your database setup functions here

    # Commit the changes
    conn.commit()


    # Close the connection
    conn.close()
#     a given table name."""
#     # arrange
    
#     # assert
#     assert fetch_data_from_tables(MockConn) == ['table1', 'table2', 'table3']
