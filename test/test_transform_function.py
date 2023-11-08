import os
import io
import moto
import pandas as pd
from pandas import Timestamp
import pytest
import boto3
from moto import mock_s3
from src.processing.transformer import transformer
from src.ingestion.convert_to_csv import convert_to_csv



@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    """Mocks the call to the AWS S3 client."""
    with mock_s3():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.fixture(scope="function")
def create_bucket(s3_client):
    s3_client.create_bucket(
        Bucket="ingestion-data-bucket-marble",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )


@pytest.fixture(scope="function")
def create_counterparty_data():
    yield {
    'table_name': 'counterparty',
    'data': [{
        'counterparty_id': 1,
        'counterparty_legal_name': 'cp_1',
        'legal_address_id': 1,
        'commercial_contact': 'test',
        'delivery_contact': 'test',
        'created_at': Timestamp('2023-01-01 00:00:00.000000'),
        'last_updated': Timestamp('2023-01-01 00:00:00.000000')},{
        'counterparty_id': 2,
        'counterparty_legal_name': 'cp_2',
        'legal_address_id': 3,
        'commercial_contact': 'test',
        'delivery_contact': 'test',
        'created_at': Timestamp('2023-01-01 00:00:00.000000'),
        'last_updated': Timestamp('2023-01-01 00:00:00.000000')},{
        'counterparty_id': 3,       
        'counterparty_legal_name': 'cp_3',
        'legal_address_id': 2,
        'commercial_contact': 'test',
        'delivery_contact': 'test',
        'created_at': Timestamp('2023-01-01 00:00:00.000000'),
        'last_updated': Timestamp('2023-01-01 00:00:00.000000')}]
        }


@pytest.fixture(scope="function")
def create_address_data():
    yield {
    'table_name': 'address',
    'data': [{
        'address_id': 1,
        'address_line_1': 'add_1',
        'address_line_2': 'add_1_2',
        'district': 'test_district',
        'city': 'test_city',
        'postal_code': 'test_post',
        'country': 'test_country',
        'phone': 'test_phone',
        'created_at': Timestamp('2023-01-01 00:00:00.000000'),
        'last_updated': Timestamp('2023-01-01 00:00:00.000000')},
        {
        'address_id': 2,
        'address_line_1': 'add_2',
        'address_line_2': 'add_2_2',
        'district': 'test_district',
        'city': 'test_city',
        'postal_code': 'test_post',
        'country': 'test_country',
        'phone': 'test_phone',
        'created_at': Timestamp('2023-01-01 00:00:00.000000'),
        'last_updated': Timestamp('2023-01-01 00:00:00.000000')},
        {
        'address_id': 3,
        'address_line_1': 'add_3',
        'address_line_2': 'add_3_3',
        'district': 'test_district',
        'city': 'test_city',
        'postal_code': 'test_post',
        'country': 'test_country',
        'phone': 'test_phone',
        'created_at': Timestamp('2023-01-01 00:00:00.000000'),
        'last_updated': Timestamp('2023-01-01 00:00:00.000000')}]
        }

def test_transformer_is_able_to_find_correct_file(create_bucket, s3_client, create_counterparty_data,create_address_data):
    cp_name, cp_csv = convert_to_csv(create_counterparty_data)
    ad_name, ad_csv = convert_to_csv(create_address_data)

    s3_client.put_object(
        Bucket="ingestion-data-bucket-marble",
        Key="2023/01/01/counterparty/00:00.csv",
        Body=cp_csv)
    s3_client.put_object(
        Bucket="ingestion-data-bucket-marble",
        Key="2023/01/01/address/00:00.csv",
        Body=ad_csv)
    s3_client.put_object(
        Bucket="ingestion-data-bucket-marble",
        Key="2023/02/01/address/00:00.csv",
        Body=ad_csv)
    buffer = io.StringIO(cp_csv)
    df = pd.read_csv(buffer)
    result = transformer(df, 'counter_party', 'address')
    print(result)
    assert False

