import os
import io
import pandas as pd
from pandas import Timestamp
import pytest
import boto3
from moto import mock_s3
from src.processing.table_merge import table_merge
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
            'last_updated': Timestamp('2023-01-01 00:00:00.000000')}, {
            'counterparty_id': 2,
            'counterparty_legal_name': 'cp_2',
            'legal_address_id': 3,
            'commercial_contact': 'test',
            'delivery_contact': 'test',
            'created_at': Timestamp('2023-01-01 00:00:00.000000'),
            'last_updated': Timestamp('2023-01-01 00:00:00.000000')}, {
            'counterparty_id': 3,
            'counterparty_legal_name': 'cp_3',
            'legal_address_id': 2,
            'commercial_contact': 'test',
            'delivery_contact': 'test',
            'created_at': Timestamp('2023-01-01 00:00:00.000000'),
            'last_updated': Timestamp('2023-01-01 00:00:00.000000')}]
            }


@pytest.fixture(scope="function")
def create_staff_data():
    yield {
        'table_name': 'counterparty',
        'data': [{
            'staff_id': 1,
            'first_name': 'staff_name_1',
            'last_name': 'staff_surname_1',
            'department_id': 1,
            'email_address': 'test@test.com',
            'created_at': Timestamp('2023-01-01 00:00:00.000000'),
            'last_updated': Timestamp('2023-01-01 00:00:00.000000')}, {
            'staff_id': 1,
            'first_name': 'staff_name_2',
            'last_name': 'staff_surname_2',
            'department_id': 2,
            'email_address': 'test@test.com',
            'created_at': Timestamp('2023-01-01 00:00:00.000000'),
            'last_updated': Timestamp('2023-01-01 00:00:00.000000')}, {
            'staff_id': 1,
            'first_name': 'staff_name_3',
            'last_name': 'staff_surname_3',
            'department_id': 3,
            'email_address': 'test@test.com',
            'created_at': Timestamp('2023-01-01 00:00:00.000000'),
            'last_updated': Timestamp('2023-01-01 00:00:00.000000')}]
            }


@pytest.fixture(scope="function")
def create_department_data():
    yield {
        'table_name': 'counterparty',
        'data': [{
            'department_id': 1,
            'department_name': 'dept_1',
            'location': 1,
            'manager': 'test',
            'created_at': Timestamp('2023-01-01 00:00:00.000000'),
            'last_updated': Timestamp('2023-01-01 00:00:00.000000')}, {
            'department_id': 2,
            'department_name': 'dept_2',
            'location': 3,
            'manager': 'test',
            'created_at': Timestamp('2023-01-01 00:00:00.000000'),
            'last_updated': Timestamp('2023-01-01 00:00:00.000000')}, {
            'department_id': 3,
            'department_name': 'dept_3',
            'location': 2,
            'manager': 'test',
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


@pytest.fixture(scope="function")
def create_updated_address_data():
    yield {
        'table_name': 'address',
        'data': [{
            'address_id': 1,
            'address_line_1': 'updated_add_1',
            'address_line_2': 'add_1_2',
            'district': 'test_district',
            'city': 'test_city',
            'postal_code': 'test_post',
            'country': 'test_country',
            'phone': 'test_phone',
            'created_at': Timestamp('2023-01-02 00:00:00.000000'),
            'last_updated': Timestamp('2023-01-02 00:00:00.000000')},
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


def test_table_merge_combines_counterparty_and_address(
        create_bucket, s3_client, create_counterparty_data,
        create_address_data):
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
    buffer = io.StringIO(cp_csv)
    source_df = pd.read_csv(buffer)
    result = table_merge(source_df)
    assert 'address_id' in result
    assert 'counterparty_id' in result
    assert result is not source_df


def test_table_merge_combines_staff_and_department(
        create_bucket, s3_client, create_staff_data,
        create_department_data):
    staff_name, staff_csv = convert_to_csv(create_staff_data)
    dept_name, dept_csv = convert_to_csv(create_department_data)

    s3_client.put_object(
        Bucket="ingestion-data-bucket-marble",
        Key="2023/01/01/staff/00:00.csv",
        Body=staff_csv)
    s3_client.put_object(
        Bucket="ingestion-data-bucket-marble",
        Key="2023/01/01/department/00:00.csv",
        Body=dept_csv)
    buffer = io.StringIO(staff_csv)
    source_df = pd.read_csv(buffer)
    result = table_merge(source_df)
    assert 'staff_id' in result
    assert 'department_id' in result
    assert result is not source_df


def test_table_merge_returns_table_if_no_merge_needed(
        create_bucket, s3_client,
        create_department_data):
    dept_name, dept_csv = convert_to_csv(create_department_data)
    s3_client.put_object(
        Bucket="ingestion-data-bucket-marble",
        Key="2023/01/01/department/00:00.csv",
        Body=dept_csv)
    buffer = io.StringIO(dept_csv)
    source_df = pd.read_csv(buffer)
    result = table_merge(source_df)
    assert result is source_df


def test_table_merge_gets_latest_version_of_file(
        create_bucket, s3_client, create_counterparty_data,
        create_address_data, create_updated_address_data):
    cp_name, cp_csv = convert_to_csv(create_counterparty_data)
    ad_name, ad_csv = convert_to_csv(create_address_data)
    updated_ad_name, updated_ad_csv = convert_to_csv(
        create_updated_address_data)

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
        Key="2023/01/01/address/00:01.csv",
        Body=updated_ad_csv)

    buffer = io.StringIO(cp_csv)
    source_df = pd.read_csv(buffer)
    result = table_merge(source_df)
    print(result)
    assert result.at[0, 'address_line_1'] == 'updated_add_1'


def test_table_merge_raises_key_error_if_table_is_not_in_bucket(
        create_staff_data, create_bucket, s3_client):
    staff_name, staff_csv = convert_to_csv(create_staff_data)
    buffer = io.StringIO(staff_csv)
    source_df = pd.read_csv(buffer)
    with pytest.raises(KeyError):
        table_merge(source_df)
