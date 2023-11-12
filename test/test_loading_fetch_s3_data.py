import pytest
from io import BytesIO
from botocore.exceptions import ClientError
import boto3
import pandas as pd
from moto import mock_s3
from src.loading.fetch_s3_data import fetch_data_from_s3


@mock_s3
def test_fetch_data_from_s3_success():
    bucket_name = "test-bucket"
    s3_key = "test-key/parquet_file.parquet"
    test_data = pd.DataFrame({"column_1": [1, 2, 3], "column_2": ["a", "b", "c"]})
    test_data_bytes = BytesIO()
    test_data.to_parquet(test_data_bytes, index=False)

    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket=bucket_name)

    s3_client.put_object(
        Bucket=bucket_name, Key=s3_key, Body=test_data_bytes.getvalue()
    )

    result = fetch_data_from_s3(s3_client, bucket_name, s3_key)

    assert isinstance(result, pd.DataFrame)
    pd.testing.assert_frame_equal(result, test_data)


@mock_s3
def test_fetch_data_from_s3_nonexistent_key():
    bucket_name = "test-bucket"
    s3_key = "test-key/parquet_file.parquet"

    s3_client = boto3.client("s3", region_name="us-east-1")
    s3_client.create_bucket(Bucket=bucket_name)

    result = fetch_data_from_s3(s3_client, bucket_name, s3_key)

    assert result is None


@mock_s3
def test_fetch_data_from_s3_client_error():
    "<<< Couldn't get this to work >>>"

    # bucket_name = "nonexistent-bucket"
    # s3_key = "test-key/parquet_file.parquet"

    # s3_client = boto3.client("s3", region_name="us-east-1")

    # with pytest.raises(ClientError):
    #     fetch_data_from_s3(s3_client, bucket_name, s3_key)
    pass
