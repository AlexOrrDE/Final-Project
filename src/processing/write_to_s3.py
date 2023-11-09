import boto3
from datetime import datetime


def write_to_s3(
        key,
        parquet,
        bucket_name="processing-data-bucket-marble"):
    """Uploads files to AWS s3 bucket.

    Puts objects in s3 bucket with a timestamp in the filename.

    Typical usage example:

      write_to_s3(file_name, file_data)
    """

    key_without_csv = key[:-4]

    s3_key = f"{key}.parquet"
    boto3.client("s3").put_object(
      Bucket=bucket_name, Key=s3_key, Body=parquet
    )

    return s3_key
