import boto3
import logging
from botocore.exceptions import ClientError
from datetime import datetime


def write_to_s3(
        table_name,
        csv_data,
        bucket_name="ingestion-data-bucket-marble"):
    """Uploads files to AWS s3 bucket.

    Puts objects in s3 bucket with a timestamp in the filename.

    Typical usage example:

      write_to_s3(file_name, file_data)
    """
    try:
        name_prefix = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        s3_key = f"{name_prefix}-{table_name}.csv"
        boto3.client("s3").put_object(
            Bucket=bucket_name, Key=s3_key, Body=csv_data
        )
    except ClientError as ce:
        logging.error("Error occured in write_to_s3")
        raise ce
    except TypeError as te:
        logging.error("Error occured in write_to_s3")
        raise te
