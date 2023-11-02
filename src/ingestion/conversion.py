import pandas as pd
import boto3
from datetime import datetime


def convert_to_csv(table_data):
    """Converts data format to .csv.

    Typical usage example:

      file_name, csv_data = convert_to_csv(file_data)
    """
    table_name = table_data["table_name"]
    csv_data = pd.DataFrame(table_data["data"]).to_csv(index=False)
    return table_name, csv_data


def write_to_s3(table_name, csv_data, bucket_name="marble-test-bucket"):
    """Uploads files to AWS s3 bucket.

    Puts objects in s3 bucket with a timestamp in the filename.

    Typical usage example:

      write_to_s3(file_name, file_data)
    """
    name_prefix = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    s3_key = f"{name_prefix}-{table_name}.csv"
    boto3.client("s3").put_object(
        Bucket=bucket_name, Key=s3_key, Body=csv_data
    )
