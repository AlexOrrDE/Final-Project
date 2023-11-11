from io import BytesIO
import botocore
import pandas as pd
import logging


def fetch_data_from_s3(s3_client, bucket_name, s3_key):
    """Fetches data from S3 and returns a DataFrame."""

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        return pd.read_parquet(BytesIO(response["Body"].read()))

    except botocore.exceptions.ClientError:
        logging.warning(
            f"The specified key '{s3_key}' does not exist in the S3 bucket."
        )
        return None
