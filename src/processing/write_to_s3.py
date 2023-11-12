import boto3


def write_to_s3(key, parquet, bucket_name="processed-data-bucket-marble"):
    """Uploads files to AWS s3 bucket, appropriately renamed.

    Puts objects in s3 bucket with a timestamp in the filename.

    Typical usage example:

      write_to_s3(file_name, file_data)
    """

    name_replacements = {
        "currency": "dim_currency",
        "address": "dim_location",
        "counterparty": "dim_counterparty",
        "design": "dim_design",
        "staff": "dim_staff",
        "sales_order": "fact_sales_order",
    }

    for old_str, new_str in name_replacements.items():
        key = key.replace(old_str, new_str)

    s3_key = f"{key}.parquet"
    boto3.client("s3").put_object(Bucket=bucket_name, Key=s3_key, Body=parquet)

    return s3_key
