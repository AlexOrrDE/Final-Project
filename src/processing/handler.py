import boto3
import io
import pandas as pd
from write_to_s3 import write_to_s3
from dimensions_fact.dim_counterparty import create_dim_counterparty
from dimensions_fact.dim_currency import create_dim_currency
from dimensions_fact.dim_date import create_dim_date
from dimensions_fact.dim_design import create_dim_design
from dimensions_fact.dim_location import create_dim_location
from dimensions_fact.dim_staff import create_dim_staff
from dimensions_fact.fact_sales_order import create_fact_sales_order
from table_merge import table_merge
import logging
from convert_to_parquet import convert_to_parquet

logging.getLogger().setLevel(logging.INFO)

def get_bucket_name_by_prefix(bucket_prefix):
    """
    Retrieve an AWS S3 bucket by searching for a specific prefix in its name.

    Parameters:
    - bucket_prefix (str): The prefix to search for in S3 bucket names.

    Returns:
    - str or None: If a matching bucket is found, returns the bucket name. If no
      matching bucket is found, returns None.
    """
    s3 = boto3.client('s3')
    response = s3.list_buckets()
    for bucket in response['Buckets']:
        if bucket['Name'].startswith(bucket_prefix):
            return bucket['Name']

function_dict = {
    "counterparty": create_dim_counterparty,
    "currency": create_dim_currency,
    "date": create_dim_date,
    "design": create_dim_design,
    "address": create_dim_location,
    "staff": create_dim_staff,
    "sales_order": create_fact_sales_order,
}


def handler(event, context):
    logging.info("Processing tables:")
    logging.info(event)
    bucket = get_bucket_name_by_prefix("ingestion-data-bucket")
    s3 = boto3.client("s3")
    

    check_dim_date = list(bucket.objects.all())
    if "dim_date" not in check_dim_date:
        our_func = function_dict["date"]
        result = our_func()
        returned_parquet = convert_to_parquet(result)
        write_to_s3("dim_date", returned_parquet)

    for table_name in event:
        logging.info("Current table is:")
        logging.info(table_name)
        if table_name in function_dict:
            key = event[table_name]
            update_data = s3.get_object(
                Bucket=bucket, Key=key)

            read_update_data = update_data["Body"].read().decode("utf-8")
            update_file = io.StringIO(read_update_data)

            df = pd.read_csv(update_file, index_col=False)
            merged = table_merge(df)
            our_func = function_dict[table_name]
            result = our_func(merged)

            key = key[:-4]
            returned_parquet = convert_to_parquet(result)

            logging.info("Writing file:")
            logging.info(key)
            write_to_s3(key, returned_parquet)
