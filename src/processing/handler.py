import boto3
import io
import pandas as pd
from src.processing.write_to_s3 import write_to_s3
from src.processing.dimensions_fact.dim_counterparty import create_dim_counterparty
from src.processing.dimensions_fact.dim_currency import create_dim_currency
from src.processing.dimensions_fact.dim_date import create_dim_date
from src.processing.dimensions_fact.dim_design import create_dim_design
from src.processing.dimensions_fact.dim_location import create_dim_location
from src.processing.dimensions_fact.dim_staff import create_dim_staff
from src.processing.dimensions_fact.fact_sales_order import create_fact_sales_order
from src.processing.table_merge import table_merge
import logging
from src.processing.convert_to_parquet import convert_to_parquet

logging.getLogger().setLevel(logging.INFO)

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
    s3 = boto3.client('s3')
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket("ingestion-data-bucket-marble")
    
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
                Bucket="ingestion-data-bucket-marble", Key=key
            )

            read_update_data = update_data["Body"].read().decode("utf-8")
            update_file = io.StringIO(read_update_data)

            df = pd.read_csv(update_file, index_col=False)
            merged = table_merge(df)
            our_func = function_dict[table_name]
            result = our_func(merged)

            # key = key[:-4]
            returned_parquet = convert_to_parquet(result)
            if "address" in key:
                key = key.replace("address", "location")
            logging.info("Writing file:")
            logging.info(key)
            write_to_s3(key, returned_parquet)
