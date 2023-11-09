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
from convert_to_parquet import convert_to_parquet
import logging

logging.getLogger().setLevel(logging.INFO)

function_dict = {
    "counterparty": create_dim_counterparty,
    "currency": create_dim_currency,
    "date": create_dim_date,
    "design": create_dim_design,
    "location": create_dim_location,
    "staff": create_dim_staff,
    "sales_order": create_fact_sales_order}


def handler(event, context):
    s3 = boto3.client('s3')
    for table_name in event:
        logging.info(event)
        key = event[table_name]
        update_data = s3.get_object(
            Bucket='ingestion-data-bucket-marble', Key=key)
        read_update_data = update_data['Body'].read().decode('utf-8')
        update_file = io.StringIO(read_update_data)
        df = pd.read_csv(update_file, index_col=False)
        merged = table_merge(df)
        try:
            our_func = function_dict[table_name]
            result = our_func(merged)
        except BaseException:
            pass
        write_to_s3(key, result)
