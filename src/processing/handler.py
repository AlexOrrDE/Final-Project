import boto3
import io
import pandas as pd
from write_to_s3 import write_to_s3

def handler(event, context):
    s3 = boto3.client('s3')
    for table_name in event:
        key = event[table_name]
        update_data = s3.get_object(Bucket='ingestion-data-bucket-marble', Key=key)
        read_update_data = update_data['Body'].read().decode('utf-8')
        update_file = io.StringIO(read_update_data)
        df = pd.read_csv(update_file, index_col=False)
        # Josh has this
        # merge_tables(df)
        # Alex, Ryman, Shabbir
        # alex_rymans_function(merge_tables(df))
        # Alex has this
        # returned_parquet = convert_to_parquet()
        # write_to_s3(key, returned_parquet)