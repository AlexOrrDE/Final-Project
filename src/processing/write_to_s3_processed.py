import pandas as df
import pyarrow
import pyarrow.parquet as pq
import boto3

# import pyarrow.Table as Table

from datetime import datetime


def write_to_s3_processed(df, s3_bucket, s3_key):

    # convert pandas dataframe to pyarrow table
    df.to_parquet("table.parquet")

    # make a new time stamped key before writing to s3 bucket
    timestamped_key = datetime.now().strftime(f"%Y/%m/%d/{s3_key}/%H:%M")

    
    # write to the given processed s3 bucket
    boto3.client("s3").put_object(
      Bucket=s3_bucket, Key=timestamped_key, Body=table)
    
    # pq.write_table(table, f"s3://{s3_bucket}/{timestamped_key}")
                   

                   
