import pandas as pd
import pyarrow as pa
# import pyarrow.parquet as pq
import boto3

# import pyarrow.Table as Table

from datetime import datetime


def write_to_s3_processed(df, s3_bucket, s3_key):

    # convert pandas dataframe to pyarrow table
    table = pa.Table.from_pandas(df)

    # make a new time stamped key before writing to s3 bucket
    timestamped_key = datetime.now().strftime(f"%Y/%m/%d/{s3_key}/%H:%M")

    
    # write to the given processed s3 bucket
    boto3.client("s3").put_object(
      Bucket=s3_bucket, s3_key=timestamped_key, Body=table)
    
    # pq.write_table(table, f"s3://{s3_bucket}/{timestamped_key}")
    return s3_key
                   


# def write_to_s3(
#         key,
#         parquet,
#         bucket_name="processing-data-bucket-marble"):
#     """Uploads files to AWS s3 bucket.

#     Puts objects in s3 bucket with a timestamp in the filename.

#     Typical usage example:

#       write_to_s3(file_name, file_data)
#     """

#     key_without_csv = key[:-4]

#     s3_key = f"{key}.parquet"
#     boto3.client("s3").put_object(
#       Bucket=bucket_name, Key=s3_key, Body=parquet
#     )

#     return s3_key

                   
