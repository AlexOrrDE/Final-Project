import pandas as pd
# import pyarrow as pa
# import pyarrow.parquet as pq
import boto3

# import pyarrow.Table as Table

from datetime import datetime


def write_to_s3_processed(df, s3_bucket, s3_key):

    # convert pandas dataframe to pyarrow table
    table = pd.Table.from_pandas(df)

    # make a new time stamped key before writing to s3 bucket
    timestamped_key = datetime.now().strftime(f"%Y/%m/%d/{s3_key}/%H:%M")

    
    # write to the given processed s3 bucket
    boto3.client("s3").put_object(
      Bucket=s3_bucket, s3_key=timestamped_key, Body=table)
    
    # pq.write_table(table, f"s3://{s3_bucket}/{timestamped_key}")
    return s3_key
                   

data = {
        'staff_id': [1, 2, 3, 4],
        'first_name': ['Jeremie', 'John', 'Jane', 'Jim'],
        'last_name': ['Franey', 'Doe', 'Doe', 'Beam'],
        'department_id': [2, 3, 4, 5],
        'email_address': ['jeremie.franey@example.com', 'john.doe@example.com', 
                          'jane.doe@example.com', 'jim.beam@example.com'],
        'created_at': [pd.Timestamp('2022-11-03 14:20:51.563000'), 
                       pd.Timestamp('2022-11-04 14:20:51.563000'), 
                       pd.Timestamp('2022-11-05 14:20:51.563000'), 
                       pd.Timestamp('2022-11-06 14:20:51.563000')],
        'last_updated': [pd.Timestamp('2022-11-03 14:20:51.563000'), 
                         pd.Timestamp('2022-11-04 14:20:51.563000'), 
                         pd.Timestamp('2022-11-05 14:20:51.563000'), 
                         pd.Timestamp('2022-11-06 14:20:51.563000')]
    }

df = pd.DataFrame(data)
s3_key = write_to_s3_processed(df, "processing-data-bucket-marble", "staff")
print(s3_key)                   
