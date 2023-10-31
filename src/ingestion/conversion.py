import pandas as pd
import boto3
from datetime import datetime

def convert_to_csv(table_data):
    table_name = table_data["table_name"]
    csv_data = pd.DataFrame(table_data["data"]).to_csv(index=False)
    return table_name, csv_data

def write_to_s3(table_name, csv_data):
    name_prefix = datetime.now()
    s3_key = f"{name_prefix}-{table_name}.csv"
    boto3.client('s3').put_object(Bucket='marble-test-bucket', Key=s3_key, Body=csv_data)