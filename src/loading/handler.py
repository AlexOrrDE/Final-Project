from src.loading.psycopg2_conn import connect_to_warehouse
from src.loading.fetch_tables_with_pk import fetch_tables_with_pk
from src.loading.fetch_s3_data import fetch_data_from_s3
from src.loading.upload_to_warehouse import upload_to_warehouse
import boto3
import logging
import re

logging.basicConfig(level=logging.INFO)


def handler(event, context):
    """Fetches parquet files from S3 and uploads data to warehouse."""

    bucket_name = "processed-data-bucket-marble"
    s3 = boto3.client("s3")
    s3_resource = boto3.resource("s3")

    try:
        conn = connect_to_database()
        logging.info("Connected to the local database")

        tables_names = fetch_tables_with_pk(conn)
        response = s3.list_objects_v2(Bucket=bucket_name)
        keys = [obj["Key"] for obj in response.get("Contents", []) if "loaded" not in obj["Key"]]
        for table in tables_names:
            table_name, primary_key_column = (
                table["table_name"],
                table["primary_key_column"],
            )
            logging.info(f"Fetched table {table_name}")

        for s3_key in keys:
            for table in tables_names:
                table_name, primary_key_column = (
                    table["table_name"],
                    table["primary_key_column"],
                )
                if table["table_name"] in s3_key:
                    df = fetch_data_from_s3(s3, bucket_name, s3_key)
                    if df is not None:
                        upload_to_warehouse(
                            conn, table_name, primary_key_column, df
                        )
                        copy_source = {'Bucket': bucket_name, 'Key': s3_key}
                        s3.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=f'loaded/{s3_key}')
                        s3.delete_object(Bucket=bucket_name, Key =s3_key)

                        
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

    finally:
        logging.info("Data insertion complete.")


handler(None, None)








#     s3 = boto3.client("s3")

#     try:
#         conn = connect_to_warehouse("warehouse")
#         logging.info("Connected to the local database")

#         tables_names = fetch_tables_with_pk(conn)

#         for table in tables_names:
#             table_name = table["table_name"]
#             primary_key = table["primary_key"]
#             logging.info(f"Fetched table {table_name}")

#             response = s3.list_objects_v2(Bucket=bucket_name)
#             keys = [obj["Key"] for obj in response.get("Contents", [])]

#             for s3_key in keys:
#                 if re.search(table_name, s3_key):
#                     df = fetch_data_from_s3(s3, bucket_name, s3_key)
#                     if df is not None:
#                         upload_to_warehouse(conn, table_name, primary_key, df)

#     except Exception as e:
#         logging.error(f"An error occurred: {e}")
#         raise

#     finally:
#         logging.info("Data insertion complete.")


# handler(None, None)
