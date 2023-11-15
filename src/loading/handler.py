from psycopg2_conn import connect_to_warehouse
from fetch_tables_with_pk import fetch_tables_with_pk
from fetch_s3_data import fetch_data_from_s3
from upload_to_warehouse import upload_to_warehouse
import boto3
import logging
from psycopg2_conn import get_bucket_name_by_prefix

logging.getLogger().setLevel(logging.INFO)


def handler(event, context):
    """Fetches parquet files from S3 and uploads data to warehouse."""

    bucket_name = get_bucket_name_by_prefix("processed-data-bucket")
    s3 = boto3.client("s3")

    try:
        conn = connect_to_warehouse()
        logging.info("Connected to the local database")

        tables_names = fetch_tables_with_pk(conn)
        response = s3.list_objects_v2(Bucket=bucket_name)
        keys = [
            obj["Key"] for obj in response.get(
                "Contents",
                []) if "loaded" not in obj["Key"]]
        for table in tables_names:
            table_name, primary_key_column = (
                table["table_name"],
                table["primary_key"],
            )
            logging.info(f"Fetched table {table_name}")

        for table in tables_names:
            for s3_key in keys:
                table_name, primary_key_column = (
                    table["table_name"],
                    table["primary_key"],
                )
                if table["table_name"] in s3_key:
                    df = fetch_data_from_s3(s3, bucket_name, s3_key)
                    if df is not None:
                        upload_to_warehouse(
                            conn, table_name, primary_key_column, df
                        )
                        copy_source = {'Bucket': bucket_name, 'Key': s3_key}
                        s3.copy_object(
                            CopySource=copy_source,
                            Bucket=bucket_name,
                            Key=f'loaded/{s3_key}')
                        s3.delete_object(Bucket=bucket_name, Key=s3_key)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

    finally:
        logging.info("Data insertion complete.")
