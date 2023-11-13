from src.ingestion.connection import connect_to_database
from src.loading.fetch_tables_with_pk import fetch_tables_with_pk
from src.loading.fetch_s3_data import fetch_data_from_s3
from src.loading.upload_to_warehouse import upload_to_warehouse
import boto3
import logging

logging.basicConfig(level=logging.INFO)


def handler(event, context):
    """Fetches parquet files from S3 and uploads data to warehouse."""

    bucket_name = "alex-test-processing"
    s3 = boto3.client("s3")

    try:
        conn = connect_to_database("warehouse")
        logging.info("Connected to the local database")

        tables_names = fetch_tables_with_pk(conn)

        for table in tables_names:
            table_name, primary_key_column = (
                table["table_name"],
                table["primary_key_column"],
            )
            logging.info(f"Fetched table {table_name}")

            prefix = f"{table_name}"
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            keys = [obj["Key"] for obj in response.get("Contents", [])]

            for s3_key in keys:
                df = fetch_data_from_s3(s3, bucket_name, s3_key)
                if df is not None:
                    upload_to_warehouse(
                        conn, table_name, primary_key_column, df
                    )

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

    finally:
        logging.info("Data insertion complete.")


handler(None, None)
