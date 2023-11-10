from src.ingestion.connection import connect_to_database
from src.loading.warehouse_connection import fetch_tables
import boto3
import pandas as pd
from io import BytesIO
import logging
import botocore
import re


def handler(event, context):
    bucket_name = "alex-test-processing"
    try:
        conn = connect_to_database("warehouse")
        logging.info("Connected to the local database")

        tables_names = fetch_tables(conn)
        logging.info(f"Fetched tables: {tables_names}")

        s3 = boto3.client("s3")

        for table in tables_names:
            table_name, primary_key_column = (
                table["table_name"],
                table["primary_key_column"],
            )

            s3_key = f"{table_name}/16_34.parquet"
            if table_name == "fact_sales_order":
                pass
            try:
                response = s3.get_object(Bucket=bucket_name, Key=s3_key)
                df = pd.read_parquet(BytesIO(response["Body"].read()))

                cursor = conn.cursor()
                print(df.iterrows())
                for row in df.iterrows():
                    print(table_name)
                    print(row[1])
                    insert_query = f"""INSERT INTO {table_name} VALUES
                                    ({', '.join(['%s'] * len(row))}) ON CONFLICT
                                    ({primary_key_column}) DO UPDATE SET
                                    {', '.join([f'{col}=EXCLUDED.{col}' for col in
                                    df.columns])};"""
                    print(insert_query)
                    cursor.execute(insert_query, tuple(row))

                logging.info(f"Data uploaded to {table_name} successfully.")

            except botocore.exceptions.ClientError as e:
                logging.error(
                    f"The specified key '{s3_key}' does not exist in the S3 bucket."
                )

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

    finally:
        conn.close()


handler(None, None)
