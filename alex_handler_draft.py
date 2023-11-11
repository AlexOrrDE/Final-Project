from src.ingestion.connection import connect_to_database
from src.loading.warehouse_tables import fetch_tables_with_pk
import boto3
import pandas as pd
from io import BytesIO
import logging
import botocore

logging.basicConfig(level=logging.INFO)


def handler(event, context):
    bucket_name = "alex-test-processing"
    s3 = boto3.client("s3")

    try:
        conn = connect_to_database("warehouse")
        cursor = conn.cursor()

        logging.info("Connected to the local database")

        tables_names = fetch_tables_with_pk(conn)

        for table in tables_names:
            table_name, primary_key_column = (
                table["table_name"],
                table["primary_key_column"],
            )
            logging.info(f"Fetched table {table_name}")

            # This needs to be made dynamic
            s3_key = f"{table_name}/16_34.parquet"

            try:
                response = s3.get_object(Bucket=bucket_name, Key=s3_key)
                df = pd.read_parquet(BytesIO(response["Body"].read()))

                for index, row in df.iterrows():
                    insert_query = f"""INSERT INTO {table_name} VALUES
                                    ({', '.join(['%s'] * len(row))}) ON CONFLICT
                                    ({primary_key_column}) DO UPDATE SET
                                    {', '.join([f'{col}=EXCLUDED.{col}' for col in
                                    df.columns if col != primary_key_column])};"""

                    cursor.execute(insert_query, tuple(row))

                conn.commit()
                logging.info(f"Data uploaded to {table_name} successfully.")

            except botocore.exceptions.ClientError as e:
                logging.warning(
                    f"The specified key '{s3_key}' does not exist in the S3 bucket."
                )

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise

    finally:
        logging.info("Data insertion complete.")


handler(None, None)
