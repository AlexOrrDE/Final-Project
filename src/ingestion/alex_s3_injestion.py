import pg8000.dbapi
import logging
import pandas as pd
import boto3
from datetime import datetime


s3 = boto3.client("s3")


def connect_to_database():
    try:
        return pg8000.dbapi.Connection(
            user="project_user_1",
            host="nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com",
            database="totesys",
            port=5432,
            password="WfAsWSh4nvEUEOw6",
        )

    except pg8000.Error as e:
        print("Error: Unable to connect to the database")
        raise e


def fetch_tables():
    conn = connect_to_database()

    try:
        cursor = conn.cursor()
        query = f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public';"""
        cursor.execute(query)
        data = cursor.fetchall()
        table_names = [row[0] for row in data]
        return table_names

    except pg8000.Error as e:
        print(f"Error: Unable to fetch table names")
        raise e


def upload_data_to_bucket(bucket_name, key, data):
    s3.put_object(Bucket=bucket_name, Key=key, Body=data)


def convert_to_csv(table_data):
    table_name = table_data["table_name"]
    csv_data = pd.DataFrame(table_data["data"]).to_csv(index=False)
    return table_name, csv_data


def fetch_data_from_tables(event, context):
    logging.info("Injesting data...")

    conn = connect_to_database()
    table_names = fetch_tables()

    s3_bucket = "marble-test-bucket"
    name_prefix = datetime.now()

    for table in table_names:
        try:
            cursor = conn.cursor()
            query = f"SELECT * FROM {table};"
            cursor.execute(query)

            rows = cursor.fetchall()
            keys = [k[0] for k in cursor.description]

            table_data = {
                "table_name": table,
                "data": pd.DataFrame(rows, columns=keys).to_dict(orient="records"),
            }

            table_name, csv_data = convert_to_csv(table_data)

            s3_key = f"{name_prefix}-{table_name}.csv"

            upload_data_to_bucket(s3_bucket, s3_key, csv_data)

        except pg8000.Error as e:
            print(f"Error: Unable to fetch {table} data")
            raise e

    logging.info("Data injested...")


def lambda_handler(event, context):
    return fetch_data_from_tables(event, context)

