from src.loading.execute_query import execute_insert_query
import logging


def upload_to_warehouse(conn, table_name, primary_key_column, df):
    """Uploads data to the database."""

    cursor = conn.cursor()
    execute_insert_query(cursor, table_name, primary_key_column, df)
    conn.commit()
    cursor.close()
    logging.info(f"Data uploaded to {table_name} successfully.")
