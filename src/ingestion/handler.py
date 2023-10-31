from conversion import convert_to_csv, write_to_s3
from connection import connect_to_database
from get_table_names import fetch_tables
from get_table_data import fetch_data_from_tables
import logging


def handler():
    try:
        conn = connect_to_database()
        logging.info('Connected to database')
        table_names = fetch_tables(conn)
        for table in table_names:
            table_data = fetch_data_from_tables(conn, table)
            table_name, csv_data = convert_to_csv(table_data)
            write_to_s3(table_name, csv_data)

    except RuntimeError as e:
        print(f'Error: {e}')
