from conversion import convert_to_csv, write_to_s3
from connection import connect_to_database
from get_table_names import fetch_tables
from get_table_data import fetch_data_from_tables
from check_objects import check_objects
from check_for_updates import check_for_updates
from find_latest import get_previous_update_dt, NoPreviousInstanceError
from botocore.exceptions import ClientError
import logging
from pg8000 import DatabaseError


logging.getLogger().setLevel(logging.INFO)

def handler():
    """Manages invocation of functions to use in AWS Lambda.

    In addition to logging and error handling, it:
        - Connects to database,
        - Gets list of table names,
        - Checks if data needs to be retrieved from database:
            - checks if bucket is empty
            - checks if database has been updated since
            previous data retrieval
        - Fetches data from each table in list,
        - Converts retrieved data to csv. format,
        - Uploads csv. data files to an AWS s3 bucket.


    Typical usage example:

      - Upload ingestion directory to AWS Lambda as .zip,
      - Add pg8000 and pandas as layers on Lambda function,
      - Set this function as the handler for the Lambda.
    """

    try:
        conn = connect_to_database()
        logging.info("Connected to database")
        table_names = fetch_tables(conn)

        update = True

        if check_objects():
            for table in table_names:
                latest_update = get_previous_update_dt(table)

                if check_for_updates(conn, table, latest_update):
                    logging.info(f"Data has been updated, pulling new dataset from {table} table.")

                else:
                    update = False
                    logging.info(f"No need to update data from {table} table.")

        else:
            logging.info("Pulling initial data.")

        if update:
            for table in table_names:
                table_data = fetch_data_from_tables(conn, table)
                table_name, csv_data = convert_to_csv(table_data)
                write_to_s3(table_name, csv_data)

    except RuntimeError as e:
        print(f"Error:", e)
    except DatabaseError as db:
        print(f"Error:", db)
    except AttributeError as ae:
        print("Error:", ae)
    except NoPreviousInstanceError as npi:
        print(npi.message)
    except ClientError as ce:
        print("Error:", ce.response["Error"]["Message"])


# 