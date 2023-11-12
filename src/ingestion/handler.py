import json
import logging
from write_to_s3 import write_to_s3
from convert_to_csv import convert_to_csv
from connection import connect_to_database, InvalidStoredCredentials
from get_table_names import fetch_tables
from get_table_data import fetch_data_from_tables
from find_latest import get_previous_update_dt
from botocore.exceptions import ClientError
import boto3
import logging
import json
from pg8000 import DatabaseError, InterfaceError

logging.getLogger().setLevel(logging.INFO)


def handler(event, context):
    """Manages invocation of functions to use in AWS Lambda.

    In addition to logging and error handling, it:
        - Connects to database,
        - Gets list of table names,
        - Checks if data needs to be retrieved from database:
            - checks if bucket is empty
            - checks if database has been updated since
            previous data retrieval
        - Moves any previous .csv files in bucket into a directory
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

        update = False
        latest_update = ""
        table_data = None

        updated_tables = {}

        for table in table_names:
            latest_update = get_previous_update_dt(table)
            if latest_update:
                table_data = fetch_data_from_tables(conn, table, latest_update)
                if table_data:
                    table_name, csv_data = convert_to_csv(table_data)
                    written_table = write_to_s3(table_name, csv_data)
                    updated_tables[table_name] = written_table
                    logging.info(f"{table} has been updated. Pulling new data")
                    update = True
            else:
                table_data = fetch_data_from_tables(conn, table)
                table_name, csv_data = convert_to_csv(table_data)
                written_table = write_to_s3(table_name, csv_data, True)
                updated_tables[table_name] = written_table
                update = True
                logging.info(f"{table} has no initial data. Pulling data")
        if not update:
            logging.info("No need to update")
        else:
            logging.info("Calling next lambda with tables:")
            logging.info(updated_tables)
            lambda_client = boto3.client("lambda")

            json_tables = json.dumps(updated_tables)
            lambda_client.invoke(
                FunctionName="processing_handler",
                InvocationType="Event",
                Payload=json_tables,
            )

    except RuntimeError as e:
        logging.error("Error:", e)
    except DatabaseError as db:
        logging.error("Error:", db)
    except AttributeError as ae:
        logging.error("Error:", ae)
    except ClientError as ce:
        logging.error(ce)
    except InterfaceError:
        logging.error("Invalid Credentials Error")
    except TypeError as te:
        logging.error("Error:", te)
    except KeyError as ke:
        logging.error("Error:", ke)
    except json.JSONDecodeError as de:
        logging.error("Error: %s", de)
    except InvalidStoredCredentials as isc:
        logging.error(isc.message)
