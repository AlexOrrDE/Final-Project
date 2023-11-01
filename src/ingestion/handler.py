from conversion import convert_to_csv, write_to_s3
from connection import connect_to_database
from get_table_names import fetch_tables
from get_table_data import fetch_data_from_tables
from check_objects import check_objects
from check_for_updates import check_for_updates
from find_latest import get_previous_update_dt
import logging
from boto3 import client, resource


def handler():
    """"connects to database - logs when successful
        uses function fetch_tables to get table_names

        initialises a need_to_update variable to False

        uses check_objects to see if bucket is empty, if so it will go straight to else block where it changes need_to_update to True

        if the bucket is not empty it will:
            get the latest_update for each table
            use that latest update time in the check_for_updates function with table name to see if any updates have taken place

            if check_for_updates returns True for any table: 
                need_to_update will get set to True
                log a message saying updates found, creating new dataset

        after checks, if need_to_update is True:
            push a new copy of each table to s3 bucket
        else:
            log message saying checks complete, no need to update
    """
    try:
        conn = connect_to_database()
        logging.info('Connected to database')
        table_names = fetch_tables(conn)
        table_names.remove('_prisma_migrations')

        need_to_update = False

        if check_objects():
            print("in check_objects, bucket has returned true")
            for table in table_names:
                latest_update = get_previous_update_dt(table)  
                print(latest_update, "LATEST UPDATE")              
                if check_for_updates(conn, table, latest_update) is True:
                    need_to_update = True
                    logging.info("Data has been updated, pulling new dataset.")
        else:
            print("bucket has returned false for check_objects")
            need_to_update = True
            logging.info("Pulling initial data.")

        if need_to_update == True:
            print("bucket is trying to load data to s3")
            for table in table_names:
                table_data = fetch_data_from_tables(conn, table)
                table_name, csv_data = convert_to_csv(table_data)
                write_to_s3(table_name, csv_data)
        else:
            print("no need to update")
            logging.info("No need to update data.")
                
        


    except RuntimeError as e:
        print(f'Error: {e}')
    #database error but needs to be somewhere else 
    # except exceptions.DatabaseError as db:
    #     print(f"Error: {db}")

handler()
