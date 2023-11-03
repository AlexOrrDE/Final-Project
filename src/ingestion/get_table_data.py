import pandas as pd
import logging
from pg8000.exceptions import DatabaseError


def fetch_data_from_tables(conn, table, date=None):
    """Gets all data from a specified table in a database.

    - Takes database connection and name of table to query as parameters.
    - Connects to database,
    - Queries specified table,
    - Uses pandas to organise retrieved data,
    - Returns data as a dictionary.

    Typical usage example:

      table_data = fetch_data_from_tables(conn, table_name)
    """
    try:
        cursor = conn.cursor()
        if date:
            query = f"SELECT * FROM {table} WHERE"
            f" last_updated > '{date}';"
        else:
            query = f"SELECT * FROM {table};"
        cursor.execute(query)
        rows = cursor.fetchall()
        if len(rows) == 0:
            return False
        keys = [k[0] for k in cursor.description]
        table_data = {
            "table_name": table,
            "data": pd.DataFrame(rows, columns=keys).to_dict(orient="records"),
        }

        return table_data

    except DatabaseError as dbe:
        logging.error(
            f"Error occured in fetch_data_from_tables, calling table {table}")
        raise dbe
