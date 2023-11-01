import pandas as pd


def fetch_data_from_tables(conn, table):
    """Gets all data from a specified table in a database.

    - Takes database connection and name of table to query as parameters.
    - Connects to database,
    - Queries specified table,
    - Uses pandas to organise retrieved data,
    - Returns data as a dictionary.

    Typical usage example:

      table_data = fetch_data_from_tables(conn, table_name)
    """
    cursor = conn.cursor()
    query = f"SELECT * FROM {table};"
    cursor.execute(query)
    rows = cursor.fetchall()
    keys = [k[0] for k in cursor.description]
    table_data = {
        "table_name": table,
        "data": pd.DataFrame(rows, columns=keys).to_dict(orient="records"),
    }

    return table_data
