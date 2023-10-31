import pandas as pd


def fetch_data_from_tables(conn, table):
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