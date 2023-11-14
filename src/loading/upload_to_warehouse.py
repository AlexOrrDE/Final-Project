import logging
from psycopg2 import sql, extras

def upload_to_warehouse(conn, table_name, primary_key, df):
    """Uploads data to the database in a single query with handling duplicates."""

    columns = ', '.join(df.columns)
    conflict_update = ', '.join([f'"{col}"=EXCLUDED."{col}"' for col in df.columns if col != primary_key])

    insert_query = sql.SQL(f"""INSERT INTO "{table_name}" ({columns}) VALUES %s
                    ON CONFLICT ("{primary_key}") DO UPDATE SET {conflict_update}""")
    
    data = [tuple(row) for index, row in df.iterrows()]

    cursor = conn.cursor()
    
    try:
        extras.execute_values(cursor, insert_query, data)

        conn.commit()
        logging.info(f"Data uploaded to {table_name} successfully.")
    finally:
        cursor.close()
