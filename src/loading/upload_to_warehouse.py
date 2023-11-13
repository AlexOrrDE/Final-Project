import logging


def upload_to_warehouse(conn, table_name, primary_key_column, df):
    """Uploads data to the database."""

    cursor = conn.cursor()

    for index, row in df.iterrows():
        insert_query = f"""INSERT INTO "{table_name}" VALUES
                    ({', '.join(['%s'] * len(row))}) ON CONFLICT
                    ("{primary_key_column}") DO UPDATE SET
                    {', '.join([f'"{col}"=EXCLUDED."{col}"' for col in
                    df.columns if col != primary_key_column])};"""

        cursor.execute(insert_query, tuple(row))

    conn.commit()
    cursor.close()
    logging.info(f"Data uploaded to {table_name} successfully.")