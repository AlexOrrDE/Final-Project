def execute_insert_query(cursor, table_name, primary_key_column, df):
    """Executes the INSERT query for each row in the DataFrame."""

    for index, row in df.iterrows():
        insert_query = f"""INSERT INTO {table_name} VALUES
                        ({', '.join(['%s'] * len(row))}) ON CONFLICT
                        ({primary_key_column}) DO UPDATE SET
                        {', '.join([f'{col}=EXCLUDED.{col}' for col in
                        df.columns if col != primary_key_column])};"""

        cursor.execute(insert_query, tuple(row))
