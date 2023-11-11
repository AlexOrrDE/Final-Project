from src.ingestion.connection import connect_to_database


def query_database():
    conn = connect_to_database("warehouse")

    try:
        cursor = conn.cursor()

        # Change the query to see whatever you want
        # from the warehouse database
        sql_query = "SELECT * FROM dim_currency;"

        cursor.execute(sql_query)

        rows = cursor.fetchall()

        for row in rows:
            print(row)

    finally:
        cursor.close()
        conn.close()


query_database()
