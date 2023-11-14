from src.ingestion.connection import connect_to_database


def query_database():
    """For testing, queries table data and deletes it."""

    conn = connect_to_database("warehouse")

    try:
        cursor = conn.cursor()

        sql_query = "SELECT * FROM fact_sales_order;"
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        for row in rows:
            print(row)

        # Comment out next 3 lines to not delete data.
        # sql_query = "DELETE FROM dim_date;"
        # cursor.execute(sql_query)
        # conn.commit()

    finally:
        cursor.close()
        conn.close()


query_database()
