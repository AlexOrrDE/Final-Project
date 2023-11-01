from src.ingestion.connection import connect_to_database
from find_latest import get_previous_update_dt

def check_for_updates(conn, table, previous_instance):
    time = previous_instance
    cursor = conn.cursor()

    # print(time, "TIME FROM PREVIOUS INSTANCE")
    # print(f"SELECT * FROM {table} WHERE last_updated > '{time}';", "QUERY")

    cursor.execute(f"SELECT * FROM {table} WHERE last_updated > '{time}';")
    rows = cursor.fetchall()
    # print(rows, "ROWS")
    if len(rows) > 0: 
        return True
    
    
print(check_for_updates(connect_to_database(), "staff", get_previous_update_dt('test')))
    