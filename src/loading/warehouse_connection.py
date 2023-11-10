from src.ingestion.connection import connect_to_database


'''
just checking if connection to data warehouse is all good,
to connect just pass the argument 'warehouse' into
connect_to_database, case-insensitive'''

warehouse_connection = connect_to_database('warehouse')


def fetch_tables(conn):
    cursor = conn.cursor()
    query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'project_team_1';"""
    cursor.execute(query)
    data = cursor.fetchall()

    table_names = [row[0]for row in data]
    return table_names


print(fetch_tables(warehouse_connection))
