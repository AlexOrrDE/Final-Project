import pg8000


def fetch_tables(conn):
    try:
        cursor = conn.cursor()
        query = f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public';"""
        cursor.execute(query)
        data = cursor.fetchall()

        table_names = [row[0] for row in data]
        table_names.remove("_prisma_migrations")

        return table_names

    except pg8000.Error as e:
        print(f"Error: Unable to fetch table names")
        raise e
