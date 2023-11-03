import pg8000
import logging


def fetch_tables(conn):
    """Retrieve names of all tables in a database.

    - Connects to database,
    - Queries table names,
    - Returns list of all names.

    Typical usage example:

      table_names = fetch_tables(conn)
      for table in table names:
          table_data = get_table_data(table)
    """

    try:
        cursor = conn.cursor()
        query = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public';"""
        cursor.execute(query)
        data = cursor.fetchall()

        table_names = [row[0] for row in data if row[0] != "_prisma_migrations"]
    
        return table_names

    except pg8000.Error as e:
        logging.error("Error occured in fetch_tables")
        raise e
