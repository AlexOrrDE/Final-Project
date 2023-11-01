import pg8000


def connect_to_database():
    """Starts connection with totesys database.

    Typical usage example:

      conn = connect_to_database()
      data = get_data(conn)
    """

    try:
        return pg8000.dbapi.Connection(
            user="project_user_1",
            host=(
                "nc-data-eng-totesys-production.chpsczt8h1nu."
                "eu-west-2.rds.amazonaws.com"
            ),
            database="totesys",
            port=5432,
            password="WfAsWSh4nvEUEOw6",
        )

    except pg8000.DatabaseError as e:
        print(f"Error: Unable to connect to the database: {e}")
