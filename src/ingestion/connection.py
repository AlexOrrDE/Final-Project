import pg8000


def connect_to_database():
    try:
        return pg8000.dbapi.Connection(
            user="project_user_1",
            host="nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com",
            database="totesys",
            port=5432,
            password="WfAsWSh4nvEUEOw6",
        )

    except pg8000.DatabaseError as e:
        print(f'Error: Unable to connect to the database: {e}')


# def get_conn(user, database, password):
#     try:
#         return pg8000.dbapi.connect(user=f"{user}", host="nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com", database=f"{database}", port=5432, password=f"{password}")
#     except pg8000.core.DatabaseError:
#         print("connection failed")