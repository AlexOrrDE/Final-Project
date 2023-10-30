import pg8000.dbapi
import pandas as pd

def connect_to_database():
    try:
        return pg8000.dbapi.Connection(
            user="project_user_1",
            host="nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com",
            database="totesys",
            port=5432,
            password="WfAsWSh4nvEUEOw6",
        )

    except pg8000.Error as e:
        print("Error: Unable to connect to the database")
        raise e


def fetch_tables():
    conn = connect_to_database()

    try:
        cursor = conn.cursor()
        query = f"""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public';"""
        cursor.execute(query)
        data = cursor.fetchall()
        table_names = [row[0] for row in data]
        return table_names

    except pg8000.Error as e:
        print(f"Error: Unable to fetch table names")
        raise e


def fetch_data_from_tables():
    conn = connect_to_database()
    table_names = fetch_tables()
    # Might be easier to store these as dictionaries
    results = []
    pandas_results = []
    csv_results = []

    for table in table_names:
        try:
            cursor = conn.cursor()
            query = f"SELECT * FROM {table};"
            cursor.execute(query)

            rows = cursor.fetchall()
            keys = [k[0] for k in cursor.description]

            # Pandas is adding an extra index column here, need to deal with this
            pandas_data = pd.DataFrame(rows)
            pandas_data.columns = keys
            pandas_results.append(pandas_data)
            csv_results.append(pandas_data.to_csv())

            result = [dict(zip(keys, row)) for row in rows]

            results.append({f"{table}": f"{result}"})

        except pg8000.Error as e:
            print(f"Error: Unable to fetch {table} data")
            raise e

    # Print all tables in Pandas form (it limits the number of rows output so it's readable)
    print(pandas_results)
    # Prints all csv results
    print(csv_results[0])
    #print(results)
    return results


fetch_data_from_tables()
