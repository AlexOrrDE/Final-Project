import logging
from pg8000 import DatabaseError


def check_for_updates(conn, table, previous_instance):
    """takes a time as previous instance
    queries the database for updates since that time
    if resulting rows are greater than 0 then it will return true
    """

    try:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT * FROM {table} WHERE last_updated > '{previous_instance}';"
        )
        rows = cursor.fetchall()

        return len(rows) > 0

    except DatabaseError as de:
        logging.info("error raised in check_for_updates")
        print("raised from check for updates")
        raise de
    except RuntimeError as re:
        logging.info("error raised in check_for_updates")
        raise re
    except AttributeError as ae:
        logging.info("error raised in check_for_updates")
        raise ae
