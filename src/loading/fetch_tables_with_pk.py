import logging


def fetch_tables_with_pk(conn):
    """Queries tables in warehouse and their associated primary keys."""
    try:
        cursor = conn.cursor()

        query = """
                SELECT table_name, column_name
                FROM information_schema.key_column_usage
                WHERE table_schema = 'project_team_1'
                AND constraint_name IN (
                SELECT constraint_name
                FROM information_schema.table_constraints
                WHERE table_schema = 'project_team_1'
                AND constraint_type = 'PRIMARY KEY'
                )
                ORDER BY table_name, ordinal_position;
                """
        cursor.execute(query)
        data = cursor.fetchall()

        table_info = [{
            "table_name": row[0], "primary_key": row[1]} for row in data]

        to_append = [
            table for table in table_info if table[
                "table_name"] == "fact_sales_order"
        ][0]
        table_info = [
            table for table in table_info if table[
                "table_name"] != "fact_sales_order"
        ]
        table_info.append(to_append)
        return table_info
    except AttributeError as ae:
        logging.error("Error occured in fetch_table_with_pk")
        raise (ae)
