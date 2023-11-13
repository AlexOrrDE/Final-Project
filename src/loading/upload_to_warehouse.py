import pg8000.exceptions
import logging


def upload_to_warehouse(engine, table_name, df):
    """Uploads data to the database using pandas to_sql method."""

    try:
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
        )

        logging.info(f"Data uploaded to {table_name} successfully.")

    except pg8000.exceptions.InterfaceError as e:
        logging.info(f"Nothing to add to {table_name}")
    except RuntimeError as e:
        logging.error(f"Error uploading data to {table_name}: {e}")
