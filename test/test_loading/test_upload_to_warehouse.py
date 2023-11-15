from src.loading.upload_to_warehouse import upload_to_warehouse
from src.loading.psycopg2_conn import connect_to_warehouse
import pytest
import pandas as pd


class MockConn:
    class cursor:
        def commit(self):
            return

        def close(self):
            return

        def connection(self):
            return


df = pd.read_parquet("test/test_loading/parquet_files/dim_design.parquet")


def test_raises_attribute_error_if_given_incorrect_table_or_primary_key():
    with pytest.raises(AttributeError):
        upload_to_warehouse(connect_to_warehouse, "dim_", "design_id", df)
    with pytest.raises(AttributeError):
        upload_to_warehouse(connect_to_warehouse, "dim_design", "des_id", df)
