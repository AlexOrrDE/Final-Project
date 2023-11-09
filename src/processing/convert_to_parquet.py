import pandas as pd
import logging
import io
import fsspec

def convert_to_parquet(df):
    """Converts pandas dataframe to parquet.

    Returns table contents in parquet format,
    ready to be given a key and uploaded to s3
    """

    try:
        df.to_parquet("memory://temp.parquet")
        with fsspec.open("memory://temp.parquet", "rb") as f:
            response = f.read()

        return response

    except AttributeError as e:
        print(
            f"Error converting to parquet: {e}."
            "Ensure convert_to_parquet() is being passed a pandas dataframe."
        )


