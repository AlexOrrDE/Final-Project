import fsspec


def convert_to_parquet(dataframe):
    """Converts pandas dataframe to parquet.

    Returns table contents in parquet format,
    ready to be given a key and uploaded to s3
    """

    dataframe.to_parquet("memory://temp.parquet")
    with fsspec.open("memory://temp.parquet", "rb") as f:
        response = f.read()
    return response