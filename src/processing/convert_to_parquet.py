import io


def convert_to_parquet(dataframe):
    """Converts pandas dataframe to parquet.

    Returns table contents in parquet format,
    ready to be given a key and uploaded to s3
    """

    try:
        with io.BytesIO() as output:
            dataframe.to_parquet(output, engine="fastparquet")
            output.seek(0)
            return output.read()

    except AttributeError as e:
        print(
            f"Error converting to parquet: {e}."
            "Ensure convert_to_parquet() is being passed a pandas dataframe."
        )
