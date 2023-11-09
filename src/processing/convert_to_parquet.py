import io


def convert_to_parquet(dataframe):
    try:
        with io.BytesIO() as output:
            dataframe.to_parquet(output)
            output.seek(0)
            return output.read()
    except AttributeError as e:
        print(
            f"Error converting to parquet: {e}."
            "Ensure convert_to_parquet() is being passed a pandas dataframe."
        )
