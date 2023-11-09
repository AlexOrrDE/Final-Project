import io

def convert_to_parquet(dataframe):
    with io.BytesIO() as output:
        dataframe.to_parquet(output, index=False)
        output.seek(0)


dataframe = ""