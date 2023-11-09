import io
from dimensions_fact.dim_date import create_dim_date


def convert_to_parquet(dataframe):
    with io.BytesIO() as output:
        dataframe.to_parquet(output)
        output.seek(0)
        return output.read()