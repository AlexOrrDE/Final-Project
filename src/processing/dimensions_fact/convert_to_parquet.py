import logging


def convert_to_parquet(df):
    """Converts a pandas dataframe to parquet format

    Typical usage example:

        convert_to_parquet(df)
    """
    try:
        return df.to_parquet()
    except KeyError as ke:
        logging.error("Error occured in convert_to_parquet")
        raise ke
