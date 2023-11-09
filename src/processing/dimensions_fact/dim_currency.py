import ccy
import logging


def create_dim_currency(currency_df):
    """
    Creates a dimension DataFrame for currencies with a format that matches the table in the warehouse

    Args:
        currency_df (pd.DataFrame): Input DataFrame
        with the columns : 'currency_id', 'currency_name', 'created_at' and last_updated

    Returns:
        pd.DataFrame: DataFrame with 'currency_id', 'currency_code', and 'currency_name'

    Raises:
        KeyError: If the input dataframe does not have the expected format.
    """
    try:
        currency_df["currency_name"] = currency_df["currency_code"].apply(
            lambda code: ccy.currency(code).name
        )
        columns_to_keep = ["currency_id", "currency_code", "currency_name"]

        dim_currency_df = currency_df[columns_to_keep]
    except KeyError as ke:
        logging.error("Error occured in create_dim_currency")
        raise ke

    return dim_currency_df
