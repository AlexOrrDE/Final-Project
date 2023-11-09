import ccy


def create_dim_currency(currency_df):
    currency_df["currency_name"] = currency_df["currency_code"].apply(
        lambda code: ccy.currency(code).name
    )
    columns_to_keep = ["currency_id", "currency_code", "currency_name"]

    dim_currency_df = currency_df[columns_to_keep]

    return dim_currency_df
