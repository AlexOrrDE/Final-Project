import pandas as pd


def create_fact_sales_order(sales_order_df):
    date_time_cols = [
        "created_date",
        "last_updated_date",
        "agreed_payment_date",
        "agreed_delivery_date",
    ]
    sales_order_df["created_time"] = pd.to_datetime(sales_order_df["created_at"], exact=False).dt.strftime("%H:%M:%S")
    sales_order_df["last_updated_time"] = pd.to_datetime(sales_order_df["last_updated"], exact=False).dt.strftime("%H:%M:%S")
    sales_order_df["created_date"] = pd.to_datetime(sales_order_df["created_at"], exact=False).dt.strftime("%Y-%m-%d")
    sales_order_df["last_updated_date"] = pd.to_datetime(sales_order_df["last_updated"], exact=False).dt.strftime("%Y-%m-%d")

    sales_order_df["unit_price"] = sales_order_df["unit_price"].round(2)

    varchar = ["agreed_delivery_date", "agreed_payment_date"]

    for column_name in varchar:
        sales_order_df[column_name] = pd.to_datetime(sales_order_df[column_name])

    column_name_mapping = {
        "staff_id": "sales_staff_id",
    }
    sales_order_df.rename(columns=column_name_mapping, inplace=True)

    columns_to_keep = [
        "sales_order_id",
        "created_date",
        "created_time",
        "last_updated_date",
        "last_updated_time",
        "sales_staff_id",
        "counterparty_id",
        "units_sold",
        "unit_price",
        "currency_id",
        "design_id",
        "agreed_payment_date",
        "agreed_delivery_date",
        "agreed_delivery_location_id",
    ]

    fact_sales_order_df = sales_order_df[columns_to_keep]

    return fact_sales_order_df
