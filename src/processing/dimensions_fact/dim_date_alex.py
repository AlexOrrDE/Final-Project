import pandas as pd


def create_dim_date(sales_order_df):
    distinct_dates = (
        pd.concat(
            [
                sales_order_df["created_at"],
                sales_order_df["agreed_payment_date"],
                sales_order_df["agreed_delivery_date"],
            ]
        )
        .apply(pd.to_datetime).dt.date.drop_duplicates().sort_values()
        .reset_index(drop=True)
    )
    
    date_components = pd.to_datetime(distinct_dates, format="mixed", dayfirst=True)
    dim_date_data = pd.DataFrame(
        {
            "date_id": date_components.dt.date,
            "year": date_components.dt.year,
            "month": date_components.dt.month,
            "day": date_components.dt.day,
            "day_of_week": date_components.dt.dayofweek + 1,
            "day_name": date_components.dt.strftime("%A"),
            "month_name": date_components.dt.strftime("%B"),
            "quarter": date_components.dt.quarter,
        }
    )

    return dim_date_data


sales_order_df = pd.read_csv(
    "/Users/alex/Downloads/2023-10-31 16_25_02.790973-sales_order.csv"
)
dim_date_df = create_dim_date(sales_order_df)
print(dim_date_df)
