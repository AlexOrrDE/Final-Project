import pandas as pd

def create_dim_date():
    date_range = pd.date_range("2020-01-01", "2025-01-01")

    dim_date_df = pd.DataFrame({'date_id': date_range})
    
    dim_date_df['year'] = dim_date_df['date_id'].dt.year
    dim_date_df['month'] = dim_date_df['date_id'].dt.month
    dim_date_df['day'] = dim_date_df['date_id'].dt.day
    dim_date_df['day_of_week'] = dim_date_df['date_id'].dt.dayofweek
    dim_date_df['day_name'] = dim_date_df['date_id'].dt.strftime('%A')
    dim_date_df['month_name'] = dim_date_df['date_id'].dt.strftime('%B')
    dim_date_df['quarter'] = dim_date_df['date_id'].dt.quarter

    return dim_date_df