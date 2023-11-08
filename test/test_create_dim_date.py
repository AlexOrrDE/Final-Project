from src.processing.create_dim_date import create_dim_date
import pandas as pd
import datetime as datetime

start_date = datetime.datetime.strptime("2023/11/08", "%Y/%m/%d")
end_date = datetime.datetime.strptime("2023/11/10", "%Y/%m/%d")

def test_create_dim_date_returns_data_frame():
    assert isinstance(create_dim_date("2022/11/08", "2023/11/08"), pd.DataFrame)
    result_df = create_dim_date("2023/11/08", "2023/11/10")

def test_returns_correct_number_of_rows_for_any_two_given_dates():
    result_df = create_dim_date("2023/11/08", "2023/11/10")
    assert len(result_df) == (end_date - start_date).days
    
def test_returns_correct_number_of_columns_for_any_two_given_dates():
    result_df = create_dim_date("2023/11/08", "2023/11/10")
    assert len(result_df.columns) == 9