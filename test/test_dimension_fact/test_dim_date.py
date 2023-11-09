from src.processing.dimensions_fact.dim_date import create_dim_date
import pandas as pd
from pytest import raises

def test_should_return_dataframe_with_correct_columns():
    result = create_dim_date()
    expected = ['date_id', 'year', 'month', 'day', 'day_of_week', 'day_name', 'month_name', 'quarter']
# different datatypes so trying == but might be flagged by pep8 compliance
    assert result.columns.values.tolist() == expected

def test_should_return_columns_with_correct_datatypes():
    result = create_dim_date()
    output_num_columns = [
        result['year'],
        result['month'],
        result['day'],
        result['day_of_week'],
        result['quarter']
    ]
    output_str_columns = [result['month_name'], result['day_name']]
    print(result.columns.tolist())
    for result in output_num_columns:
        assert pd.api.types.is_numeric_dtype(result)
    
    
    for result in output_str_columns:
        assert pd.api.types.is_string_dtype(result)