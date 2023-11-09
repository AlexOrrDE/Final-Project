from src.processing.dimensions_fact.dim_design import create_dim_design
import pandas as pd
from pytest import raises

columns = ['design_id', 'created_at', 'design_name', 'file_location', 'file_name', 'last_updated']
values = [[8, '2022-11-03 14:20:49.962', 'Wooden', '/usr', 'wooden-20220717-npgz.json', '2022-11-03 14:20:49.962']]


df = pd.DataFrame(values, columns=columns)

def test_should_return_a_pandas_dataframe():
    result = create_dim_design(df)

    assert isinstance(result, pd.core.frame.DataFrame)


def test_should_return_dataframe_with_correct_columns():
    result = create_dim_design(df)
    expected = ['design_id', 'design_name', 'file_location', 'file_name']
# different datatypes so trying == but might be flagged by pep8 compliance
    assert result.columns.values.tolist() == expected

def test_should_return_columns_with_correct_datatypes():
    result = create_dim_design(df)

    assert pd.api.types.is_numeric_dtype(result['design_id'])
    for result in [result['design_name'], result['file_location'], result['file_name']]:
        assert pd.api.types.is_string_dtype(result)

def test_should_raise_error_if_given_incorrect_dataframe():
    with raises(KeyError):
        incorrect_values = [[1, 'Fahey and Sons', 15, 'Micheal Toy', 'Mrs. Lucy Runolfsdottir', '2022-11-03 14:20:51.563', '2022-11-03 14:20:51.563']]

        create_dim_design(pd.DataFrame(incorrect_values))