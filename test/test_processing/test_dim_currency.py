from src.processing.dimensions_fact.dim_currency import create_dim_currency
import pandas as pd
from pytest import raises


def test_create_dim_currency_returns_a_pandas_dataframe():
    """
    Tests if create_dim_currency returns a pandas DataFrame.
    """
    test_data = {
        "currency_id": [1],
        "currency_code": ["GBP"],
        "created_at": ["2023-11-09 00:00:00"],
        "last_updated": ["2023-11-08 00:00:00"]
    }

    test_df = pd.DataFrame(test_data)
    result_df = create_dim_currency(test_df)

    assert isinstance(result_df, pd.core.frame.DataFrame)


def test_create_dim_currency_returns_correct_columns():
    """
    Tests if create_dim_currency returns a DataFrame with the correct columns.
    """
    test_data = {
        "currency_id": [1, 2],
        "currency_code": ["USD", "GBP"],
        "created_at": ["2023-11-09 00:00:00", "2023-11-09 00:00:00"],
        "last_updated": ["2023-11-08 00:00:00", "2023-11-09 00:00:00"]
    }
    expected_result = {
        "currency_id": [1, 2],
        "currency_code": ["USD", "GBP"],
        "currency_name": ["US Dollar", "British Pound"]
    }

    test_df = pd.DataFrame(test_data)

    result_df = create_dim_currency(test_df)

    pd.testing.assert_frame_equal(result_df, pd.DataFrame(expected_result))


def test_should_return_columns_with_correct_datatypes():
    """
    Test dim_currency returns a dataframe
    with columns that have the required datatypes
    """
    test_data = {
        "currency_id": [1],
        "currency_code": ["GBP"],
        "created_at": ["2023-11-09 00:00:00"],
        "last_updated": ["2023-11-08 00:00:00"]
    }
    test_df = pd.DataFrame(test_data)
    result_df = create_dim_currency(test_df)

    assert pd.api.types.is_numeric_dtype(result_df['currency_id'])
    assert pd.api.types.is_string_dtype(result_df['currency_code'])
    assert pd.api.types.is_string_dtype(result_df['currency_name'])


def test_should_raise_error_if_given_incorrect_dataframe():
    """
    Tests if create_dim_currency raises a KeyError
    when given an incorrect dataframe.
    """
    with raises(KeyError):
        incorrect_values = [[1,
                             'Fahey and Sons',
                             15,
                             'Micheal Toy',
                             'Mrs. Lucy Runolfsdottir',
                             '2022-11-03 14:20:51.563',
                             '2022-11-03 14:20:51.563']]

        create_dim_currency(pd.DataFrame(incorrect_values))
