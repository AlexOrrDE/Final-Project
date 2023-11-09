from src.processing.dimensions_fact.dim_location import create_dim_location
import pytest
import pandas as pd
from pytest import raises

@pytest.fixture(scope="function")
def create_address_data():
    data = {'address_id' : [1,2], 'address_line_1' : ['test_address_1', 'test_address_2'], 'address_line_2' : ['test_second_address_1', 'test_second_address_2'], 'district' : ['test_dist_1', 'test_dict_2'], 'city' : ['test_city_1', 'test_city_2'], 'postal_code' : ['test_postal_1', 'test_postal_2'], 'country' : ['test_country_1', 'tst_country_2'], 'phone': ['059695', '284949'], 'created_at' : ['2022-11-03 14:20:49.962', '2023-06-03 16:20:49.962']}

    df = pd.DataFrame(data=data)

    yield df

@pytest.fixture(scope="function")
def create_bad_address_data():
    data = {'fake_column_name' : [1,2], 'address_line_1' : ['test_address_1', 'test_address_2'], 'address_line_2' : ['test_second_address_1', 'test_second_address_2'], 'district' : ['test_dist_1', 'test_dict_2'], 'city' : ['test_city_1', 'test_city_2'], 'postal_code' : ['test_postal_1', 'test_postal_2'], 'country' : ['test_country_1', 'tst_country_2'], 'phone': ['059695', '284949'], 'created_at' : ['2022-11-03 14:20:49.962', '2023-06-03 16:20:49.962']}

    df = pd.DataFrame(data=data)

    yield df

def test_dim_design_returns_dataframe_with_correct_columns(create_address_data):
    transformed_data = create_dim_location(create_address_data)

    remaining_cols = set(transformed_data.columns.tolist())
    expected_cols = set(['location_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone'])

    assert len(remaining_cols) == 8
    assert expected_cols <= remaining_cols

def test_dim_design_doesnt_change_data_in_columns_it_keeps(create_address_data):
    transformed_data = create_dim_location(create_address_data)

    row_zero = transformed_data.iloc[0].values.tolist()

    expected_row_zero = [1, 'test_address_1', 'test_second_address_1', 'test_dist_1', 'test_city_1', 'test_postal_1', 'test_country_1', '059695']
    for idx, elt in enumerate(row_zero):
        assert elt == expected_row_zero[idx]

    row_one = transformed_data.iloc[1].values.tolist()
    expected_row_one = [2, 'test_address_2', 'test_second_address_2', 'test_dict_2', 'test_city_2', 'test_postal_2', 'tst_country_2', '284949']
    for idx, elt in enumerate(row_one):
        assert elt == expected_row_one[idx]

def test_should_raise_error_if_given_incorrect_dataframe():
    with raises(AttributeError):
        create_dim_location(create_bad_address_data)
