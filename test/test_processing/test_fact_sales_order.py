import pytest
import pandas as pd
from src.processing.dimensions_fact.fact_sales_order import create_fact_sales_order
import datetime
from pandas import Timestamp
from pytest import raises


@pytest.fixture(scope="function")
def create_sales_order_data():
    data = {'sales_order_id' : [1278,23], 'created_at' : ['2022-11-03 14:20:49.962', '2023-06-03 16:20:49.962'], 'last_updated' : ['2011-04-03 12:20:49.962', '2009-04-03 12:20:49.962'], 'design_id' : [73, 111], 'staff_id' : [3, 1113], 'counterparty_id' : [18, 1], 'units_sold' : [1111, 9066], 'unit_price': [3.80, 61.11], 'currency_id' : [3, 11], 'agreed_delivery_date' : ['2023-11-13', '2011-01-12'], 'agreed_payment_date' : ['2023-11-11', '2011-06-06'], 'agreed_delivery_location_id' : [3, 12]}

    df = pd.DataFrame(data=data)

    yield df

@pytest.fixture(scope="function")
def create_bad_sales_order_data():
    data = {'sales_ordexxyxyxyxr_id' : [1278,23], 'created_at' : ['2022-11-03 14:20:49.962', '2023-06-03 16:20:49.962'], 'last_updated' : ['2011-04-03 12:20:49.962', '2009-04-03 12:20:49.962'], 'design_id' : [73, 111], 'staff_id' : [3, 1113], 'counterparty_id' : [18, 1], 'units_sold' : [1111, 9066], 'unit_price': [3.80, 61.11], 'currency_id' : [3, 11], 'agreed_delivery_date' : ['2023-11-13', '2011-01-12'], 'agreed_payment_date' : ['2023-11-11', '2011-06-06'], 'agreed_delivery_location_id' : [3, 12]}

    df = pd.DataFrame(data=data)

    yield df


def test_fact_sales_order_returns_dataframe_with_correct_columns(create_sales_order_data):
    transformed_data = create_fact_sales_order(create_sales_order_data)

    remaining_cols = set(transformed_data.columns.tolist())
    expected_cols = set(['currency_id', 'sales_staff_id', 'agreed_payment_date', 'units_sold', 'design_id', 'agreed_delivery_location_id', 'created_date', 'created_time', 'last_updated_date', 'sales_order_id', 'unit_price', 'agreed_delivery_date', 'counterparty_id', 'last_updated_time'])

    assert len(remaining_cols) == 14
    assert expected_cols <= remaining_cols

def test_fact_fales_order_doesnt_change_data_in_columns_it_keeps(create_sales_order_data):
    transformed_data = create_fact_sales_order(create_sales_order_data)

    row_zero = transformed_data.iloc[0].values.tolist()
    
    expected_row_zero = [1278, Timestamp('2022-11-03 00:00:00'), '14:20:49', Timestamp('2011-04-03 00:00:00'), '12:20:49', 3, 18, 1111, '3.80', 3, 73, Timestamp('2023-11-11 00:00:00'), Timestamp('2023-11-13 00:00:00'), 3]
    for idx, elt in enumerate(row_zero):
        assert elt == expected_row_zero[idx]

    row_one = transformed_data.iloc[1].values.tolist()
    expected_row_one = [23, Timestamp('2023-06-03 00:00:00'), '16:20:49', Timestamp('2009-04-03 00:00:00'), '12:20:49', 1113, 1, 9066, '61.11', 11, 111, Timestamp('2011-06-06 00:00:00'), Timestamp('2011-01-12 00:00:00'), 12]
    for idx, elt in enumerate(row_one):
        assert elt == expected_row_one[idx]

def test_should_raise_error_if_given_incorrect_dataframe():
    with raises(TypeError):
        create_fact_sales_order(create_bad_sales_order_data)