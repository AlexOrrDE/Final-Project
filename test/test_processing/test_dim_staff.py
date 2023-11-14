import pytest
import pandas as pd
from src.processing.dimensions_fact.dim_staff import create_dim_staff


@pytest.fixture
def table_data():
    column_headers = ['staff_id', 'first_name', 'last_name', 'department_id', 'email_address', 'department_name', 'location', 'manager', 'created_at', 'last_updated']
    values = [[1, 'test_first_name', 'test_last_name', 'test_department_id', 'test_email', 'test_dept_name', 'test_location','test_manager', '2023-11-09 00:00:00', '2023-11-09 00:00:00'], [2, 'test_first_name2', 'test_last_name2', 'test_department_id2', 'test_email2', 'test_dept_name2', 'test_location2', 'test_manager2', '2023-11-09 00:00:00', '2023-11-09 00:00:00']]
    return pd.DataFrame(values, columns=column_headers)


def test_should_return_a_dataframe(table_data):
    result = create_dim_staff(table_data)
    assert isinstance(result, pd.core.frame.DataFrame)


def test_should_return_all_columns(table_data):
    expected = ['staff_id', 'first_name', 'last_name', 'department_name', 'location', 'email_address']
    result = list(create_dim_staff(table_data))
    assert expected == result
    

def test_should_raise_key_error_if_passed_bad_data():
    bad_data = pd.DataFrame(['bad_data'])
    with pytest.raises(KeyError):
        create_dim_staff(bad_data)