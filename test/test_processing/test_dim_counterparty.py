from src.processing.dimensions_fact.dim_counterparty import (
    create_dim_counterparty)
import pandas as pd
from pytest import raises

columns = ['counterparty_id',
           'counterparty_legal_name',
           'legal_address_id',
           'commercial_contact',
           'delivery_contact',
           'created_at',
           'last_updated',
           'address_id',
           'address_line_1',
           'address_line_2',
           'district',
           'city',
           'postal_code',
           'country',
           'phone',
           'created_at',
           'last_updated']

values = [[7,
           'Padberg, Lueilwitz and Johnson',
           16,
           'Ms. Wilma Witting',
           "Christy O'Hara",
           '2022-11-03 14:20:51.563',
           '2022-11-03 14:20:51.563',
           16,
           '511 Orin Extension',
           'Cielo Radial',
           'Buckinghamshire',
           'South Wyatt',
           '04524-5341',
           'Iceland',
           '2372 180716',
           '2022-11-03 14:20:49.962',
           '2022-11-03 14:20:49.962']]

# # list with nan values
values2 = [[1,
            'Fahey and Sons',
            15,
            'Micheal Toy',
            'Mrs. Lucy Runolfsdottir',
            '2022-11-03 14:20:51.563',
            '2022-11-03 14:20:51.563',
            15,
            '605 Haskell Trafficway',
            'Axel Freeway',
            "nan",
            'East Bobbie',
            '88253-4257',
            'Heard Island and McDonald Islands',
            '9687 937447',
            '2022-11-03 14:20:49.962',
            '2022-11-03 14:20:49.962']]


df = pd.DataFrame(values, columns=columns)


def test_should_return_a_pandas_dataframe():
    result = create_dim_counterparty(df)

    assert isinstance(result, pd.core.frame.DataFrame)


def test_should_return_dataframe_with_correct_columns():
    result = create_dim_counterparty(df)
    expected = [
        "counterparty_id",
        "counterparty_legal_name",
        "counterparty_legal_address_line_1",
        "counterparty_legal_address_line_2",
        "counterparty_legal_district",
        "counterparty_legal_city",
        "counterparty_legal_postal_code",
        "counterparty_legal_country",
        "counterparty_legal_phone_number",
    ]
# different datatypes so trying == but might be flagged by pep8 compliance
    assert result.columns.values.tolist() == expected


def test_should_return_columns_with_correct_datatypes():
    result = create_dim_counterparty(df)
    output_columns = [result['counterparty_legal_name'],
                      result['counterparty_legal_name'],
                      result['counterparty_legal_address_line_1'],
                      result['counterparty_legal_address_line_2'],
                      result['counterparty_legal_district'],
                      result['counterparty_legal_city'],
                      result['counterparty_legal_postal_code'],
                      result['counterparty_legal_country'],
                      result['counterparty_legal_phone_number']]

    assert pd.api.types.is_numeric_dtype(result['counterparty_id'])
    for result in output_columns:
        assert pd.api.types.is_string_dtype(result)


def test_should_raise_key_error_if_given_incorrect_dataframe():
    with raises(KeyError):
        incorrect_values = [[8,
                             '2022-11-03 14:20:49.962',
                             'Wooden',
                             '/usr',
                             'wooden-20220717-npgz.json',
                             '2022-11-03 14:20:49.962']]

        create_dim_counterparty(pd.DataFrame(incorrect_values))
