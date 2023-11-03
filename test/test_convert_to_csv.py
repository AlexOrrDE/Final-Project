from src.ingestion.convert_to_csv import convert_to_csv
from pandas import Timestamp
from pytest import raises


def test_should_accept_a_filetype_in_the_form_returned_by_get_table_data():
    """filetype returned by get_table_data is a dictionary
    with 2 key value pairs, representing the table_name and
    a list of dictionaries of data"""

    input = {'table_name': 'staff',
             'data': [{'staff_id': 1,
                       'first_name': 'Jeremie',
                       'last_name': 'Franey',
                       'department_id': 2,
                       'email_address': 'jeremie.franey@terrifictotes.com',
                       'created_at': Timestamp('2022-11-03 14:20:51.563000'),
                       'last_updated': Timestamp('2022-11-03 14:20:51.563000')
                       }]
             }

    assert convert_to_csv(input)


def test_should_return_a_csv_formatted_data():
    """filetype returned by get_table_data is a dictionary with 2
    value pairs, representing the table_name and a list of
    dictionaries of data"""

    input = {'table_name': 'staff',
             'data': [{'staff_id': 1,
                       'first_name': 'Jeremie',
                       'last_name': 'Franey',
                       'department_id': 2,
                       'email_address': 'jeremie.franey@terrifictotes.com',
                       'created_at': Timestamp('2022-11-03 14:20:51.563000'),
                       'last_updated': Timestamp('2022-11-03 14:20:51.563000')
                       }]
             }

    expected = ('staff', 'staff_id,first_name,last_name,department_id,'
                'email_address,created_at,last_updated\n'
                '1,Jeremie,Franey,2,jeremie.franey@terrifictotes.com,'
                '2022-11-03 14:20:51.563,2022-11-03 14:20:51.563\n')

    assert convert_to_csv(input) == expected


def test_should_raise_key_error_if_input_object_is_incorrect_format():
    with raises(KeyError):
        input = {
            'data': [1, 2, 3, 4]
        }
        convert_to_csv(input)

    with raises(KeyError):
        input2 = {'error': 'staff',
                  'test': [
                      {'staff_id': 1,
                       'first_name': 'Jeremie',
                       'last_name': 'Franey',
                       'department_id': 2,
                       'email_address': 'jeremie.franey@terrifictotes.com',
                       'created_at': Timestamp('2022-11-03 14:20:51.563000'),
                       'last_updated': Timestamp('2022-11-03 14:20:51.563000')}
                  ]}
        convert_to_csv(input2)
