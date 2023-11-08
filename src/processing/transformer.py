import pandas as pd
import boto3
import io

def find_table_pair(table_name):
    table_dict = {
        'counterparty': 'address',
        'staff': 'a',
        'date': 'a',
        'location': 'a',
        'design': 'a', 
        'currency': 'a'
    }
    for table in table_dict:
        if table_name == table:
            table_1 = table
            table_2 = table_dict[table_name]
            break
        else:
            print(table, 'not found')

    return table_1, table_2


def transformer(df, table_1, table_2):
    # call to s3 client, lists objects in data bucket
    s3 = boto3.client('s3')
    response = s3.list_objects(Bucket='ingestion-data-bucket-marble')
    second_tables = [obj['Key'] for obj in response['Contents'] if table_2 in obj['Key']]
    second_table_data = s3.get_object(Bucket='ingestion-data-bucket-marble', Key=f'{second_tables[0]}')

    # reads the data from a given address table in s3
    read_secondary_data = second_table_data['Body'].read().decode('utf-8')
    secondary_file = io.StringIO(read_secondary_data)

    # converts the secondary table to a dataframe
    second_df = pd.read_csv(secondary_file, index_col=False)

    return_table = pd.merge(df, second_df, left_on='legal_address_id', right_on='address_id')
    pd.set_option('display.max_columns', None)
    return return_table