import pandas as pd
import boto3
import io

def find_table_pair(table_name):
    table_dict = {
        'counterparty': ['address', 'legal_address_id'],
        'staff': ['department', 'department_id'],
    }
    return table_dict[table_name]


def table_merge(df, table_dict):
    # call to s3 client, lists objects in data bucket
    table_1_key = table_dict[1]
    table_2 = table_dict[0]
    print(table_1_key, table_2)
    s3 = boto3.client('s3')
    response = s3.list_objects(Bucket='ingestion-data-bucket-marble')
    second_tables = [obj['Key'] for obj in response['Contents'] if table_2 in obj['Key']]
    second_table_data = s3.get_object(Bucket='ingestion-data-bucket-marble', Key=f'{second_tables[0]}')

    # reads the data from a given address table in s3
    read_secondary_data = second_table_data['Body'].read().decode('utf-8')
    secondary_file = io.StringIO(read_secondary_data)

    # converts the secondary table to a dataframe
    second_df = pd.read_csv(secondary_file, index_col=False)

    # merges the tables by the keys needed
    return_table = pd.merge(df, second_df, left_on=f'{table_1_key}', right_on=f'{table_2}_id')
    pd.set_option('display.max_columns', None)
    return return_table