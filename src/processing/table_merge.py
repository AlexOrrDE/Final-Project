import io
import logging
import pandas as pd
import boto3
from botocore.exceptions import ClientError

def table_merge(source_df):
    try:
        table_1 = source_df.columns[0]
        table_dict = {
        'counterparty_id': ['address', 'legal_address_id'],
        'staff_id': ['department', 'department_id']
        }
        if table_1 in table_dict:
            # defines the table names and keys to merge on
            table_2 = table_dict[table_1][0]
            table_1_key = table_dict[table_1][1]
            table_2_key = f'{table_dict[table_1][0]}_id'

            # call to s3 client, lists objects in data bucket
            s3 = boto3.client('s3')
            response = s3.list_objects(Bucket='ingestion-data-bucket-marble')

            # TODO add in capability to search for the most recent update
            second_tables = [obj['Key'] for obj in response['Contents'] if table_2 in obj['Key']]
            second_table_data = s3.get_object(Bucket='ingestion-data-bucket-marble', Key=f'{second_tables[0]}')

            # reads the data from a given address table in s3
            read_secondary_data = second_table_data['Body'].read().decode('utf-8')
            secondary_file = io.StringIO(read_secondary_data)

            # converts the secondary table to a dataframe
            second_source_df = pd.read_csv(secondary_file, index_col=False)

            # merges the tables by the keys needed
            merged_table = pd.merge(source_df, second_source_df, left_on=f'{table_1_key}', right_on=f'{table_2_key}')
            pd.set_option('display.max_columns', None)
            logging.info('Tables %s and %s have been merged.', table_1, table_2)
            return merged_table
        else:
            logging.info('No need to merge')
            return source_df
    except RuntimeError as e:
        raise(e)
    except KeyError as ke:
        raise(ke)
    except ClientError as ce:
        raise(ce)