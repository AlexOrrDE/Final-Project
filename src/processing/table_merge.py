import io
import logging
import pandas as pd
import boto3
from botocore.exceptions import ClientError


def table_merge(source_df):
    """Merges the source dataframe with the appropriate secondary table.

    Args:
        source_df: The dataframe taken from the target csv file.
    Returns:
        source_df: If the table doesn't need to be merged,
        returns the input dataframe
        merged_table: If it does need to be merged,
        returns the two dataframes joined on the appropriate key
    Raises:
        RuntimeError: Generic error handling
        KeyError: If the source_df doesn't correlate
        to any files in the bucket
        ClientError: If AWS encounters any errors
    """
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
            second_tables = [
                obj['Key'] for obj in response['Contents']
                if table_2 in obj['Key']]

            # get latest version of file
            latest_table = max(second_tables)

            # reads the data from a given address table in s3
            second_table_data = s3.get_object(
                Bucket='ingestion-data-bucket-marble', Key=latest_table)
            read_secondary_data = (
                second_table_data['Body'].read().decode('utf-8'))
            secondary_file = io.StringIO(read_secondary_data)

            # converts the secondary table to a dataframe
            second_source_df = pd.read_csv(secondary_file, index_col=False)

            # merges the tables by the keys needed
            merged_table = pd.merge(
                source_df, second_source_df,
                left_on=f'{table_1_key}', right_on=f'{table_2_key}')
            pd.set_option('display.max_columns', None)
            logging.info(
                'Tables %s and %s have been merged.', table_1, table_2)
            return merged_table
        else:
            logging.info('No need to merge')
            return source_df
    except RuntimeError:
        raise
    except KeyError:
        raise
    except ClientError:
        raise
