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
            "counterparty_id": ["address", "legal_address_id"],
            "staff_id": ["department", "department_id"],
        }
        if table_1 in table_dict:
            # defines the table names and keys to merge on
            table_2 = table_dict[table_1][0]
            table_1_key = table_dict[table_1][1]
            table_2_key = f"{table_dict[table_1][0]}_id"
            # call to s3 client, lists objects in data bucket
            s3 = boto3.client("s3")
            response = s3.list_objects(
                Bucket="ingestion-data-bucket-marble"
                )
            second_tables = [
                obj["Key"] for obj in response["Contents"]
                if table_2 in obj["Key"]
            ]
            # list files in date order
            sorted_tables = sorted(second_tables, reverse=True)
            merged_table = []
            # reads the data from each address table in s3
            for index, row in source_df.iterrows():
                source_key = row[table_1_key]
                for table in sorted_tables:
                    table_data = s3.get_object(
                        Bucket="ingestion-data-bucket-marble", Key=table
                    )
                    read_table_data = table_data["Body"].read().decode("utf-8")
                    table_file = io.StringIO(read_table_data)
                    second_df = pd.read_csv(table_file, index_col=False)
                    match = second_df.loc[second_df[table_2_key] == source_key]
                    pd.set_option("display.max_columns", None)
                    if len(match) > 0:
                        result = pd.merge(
                            row.to_frame().T,
                            match,
                            left_on=f"{table_1_key}",
                            right_on=f"{table_2_key}",
                        )
                        merged_columns = result.columns.tolist()
                        merged_table.append(result.values.tolist()[0])

                        break
            logging.info("Tables %s and %s have been merged.",
                         table_1, table_2)
            pd.set_option("display.max_columns", None)
            merged_df = pd.DataFrame(merged_table)
            merged_df.columns = merged_columns
            return merged_df
        else:
            logging.info("No need to merge")
            return source_df
    except RuntimeError:
        raise
    except KeyError:
        raise
    except ClientError:
        raise
