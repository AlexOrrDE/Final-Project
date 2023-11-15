import boto3
import re
import logging
from botocore.exceptions import ClientError
from datetime import datetime
from connection import get_bucket_name_by_prefix


def get_previous_update_dt(
        table_name):
    """Connects to s3 bucket using boto resource
    searches the bucket for keys with table name
    pushes the date from the key to a previous updates list
    sorts the previous updates list
    returns the most recent date
    """

    try:
        bucket = get_bucket_name_by_prefix("ingestion-data-bucket")
        objects = list(bucket.objects.all())
        previous_updates = []
        for object in objects:
            if f'{table_name}/' in object.key:
                remove_table_name = object.key.split(f"{table_name}/")
                find_date = re.search(
                    r'\d{4}\/\d{2}\/\d{2}',
                    remove_table_name[0]).group()
                find_time = re.search(
                    r'\d{2}:\d{2}', remove_table_name[1]).group()
                find_date = find_date.replace('/', '-')
                date_str = find_date + " " + find_time
                date_format = '%Y-%m-%d %H:%M'

                date_obj = datetime.strptime(date_str, date_format)
                previous_updates.append(date_obj)

        if len(previous_updates) == 0:
            logging.warning("No previous data for this found")
            return False

        return max(previous_updates)

    except ClientError as e:
        logging.error("Error occured in get_previous_update_dt")
        raise e
