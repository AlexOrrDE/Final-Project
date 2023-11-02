import boto3
import re
from botocore.exceptions import ClientError
import logging


def get_previous_update_dt(table_name, bucket_name="marble-test-bucket"):
    """Connects to s3 bucket using boto resource
    searches the bucket for keys with table name
    pushes the date from the key to a previous updates list
    sorts the previous updates list
    returns the most recent date

    the sort functionality may need some work as at the moment it just sorts strings which is not reliable
    """
    try:
        s3 = boto3.resource("s3")
        # CHANGEBUCKETNAME
        bucket = s3.Bucket(bucket_name)
        previous_updates = []
        for obj in bucket.objects.all():
            if f'{obj.key}'.endswith(f'{table_name}.csv'):
                date = re.search(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d{6}", obj.key)
                previous_updates.append(date.group())

        if len(previous_updates) == 0:
            raise NoPreviousInstanceError(table_name)
        if len(previous_updates) > 0:
            previous_updates.sort(reverse=True)
            return previous_updates[0]
    except NoPreviousInstanceError as npi:
        raise npi
    except ClientError as e:
        raise e

class NoPreviousInstanceError(Exception):
    """catches if no matches are found in the bucket"""
    def __init__(self, table):
        self.table = table
        self.message = f"There are no previous instances of '{self.table}' table"

# 