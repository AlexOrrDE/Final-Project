from boto3 import client
from botocore.exceptions import ClientError
import logging

def check_objects(bucket_name="marble-test-bucket"):
    """Connects to the s3 client and checks if bucket is empty.

    If bucket is not empty, it will have a Contents key value pair
    and the function will return True.
    """
    try:
        s3client = client("s3")
        # CHANGE BUCKET NAME
        response = s3client.list_objects(Bucket=bucket_name)
        return "Contents" in response
    except ClientError as ce:
        logging.error("error occured in check_objects")
        raise ce

# 