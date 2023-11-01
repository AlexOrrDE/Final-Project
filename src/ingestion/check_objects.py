from boto3 import client


def check_objects():
    """connects to s3 client.

    If bucket is not empty it will have a Contents key value pair
    check for Contents key, if exists return True.
    """

    s3client = client("s3")
    response = s3client.list_objects(Bucket="marble-test-bucket")
    return "Contents" in response
