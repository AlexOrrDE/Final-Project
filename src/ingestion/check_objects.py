from boto3 import client


def check_objects():
    """Connects to the s3 client and checks if bucket is empty.

    If bucket is not empty it will have a Contents key value pair
    check for Contents key, if exists return True.
    """

    s3client = client("s3")
    response = s3client.list_objects(Bucket="marble-test-bucket")
    return "Contents" in response
