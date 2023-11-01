from boto3 import client

# s3resource = resource('s3')
# s3resource.Bucket('totesys-test').put_object(Key=f'test{datetime.now()}.txt', Body="../../data.txt")
def check_objects():
    """connects to s3 client
        if bucket is not empty it will have a Contents key value pair
        check for Contents key, if exists return True
    """
    s3client = client('s3')
    response = s3client.list_objects(Bucket='totesys-test')
    if "Contents" in response:
        return True
    
print(check_objects())