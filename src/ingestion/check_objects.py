from boto3 import client, resource
from datetime import datetime

# s3resource = resource('s3')
# s3resource.Bucket('totesys-test').put_object(Key=f'test{datetime.now()}.txt', Body="../../data.txt")
def check_objects():
    s3client = client('s3')
    response = s3client.list_objects(Bucket='totesys-test')
    if "Contents" in response:
        return True
    
print(check_objects())