# This is a placeholder lambda for the terraform
# If you change the name of this file or the function name tell Joe,
# or fix the terraform yourself!

# In that case we will need to change (at least) the handler argument in
# lambda.tf

# I'm not sure if we can call this just "handler". It might cause problems
# with AWS

import logging

logging.getLogger().setLevel(logging.INFO)


import json
import urllib.parse
import boto3

print('Loading function')

s3 = boto3.client('s3')


def processing_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        table = response['Body']
        logging.info(table)
        
        logging.info(table.read().decode("utf-8"))
        return response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
    
# The test event JSON:

# {
#   "Records": [
#     {
#       "eventVersion": "2.0",
#       "eventSource": "aws:s3",
#       "awsRegion": "eu-west-2",
#       "eventTime": "1970-01-01T00:00:00.000Z",
#       "eventName": "ObjectCreated:Put",
#       "userIdentity": {
#         "principalId": "3775-1597-0402"
#       },
#       "requestParameters": {
#         "sourceIPAddress": "127.0.0.1"
#       },
#       "responseElements": {
#         "x-amz-request-id": "EXAMPLE123456789",
#         "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
#       },
#       "s3": {
#         "s3SchemaVersion": "1.0",
#         "configurationId": "testConfigRule",
#         "bucket": {
#           "name": "ingestion-data-bucket-marble",
#           "ownerIdentity": {
#             "principalId": "3775-1597-0402"
#           },
#           "arn": "arn:aws:s3:::ingestion-data-bucket-marble"
#         },
#         "object": {
#           "key": "2023/11/07/address/16-50.csv",
#           "size": 1024,
#           "eTag": "0123456789abcdef0123456789abcdef",
#           "sequencer": "0A1B2C3D4E5F678901"
#         }
#       }
#     }
#   ]
# }