"""Defines lambda function to handle creation of S3 text object."""

import logging
import boto3
from botocore.exceptions import ClientError
import pg8000

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """Handles S3 PutObject event and logs the contents of file.

    On receipt of a PutObject event, checks that the file type is txt and
    then logs the contents.

    Args:
        event:
            a valid S3 PutObject event -
            see https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html
        context:
            a valid AWS lambda Python context object - see
            https://docs.aws.amazon.com/lambda/latest/dg/python-context.html

    Raises:
        RuntimeError: An unexpected error occurred in execution. Other errors
        result in an informative log message.
    """  # noqa: E501

    try:
        connect_to_database()
        s3_bucket_name, s3_object_name = get_object_path(event['Records'])
        logger.info(f'Bucket is {s3_bucket_name}')
        logger.info(f'Object key is {s3_object_name}')

        if s3_object_name[-3:] != 'txt':
            raise InvalidFileTypeError

        s3 = boto3.client('s3')
        text = get_text_from_file(s3, s3_bucket_name, s3_object_name)
        logger.info('File contents...')
        logger.info(f'{text}')
    except KeyError as k:
        logger.error(f'Error retrieving data, {k}')
    except ClientError as c:
        if c.response['Error']['Code'] == 'NoSuchKey':
            logger.error(f'No object found - {s3_object_name}')
        elif c.response['Error']['Code'] == 'NoSuchBucket':
            logger.error(f'No such bucket - {s3_bucket_name}')
        else:
            raise
    except UnicodeError:
        logger.error(f'File {s3_object_name} is not a valid text file')
    except InvalidFileTypeError:
        logger.error(f'File {s3_object_name} is not a valid text file')
    except Exception as e:
        logger.error(e)
        raise RuntimeError


def get_object_path(records):
    """Extracts bucket and object references from Records field of event."""
    return records[0]['s3']['bucket']['name'], \
        records[0]['s3']['object']['key']


def get_text_from_file(client, bucket, object_key):
    """Reads text from specified file in S3."""
    data = client.get_object(Bucket=bucket, Key=object_key)
    contents = data['Body'].read()
    return contents.decode('utf-8')


class InvalidFileTypeError(Exception):
    """Traps error where file type is not txt."""
    pass

def connect_to_database():
    try:
        return pg8000.dbapi.Connection(
            user="project_user_1",
            host="nc-data-eng-totesys-production.chpsczt8h1nu.eu-west-2.rds.amazonaws.com",
            database="totesys",
            port=5432,
            password="WfAsWSh4nvEUEOw6",
        )

    except pg8000.Error as e:
        print("Error: Unable to connect to the database")
        raise e
