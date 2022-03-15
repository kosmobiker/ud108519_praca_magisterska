import os
import json
from logger import get_logger
from read_config import read_config


import boto3
from botocore.exceptions import ClientError

log = get_logger(__name__)


def create_bucket(bucket_name, session, region=None):
    """Create an S3 bucket in a specified region if it is not exist

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :profile_name: profile name from credentials 
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        if region is None:
            s3_client = session.client('s3')
            response = s3_client.list_buckets()
            if not bucket_name in [bucket['Name'] for bucket in response['Buckets']]:
                s3_client.create_bucket(Bucket=bucket_name)
                log.debug(f'Bucket {bucket_name} was created')
        else:
            s3_client = session.client('s3', region_name=region)
            if not bucket_name in [bucket['Name'] for bucket in response['Buckets']]:
                location = {'LocationConstraint': region}
                s3_client.create_bucket(Bucket=bucket_name,
                                        CreateBucketConfiguration=location)
                log.debug(f'Bucket {bucket_name} was created')
    except ClientError as e:
        log.error(e)
        return False
    return True


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
        log.debug(f"{file_name} is uploaded")
    except ClientError as e:
        log.error(e)
        return False
    return True


    