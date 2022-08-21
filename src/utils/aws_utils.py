"""
Some additional functions for work with AWS Services
"""
import os
import json
import boto3
from botocore.exceptions import ClientError
from utils.logger import get_logger
from utils.read_config import read_toml_config

log = get_logger(__name__)
ROOT_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.abspath(os.path.join(ROOT_DIR, '..', '..', 'config', 'config.yaml'))
config = read_toml_config(CONFIG_PATH)


def create_bucket(bucket_name: str, s3_client=None, region=None):
    """
    Create an S3 bucket in a specified region if it is not exist

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    Params
        bucket_name: Bucket to create
        s3_client: client for S3
        region: String region to create bucket in, e.g., 'us-west-2'
        
    Return
        True if bucket was created, otherwise False
    """
    if not s3_client:
        s3_client = boto3.client('s3')
    # Create bucket
    try:
        if region is None:
            response = s3_client.list_buckets()
            if bucket_name not in [bucket['Name'] for bucket in response['Buckets']]:
                s3_client.create_bucket(Bucket=bucket_name)
                log.debug(f'Bucket {bucket_name} was created')
        else:
            response = s3_client.list_buckets()
            if bucket_name not in [bucket['Name'] for bucket in response['Buckets']]:
                location = {'LocationConstraint': region}
                s3_client.create_bucket(Bucket=bucket_name,
                                        CreateBucketConfiguration=location)
                log.debug(f'Bucket {bucket_name} was created')
    except ClientError as e:
        log.error(e)
        return False
    return True


def upload_json_s3(data, bucket: str, path: str, s3_client=None):
    """
    This function is used to upload a raw json data to an S3 bucket
    Params
        data: some data to upload to S3
        bucket: name of the bucket
        path: path for file within the bucket
        s3_client: client for S3
    Return
        True if tables were created, otherwise False 
    """
    if not s3_client:
        s3_client = boto3.client('s3')
    try:
        s3_client.put_object(Body=json.dumps(data), Bucket=bucket, Key=path)        
    except ClientError as e:
        log.error(e)
        return False
    return True

def get_json_s3(bucket, key, s3=None):
    """
    Function is used to get JSON file
    from S3 bucket
    """
    if not s3:
        s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj['Body'].read().decode('utf-8')
    return json.loads(data)
