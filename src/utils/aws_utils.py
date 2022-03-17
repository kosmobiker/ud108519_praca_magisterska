import os
import json
from utils.logger import get_logger


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
            if bucket_name not in [bucket['Name'] for bucket in response['Buckets']]:
                s3_client.create_bucket(Bucket=bucket_name)
                log.debug(f'Bucket {bucket_name} was created')
        else:
            s3_client = session.client('s3', region_name=region)
            if bucket_name not in [bucket['Name'] for bucket in response['Buckets']]:
                location = {'LocationConstraint': region}
                s3_client.create_bucket(Bucket=bucket_name,
                                        CreateBucketConfiguration=location)
                log.debug(f'Bucket {bucket_name} was created')
    except ClientError as e:
        log.error(e)
        return False
    return True


def upload_json_s3(data, session, bucket, path):
    """Upload a raw json data to an S3 bucket


    """
    s3_client = session.client('s3')
    try:
        s3_client.put_object(Body=json.dumps(data), Bucket=bucket, Key=path)        
    except ClientError as e:
        log.error(e)
        return False
    return True


    