import os
import json
from utils.logger import get_logger
from utils.read_config import read_toml_config

import boto3
from botocore.exceptions import ClientError

log = get_logger(__name__)
ROOT_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.abspath(os.path.join(ROOT_DIR, '..', '..', 'config', 'config.yaml'))
config = read_toml_config(CONFIG_PATH)


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
    """
    Upload a raw json data to an S3 bucket
    """
    s3_client = session.client('s3')
    try:
        s3_client.put_object(Body=json.dumps(data), Bucket=bucket, Key=path)        
    except ClientError as e:
        log.error(e)
        return False
    return True

def create_dynamodb_tables(session, dynamodb=None):
    if not dynamodb:
        dynamodb = session.resource('dynamodb')
    try:
        dynamodb.create_table(
            TableName='Schemas',
            KeySchema=[
                {
                    'AttributeName': 'SchemaName',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'SchemaVersion',
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'SchemaName',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'SchemaVersion',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
            }
        )
        log.info("***Schemas*** was created!")
        dynamodb.create_table(
            TableName='Metadata',
            KeySchema=[
                {
                    'AttributeName': 'JobName',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'ExecutionDate',
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'JobName',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'ExecutionDate',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
        )
        log.info("***Metadata*** was created!")
        dynamodb.create_table(
            TableName='Pathes',
            KeySchema=[
                {
                    'AttributeName': 'Path',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'Type',
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'Path',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'Type',
                    'AttributeType': 'S'
                },
            ],
            ProvisionedThroughput={
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
        )
        log.info("***Pathes*** was created!")
        
    except Exception as err:
        log.error('DynamoDB tables were not created. Check parameters')
        log.error(err)

def dynamo_put_item(session, table_name, body, dynamodb=None):
    if not dynamodb:
        dynamodb = session.resource('dynamodb')

    table = dynamodb.Table(table_name)
    try:
        table.put_item(Item=body)
        log.info('Data sucessfully uploaded to DynamoDB')
        return True
    except Exception as err:
        log.error('Faield to upload to DynamoDB')
        log.error(err)
        return False

def dynamo_get_item(session, table_name, key, dynamodb=None):
    if not dynamodb:
        dynamodb = session.resource('dynamodb')
    table = dynamodb.Table(table_name)
    try:
        response = table.get_item(Key=key)
        log.debug('Get response from DynamoDB....')
    except ClientError as e:
        log.error(e.response['Error']['Message'])
    else:
        return response['Item']
