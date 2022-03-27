"""
Some additional functions for work with AWS
Used for creating of buckes, tables in Dynamo DB,
for geeting/ putting objects in these services
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

def create_dynamodb_tables(dynamodb=None):
    """
    Service function used to create appropriate DynamoDB tables
    """
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
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
    except Exception as err:
        log.error('DynamoDB tables were not created. Check parameters')
        log.error(err)
        return False
    else:
        log.info("***Pathes*** was created!")
        return True

def dynamo_put_item(table_name: str, body: dict, dynamodb=None):
    """
    This function puts items to Dynamo DB tables
    Params:
        table_name: str name of the tables in DynamoDB
        body: dict a body of data which is put to DynamoDB
        dynamodb: client to connect to DynamoDB
    Return
        True if data were added, otherwise False 
    """
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    try:
        table.put_item(Item=body)
        return True
    except Exception as err:
        log.error('Faield to upload to DynamoDB')
        log.error(err)
        return False

def dynamo_get_item(table_name: str, key: dict, dynamodb=None):
    """
    This function gets items from Dynamo DB tables
    Params:
        table_name: str name of the tables in DynamoDB
        key: dict keys to get appropriate data from DynamoDB
        dynamodb: client to connect to DynamoDB
    Return
        True if data were got, otherwise False
    """
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    try:
        response = table.get_item(Key=key)
        log.debug('Get response from DynamoDB....')
    except ClientError as e:
        log.error(e.response['Error']['Message'])
    else:
        return response['Item']
