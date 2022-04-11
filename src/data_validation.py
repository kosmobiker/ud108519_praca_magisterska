"""
Data Validation Module

To-do:
1. Get list of all files in {bucket}/data/raw
2. Read schema
2. Iterate list of jsons:
    - check if file valid to schema
    - check if there are more than 100 records in json
3. Save results of validation (to Dynamo DB):
    - date of validation
    - id of schema
    - number of valid jsons
    - number of invalid jsons
    - pathes for valid jsons

"""
import os
import json
import boto3
import re
from jsonschema import validate
from datetime import datetime
from decimal import Decimal
from utils.read_config import read_toml_config
from utils.logger import setup_applevel_logger
from utils.aws_utils import dynamo_put_item, dynamo_get_item, get_json_s3, create_dynamodb_tables


ROOT_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.abspath(os.path.join(ROOT_DIR, '..', 'config', 'config.toml'))
config = read_toml_config(CONFIG_PATH)
PATH_TO_LOGS = os.path.abspath(os.path.join(ROOT_DIR, '..', config['dev']['path_to_logs']))
TODAY = datetime.today().strftime("%Y%m%d")
PATH_RAW_DATA = config['dev']['path_raw_data']
AWS_PROFILE = config['dev']['aws_profile']
AWS_BUCKET = config['dev']['aws_bucket']
SCHEMA_NAME = config['dev']['schema_name']
SCHEMA_VERSION = config['dev']['schema_version']

log = setup_applevel_logger(file_name = PATH_TO_LOGS + "/logging_{}".format(TODAY))

session = boto3.Session(profile_name=AWS_PROFILE)
dynamo = session.resource('dynamodb')
s3_client = session.client('s3')

# with open('/home/vlad/master_project/config/schema.json') as f:
#     schema = json.load(f)

def get_list_of_objects_s3(operation_parameters, s3=None):
    """
    List files in specific S3 URL
    """
    if not s3:
        s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(**operation_parameters)
    for page in page_iterator:
        for content in page.get('Contents'):
            yield content.get('Key')

def validate_json(data, schema):
    """
    Validation of JSON file according to schema
    """
    try:
        validate(instance=data, schema=schema)
        return True
    except Exception:
        return False

def data_validation(params: dict, schema):
    """
    Main function for data validation
    """
    good = 0
    bad = 0
    dubious = 0
    gen = get_list_of_objects_s3(params, s3_client) #generator
    for key in gen:
        try:
            token = re.findall("(?<=data/raw/)(.+)(?=_historical_prices.json)", key)[0]
            data = get_json_s3(params["Bucket"], key, s3_client)
            if len(data['prices']) < 100:
                dynamo_params = {
                    'PathS3' : key,
                    'TypeOfRecord' : "bad",
                    'TokenName' : token
                }
                dynamo_put_item("Pathes", dynamo_params, dynamodb=dynamo)
                bad += 1
                log.info(f"{key} is too short")
            else:
                if validate_json(data, schema):
                    dynamo_params = {
                        'PathS3' : key,
                        'TypeOfRecord' : "good",
                        'TokenName' : token
                    }
                    dynamo_put_item("Pathes", dynamo_params, dynamodb=dynamo)
                    good += 1
                    log.info(f'{key} is OK')
                else:
                    dynamo_params = {
                        'PathS3' : key,
                        'TypeOfRecord' : "dubious",
                        'TokenName' : token
                    }
                    dynamo_put_item("Pathes", dynamo_params, dynamodb=dynamo)
                    dubious += 1
                    log.info(f"{key} - schema not matched")
        except Exception as err:
            dynamo_params = {
                    'PathS3' : key,
                    'TypeOfRecord' : "bad",
                    'TokenName' : token
                }
            dynamo_put_item("Pathes", dynamo_params, dynamodb=dynamo)
            bad += 1
            log.error(f"{key} was added to bad pathes")
            log.error(err)
    log.info(f"Number of good files is {good}")
    log.info(f"Number of bad files is {bad}")
    log.info(f"Number of dubious files is {dubious}")
    try:
        validation_params = {
            'JobName' : "Data Validation",
            'ExecutionDate' : datetime.now().strftime("%Y-%m-%d, %H:%M:%S"),
            'SuccedFiles' : good,
            'FailedFiles' : bad,
            'DubiousFiles' : dubious,
            'TotalNumberFiles' : good + bad + dubious
        }
        dynamo_put_item("Metadata", validation_params, dynamodb=dynamo)
        log.info("Results of data validations were uploaded to DynamoDB")
    except Exception as err:
        log.error("Results of data validations were not updated to DynamoDB")
        log.error(err)
        return False
    return True
    

if __name__ == "__main__":
    # create_dynamodb_tables(dynamo)
    # body = {
    #     'SchemaName' : 'u.darhevich-schema',
    #     'SchemaVersion' : '0.2-test',
    #     'schema_body': json.dumps(schema)
    # }
    # dynamo_put_item("Schemas", body, dynamo)
    key = {
        'SchemaName' : SCHEMA_NAME,
        'SchemaVersion' : SCHEMA_VERSION
    }
    response = dynamo_get_item("Schemas", key, dynamo)['schema_body']
    schema = json.loads(response)
    params = {
        'Bucket' : AWS_BUCKET,
        'Prefix' : PATH_RAW_DATA
    }
    data_validation(params, schema)

