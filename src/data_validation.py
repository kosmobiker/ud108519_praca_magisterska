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
from jsonschema import validate
from pprint import pprint
from utils.read_config import read_toml_config
from datetime import datetime
from decimal import Decimal
from utils.logger import setup_applevel_logger
from utils.aws_utils import dynamo_put_item, dynamo_get_item


ROOT_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.abspath(os.path.join(ROOT_DIR, '..', 'config', 'config.toml'))
config = read_toml_config(CONFIG_PATH)
print(config)
PATH_TO_LOGS = os.path.abspath(os.path.join(ROOT_DIR, '..', config['dev']['path_to_logs']))
TODAY = datetime.today().strftime("%Y%m%d")
PATH_RAW_DATA = config['dev']['path_raw_data']
AWS_PROFILE = config['dev']['aws_profile']
AWS_BUCKET = config['dev']['aws_bucket']
log = setup_applevel_logger(file_name = PATH_TO_LOGS + "/logging_{}".format(TODAY))


session = boto3.Session(profile_name=AWS_PROFILE)

# with open('/home/vlad/master_project/config/schema.json') as f:
#     schema = json.load(f)

def get_list_of_objects_s3(session, operation_parameters):
    """
    List files in specific S3 URL
    """
    s3 = session.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(**operation_parameters)
    for page in page_iterator:
        for content in page.get('Contents'):
            yield content.get('Key')


def validate_json(data, schema):
    try:
        validate(instance=data, schema=schema)
        return True
    except Exception:
        return False

def get_json_from_s3(session, bucket, key):
    s3 = session.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj['Body'].read().decode('utf-8')
    return json.loads(data)

def data_validation(session, params, schema):
    good_pathes = []
    bad_pathes = []
    dubious_pathes = []
    gen = get_list_of_objects_s3(session, params)
    for key in gen:
        try:
            data = get_json_from_s3(session, params["Bucket"], key)
            if len(data['prices']) < 100:
                bad_pathes.append(key)
                log.info(f"{key} is too short")
            else:
                if validate_json(data, schema):
                    good_pathes.append(key)
                    log.info(f'{key} is OK')
                else:
                    dubious_pathes.append(key)
                    log.info(f"{key} - schema not matched")
        except Exception as err:
            bad_pathes.append(key)
            log.error(f"{key} was added to bad pathes")
            log.error(err)

    return good_pathes, bad_pathes, dubious_pathes

if __name__ == "__main__":
    key = {
        'SchemaName' : 'u.darhevich-schema',
        'SchemaVersion' : '0.2-test',
    }
    response = dynamo_get_item(session, "Schemas", key)['schema_body']
    schema = json.loads(response)
    params = {
        'Bucket' : AWS_BUCKET,
        'Prefix' : PATH_RAW_DATA
    }
    g, b, d = data_validation(session, params, schema)
    log.info(f"Number of good files is {len(g)}")
    log.info(f"Number of bad files is {len(b)}")
    log.info(f"Number of dubious files is {len(d)}")
    # body = {
    #     'SchemaName' : 'u.darhevich-schema',
    #     'SchemaVersion' : '0.2-test',
    #     'schema_body': json.dumps(schema)
    # }
    # dynamo_put_item(session, "Schemas", body)
