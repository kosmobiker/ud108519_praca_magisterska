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
from utils.read_config import read_toml_config
from datetime import datetime
from utils.logger import setup_applevel_logger


ROOT_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.abspath(os.path.join(ROOT_DIR, '..', 'config', 'config.toml'))

config = read_toml_config(CONFIG_PATH)
PATH_TO_LOGS = os.path.abspath(os.path.join(ROOT_DIR, '..', config['dev']['path_to_logs']))
TODAY = datetime.today().strftime("%Y%m%d")
PATH_RAW_DATA = config['dev']['path_raw_data']
AWS_PROFILE = config['dev']['aws_profile']
AWS_BUCKET = config['dev']['aws_bucket']
log = setup_applevel_logger(file_name = PATH_TO_LOGS + "/logging_{}".format(TODAY))



session = boto3.Session(profile_name=AWS_PROFILE)

def get_list_of_objects_s3(session, bucket):
    """
    List files in specific S3 URL
    """
    s3 = session.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket)
    for page in page_iterator:
        for content in page.get('Contents'):
            yield content.get('Key')


with open('/home/vlad/master_project/config/schema.json') as f:
    schema = json.load(f)

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

def data_validation(session, bucket, schema):
    good_pathes = []
    bad_pathes = []
    dubious_pathes = []
    gen = get_list_of_objects_s3(session, bucket)
    for _ in gen:
        try:
            key = next(gen)
            data = get_json_from_s3(session, bucket, key)
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
    g, b, d = data_validation(session, AWS_BUCKET, schema)
    log.info(f"Number of good files is {len(g)}")
    log.info(f"Number of bad files is {len(b)}")
    log.info(f"Number of dubious files is {len(d)}")