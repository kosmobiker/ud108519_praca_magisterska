"""
This module is used to get current data
from list of TOP coins
It takes open, high, low, and closing prices.
This data will be used for OHLC Charts
"""
import os
import json
from typing import List
import awswrangler as wr
import pandas as pd
import boto3
from tenacity import *
from yarl import URL
from datetime import datetime
from jsonschema import validate

from utils.read_config import read_toml_config
from utils.call_get import call_get
from utils.aws_utils import upload_json_s3
from utils.logger import setup_applevel_logger


ROOT_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.abspath(os.path.join(ROOT_DIR, '..', 'config', 'config.toml'))

config = read_toml_config(CONFIG_PATH)
API_ENDPOINT = URL(config['dev']['api_endpoint'])
PATH_COIN_LIST = config['dev']['path_coin_list']
PATH_RAW_DATA = config['dev']['path_raw_data']
PATH_CURRENT_DATA = config['dev']['path_current_data']
PATH_TO_LOGS = os.path.abspath(os.path.join(ROOT_DIR, '..', config['dev']['path_to_logs']))
TODAY = datetime.today().strftime("%Y%m%d")
AWS_PROFILE = config['dev']['aws_profile']
AWS_BUCKET = config['dev']['aws_bucket']
LAMBA_FUNC = config['dev']['lambda_func_name']

log = setup_applevel_logger(file_name = PATH_TO_LOGS + "/logging_{}".format(TODAY))

session = boto3.Session(profile_name=AWS_PROFILE)
s3_client = session.client('s3')
lambda_client = session.client('lambda')

def validate_json(data, schema):
    #move to utils
    """
    Validation of JSON file according to schema
    """
    try:
        validate(instance=data, schema=schema)
        return True
    except Exception:
        return False

def get_current_ohlc_data(id: str, params: dict, bucket: str, schema):
    try:
        data = json.loads(
            call_get(API_ENDPOINT / "coins" / id / 'ohlc', params)
            )
        #save to s3
        path = f"{PATH_CURRENT_DATA}/{TODAY}_{id}.json"
        upload_json_s3(data, bucket, path, s3_client)
        #schema validation
        if validate_json(data, schema):
            #transformation
            temp_df = pd.DataFrame(data, columns = ['timestamp', 'open', 'high', 'low', 'close'])
            temp_df['datetime'] = pd.to_datetime(temp_df['timestamp'], unit='ms')
            temp_df['coin'] = id
            temp_df['formated_date'] = temp_df['datetime'].dt.strftime('%Y-%m-%d')
            #append to table
        else:
            log.info(f"Data for {id} for {TODAY} is not valid")
            log.error(f"Data for {id} for {TODAY} was not added to Hive table")
    except Exception as err:
        log.error(f"Data for {id} for {TODAY} was not added to Hive table")
        log.error(err)
        return False
    else:
        return True