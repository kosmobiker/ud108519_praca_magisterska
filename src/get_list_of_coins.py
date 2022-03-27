import os
import json
import boto3
import awswrangler as wr
import pandas as pd
from typing import List
from tenacity import *
from yarl import URL
from datetime import datetime
from utils.read_config import read_toml_config
from utils.call_get import call_get
from utils.logger import setup_applevel_logger
from utils.aws_utils import create_bucket


ROOT_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.abspath(os.path.join(ROOT_DIR, '..', 'config', 'config.toml'))

config = read_toml_config(CONFIG_PATH)
API_ENDPOINT = URL(config['dev']['api_endpoint'])
PATH_TO_LOGS = os.path.abspath(os.path.join(ROOT_DIR, '..', config['dev']['path_to_logs']))
PATH_COIN_LIST = config['dev']['path_coin_list']
AWS_PROFILE = config['dev']['aws_profile']
AWS_BUCKET = config['dev']['aws_bucket']
TODAY = datetime.today().strftime("%Y%m%d")

log = setup_applevel_logger(file_name = PATH_TO_LOGS + "/logging_{}".format(TODAY))
session = boto3.Session(profile_name=AWS_PROFILE)
s3_client = session.client('s3')


def get_coin_list() -> List[str]:
    """
    Function used to get list of coins from API
    Return
        List of strings for each coin
    """
    return [
        result for result in json.loads(
            call_get(API_ENDPOINT / "coins/list", {"include_platform" : "false"})
        )
    ]

if __name__ == "__main__":
    try:
        create_bucket(AWS_BUCKET, s3_client=s3_client)
        list_of_coins = pd.json_normalize(get_coin_list())
        path = f"s3://{AWS_BUCKET}/{PATH_COIN_LIST}/list_of_coins.csv"
        wr.s3.to_csv(list_of_coins, path, index=False, boto3_session=session)
        log.info('List of coins was uploaded to data lake')
    except Exception as err:
        log.info('List of coins was not uploaded to data lake')
        log.error(err)

