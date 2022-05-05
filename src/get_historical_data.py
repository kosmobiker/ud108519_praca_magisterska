"""
This module is used to get historical data
from list of coins provided by API
"""
import os
import json
from typing import List
import concurrent.futures
import threading
import awswrangler as wr
import boto3
from tenacity import *
from yarl import URL
from datetime import datetime
from utils.read_config import read_toml_config
from utils.call_get import call_get
from utils.logger import setup_applevel_logger


ROOT_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.abspath(os.path.join(ROOT_DIR, '..', 'config', 'config.toml'))

config = read_toml_config(CONFIG_PATH)
API_ENDPOINT = URL(config['dev']['api_endpoint'])
PATH_COIN_LIST = config['dev']['path_coin_list']
PATH_RAW_DATA = config['dev']['path_raw_data']
PATH_TO_LOGS = os.path.abspath(os.path.join(ROOT_DIR, '..', config['dev']['path_to_logs']))
TODAY = datetime.today().strftime("%Y%m%d")
AWS_PROFILE = config['dev']['aws_profile']
AWS_BUCKET = config['dev']['aws_bucket']
LAMBA_FUNC = config['dev']['lambda_func_name']

log = setup_applevel_logger(file_name = PATH_TO_LOGS + "/logging_{}".format(TODAY))

session = boto3.Session(profile_name=AWS_PROFILE)
s3_client = session.client('s3')
lambda_client = session.client('lambda')


def lambda_get_data(func_name: str, params: dict, client=None):
    """
    Function used to invoke lambda function
    with various parameters
    """
    if not client:
        client = boto3.client('lambda')
    response = client.invoke(
            FunctionName=func_name,
            InvocationType='Event',
            Payload=json.dumps(params)
        )
    return response
    
def download_coins(coin: str):
    thread_local.coin = coin
    params = {
            "bucket": AWS_BUCKET,
            "path_raw_data": PATH_RAW_DATA
        }
    params['coin'] = coin
    lambda_get_data(LAMBA_FUNC, params, lambda_client)
    log.info(f"Lambda function to get {coin} was invoked ")

def download_all_coins(coins: List):
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        executor.map(download_coins, coins)


if __name__ == "__main__":  
    thread_local = threading.local()
    path_list_of_coins = f"s3://{AWS_BUCKET}/{PATH_COIN_LIST}/list_of_coins.csv"
    list_of_coins = wr.s3.read_csv([path_list_of_coins], boto3_session=session, encoding='utf8')['id'].to_list()[1700:1800] #for test only
    download_all_coins(list_of_coins)
