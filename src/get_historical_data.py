"""
This module is used to get historical data
from list of coins provided by API
"""
import os
import json
from typing import List
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

def get_historical_data(id: str) -> List[str]:
    """
    Function used to get historical data for each coin
    Params
        id: str name of the coin
    Return
        List with strings
    """
    return json.loads(
            call_get(API_ENDPOINT / "coins" / id / "market_chart", {"vs_currency" : "usd", "days" : "max"})
        )

# def save_raw_json(data, coin: str):
#     with open(PATH_RAW_DATA + '/{}_historical_prices.json'.format(coin), 'w') as f:
#         json.dump(data, f)

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

if __name__ == "__main__":  
    path_list_of_coins = f"s3://{AWS_BUCKET}/{PATH_COIN_LIST}/list_of_coins.csv"
    list_of_coins = wr.s3.read_csv([path_list_of_coins], boto3_session=session, encoding='utf8')['id'].to_list()[1700:1800] #for test only
    params = {
            "bucket": AWS_BUCKET,
            "path_raw_data": PATH_RAW_DATA
        }
    for coin in list_of_coins:
        # try:
        #     result = get_historical_data(coin)
        #     path = f"{PATH_RAW_DATA}/{coin}_historical_prices.json"
        #     upload_json_s3(result, session, AWS_BUCKET, path)
        #     log.info(f"{coin} - raw json file was saved")
        # except Exception as err:
        #     log.error(f"{coin} - raw json file was not saved")
        #     log.error(err)
        try:
            params['coin'] = coin
            lambda_get_data(LAMBA_FUNC, params, lambda_client)
            log.info(f"{coin} - raw json file was saved")
        except Exception as err:
            log.error(f"{coin} - raw json file was not saved")
            log.error(err)        
            


