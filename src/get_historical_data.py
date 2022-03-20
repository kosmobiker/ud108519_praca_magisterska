import os
import json
from typing import List
import awswrangler as wr
import pandas as pd
import boto3
import pandas as pd
from tenacity import *
from yarl import URL
from datetime import datetime
from utils.read_config import read_toml_config
from utils.call_get import call_get
from utils.logger import setup_applevel_logger
from utils.aws_utils import upload_json_s3


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

log = setup_applevel_logger(file_name = PATH_TO_LOGS + "/logging_{}".format(TODAY))

def get_historical_data(id: str) -> List[str]:
    return json.loads(
            call_get(API_ENDPOINT / "coins" / id / "market_chart", {"vs_currency" : "usd", "days" : "max"})
        )

def save_raw_json(data, coin: str):
    with open(PATH_RAW_DATA + '/{}_historical_prices.json'.format(coin), 'w') as f:
        json.dump(data, f)

def lambda_get_data(func_name: str, params: dict, client):
    response = client.invoke(
            FunctionName=func_name,
            InvocationType='Event',
            Payload=json.dumps(params)
        )
    return response

if __name__ == "__main__":
    #     if not os.path.exists(PATH_RAW_DATA):
    #         os.makedirs(PATH_RAW_DATA)
    #         log.info("{} was created".format(PATH_RAW_DATA))
    #     for coin in list_of_coins:
    #         try:
    #             result = get_historical_data(coin)
    #             save_raw_json(result, coin)
    #             log.info("{} - raw json file was saved".format(coin))
    #         except Exception as err:
    #             log.error("{} - raw json file was not save, see details".format(coin), err)
    #     log.info('Uploading of raw json files is complete')
    # else:
    #session = boto3.Session(profile_name='default')
    session = boto3.Session(profile_name=AWS_PROFILE)
    lambda_client = session.client('lambda')
    path_list_of_coins = f"s3://{AWS_BUCKET}/{PATH_COIN_LIST}/list_of_coins.csv"
    list_of_coins = wr.s3.read_csv([path_list_of_coins], boto3_session=session)['id'].to_list()[666:777] #for test only
    params = {
            "bucket": AWS_BUCKET,
            "path_raw_data": PATH_RAW_DATA
        }
    func = 'Kosmo_Test_Lambda_Function'

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
            lambda_get_data(func, params, lambda_client)
            log.info(f"{coin} - raw json file was saved")
        except Exception as err:
            log.error(f"{coin} - raw json file was not saved")
            log.error(err)        
            


