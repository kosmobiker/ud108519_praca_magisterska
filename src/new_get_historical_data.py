"""
this module is used to get historical data for 
selected cryptocurrencies using Cryptocompare API
with 1 hour interval

Contains 3 steps:

1. Extract - get info from Crytocurrency API
2. Load - save raw files to S3
3. Transform - make data dtransformation and upload it to HIVE table
"""
import os
import boto3
import cryptocompare
import pandas as pd
import awswrangler as wr
from datetime import datetime
from utils.read_config import read_toml_config
from utils.logger import setup_applevel_logger


ROOT_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.abspath(os.path.join(ROOT_DIR, '..', 'config', 'config.toml'))

config = read_toml_config(CONFIG_PATH)
CRYPTO_COMPARE_KEY = config['dev']['api_endpoint']
cryptocompare.cryptocompare._set_api_key_parameter(CRYPTO_COMPARE_KEY)
PATH_COIN_LIST = config['dev']['path_coin_list']
PATH_TO_LOGS = os.path.abspath(os.path.join(ROOT_DIR, '..', config['dev']['path_to_logs']))
TODAY = datetime.today().strftime("%Y%m%d")
AWS_PROFILE = config['dev']['aws_profile']
AWS_BUCKET = config['dev']['aws_bucket']
LIST_OF_COINS = config['dev']['list_of_coins']
LIST_OF_CURRENCIES = config['dev']['list_of_currencies']

log = setup_applevel_logger(file_name = PATH_TO_LOGS + "/logging_{}".format(TODAY))

session = boto3.Session(profile_name=AWS_PROFILE)

#get dates of coin creation
coin_list = wr.s3.read_csv(f's3://{AWS_BUCKET}/{PATH_COIN_LIST}', boto3_session=session)
tmp_df = coin_list[coin_list['Name'].isin(LIST_OF_COINS)][['Name', 'ContentCreatedOn']]
created_on = dict(zip(tmp_df.Name,tmp_df.ContentCreatedOn))

def extract(coin:str,
            cur:str,
            created_on:dict,
            ts=int(datetime.now().timestamp()),
            limit=2000) -> pd.DataFrame:
    data = []
    done = False
    while not done:
        try:
            tmp_json = cryptocompare.get_historical_price_hour(coin, cur, limit=limit, toTs=ts)
            if ts < created_on[coin]:
                done = True
            else:
                data.extend(tmp_json)
                ts -= 3600000
                done = False
        except Exception as err:
            print(err)
            done = True
    return pd.DataFrame(data)

def transform(df: pd.DataFrame) -> pd.DataFrame:

