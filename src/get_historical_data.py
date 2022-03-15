import os
import json
from typing import List

import pandas as pd
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
PATH_COIN_LIST = os.path.abspath(os.path.join(ROOT_DIR, '..', config['dev']['path_coin_list']))
PATH_RAW_DATA = os.path.abspath(os.path.join(ROOT_DIR, '..', config['dev']['path_raw_data']))
PATH_TO_LOGS = os.path.abspath(os.path.join(ROOT_DIR, '..', config['dev']['path_to_logs']))
TODAY = datetime.today().strftime("%Y%m%d")

log = setup_applevel_logger(file_name = PATH_TO_LOGS + "/logging_{}".format(TODAY))

# get list of coins
list_of_coins = pd.read_csv(PATH_COIN_LIST + "/list_of_coins.csv")['id'].to_list()
#or get data from hive using sql

def get_historical_data(id: str) -> List[str]:
    return json.loads(
            call_get(API_ENDPOINT / "coins" / id / "market_chart", {"vs_currency" : "usd", "days" : "max"})
        )

def save_raw_json(data, coin: str):
    with open(PATH_RAW_DATA + '/{}_historical_prices.json'.format(coin), 'w') as f:
        json.dump(data, f)

if __name__ == "__main__":
    if not os.path.exists(PATH_RAW_DATA):
        os.makedirs(PATH_RAW_DATA)
        log.info("{} was created".format(PATH_RAW_DATA))
    for coin in list_of_coins:
        try:
            result = get_historical_data(coin)
            save_raw_json(result, coin)
            log.info("{} - raw json file was saved".format(coin))
        except Exception as err:
            log.error("{} - raw json file was noot save, see details".format(coin), err)
    log.info('Upload of raw json files is complete')
            


