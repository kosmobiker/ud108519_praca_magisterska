"""
This is used to get all historical data
from the Coingecko API

test_conenction >> get_list_of_coins >> get_historical_data
"""
import json
import os
import random
import time
import requests
import pandas as pd
from datetime import datetime
import sys; sys.path.insert(0, "utils") 
from utils.read_config import read_config
from utils.funcs import check_limit, simple_request
from utils.logger import setup_applevel_logger

ROOT_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.abspath(os.path.join(ROOT_DIR, '..', 'config/config.yaml'))
TODAY = datetime.today().strftime("%Y%m%d")

configs_dict = read_config(CONFIG_PATH)
BASE_URL = configs_dict["baseUrl"]
PATH_TO_SAVE = configs_dict["path_to_save_historical_data"]
PATH_TO_LOGS = configs_dict["path_to_save_logs"]

log = setup_applevel_logger(file_name = PATH_TO_LOGS + "logging_{}".format(TODAY))


def get_historical_data(coin: str):
    check_limit()
    try:
        req = BASE_URL + 'coins/{}/market_chart?vs_currency=USD&days=max&interval=daily'.format(coin)
        res = requests.get(req).json()
        return res
    except:
        pass

def get_all_historical():
    log.info('Starting to upload historical data for cryptocurrencies!')
    full_list = simple_request('coins/list')
    names_of_coins = [item['id'] for item in full_list][:200] # for debug purposes
    while len(names_of_coins) > 0:
        try: 
            coin = names_of_coins[0]
            tmp = get_historical_data(coin)
            with open('{}/{}_historical_prices.json'.format(PATH_TO_SAVE, coin), 'w') as f:
                json.dump(tmp, f)
            names_of_coins.remove(coin)
            log.debug("{} is uploaded".format(coin))
        except:
            pass
    log.info('Historical data for cryptocurrency is uploaded')

if __name__ == '__main__':
    get_all_historical()