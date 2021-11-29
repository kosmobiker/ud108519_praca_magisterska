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
from utils.logger import setup_applevel_logger
import pandas as pd
from datetime import datetime 
from utils.utils import read_config, simple_request, check_limit
from utils.logger import setup_applevel_logger

CONFIG_PATH = "/home/vlad/master_project/config/cofig.yaml"

configs_dict = read_config(CONFIG_PATH)
BASE_URL = configs_dict["baseUrl"]
PATH_TO_SAVE = configs_dict["path_to_save_historical_data"]


log = setup_applevel_logger(file_name = '../logs/my_debug.log')


def get_historical_data(coin: str):
    check_limit()
    try:
        req = BASE_URL + 'coins/{}/market_chart?vs_currency=USD&days=max&interval=daily'.format(coin)
        res = requests.get(req).json()
        return res
    except:
        log.error("Didn't fetch {}. Will try later".format(coin))

def get_all_historical():
    print(datetime.now())
    log.info('start!')
    full_list = simple_request('coins/list')
    names_of_coins = [item['id'] for item in full_list][:200]
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
if __name__ == '__main__':
    get_all_historical()