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
from ratelimit import limits, sleep_and_retry
from utils.utils import read_config, simple_request, check_limit

CONFIG_PATH = "C:\\Users\\Vlad\\Documents\\mastesis\\config\\cofig.yaml"
configs_dict = read_config(CONFIG_PATH)
BASE_URL = configs_dict["baseUrl"]
PATH_TO_SAVE = configs_dict["path_to_save_historical_data"]

def get_historical_data(coin: str):
    check_limit()
    try:
        req = BASE_URL + 'coins/{}/market_chart?vs_currency=USD&days=max&interval=daily'.format(coin)
        res = requests.get(req).json()
        return res
    except:
        print("Didn't fetch {}".format(coin))

def get_all_historical():
    print(datetime.now())
    print('start!')
    full_list = simple_request('coins/list')
    names_of_coins = [item['id'] for item in full_list]
    for coin in names_of_coins[5555:5655]:
        tmp = get_historical_data(coin)
        with open('{}/{}_historical_prices.json'.format(PATH_TO_SAVE, coin), 'w') as f:
            json.dump(tmp, f)
        print('{} is uploaded'.format(coin))
    print('done!')

get_all_historical()