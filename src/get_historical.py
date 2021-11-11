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

BASE_URL = 'https://api.coingecko.com/api/v3/'

def simple_request(req):
    api_url = BASE_URL + req
    raw = requests.get(api_url).json()
    return raw 


def get_historical_data(coin: str):
    try:
        req = BASE_URL + 'coins/{}/market_chart?vs_currency=USD&days=max&interval=daily'.format(coin)
        res = requests.get(req).json()
        return res
    except:
        time.sleep(10)
        req = BASE_URL + 'coins/{}/market_chart?vs_currency=USD&days=max&interval=daily'.format(coin)
        res = requests.get(req).json()
        return res

if __name__ == "__main__":
    print(datetime.now())
    print('start!')
    full_list = simple_request('coins/list')
    names_of_coins = [item['id'] for item in  full_list]
    sampled_list = random.sample(names_of_coins, 1000)
    path_to_save = '../data/historical'
    for coin in sampled_list:
        tmp = get_historical_data(coin)
        with open('{}/{}_historical_prices.json'.format(path_to_save, coin), 'w') as f:
            json.dump(tmp, f)
        print('{} is uploaded'.format(coin))
    print('done!')

