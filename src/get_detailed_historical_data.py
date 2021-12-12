"""
This is used to get detailed histotical data
for the perios from the date o initial upload
to N dats (f.e. 1000)
Data is fetched from the Coingecko API
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
PATH_TO_SAVE = configs_dict["path_to_save_data"]
PATH_TO_LOGS = configs_dict["path_to_save_logs"]
LIST_OF_COINS = configs_dict["list_of_coins"]


log = setup_applevel_logger(file_name = PATH_TO_LOGS + "logging_{}".format(TODAY))


def get_detailed_data(coin: str, date: str):
    check_limit()
    try:
        req = BASE_URL + '/coins/{}/history?date={}'.format(coin, date)
        res = requests.get(req).json()
        return res
    except:
        pass

def get_detailed_historical_data(list_coins: str, start_date: str, end_date: str):
    log.info('Starting to detailed historical data for selected cryptocurrencies!')
    for coin in list_coins:
        datelist = pd.date_range(start=start_date, end=end_date).tolist()
        while datelist:
            try:
                date = datelist[0]
                normal_date = date.strftime("%Y-%m-%d")
                converted_date = date.strftime("%d-%m-%Y")
                tmp = get_detailed_data(coin, converted_date)
                directory = PATH_TO_SAVE + "/" + coin
                if not os.path.exists(directory):
                    os.makedirs(directory)
                with open('{}/{}_{}.json'.format(directory, coin, normal_date), 'w') as f:
                    json.dump(tmp, f)
                datelist.remove(date)
                log.debug("{}_{} is uploaded".format(coin, normal_date))
            except Exception as e:
                print(e)
    log.info('Detailed historical data for selected cryptocurrencies is ready!')

if __name__ == "__main__":
    get_detailed_historical_data(LIST_OF_COINS, '2021-12-01', '2021-12-12')



    

