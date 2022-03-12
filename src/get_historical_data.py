import os
import json
from typing import List

import pandas as pd
from tenacity import *
from yarl import URL
from utils.read_config import read_toml_config
from utils.call_get import call_get

ROOT_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.abspath(os.path.join(ROOT_DIR, '..', 'config', 'config.toml'))

config = read_toml_config(CONFIG_PATH)
API_ENDPOINT = URL(config['dev']['api_endpoint'])
PATH_COIN_LIST = os.path.abspath(os.path.join(ROOT_DIR, '..', config['dev']['path_coin_list']))
PATH_RAW_DATA = os.path.abspath(os.path.join(ROOT_DIR, '..', config['dev']['path_raw_data']))

# get list of coins
list_of_coins = pd.read_csv(PATH_COIN_LIST + "/list_of_coins.csv")['id'].to_list()
#or get data from hive using sql

def get_historical_data(id: str) -> List[str]:
    return [
        result for result in json.loads(
            call_get(API_ENDPOINT / "/coins/{id}/market_chart", dict())
        )
    ]