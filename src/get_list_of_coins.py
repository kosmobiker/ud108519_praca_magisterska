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


def get_coin_list() -> List[str]:
    return [
        result for result in json.loads(
            call_get(API_ENDPOINT / "coins/list", {"include_platform" : "false"})
        )
    ]

if __name__ == "__main__":
    if not os.path.exists(PATH_COIN_LIST):
        os.makedirs(PATH_COIN_LIST)
    pd.json_normalize(get_coin_list()).to_csv(PATH_COIN_LIST + '/list_of_coins.csv')
    #it will be a function to create a hive table with list of coins

