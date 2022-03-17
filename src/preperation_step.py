import os
import requests
from yarl import URL
from datetime import datetime
from utils.read_config import read_toml_config
from utils.call_get import call_get
from utils.logger import setup_applevel_logger

ROOT_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.abspath(os.path.join(ROOT_DIR, '..', 'config', 'config.toml'))
TODAY = datetime.today().strftime("%Y%m%d")

config = read_toml_config(CONFIG_PATH)

API_ENDPOINT = URL(config['dev']['api_endpoint'])
PATH_TO_LOGS = os.path.abspath(os.path.join(ROOT_DIR, '..', config['dev']['path_to_logs']))
PATH_COIN_LIST = os.path.abspath(os.path.join(ROOT_DIR, '..', config['dev']['path_coin_list']))
PATH_RAW_DATA = os.path.abspath(os.path.join(ROOT_DIR, '..', config['dev']['path_raw_data']))
PATH_TO_DATALAKE = os.path.abspath(os.path.join(ROOT_DIR, '..', config['dev']['path_to_datalake']))


if __name__ == "__main__":
    # try:
    #     if not os.path.exists(PATH_TO_LOGS):
    #         os.makedirs(PATH_TO_LOGS)
    #     log = setup_applevel_logger(file_name = PATH_TO_LOGS + "/logging_{}".format(TODAY))
    #     if not os.path.exists(PATH_COIN_LIST):
    #         os.makedirs(PATH_COIN_LIST)
    #     if not os.path.exists(PATH_RAW_DATA):
    #         os.makedirs(PATH_RAW_DATA)
    #     if not os.path.exists(PATH_TO_DATALAKE):
    #         os.makedirs(PATH_TO_DATALAKE)
    #     assert requests.get(API_ENDPOINT / "ping").status_code == 200
    #     log.info('Everything is OK')
    # except Exception as err:
    #     log.info('Something is wrong')
    #     log.error(err)
    