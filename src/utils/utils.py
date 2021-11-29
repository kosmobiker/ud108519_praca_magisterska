"""
Common utils used in various scripts
"""
import json
import os
import random
import time
import requests
import pandas as pd
import yaml
from datetime import datetime 
from ratelimit import limits, sleep_and_retry
import logger

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = "/home/vlad/master_project/config/cofig.yaml"
log = logger.get_logger(__name__)


def read_config(path: str):
    """
    It is used to read configs
    """
    with open(CONFIG_PATH, "r") as conf:
        try:
            return yaml.safe_load(conf)
        except yaml.YAMLError as exc:
            log.error(exc)

configs_dict = read_config(CONFIG_PATH)
BASE_URL = configs_dict["baseUrl"]
CALLS = configs_dict["calls"]
RATE_LIMIT = configs_dict["rateLimit"]

@sleep_and_retry
@limits(calls=CALLS, period=RATE_LIMIT)
def check_limit():
    """
    It is used to deal with API limitations
    """
    pass

def simple_request(req: str):
    """
    Get simple request from API
    """
    api_url = BASE_URL + req
    raw = requests.get(api_url).json()
    return raw 