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
from logger import get_logger
from read_config import read_config

log = get_logger(__name__)
ROOT_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.abspath(os.path.join(ROOT_DIR, '..', '..', 'config/config.yaml'))
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
 