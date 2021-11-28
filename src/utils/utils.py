"""
Common utils used in various scripts
"""
import json
import os
import random
import time
import confuse
import requests
import pandas as pd
from datetime import datetime 
from ratelimit import limits, sleep_and_retry

CONFIG_PATH = '../../config/config.yaml'

def config_reader(path: str, section: str):
    config = confuse.Configuration('MyGreatApp', __name__)
    config.set_file(CONFIG_PATH)
    v = config[section].get()
    return v

print(CONFIG_PATH)

BASE_URL = config_reader(CONFIG_PATH, "baseUrl")
CALLS = config_reader(CONFIG_PATH, "calls")
RATE_LIMIT = config_reader(CONFIG_PATH, "ratelimit")

# print(BASE_URL, CALLS, RATE_LIMIT)

# @sleep_and_retry
# @limits(calls=CALLS, period=RATE_LIMIT)
# def check_limit():
#     """
#     It is used to deal with API limitations
#     """
#     pass

# def simple_request(req: str) -> dict:
#     """
#     Get simple request from API
#     """
#     api_url = BASE_URL + req
#     raw = requests.get(api_url).json()
#     return raw 