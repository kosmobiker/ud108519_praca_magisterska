"""
Module for data transformation
"""
from datetime import datetime
import pandas as pd

def get_pathes_from_dynamo

def transform_json_dataframe(data: dict) -> pd.DataFrame:
    ts = [i[0] for i in data['prices']]
    prices = [i[1] for i in data['prices']]
    market_caps = [i[1] for i in data['market_caps']]
    total_volumes = [i[1] for i in data['total_volumes']]
    temp = pd.DataFrame({
        "timestamp" : ts,
        "prices" : prices,
        "market_caps" : market_caps,
        "total_volumes" : total_volumes,
        "formated_date" : [datetime.fromtimestamp(i/1000).strftime('%Y-%m-%d') for i in ts]
        })
    temp['coin'] = 'bitcoin'
    temp['currency'] = 'usd'
    return temp

