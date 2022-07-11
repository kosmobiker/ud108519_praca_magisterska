"""
This is a lmabda function which is using to fetch daily data regarding 
cryptocurrencies - OHLC + volumes
"""
import os
import cryptocompare
import datetime
import logging
import boto3
import json
from typing import Dict, List

#logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

#someimports
crypto_compare_key= os.environ['CRYPTO_COMPARE_KEY']
bucket = os.environ['AWS_BUCKET']
cryptocompare.cryptocompare._set_api_key_parameter(crypto_compare_key)

def get_daily_data(coin:str, cur:str) -> List[Dict]:
    """
    Retrieve daily data as list of dicts
    """
    try:
        return cryptocompare.get_historical_price_minute(coin, cur)
    except Exception as err:
        logger.error(err)

def upload_json_s3(data, bucket, path):
    """
    Upload a raw json data to an S3 bucket
    """
    s3_client = boto3.client('s3')
    s3_client.put_object(Body=json.dumps(data), Bucket=bucket, Key=path)        
    return True

def lambda_handler(event, context):
    """
    A handeler for lambda function
    """
    try:
        list_of_coins = event['list_of_coins']
        list_of_currencies = event['list_of_currencies']
        basic_folder = event['basic_folder']
        year = datetime.datetime.now().strftime("%Y")
        month = datetime.datetime.now().strftime("%m")
        day = datetime.datetime.now().strftime("%d")
        for coin in list_of_coins:
            for currency in list_of_currencies:
                if coin != currency:
                    result = get_daily_data(coin, currency)
                    path = f"data/{basic_folder}/{coin}-{currency}/{year}_{month}_{day}_{coin}-{currency}/.json"
                    upload_json_s3(result, bucket, path)
        logger.info(f"{coin}-{currency} was uploaded to S3")
        return True
    except Exception as err:
        logger.info(f"FAILED: {coin}-{currency} was not uploaded to S3")
        logger.error(err)