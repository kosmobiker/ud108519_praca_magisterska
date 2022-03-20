"""
this is a lambda function used to get historical data of cryptocurrencies.
API of coingecko is limited: it allows to call only 50 requests/minute
"""
import json
import requests
import boto3
from typing import List

from ratelimit import RateLimitException, limits
from tenacity import *
from yarl import URL
import logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

API_ENDPOINT = URL("https://api.coingecko.com/api/v3")

@retry(
    retry=retry_if_exception_type(requests.exceptions.RequestException),
    wait=wait_exponential(max=60),
    stop=stop_after_attempt(3),
)
@retry(
    retry=retry_if_exception_type(RateLimitException),
    wait=wait_fixed(60),
    stop=stop_after_delay(360),
)
@limits(calls=50, period=60)
def call_get(url: URL, params: dict) -> str:
    """
    Get the call from API
    """
    return requests.get(url.update_query(params)).text

def get_historical_data(id: str) -> List[str]:
    return json.loads(
            call_get(API_ENDPOINT / "coins" / id / "market_chart", {"vs_currency" : "usd", "days" : "max"})
        )

def upload_json_s3(data, bucket, path):
    """
    Upload a raw json data to an S3 bucket
    """
    s3_client = boto3.client('s3')
    s3_client.put_object(Body=json.dumps(data), Bucket=bucket, Key=path)        

    return True

def lambda_handler(event, context):
    try:
        coin = event['coin']
        bucket = event['bucket']
        path_raw_data = event['path_raw_data']
        result = get_historical_data(coin)
        path = f"{path_raw_data}/{coin}_historical_prices.json"
        upload_json_s3(result, bucket, path)
        logger.info(f"{coin} was uploaded to S3")
        return True
    except Exception as err:
        logger.info(f"FAILED: {coin} was not uploaded to S3")
        logger.error(err)
