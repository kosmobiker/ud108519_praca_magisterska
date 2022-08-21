"""
Lambda function to get tweets
It is invoked by EventBridge trigger each 4 hours
"""

import tweepy
import os
import json
import boto3
from datetime import datetime, timedelta
import logging


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

consumer_key = os.environ['CONSUMER_KEY']
consumer_secret = os.environ['CONSUMER_SECRET']
bucket = os.environ['AWS_BUCKET']


auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)

def ceil_dt(dt, delta):
    return dt + (datetime.min - dt) % delta

def upload_json_s3(data, bucket, path):
    """
    Upload a raw json data to an S3 bucket
    """
    s3_client = boto3.client('s3')
    s3_client.put_object(Body=json.dumps(data), Bucket=bucket, Key=path)        
    return True

def get_tweets(tweet_search: dict):
    raw_json_data = []
    for key, value in tweet_search.items():
        ticker = key
        query = value
        for tweet in tweepy.Cursor(api.search_tweets, q=f"{query}", lang="en", result_type='mixed').items(250):
            new_datetime = datetime.strptime(tweet._json["created_at"],'%a %b %d %H:%M:%S +0000 %Y')
            ceil_datetime = ceil_dt(new_datetime, timedelta(minutes=15))
            month = int(ceil_datetime.strftime("%-m"))
            weekday = int(ceil_datetime.strftime("%w"))
            day = int(ceil_datetime.strftime("%-d"))
            hour = int(ceil_datetime.strftime("%-H"))
            minute = int(ceil_datetime.strftime("%-M"))
            string_encode = tweet._json["text"].encode("ascii", "ignore")
            string_decode = string_encode.decode()
            result = {
                "id" : tweet._json["id"], 
                "created_at": datetime.strftime(new_datetime, '%Y-%m-%d %H:%M:%S'),
                "ceil_datetime": datetime.strftime(ceil_datetime, '%Y-%m-%d %H:%M:%S'),
                "month": month,
                "weekday": weekday,
                "day": day, 
                "hour": hour, 
                "minute": minute, 
                "QUERY": f"{query}",
                "ticker":ticker,
                "text": string_decode,
                "favorite_count": tweet._json["favorite_count"],
                "result_type": tweet._json["metadata"]["result_type"],
                "user_name": tweet._json["user"]["screen_name"],
                "followers_count": tweet._json["user"]["followers_count"],
                "retweet_count": tweet._json["retweet_count"]
            }
            raw_json_data.append(result)

    return raw_json_data

def lambda_handler(event, context):
    """
    A handeler for lambda function
    """
    try:
        tweet_search = event['tweet_search']
        basic_folder = event['basic_folder']
        logger.debug('Start fetching tweets')
        data = get_tweets(tweet_search)
        now = int(datetime.now().timestamp())
        path = path = f"data/{basic_folder}/tweets_{now}.json"
        upload_json_s3(data, bucket, path)
        logger.debug('Uploaded to S3')
    except Exception as err:
        logger.info(f"FAILED: Not uploaded to S3")
        logger.error(err)