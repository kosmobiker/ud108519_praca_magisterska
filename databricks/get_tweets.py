# Databricks notebook source
import os
import cryptocompare
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import yfinance as yahooFinance
import pandas as pd
import datetime

import datetime
import requests
import json
from functools import reduce
import matplotlib.pyplot as plt
import numpy as np
%matplotlib inline

import tweepy
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, TimestampType, DateType
import pandas as pd
import numpy as np
import json
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

#setup twitter

consumer_key = dbutils.secrets.getBytes(scope="demo_secrets", key="CONSUMER_KEY").decode("utf-8")
consumer_secret = dbutils.secrets.getBytes(scope="demo_secrets", key="CONSUMER_SECRET").decode("utf-8")

auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)

# COMMAND ----------

tweet_search = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "BUSD": "Binance USD",
    "USDT": "Tether",
    "XRP": "XRP",
    "SOL": "Solana",
    "BNB": "BNB",
    "DOT": "Polkadot",
    "SHIB": "Shibtoken",
    "LTC": "Litecoin"
  }
twitter_schema = StructType(fields=[
    StructField('id', StringType(), True),
    StructField('created_at', TimestampType(), True),
    StructField('ceil_datetime', TimestampType(), True),
    StructField('month', IntegerType(), True),
    StructField('weekday', IntegerType(),True),
    StructField('day', IntegerType(), True),
    StructField('hour', IntegerType(),True),
    StructField('minute', IntegerType(), True),
    StructField('QUERY', StringType(), True),
    StructField('ticker', StringType(), True),
    StructField('text', StringType(), True),
    StructField('favorite_count', IntegerType(), True),
    StructField('result_type', StringType(), True),
    StructField('user_name', StringType(), True),
    StructField('followers_count', IntegerType(), True),
    StructField('retweet_count', IntegerType(), True),
])

# COMMAND ----------

def ceil_dt(dt, delta):
    return dt + (datetime.min - dt) % delta

def get_tweets(tweet_search: dict):
    raw_json_data = []
    for key, value in tweet_search.items():
        ticker = key
        query = value
        for tweet in tweepy.Cursor(api.search_tweets, q=f"{query}", lang="en", result_type='mixed').items(200):
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

# COMMAND ----------

tweet_data = get_tweets(tweet_search)

# COMMAND ----------

spark_df = sqlContext.read.json(sc.parallelize(tweet_data), schema=twitter_schema)

# COMMAND ----------

spark_df.createOrReplaceTempView('tmp_df')
spark_df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO BRONZE_TWEET_DATA AS target
# MAGIC USING tmp_df AS source
# MAGIC ON source.id=target.id AND source.created_at=target.created_at AND source.ceil_datetime=target.ceil_datetime
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *

# COMMAND ----------


