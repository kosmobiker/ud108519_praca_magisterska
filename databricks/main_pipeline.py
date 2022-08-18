# Databricks notebook source
import os
import cryptocompare

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from delta.tables import *
from pyspark.sql.types import *

import yfinance as yahooFinance
import pandas as pd
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

# COMMAND ----------

AZURE_KEY = dbutils.secrets.getBytes(scope="demo_secrets", key="azure_key").decode("utf-8")
spark.conf.set("fs.azure.account.key.databrickstore.blob.core.windows.net", AZURE_KEY)

# COMMAND ----------

container_name = "ud108519"
storage_account_name = "kosmobiker"
dbutils.fs.ls(f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/data")

# COMMAND ----------

# dbutils.fs.mount(
#   source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
#   mount_point = "/mnt/data",
#   extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": AZURE_KEY})
dbutils.fs.refreshMounts()

# COMMAND ----------

#setup cryptocompare
crypto_compare_key = dbutils.secrets.getBytes(scope="demo_secrets", key="CRYPTO_COMPARE_KEY").decode("utf-8")
cryptocompare.cryptocompare._set_api_key_parameter(crypto_compare_key)

#setup twitter

consumer_key = dbutils.secrets.getBytes(scope="demo_secrets", key="CONSUMER_KEY").decode("utf-8")
consumer_secret = dbutils.secrets.getBytes(scope="demo_secrets", key="CONSUMER_SECRET").decode("utf-8")

auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS delta_lake
# MAGIC COMMENT 'main schema to be used'
# MAGIC LOCATION 'dbfs:/mnt/data/delta_lake';
# MAGIC 
# MAGIC USE delta_lake;

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Level

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coin list

# COMMAND ----------

coin_info_schema = StructType([
        StructField("Id", StringType(), True),
        StructField("Name",StringType(),True),
        StructField("FullName", StringType(),True),
        StructField("CoinName", StringType(),True),
        StructField("Symbol", StringType(),True),
        StructField("Description", StringType(),True),
        StructField("ContentCreatedOn", LongType(), True),
        StructField("Algorithm",StringType(),True),
        StructField("ProofType",StringType(),True),
        StructField("AssetTokenStatus", StringType(),True),
        StructField("ImageUrl", StringType(),True),
        StructField("Url", StringType(),True),
        StructField("Sponsored", BooleanType(), True),
        StructField("Taxonomy", MapType(StringType(), StringType(), True), True),
        StructField("Rating", MapType(StringType(), MapType(StringType(), StringType(), True), True), True)        
])


coin_info = cryptocompare.get_coin_list()
coin_info_df = spark.createDataFrame(coin_info.values(), schema=coin_info_schema)

# COMMAND ----------

coin_info_df\
    .coalesce(1)\
    .write\
    .format('delta')\
    .mode('overwrite')\
    .saveAsTable("delta_lake.coin_list")

# COMMAND ----------

display(spark.read.table("coin_list").orderBy('ContentCreatedOn').limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Historical data

# COMMAND ----------

df = (spark.read.table("coin_list"))
list_of_coins = [
    "BTC",
    "ETH",
    "BUSD",
    "USDT",
    "XRP",
    "SOL",
    "BNB",
    "DOT",
    "SHIB",
    "LTC"
  ]
# list_of_currencies = [
#     "USD",
#     "EUR",
#     "JPY",
#     "BTC"
#   ]
list_of_currencies = ['USD']
created_on = {row['Name']:row['ContentCreatedOn'] for row in df.collect() if row['Name'] in list_of_coins}
created_on

# COMMAND ----------

list_of_currencies = ["USD", "BTC"]
dataframe_schema = StructType([
        StructField("time",LongType(), True),
        StructField("high",DoubleType(),True),
        StructField("low", DoubleType(),True),
        StructField("open", DoubleType(),True),
        StructField("volumefrom", DoubleType(),True),
        StructField("volumeto", DoubleType(),True),
        StructField("close", DoubleType(),True),
        StructField("conversionType", StringType(),True),
        StructField("conversionSymbol", StringType(),True),
        StructField("coin_currency", StringType(),True)
])

# COMMAND ----------

def get_historical_data(coin:str,
                        cur:str,
                        created_on:dict,
                        schema,
                        ts=int(datetime.now().timestamp()),
                        limit=2000):
    data = []
    done = False
    while not done:
        try:
            tmp_json = cryptocompare.get_historical_price_hour(coin, cur, limit=limit, toTs=ts)
            if ts < created_on[coin]:
                done = True
            else:
                data.extend(tmp_json)
                ts -= 3600000
                done = False
        except Exception as err:
            print(err)
            done = True
    return sqlContext.read.json(sc.parallelize(data), schema=schema)

# COMMAND ----------

for coin in list_of_coins:
    for cur in list_of_currencies:
        if coin != cur:
            try:
                df = get_historical_data(coin, cur, created_on, dataframe_schema)
                df = df.fillna(value=f"{coin}_{cur}", subset=['coin_currency']) 
                (df.coalesce(1)
                    .write
                    .format('delta')
                    .mode('append')
                    .saveAsTable("BRONZE_OHLC_DATA")
                )
                print(f"{coin}_{cur} was uploaded!")
            except Exception as err:
                print(err)

# COMMAND ----------

df = spark.read.table('BRONZE_OHLC_DATA')
display(df.limit(10))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY BRONZE_OHLC_DATA
# MAGIC LIMIT 5
# MAGIC 
# MAGIC --RESTORE TABLE BRONZE_OHLC_DATA TO VERSION AS OF 38

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily data

# COMMAND ----------

path_to_daily_data = "dbfs:/mnt/data/data/daily_crypto_data"
list_of_pathes = [path for path in dbutils.fs.ls(path_to_daily_data)]
def get_minutes_data(path):
    data = []
    for path in list_of_pathes:
        name = path.name[:-1]
        path_json = path.path
        tmp = spark.read.json(path_json, schema=dataframe_schema)
        tmp = tmp.fillna(value=f"{name}", subset=['coin_currency']) 
        data.append(tmp)
    return reduce(DataFrame.unionAll, data)

# COMMAND ----------

per_minute_data = get_minutes_data(list_of_pathes).createOrReplaceTempView('per_minute_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO BRONZE_OHLC_DATA AS target
# MAGIC USING per_minute_data AS source
# MAGIC ON target.time = source.time AND target.coin_currency = source.coin_currency
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *

# COMMAND ----------

# MAGIC %md ## Twitter data

# COMMAND ----------

path_to_twitter_data = "dbfs:/mnt/data/data/daily_tweets"

twitter_schema = StructType(fields=[
    StructField('id', LongType(), True),
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

tweets_df = spark.read.json(path_to_twitter_data, schema=twitter_schema)
tweets_df.limit(10).display()

# COMMAND ----------

tweets_df\
    .write\
    .format('delta')\
    .mode('append')\
    .saveAsTable("BRONZE_TWEET_DATA")

# COMMAND ----------

# MAGIC %md # SILVER LEVEL

# COMMAND ----------

# MAGIC %md ## Enrichment of daily OHLC data

# COMMAND ----------

bronze_ohlc = spark.read.table("BRONZE_OHLC_DATA").dropDuplicates(['time', 'coin_currency'])
def bronze_to_silver_ohlc(df):
    df = (df.withColumn('date_time', F.from_unixtime(F.col('time'), 'yyyy-MM-dd HH:mm:ss'))
                             .withColumn('year', F.from_unixtime(F.col("time"),"yyyy").cast(IntegerType()))
                             .withColumn('month', F.from_unixtime(F.col("time"),"MM").cast(IntegerType()))
                             .withColumn('day', F.from_unixtime(F.col("time"),"dd").cast(IntegerType()))
                             .withColumn('hour', F.from_unixtime(F.col("time"),"HH").cast(IntegerType()))
                             .withColumn('minute', F.from_unixtime(F.col("time"),"mm").cast(IntegerType()))
                             .withColumn('coin', F.split(F.col('coin_currency'), '_').getItem(0))
                             .withColumn('currency', F.split(F.col('coin_currency'), '_').getItem(1))
                             .withColumn('delta', (F.col('close') - F.col('open'))*100/F.col('open'))
                             .withColumnRenamed('time', 'time_stamp')
                             .withColumnRenamed('volumefrom', 'volume_fsym')
                             .withColumnRenamed('volumeto', 'volume_tsym')
                             .withColumnRenamed('coin', 'ticker')
                    )
    return df.select('ticker', 'date_time', 'open', 'high', 'low', 'close', 'volume_fsym', 'volume_tsym',
                     'currency', 'delta', 'time_stamp', 'year', 'month', 'day', 'hour', 'minute')

# COMMAND ----------

silver_df = bronze_to_silver_ohlc(bronze_ohlc)
silver_df.write\
    .partitionBy('year')\
    .format('delta')\
    .mode('overwrite')\
    .saveAsTable("SILVER_OHLC_DATA")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MERGE INTO SILVER_OHLC_DATA AS target
# MAGIC -- USING tmp_silver AS source
# MAGIC -- ON source.time_stamp=target.time_stamp AND source.ticker=target.ticker AND source.currency=target.currency
# MAGIC -- WHEN NOT MATCHED
# MAGIC --    THEN INSERT *

# COMMAND ----------

# MAGIC %md ## Cleaning of Twitter Data

# COMMAND ----------

tweets_df = spark.read.table('BRONZE_TWEET_DATA')
def transform_daily_tweets(df):
    df = df.withColumn('year', F.year(F.col("created_at")).cast(IntegerType()))
    w = Window.partitionBy("id").orderBy(*[F.desc(c) for c in ["favorite_count","followers_count", "retweet_count"]])
    return df.withColumn("row_num", F.row_number().over(w))\
                        .filter(F.col('row_num') == 1)\
                        .drop(F.col('row_num'))

# COMMAND ----------

output = transform_daily_tweets(tweets_df)
output.write\
    .format('delta')\
    .mode('overwrite')\
    .option("overwriteSchema", "true") \
    .saveAsTable("SILVER_TWEET_DATA")

# COMMAND ----------

# MAGIC %md
# MAGIC # GOLD LEVEL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge to Gold Table

# COMMAND ----------

silver_ohlc = spark.read.table("silver_ohlc_data")
silver_tweets = spark.read.table("silver_tweet_data")

gold_df = silver_tweets.join(
                            silver_ohlc,
                            how='inner', 
                            on=['ticker', 'year', 'month', 'day', 'hour', 'minute']
                            )
gold_df.write\
        .partitionBy('year')\
        .format('delta')\
        .mode('overwrite')\
        .saveAsTable("GOLD_TABLE")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM GOLD_TABLE
# MAGIC WHERE year = 2022
# MAGIC ORDER BY created_at DESC
# MAGIC LIMIT 25

# COMMAND ----------


