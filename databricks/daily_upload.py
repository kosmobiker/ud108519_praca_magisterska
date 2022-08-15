# Databricks notebook source
import os
import cryptocompare
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import yfinance as yahooFinance
import pandas as pd
from datetime import datetime, timedelta

import requests
import json
from functools import reduce
import matplotlib.pyplot as plt
import numpy as np
%matplotlib inline

import tweepy
import pandas as pd
import pandas as pd
import numpy as np
import json

from typing import Dict, List

# COMMAND ----------

crypto_compare_key = dbutils.secrets.getBytes(scope="demo_secrets", key="CRYPTO_COMPARE_KEY").decode("utf-8")
cryptocompare.cryptocompare._set_api_key_parameter(crypto_compare_key)

# COMMAND ----------

def get_daily_data(coin:str, cur:str) -> List[Dict]:
    """
    Retrieve daily data as list of dicts
    """
    try:
        return cryptocompare.get_historical_price_minute(coin, cur)
    except Exception as err:
        print(err)

# COMMAND ----------

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
list_of_currencies = [
    "USD",
    "EUR",
    "JPY",
    "BTC"
  ]

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

data = []
for coin in list_of_coins:
    for currency in list_of_currencies:
        if coin != currency:
            tmp = get_daily_data(coin, currency)
            res = [dict(item, **{'coin_currency':f'{coin}_{currency}'}) for item in tmp]
            data.extend(res)


# COMMAND ----------

spark_df = sqlContext.read.json(sc.parallelize(data), schema=dataframe_schema)
spark_df.createOrReplaceTempView('tmp_df')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO BRONZE_OHLC_DATA AS target
# MAGIC USING tmp_df AS source
# MAGIC ON target.time = source.time AND target.coin_currency = source.coin_currency
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *

# COMMAND ----------


