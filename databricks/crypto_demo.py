# Databricks notebook source
# MAGIC %md # Analyzing Crypto Data Using Databricks
# MAGIC 
# MAGIC ![Databricks logo](https://upload.wikimedia.org/wikipedia/commons/thumb/6/63/Databricks_Logo.png/640px-Databricks_Logo.png)
# MAGIC 
# MAGIC **Goals**:
# MAGIC - Get familiar with Databricks platform
# MAGIC - Explore possibilities of Databricks notrebooks
# MAGIC - Build a small data lake using *Delta lake* technology
# MAGIC - Learn how to use different Databricks tools
# MAGIC - Analyze the OHLC data regarding the selected cryptocurrencies and associated tweets
# MAGIC - Try to find some insights
# MAGIC 
# MAGIC <div>
# MAGIC <img src="https://geekyblaze.b-cdn.net/f001.backblazeb2.com/file/GeekyNews/Elon-Musk-made-a-typo-and-Twitter-wants-you-to.png" width="700"/>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Databricks Notebook possibilities
# MAGIC 
# MAGIC support of magic commands from Jupyter + special magic commands from Databricks
# MAGIC 
# MAGIC More info is [here](https://docs.databricks.com/notebooks/notebooks-use.html)

# COMMAND ----------

# MAGIC %lsmagic

# COMMAND ----------

# MAGIC %fs

# COMMAND ----------

dbutils.help()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC $$\text{Databricks Notebook supports \LaTeX and \KaTeX}$$
# MAGIC 
# MAGIC \\(c = \\pm\\sqrt{a^2 + b^2} \\)
# MAGIC 
# MAGIC \\(A{_i}{_j}=B{_i}{_j}\\)
# MAGIC 
# MAGIC $$c = \\pm\\sqrt{a^2 + b^2}$$
# MAGIC 
# MAGIC \\[A{_i}{_j}=B{_i}{_j}\\]

# COMMAND ----------

displayHTML("""<!DOCTYPE html>
<html>
<body>

<h2>Width and Height Attributes</h2>

<p>The width and height attributes of the img tag, defines the width and height of the image:</p>

<img src="https://cdn.pixabay.com/photo/2017/08/05/11/16/logo-2582748_960_720.png" width="300" height="300">

</body>
</html>
""")

# COMMAND ----------

# MAGIC %md You can use comments

# COMMAND ----------

print('hello world')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crypto Data + Tweets
# MAGIC 
# MAGIC Data **Exrtraction**:
# MAGIC   - Historical  OHLC (open, high, low, close) data for selected cryptocurrencies (1 hour interval) using [cryptocompare API](https://min-api.cryptocompare.com/).
# MAGIC   - Daily data OHLC data from S3 bucket (1 minutes interval)
# MAGIC   - Daily tweets from S3 bucket
# MAGIC 
# MAGIC Data **Loading** to Delta Tables
# MAGIC 
# MAGIC Data **Transformation**:
# MAGIC   - OHLC data enrichment
# MAGIC   - sentimental analysis of tweets (positive, neutral, negative)
# MAGIC 
# MAGIC Delta Lake:
# MAGIC - bronze tables for raw data
# MAGIC - silver tables for enriched data
# MAGIC - gold table for merged data prepared for analysis

# COMMAND ----------

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
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from sparknlp.annotator import *
from sparknlp.base import *
import sparknlp
from sparknlp.pretrained import PretrainedPipeline

# COMMAND ----------

# MAGIC %md ## Setup

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

crypto_compare_key = dbutils.secrets.getBytes(scope="demo_secrets", key="CRYPTO_COMPARE_KEY").decode("utf-8")
cryptocompare.cryptocompare._set_api_key_parameter(crypto_compare_key)

access_key = dbutils.secrets.getBytes(scope="demo_secrets", key="ACCESS_KEY").decode("utf-8")
secret_key = dbutils.secrets.getBytes(scope="demo_secrets", key="SECRET_KEY").decode("utf-8")
encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name = "databricks-demo-vlad"
mount_name = "databricks"


# dbutils.fs.unmount("/mnt/databricks/")
# dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name), "/mnt/%s" % mount_name)
dbutils.fs.refreshMounts()
display(dbutils.fs.ls("/mnt/"))

# COMMAND ----------

datalake_name = "databricks_demo"
path_to_lake = f"dbfs:/mnt/databricks/{datalake_name}"

# COMMAND ----------

#setup twitter

consumer_key = dbutils.secrets.getBytes(scope="demo_secrets", key="CONSUMER_KEY").decode("utf-8")
consumer_secret = dbutils.secrets.getBytes(scope="demo_secrets", key="CONSUMER_SECRET").decode("utf-8")

auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
api = tweepy.API(auth, wait_on_rate_limit=True)

# COMMAND ----------

# MAGIC %md ## Basic info about coins

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

(coin_info_df
    .coalesce(1)
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable("coin_list")
)

# COMMAND ----------

display(spark.read.table("coin_list"))

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

# MAGIC %md ## BRONZE LEVEL
# MAGIC ### Historical OHLC data

# COMMAND ----------

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

# def deep_ls(path: str):
#     """List all files in base path recursively."""
#     for x in dbutils.fs.ls(path):
#         if x.path[-1] != '/':
#             yield x
#         else:
#             for y in deep_ls(x.path):
#                 yield y
# list_of_pathes = [i for i in deep_ls(path_to_daily)]
# data = []

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
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY BRONZE_OHLC_DATA

# COMMAND ----------

# %sql
# RESTORE TABLE BRONZE_OHLC_DATA TO VERSION AS OF 38


# COMMAND ----------

# %sql
# -- query to delete duplicates directly from delta lake
# MERGE INTO BRONZE_OHLC_DATA AS target
# USING (
#       WITH t AS(
#       SELECT *, ROW_NUMBER() OVER (PARTITION BY time, coin_currency ORDER BY time DESC) AS rn FROM BRONZE_OHLC_DATA
#       )
#       SELECT * FROM t WHERE rn > 1
# ) 
# AS source
# ON source.time=target.time AND source.coin_currency=target.coin_currency
# WHEN MATCHED THEN DELETE

# COMMAND ----------

# MAGIC %md ###Daily OHLC data

# COMMAND ----------

path_to_daily_data = f"{path_to_lake}/daily_crypto_data"
path_to_daily_data

# COMMAND ----------

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

# MAGIC %md ### Twitter data

# COMMAND ----------

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

tweets_df = spark.read.json(f"{path_to_lake}/daily_tweets/", schema=twitter_schema)
tweets_df.display()

# COMMAND ----------

(tweets_df
    .coalesce(1)
    .write
    .format('delta')
    .mode('append')
    .saveAsTable("BRONZE_TWEET_DATA")
    )

# COMMAND ----------

# MAGIC %md ## SILVER LEVEL

# COMMAND ----------

# MAGIC %md ### Enrichment of daily OHLC data

# COMMAND ----------

bronze_ohlc = spark.read.table("BRONZE_OHLC_DATA").dropDuplicates(['time', 'coin_currency'])

# COMMAND ----------

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
# silver_df.createOrReplaceTempView('tmp_silver')

# COMMAND ----------

(silver_df
    .coalesce(1)
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable("SILVER_OHLC_DATA")
                )

# COMMAND ----------

# %sql
# MERGE INTO SILVER_OHLC_DATA AS target
# USING tmp_silver AS source
# ON source.time_stamp=target.time_stamp AND source.ticker=target.ticker AND source.currency=target.currency
# WHEN NOT MATCHED
#   THEN INSERT *

# COMMAND ----------

# MAGIC %md ### Sentimetal analysis of tweets

# COMMAND ----------

tweets_df = spark.read.table('BRONZE_TWEET_DATA')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- query to delete duplicates directly from delta lake
# MAGIC MERGE INTO BRONZE_TWEET_DATA AS target
# MAGIC USING (
# MAGIC       WITH t AS(
# MAGIC       SELECT *, ROW_NUMBER() OVER (
# MAGIC           PARTITION BY 'id', created_at, ticker 
# MAGIC           ORDER BY favorite_count DESC, followers_count DESC, retweet_count DESC) AS rn FROM BRONZE_TWEET_DATA
# MAGIC       )
# MAGIC       SELECT * FROM t WHERE rn > 1
# MAGIC ) 
# MAGIC AS source
# MAGIC ON source.id=target.id AND source.created_at=target.created_at AND source.ticker=target.ticker
# MAGIC WHEN MATCHED THEN DELETE

# COMMAND ----------

spark = sparknlp.start()
MODEL_NAME='sentimentdl_use_twitter'

# COMMAND ----------

def bronze_to_silver_tweets(tweets_df):
    #Prepares data into a format that is processable by Spark NLP. This is the entry point for every Spark NLP pipeline.
    documentAssembler = DocumentAssembler()\
            .setInputCol("text")\
            .setOutputCol("document")
    #encodes text into high dimensional vectors
    use = UniversalSentenceEncoder.pretrained(name="tfhub_use", lang="en")\
            .setInputCols(["document"])\
            .setOutputCol("sentence_embeddings")
    #an annotator for multi-class sentiment analysis.
    sentimentdl = SentimentDLModel.pretrained(name=MODEL_NAME, lang="en")\
            .setInputCols(["sentence_embeddings"])\
            .setOutputCol("sentiment")

    nlpPipeline = Pipeline(
          stages = [
              documentAssembler,
              use,
              sentimentdl
          ])

    empty_df = spark.createDataFrame([['']]).toDF("text")
    pipelineModel = nlpPipeline.fit(empty_df)
    result = pipelineModel.transform(tweets_df.select("text"))
    tmp = (result.select(
                     F.col('document.result').alias('document'),
                     F.col('sentiment.result').alias('sentiment')
                    )
            .withColumn('text', F.explode('document'))
            .withColumn('sentiment', F.explode('sentiment'))
            .select('text', 'sentiment')
       )
    tmp = tmp.withColumn('row_index', F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
    tweets_df = tweets_df.withColumn('row_index', F.row_number().over(Window.orderBy(F.monotonically_increasing_id())))
    tweeter_silver = tweets_df.join(tmp.select('row_index', 'sentiment'), on=["row_index"]).drop("row_index")
    
    return tweeter_silver

# COMMAND ----------

twitter_df_silver = bronze_to_silver_tweets(tweets_df)
twitter_df_silver.createOrReplaceTempView('tmp_tweets')

# COMMAND ----------

twitter_df_silver.display()

# COMMAND ----------

(twitter_df_silver
    .coalesce(1)
    .write
    .format('delta')
    .mode('append')
    .saveAsTable("silver_twitter_data")
    )

# %sql
# MERGE INTO silver_twitter_data AS target
# USING tmp_tweets AS source
# ON source.id=target.id AND source.created_at=target.created_at AND source.ceil_datetime=target.ceil_datetime
# WHEN NOT MATCHED
#   THEN INSERT *

# COMMAND ----------

# MAGIC %md ## DATA ANALYSIS

# COMMAND ----------

# MAGIC %md ### Crypto Analysis Examples

# COMMAND ----------

silver_ohlc = spark.read.table("silver_ohlc_data")
silver_ohlc.createOrReplaceTempView('silver_ohlc')

# COMMAND ----------

# MAGIC %md Some analysis of the coins
# MAGIC 
# MAGIC - How did Etherium price in BTC vary over time?
# MAGIC - How did Etherium daily returns vary over time? Which days had the worst and best returns?
# MAGIC - Which cryptocurrencies had the top daily return?

# COMMAND ----------

# MAGIC %sql CREATE
# MAGIC OR REPLACE TEMP VIEW closing_price_weekly AS
# MAGIC SELECT
# MAGIC   ticker,
# MAGIC   currency,
# MAGIC   date_trunc('week', date_time) AS time_period,
# MAGIC   FIRST_VALUE(close) AS closing_price
# MAGIC FROM
# MAGIC   silver_ohlc
# MAGIC GROUP BY
# MAGIC   ticker,
# MAGIC   currency,
# MAGIC   time_period
# MAGIC ORDER BY
# MAGIC   time_period;
# MAGIC CREATE
# MAGIC   OR REPLACE TEMP VIEW closing_price_daily AS
# MAGIC SELECT
# MAGIC   ticker,
# MAGIC   currency,
# MAGIC   date_trunc('day', date_time) AS time_period,
# MAGIC   FIRST_VALUE(close) AS closing_price
# MAGIC FROM
# MAGIC   silver_ohlc
# MAGIC GROUP BY
# MAGIC   ticker,
# MAGIC   currency,
# MAGIC   time_period
# MAGIC ORDER BY
# MAGIC   time_period;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   closing_price_weekly
# MAGIC WHERE
# MAGIC   ticker = 'ETH'
# MAGIC   AND currency = 'USD'

# COMMAND ----------

df = _sqldf.toPandas()

plt.figure(figsize=(21, 6))
xs=df['time_period']
ys=df['closing_price']
plt.plot(xs, ys, label='ETH/USD', lw=3, color='navy')
plt.legend(fontsize=15)
plt.xlabel('years')
plt.ylabel('BTC')
plt.grid()
plt.show()

# COMMAND ----------

# MAGIC %r
# MAGIC library(SparkR)
# MAGIC library(ggplot2)
# MAGIC 
# MAGIC 
# MAGIC #transform data
# MAGIC r_df <- collect(sql("SELECT * FROM closing_price_weekly WHERE ticker = 'ETH' AND currency = 'USD'")) 
# MAGIC 
# MAGIC #plot itself
# MAGIC options(repr.plot.width=1200, repr.plot.height=500)
# MAGIC img1 <- ggplot(data = r_df, aes(x=time_period, y=closing_price)) + 
# MAGIC                         geom_line(size=1, color='navy') +
# MAGIC                         ggtitle("ETH/USD closing prices") +
# MAGIC                         labs(x = "Date",y = "USD") +
# MAGIC                         theme(
# MAGIC                             plot.margin = margin(0.5, 0.666, 0.45, 1, "cm"),
# MAGIC                             panel.background = element_rect(fill = "orange"),
# MAGIC                              plot.background = element_rect(
# MAGIC                                 fill = "skyblue",
# MAGIC                                 colour = "black"
# MAGIC                                 )
# MAGIC                           )
# MAGIC img1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT time_period, closing_price / LEAD(closing_price) OVER prices AS daily_factor
# MAGIC FROM (
# MAGIC   SELECT ticker, currency, time_period, closing_price
# MAGIC   FROM closing_price_daily
# MAGIC   WHERE ticker = 'ETH' AND currency = 'USD' AND time_period > '2020-03-01'
# MAGIC ) sub WINDOW prices AS (ORDER BY time_period DESC)

# COMMAND ----------

# MAGIC %md
# MAGIC - https://www.cnbc.com/2020/03/13/bitcoin-loses-half-of-its-value-in-two-day-plunge.html
# MAGIC - https://www.coindesk.com/markets/2020/03/12/ether-suffers-record-setting-33-drop-amid-global-market-turmoil/

# COMMAND ----------

sp = yahooFinance.Ticker("^GSPC").history(interval='1d', start='2013-06-29', end='2022-07-16')

sp = sp.resample('1d').ffill()
sp['daily_factor'] = sp["Close"].pct_change() + 1
sp.reset_index(inplace=True)
sp['ticker'] = 'S&P 500'
sp = sp.rename(columns={'Date' : 'time_period', 'Close' : 'closing_price'})
sp = sp[['ticker', 'time_period', 'daily_factor']]
spark.createDataFrame(sp).createOrReplaceTempView('sp_db')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ticker, time_period, closing_price / LEAD(closing_price) OVER prices AS daily_factor
# MAGIC FROM (
# MAGIC   SELECT ticker, currency, time_period, closing_price
# MAGIC   FROM closing_price_daily
# MAGIC   WHERE ticker = 'ETH' AND currency = 'USD' AND time_period > '2020-01-01'
# MAGIC ) sub WINDOW prices AS (ORDER BY time_period DESC)
# MAGIC UNION
# MAGIC SELECT * FROM sp_db
# MAGIC WHERE time_period > '2020-01-01'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW highest_daily_returns AS
# MAGIC WITH
# MAGIC prev_day_closing AS (
# MAGIC     SELECT
# MAGIC       ticker,
# MAGIC       currency,
# MAGIC       time_period,
# MAGIC       closing_price,
# MAGIC       LEAD(closing_price) OVER (PARTITION BY ticker ORDER BY time_period DESC) AS prev_day_closing_price
# MAGIC     FROM closing_price_daily
# MAGIC     WHERE currency = 'USD'
# MAGIC ),
# MAGIC daily_factor AS (
# MAGIC     SELECT 
# MAGIC       ticker,
# MAGIC       time_period,
# MAGIC       CASE WHEN prev_day_closing_price = 0 THEN 0 ELSE closing_price/prev_day_closing_price END AS daily_factor
# MAGIC     FROM prev_day_closing
# MAGIC ),
# MAGIC ranking_daily AS (
# MAGIC     SELECT
# MAGIC       time_period, ticker, daily_factor,
# MAGIC       ROW_NUMBER() OVER (PARTITION BY time_period ORDER BY daily_factor DESC) AS ranking
# MAGIC     FROM daily_factor
# MAGIC )
# MAGIC SELECT time_period, ticker, daily_factor
# MAGIC FROM ranking_daily
# MAGIC WHERE ranking = 1
# MAGIC ORDER BY time_period DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM highest_daily_returns
# MAGIC WHERE time_period  > '2022-01-01'
# MAGIC ORDER BY time_period

# COMMAND ----------

# MAGIC %md ### Tweets vs Crypto Analysis

# COMMAND ----------

silver_ohlc = spark.read.table("silver_ohlc_data")
silver_tweets = spark.read.table("silver_twitter_data")

# COMMAND ----------

gold_df = silver_tweets.join(
                            silver_ohlc,
                            how='inner', 
                            on=['ticker', 'month', 'day', 'hour', 'minute'])\
                        .filter("currency = 'USD'")
(gold_df.write
        .format('delta')
        .mode('overwrite')
        .saveAsTable("GOLD_TABLE")
        )

# COMMAND ----------

gold_df.createOrReplaceTempView('gold_tmp')
gold_df.display()

# COMMAND ----------

# MAGIC %md Tweets analysis
# MAGIC - Net Sentiment by Crypto Ticker and possible correlation
# MAGIC - Average number of tweets, retweetts, favourites
# MAGIC - Most Active Twitter User
# MAGIC - Best & Worst Performing Coin

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ticker,
# MAGIC   DATE(created_at) as Day,
# MAGIC   hour,
# MAGIC   count(CASE WHEN sentiment = 'positive' THEN 1 END) AS positive,
# MAGIC   count(CASE WHEN sentiment = 'neutral' THEN 1 END) AS neutral,
# MAGIC   count(CASE WHEN sentiment = 'negative' THEN 1 END) AS negative,
# MAGIC   count(CASE WHEN sentiment = 'positive' THEN 1 END) - count(CASE WHEN sentiment = 'negative' THEN 1 END) AS net_sentiment,
# MAGIC   avg(delta)
# MAGIC FROM
# MAGIC     gold_tmp
# MAGIC GROUP BY ticker, DATE(created_at), hour
# MAGIC ORDER BY ticker, DATE(created_at), hour

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte AS (
# MAGIC     SELECT
# MAGIC       ticker,
# MAGIC       DATE(created_at) as Day,
# MAGIC       hour,
# MAGIC       count(CASE WHEN sentiment = 'positive' THEN 1 END) AS positive,
# MAGIC       count(CASE WHEN sentiment = 'neutral' THEN 1 END) AS neutral,
# MAGIC       count(CASE WHEN sentiment = 'negative' THEN 1 END) AS negative,
# MAGIC       count(CASE WHEN sentiment = 'positive' THEN 1 END) - count(CASE WHEN sentiment = 'negative' THEN 1 END) AS net_sentiment,
# MAGIC       avg(delta) AS AVG_DELTA
# MAGIC     FROM
# MAGIC         gold_tmp
# MAGIC     GROUP BY ticker, DATE(created_at), hour
# MAGIC     ORDER BY ticker, DATE(created_at), hour
# MAGIC )
# MAGIC SELECT ticker,
# MAGIC         corr(net_sentiment, AVG_DELTA) AS `corr net avg`,
# MAGIC         corr(positive, AVG_DELTA) AS `corr pos avg`,
# MAGIC         corr(neutral, AVG_DELTA) as `corr neu avg`,
# MAGIC         corr(negative, AVG_DELTA) as `corr neg avg`
# MAGIC FROM cte
# MAGIC GROUP BY ticker

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   QUERY,
# MAGIC   count(result_type) AS num_recent,
# MAGIC   avg(favorite_count) AS avg_favorite,
# MAGIC   avg(followers_count) AS avg_followers,
# MAGIC   avg(retweet_count) AS avg_retweets
# MAGIC FROM
# MAGIC   gold_tmp
# MAGIC WHERE
# MAGIC     created_at > '2022-07-10' AND created_at < '2022-07-31'
# MAGIC GROUP BY
# MAGIC     QUERY
# MAGIC ORDER BY
# MAGIC     avg(retweet_count) DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   user_name as top_tweeter,
# MAGIC   count(id) as tweets
# MAGIC FROM
# MAGIC   gold_tmp
# MAGIC WHERE
# MAGIC     created_at > '2022-07-10' AND created_at < '2022-07-31'
# MAGIC GROUP BY
# MAGIC     user_name
# MAGIC ORDER BY 
# MAGIC     tweets DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   QUERY,
# MAGIC   count(QUERY) as num_tweets
# MAGIC FROM
# MAGIC   gold_tmp
# MAGIC WHERE
# MAGIC     created_at > '2022-07-10' AND created_at < '2022-07-31'
# MAGIC GROUP BY
# MAGIC     QUERY
# MAGIC ORDER BY
# MAGIC     num_tweets DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   ticker as stock,
# MAGIC   SUM(delta),
# MAGIC   min(delta),
# MAGIC   max(delta)
# MAGIC FROM
# MAGIC   gold_tmp
# MAGIC WHERE
# MAGIC     created_at > '2022-07-10' AND created_at < '2022-07-31'
# MAGIC GROUP BY
# MAGIC     stock
# MAGIC ORDER BY 
# MAGIC     SUM(delta) DESC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusion 
# MAGIC 
# MAGIC Our Findings:
# MAGIC   1. Cryptocoins is still very risky and volatile kind of security in comparison with "traditional" bonds and stocks
# MAGIC   2. No correlation was found between Twitter activity and coins fluctuation
# MAGIC   3. Databricks platform was incredibly useful for solving complex problems like merging Twitter and stock data.
# MAGIC 
# MAGIC Future improvememnts:
# MAGIC   1. More data regarding coins and tweets
# MAGIC   2. Using of paid versions of API
# MAGIC   3. Automations of adding new data with scheduled jobs
# MAGIC   4. More complex model for sentimental analysis; using of MLops
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <div>
# MAGIC <img src="https://memegenerator.net/img/instances/66665424.jpg" width="700"/>
# MAGIC </div>
