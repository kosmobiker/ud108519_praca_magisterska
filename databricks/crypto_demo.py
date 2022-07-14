# Databricks notebook source
#%pip install cryptocompare yfinance

# COMMAND ----------

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
from sparknlp.annotator import *
from sparknlp.base import *
import sparknlp
from sparknlp.pretrained import PretrainedPipeline




# COMMAND ----------

# MAGIC %md ### Setup

# COMMAND ----------

CRYPTO_COMPARE_KEY = "6188db038cad874de5eb7da3821490d45967ae07911bb365904d2b1759400ea4"
cryptocompare.cryptocompare._set_api_key_parameter(CRYPTO_COMPARE_KEY)

access_key = "AKIASCI2223QIRILVKE4"
secret_key = "gz4SrFF/E9sXLUGV2F1wDF1JPXrBm8AIFR6pdHYF"
encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name = "databricks-demo-vlad"
mount_name = "databricks"

# access_key = "AKIASCI2223QIRILVKE4"
# secret_key = "gz4SrFF/E9sXLUGV2F1wDF1JPXrBm8AIFR6pdHYF"
# encoded_secret_key = secret_key.replace("/", "%2F")
# aws_bucket_name = "kosmobiker-masterproject"
# mount_name = "databricks"

# dbutils.fs.unmount("/mnt/databricks/")
dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name), "/mnt/%s" % mount_name)
# dbutils.fs.refreshMounts()
display(dbutils.fs.ls("/mnt/"))

# COMMAND ----------

datalake_name = "databricks_demo"
path_to_lake = f"dbfs:/mnt/databricks/{datalake_name}"

# COMMAND ----------

#setup twitter

consumer_key = "A2sOv6W6ntXbmzXMzfRwDVRNN"
consumer_secret = "i6ch2aVSnxNSBQ6s5ztZP1OiTM0zvianPlugLPIorbDZJ00w20"
bear_tocken = "AAAAAAAAAAAAAAAAAAAAAOc1cQEAAAAAKMCNMPEqMTBUR4X6dm5QXMJqLT8%3Du2QFddiDUu8dLqqC6qHknGvfYdXPIslKViSDEDp3kqhJs8b7PW"

client_id = "RlRheEY2aFEwOXRSRUhaUlhZU2g6MTpjaQ"
client_secret = "YPRlTg7Yag9-AE2oQr8a5GzcfdogIQExmVSpnnT6iBnJB8K9pU"

auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
api = tweepy.API(auth,wait_on_rate_limit=True)

# COMMAND ----------

# MAGIC %md ### Basic info about coins

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
    .save(f"{path_to_lake}/coin_list")
)

# COMMAND ----------

display(spark.read.format('delta').load(f"{path_to_lake}/coin_list"))

# COMMAND ----------

df = (spark.read.format('delta').load(f"{path_to_lake}/coin_list"))
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

created_on = {row['Name']:row['ContentCreatedOn'] for row in df.collect() if row['Name'] in list_of_coins}
created_on

# COMMAND ----------

# MAGIC %md ### BRONZE LEVEL

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
# for path in list_of_pathes:
#     name = path.name[:-5]
#     path_json = path.path
#     tmp = spark.read.json(path_json, schema=dataframe_schema)
#     tmp = tmp.withColumn('coin-currency', F.lit(name))
#     data.append(tmp)
# silver_df = reduce(DataFrame.unionAll, data)
# display(silver_df)

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
                df = df.fillna(value=f"{coin}-{cur}", subset=['coin_currency']) 
                (df.coalesce(1)
                    .write
                    .format('delta')
                    .mode('overwrite')
                    .option("overwriteSchema", "true")
                    .save(f"{path_to_lake}/BRONZE/historical_data/{coin}_{cur}")
                )
                print(f"{coin}-{cur} was uploaded!")
            except Exception as err:
                print(err)

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

#read tweets from S3
tweets_df = spark.read.json(f"{path_to_lake}/BRONZE/daily_tweets/", schema=twitter_schema).dropDuplicates()
(tweets_df.coalesce(1)
    .write
    .format('delta')
    .mode('overwrite')
    .save(f"{path_to_lake}/BRONZE/daily_tweets_delta/")
)

# COMMAND ----------

# tweets_df.select('id').distinct().count()

# COMMAND ----------

# MAGIC %md ### SILVER LEVEL

# COMMAND ----------

display(spark.read.format('delta').load(f"{path_to_lake}/BRONZE/historical_data/BNB_BTC"))

# COMMAND ----------

list_of_pathes = [path[0] for path in dbutils.fs.ls(f"{path_to_lake}/BRONZE/historical_data")]

# COMMAND ----------

def bronze_to_silver_ohlc(list_of_pathes):
    try:
        data = []
        for path in list_of_pathes:
            tmp = spark.read.format('delta').load(path)
            data.append(tmp)
        silver_df = reduce(DataFrame.unionAll, data)
        silver_df = (silver_df.withColumn('date_time', F.from_unixtime(F.col('time'), 'yyyy-MM-dd HH:mm:ss'))
                             .withColumn('year', F.from_unixtime(F.col("time"),"yyyy"))
                             .withColumn('coin', F.split(F.col('coin_currency'), '-').getItem(0))
                             .withColumn('currency', F.split(F.col('coin_currency'), '-').getItem(1))
                             .withColumn('delta', (F.col('close') - F.col('open'))*100/F.col('open'))
                             .withColumnRenamed('time', 'time_stamp')
                             .withColumnRenamed('volumefrom', 'volume_fsym')
                             .withColumnRenamed('volumeto', 'volume_tsym')
                    )
        silver_df = silver_df.select('coin', 'date_time', 'open', 'high', 'low', 'close', 'volume_fsym', 'volume_tsym', 'currency', 'delta', 'time_stamp', 'year')
        return silver_df
    except Exception as err:
        print(err)  


# COMMAND ----------

silver_df = bronze_to_silver_ohlc(list_of_pathes)
display(silver_df)

# COMMAND ----------

#add daily data
path_to_daily_data = ""
daily_ohlc_data = ""

# COMMAND ----------

(silver_df.coalesce(1)
        .write
        .format('delta')
        .mode('overwrite')
        .save(f"{path_to_lake}/SILVER/OHLC_data")
        )
# (daily_ohlc_data.write
#         .format('delta')
#         .partitionBy("year")
#         .mode('append')
#         .save(f"{path_to_lake}/SILVER/OHLC_data")
#         )

# COMMAND ----------

tweets_df = spark.read.format('delta').load(f"{path_to_lake}/BRONZE/daily_tweets_delta/")

# COMMAND ----------

spark = sparknlp.start()
MODEL_NAME='sentimentdl_use_twitter'

# COMMAND ----------

def bronze_to_silver_tweets(tweets_df):
    documentAssembler = DocumentAssembler()\
            .setInputCol("text")\
            .setOutputCol("document")

    use = UniversalSentenceEncoder.pretrained(name="tfhub_use", lang="en")\
            .setInputCols(["document"])\
            .setOutputCol("sentence_embeddings")

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

# COMMAND ----------

twitter_df_silver.display()

# COMMAND ----------

twitter_df_silver.groupBy('sentiment').count().show()

# COMMAND ----------

# MAGIC %md ### GOLD LEVEL

# COMMAND ----------

#merge ohlc_data and tweets data

# COMMAND ----------

df = spark.read.format('delta').load(f"{path_to_lake}/GOLD")
df.createOrReplaceTempView('coins_db')
df.display()

# COMMAND ----------

df.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# COMMAND ----------

# MAGIC %md Some analysis of the coins
# MAGIC 
# MAGIC - How did Etherium price in USD vary over time?
# MAGIC - How did Etherium daily returns vary over time? Which days had the worst and best returns?
# MAGIC - Which cryptocurrencies had the top daily return?
# MAGIC - What is the most popular cryptocurencies?

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW closing_price_weekly AS
# MAGIC SELECT coin, currency, date_trunc('week', date_time) AS time_period, FIRST_VALUE(close) AS closing_price
# MAGIC FROM coins_db
# MAGIC GROUP BY coin, currency, time_period
# MAGIC ORDER BY time_period;
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW closing_price_daily AS
# MAGIC SELECT coin, currency, date_trunc('day', date_time) AS time_period, FIRST_VALUE(close) AS closing_price
# MAGIC FROM coins_db
# MAGIC GROUP BY coin, currency, time_period
# MAGIC ORDER BY time_period;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM closing_price_weekly
# MAGIC WHERE coin = 'ETH' AND currency = 'USD'

# COMMAND ----------

df = spark.sql("SELECT * FROM closing_price_weekly WHERE coin = 'ETH' AND currency = 'USD'").toPandas()

plt.figure(figsize=(16, 6))
xs=df['time_period']
ys=df['closing_price'].astype('float')
plt.plot(xs, ys, label='ETH/USD', lw=3, color='orange')
plt.legend(fontsize=15)
plt.xlabel('years')
plt.ylabel('USD, $')
plt.grid()
plt.show()

# COMMAND ----------

# MAGIC %r
# MAGIC library(SparkR)
# MAGIC library(ggplot2)
# MAGIC 
# MAGIC #transform data
# MAGIC r_df <- collect(sql("SELECT * FROM closing_price_weekly WHERE coin = 'ETH' AND currency = 'USD'")) 
# MAGIC r_df$closing_price <- as.numeric(as.character(r_df$closing_price))
# MAGIC 
# MAGIC #plot itself
# MAGIC options(repr.plot.width=900, repr.plot.height=600)
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
# MAGIC   SELECT coin, currency, time_period, closing_price
# MAGIC   FROM closing_price_daily
# MAGIC   WHERE coin = 'ETH' AND currency = 'USD' AND time_period > '2020-01-01'
# MAGIC ) sub WINDOW prices AS (ORDER BY time_period DESC)

# COMMAND ----------

sp = yahooFinance.Ticker("^GSPC").history(interval='1d', start='2013-06-29', end='2022-07-02')

sp = sp.resample('1d').ffill()
sp['daily_factor'] = sp["Close"].pct_change() + 1
sp.reset_index(inplace=True)
sp['coin'] = 'S&P 500'
sp = sp.rename(columns={'Date' : 'time_period', 'Close' : 'closing_price'})
sp = sp[['coin', 'time_period', 'daily_factor']]
spark.createDataFrame(sp).createOrReplaceTempView('sp_db')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT coin, time_period, closing_price / LEAD(closing_price) OVER prices AS daily_factor
# MAGIC FROM (
# MAGIC   SELECT coin, currency, time_period, closing_price
# MAGIC   FROM closing_price_daily
# MAGIC   WHERE coin = 'BTC' AND currency = 'USD' AND time_period > '2020-01-01'
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
# MAGIC       coin,
# MAGIC       currency,
# MAGIC       time_period,
# MAGIC       closing_price,
# MAGIC       LEAD(closing_price) OVER (PARTITION BY coin ORDER BY time_period DESC) AS prev_day_closing_price
# MAGIC     FROM closing_price_daily
# MAGIC     WHERE currency = 'USD'
# MAGIC ),
# MAGIC daily_factor AS (
# MAGIC     SELECT 
# MAGIC       coin,
# MAGIC       time_period,
# MAGIC       CASE WHEN prev_day_closing_price = 0 THEN 0 ELSE closing_price/prev_day_closing_price END AS daily_factor
# MAGIC     FROM prev_day_closing
# MAGIC ),
# MAGIC ranking_daily AS (
# MAGIC     SELECT
# MAGIC       time_period, coin, daily_factor,
# MAGIC       ROW_NUMBER() OVER (PARTITION BY time_period ORDER BY daily_factor DESC) AS ranking
# MAGIC     FROM daily_factor
# MAGIC )
# MAGIC SELECT time_period, coin, daily_factor
# MAGIC FROM ranking_daily
# MAGIC WHERE ranking = 1
# MAGIC ORDER BY time_period DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM highest_daily_returns
