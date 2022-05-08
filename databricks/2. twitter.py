# Databricks notebook source
# MAGIC %sh
# MAGIC if python -c "import tweepy" &> /dev/null; then
# MAGIC     echo 'tweepy already installed'
# MAGIC else
# MAGIC     pip install tweepy
# MAGIC fi
# MAGIC 
# MAGIC if [python -c "import sparknlp" &> /dev/null] || [python -c "import mlflow" &> /dev/null]; then
# MAGIC     echo 'sparknlp and other stuff is already installed'
# MAGIC else
# MAGIC     pip install spark-nlp==3.3.3 wordcloud mlflow contractions gensim pyldavis==3.2.0
# MAGIC fi

# COMMAND ----------

import tweepy
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, TimestampType, DateType


# COMMAND ----------

databaseName ="databricks_test" 
spark.sql(f"CREATE DATABASE IF NOT EXISTS {databaseName} LOCATION '/mnt/databricks/data/datalake'")
spark.sql(f"USE {databaseName}")

# COMMAND ----------

consumer_key = "<enter here>"
consumer_secret = "<enter here>"
bear_tocken = "<enter here>"

client_id = "<enter here>"
client_secret = "<enter here>"

auth = tweepy.AppAuthHandler(consumer_key, consumer_secret)
api = tweepy.API(auth,wait_on_rate_limit=True)

# COMMAND ----------

df1 = spark.sql("SELECT yfinance_ticker, twitter_hashtag_1, twitter_hashtag_2 FROM ticker_list_tbl")
ticker_list = df1.toPandas()
display(ticker_list)

# COMMAND ----------

def ceil_dt(dt, delta):
    return dt + (datetime.min - dt) % delta

# COMMAND ----------

raw_json_data = []

for index, row in  ticker_list.iterrows():
    ticker = row['yfinance_ticker']
    hashtag_1 = row['twitter_hashtag_1']
    hashtag_2 = row['twitter_hashtag_2']
    for tweet in tweepy.Cursor(api.search_tweets, q=f"{hashtag_1} {hashtag_2}", lang="en").items(1000):
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
            "QUERY": f"{hashtag_1} {hashtag_2}",
            "yfinance_ticker":ticker,
            "text": string_decode,
            "favorite_count": tweet._json["favorite_count"],
            "result_type": tweet._json["metadata"]["result_type"],
            "user_name": tweet._json["user"]["screen_name"],
            "followers_count": tweet._json["user"]["followers_count"],
            "retweet_count": tweet._json["retweet_count"]
       }

        raw_json_data.append(result)

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
    StructField('yfinance_ticker', StringType(), True),
    StructField('text', StringType(), True),
    StructField('favorite_count', IntegerType(), True),
    StructField('result_type', StringType(), True),
    StructField('user_name', StringType(), True),
    StructField('followers_count', IntegerType(), True),
    StructField('retweet_count', IntegerType(), True),
])

crypto_df = spark.read.json(sc.parallelize(raw_json_data), schema=twitter_schema)

# COMMAND ----------

(crypto_df.write
    .format('delta')
    .mode('append')
    .option("overwriteSchema", "true")
    .saveAsTable("crypto_df_tweets")
)

display(spark.read.table("crypto_df_tweets"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC select yfinance_ticker, count(*) 
# MAGIC from crypto_df_tweets 
# MAGIC group by yfinance_ticker

# COMMAND ----------

sdf_twitter = spark.sql("SELECT * FROM crypto_df_tweets")

# COMMAND ----------

import mlflow
import mlflow.spark

# COMMAND ----------

model_name = "best_model_sentimentDL"
client = mlflow.tracking.MlflowClient()
latest_prod_model_detail = client.get_latest_versions(model_name, stages=["Production"])[0]
latest_prod_model =  mlflow.spark.load_model(f"runs:/{latest_prod_model_detail.run_id}/sentimentDL_model")

# Make prediction
sdf_preds = latest_prod_model.transform(sdf_twitter)

# Unnest result column
sdf_preds_trans = sdf_preds.select("id", "text", "QUERY", "yfinance_ticker", "user_name", "result_type", "favorite_count", "followers_count", "retweet_count", "created_at","day","hour","minute", F.col("class.result").getItem(0))

# Rename prediction column
sdf_preds_trans = sdf_preds_trans.withColumnRenamed(sdf_preds_trans.columns[-1], "sentiment")

# COMMAND ----------


