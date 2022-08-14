import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import boto3
import re
import pytz
from datetime import datetime, timedelta
from functools import reduce

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from pyspark.sql.types import *

utc=pytz.UTC
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
bucket = 'kosmobiker-masterproject'
daily_crypto_data_params = {
        'Bucket' : bucket,
        'Prefix' : "data/daily_crypto_data/",
        }
dc_table_path = f"s3://{bucket}/data/my_database/ohlc_data/"
db_name = 'darhevich_data_lake'
dc_table_name = 'ohlc_data'
tweets_params = {
        'Bucket' : bucket,
        'Prefix' : "data/daily_tweets/",
        }
tweets_path = f"s3://{bucket}/data/daily_tweets/"
tweets_table_path =  f"s3://{bucket}/data/my_database/tweets_data/"
tweets_table_name = 'tweets_data'
daily_crypto_schema = StructType([
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

tweets_schema = StructType(fields=[
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
def get_list_of_objects_s3(operation_parameters):
    """
    List files in specific S3 URL
    """
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(**operation_parameters)
    for page in page_iterator:
        for content in page.get('Contents'):
            if content.get('LastModified') > utc.localize(datetime.now() - timedelta(days=7)):
                yield content.get('Key')
def extract_daily_crypto_data(params):
    data = []
    for key in get_list_of_objects_s3(params):
        try:
            if key.endswith('.json'):
                name = re.findall("(?:[0-9]{4}_[0-9]{2}_[0-9]{2})_(.+)(?:.json)", key)[0]
                path_json = f"s3://{bucket}/{key}"
                tmp = spark.read.json(path_json, schema=daily_crypto_schema)
                tmp = tmp.fillna(value=f"{name}", subset=['coin_currency']) 
                data.append(tmp)
        except Exception as err:
            print(err)
    return reduce(DataFrame.unionAll, data)
def transform_daily_crypto_data(df):
    df = (df.withColumn('date_time', F.from_unixtime(F.col('time'), 'yyyy-MM-dd HH:mm:ss'))
                 .withColumn('year', F.from_unixtime(F.col("time"),"yyyy").cast(IntegerType()))
                 .withColumn('month', F.from_unixtime(F.col("time"),"MM").cast(IntegerType()))
                 .withColumn('day', F.from_unixtime(F.col("time"),"dd").cast(IntegerType()))
                 .withColumn('hour', F.from_unixtime(F.col("time"),"HH").cast(IntegerType()))
                 .withColumn('minute', F.from_unixtime(F.col("time"),"mm").cast(IntegerType()))
                 .withColumn('coin', F.split(F.col('coin_currency'), '_').getItem(0))
                 .withColumn('currency', F.split(F.col('coin_currency'), '_').getItem(1))
                 .withColumn('delta', (F.col('close') - F.col('open'))*100/F.col('open'))
                 .withColumn('partition_col', F.from_unixtime(F.col("time"),"yyyy").cast(StringType()))
                 .withColumnRenamed('time', 'time_stamp')
                 .withColumnRenamed('volumefrom', 'volume_fsym')
                 .withColumnRenamed('volumeto', 'volume_tsym')
                 .withColumnRenamed('coin', 'ticker')
        )
    return df.select('ticker', 'date_time', 'open', 'high', 'low', 'close', 'volume_fsym', 'volume_tsym',
                     'currency', 'delta', 'time_stamp', 'year', 'month', 'day', 'hour', 'minute', 'partition_col')
def load_daily_crypto_data(df, table_path, db_name, table_name):
     df.coalesce(1).write.partitionBy('partition_col').mode("append").option("path", table_path).format("parquet").saveAsTable(f"{db_name}.{table_name}")

dfs = extract_daily_crypto_data(daily_crypto_data_params)
output = transform_daily_crypto_data(dfs)
load_daily_crypto_data(output, dc_table_path,  db_name, dc_table_name)
def extract_daily_tweets(tweets_path, tweets_schema):
    data = []
    for key in get_list_of_objects_s3(params):
        try:
            if key.endswith('.json'):
                path_json = f"s3://{bucket}/{key}"
                tmp = spark.read.json(path_json, schema=tweets_schema)
                data.append(tmp)
        except Exception as err:
            print(err)
    return reduce(DataFrame.unionAll, data)

def transform_daily_tweets(df):
    w = Window.partitionBy("id").orderBy(*[F.desc(c) for c in ["favorite_count","followers_count", "retweet_count"]])
    return df.withColumn("row_num", F.row_number().over(w))\
                        .filter(F.col('row_num') == 1)\
                        .drop(F.col('row_num'))

def load_daily_tweets(df, table_path, db_name, table_name):
    df.coalesce(1).write.mode("append").option("path", table_path).format("parquet").saveAsTable(f"{db_name}.{table_name}")        
dfs = extract_daily_tweets(tweets_path, tweets_schema)
output = transform_daily_tweets(dfs)
load_daily_tweets(output, tweets_table_path, db_name, tweets_table_name)
job.commit()