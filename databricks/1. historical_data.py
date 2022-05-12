# Databricks notebook source
# MAGIC %sh
# MAGIC if python -c "import yfinance" &> /dev/null; then
# MAGIC     echo 'yfinance already installed'
# MAGIC else
# MAGIC     pip install yfinance
# MAGIC fi

# COMMAND ----------

access_key = "<enter here>"
secret_key = "<enter here>"
encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name = "kosmobiker-masterproject"
mount_name = "databricks"

dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name), "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

databaseName ="databricks_test" 
spark.sql(f"CREATE DATABASE IF NOT EXISTS {databaseName} LOCATION '/mnt/databricks/data/datalake'")
spark.sql(f"USE {databaseName}")

# COMMAND ----------

from io import StringIO
import pandas as pd
import yfinance as yf
import time

tickers = """
yfinance_ticker,twitter_hashtag_1,twitter_hashtag_2
BTC-USD,"@bitcoin","$BTC"
ETH-USD,"@ethereum","$ETH"
DOGE-USD,"@dogecoin","$DOGE"
SHIB-USD,"@shibainucoin","$SHIB"
LTC-USD,"@litecoin","$LTC"
SOL1-USD,"@solana","$SOL"
ADA-USD,"@cardano","$ADA"
XRP-USD,"@ripple","$XRP"
DOT1-USD,"@polkadot","$DOT"
LUNA1-USD,"#terra_money","$LUNA"
AVAX-USD,"@avalancheavax","$AVAX"
ALGO-USD,"@algorand","$ALGO"
UNI3-USD,"@uniswap","$UNI"
BCH-USD,"@bitcoincashorg","$BCH"
XLM-USD,"@stellarorg","$XLM"
"""

tickers_df = pd.read_csv(StringIO(tickers))
display(tickers_df)

# COMMAND ----------

def fetch_ticker_info(row):
    ticker = row['yfinance_ticker']
    ticker_obj = yf.Ticker(ticker)
    row["crypto_name"] = ticker_obj.info["name"]
    row["start_date"] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ticker_obj.info["startDate"]))

    return row

# COMMAND ----------

ticker_lookup_df = tickers_df.apply(fetch_ticker_info, axis=1)
(spark
    .createDataFrame(ticker_lookup_df)
    .select('yfinance_ticker', 'crypto_name', 'start_date','twitter_hashtag_1','twitter_hashtag_2')
    .write
    .format('delta')
    .mode('overwrite')
    .option("overwriteSchema", "true")
    .saveAsTable("ticker_list_tbl")
)
display(spark.read.table("ticker_list_tbl"))

# COMMAND ----------

def get_raw_data_ticker(ticker: str, period="60d", interval="15m"):
    raw_ticker_data = yf.download(ticker, period=period, interval=interval)
    raw_ticker_data['date'] = raw_ticker_data.index
    raw_ticker_data['month'] = raw_ticker_data.index.strftime("%-m")
    raw_ticker_data['weekday'] = raw_ticker_data.index.strftime("%w")
    raw_ticker_data['day'] = raw_ticker_data.index.strftime("%Y-%m-%d")
    raw_ticker_data['hour'] = raw_ticker_data.index.strftime("%H")
    raw_ticker_data['minute'] = raw_ticker_data.index.strftime("%M")
    raw_ticker_data['ticker'] = ticker
    raw_ticker_data['delta'] = raw_ticker_data.apply(lambda row: (row['Close'] - row['Open'])*100/row['Open'], axis=1)
    
    return raw_ticker_data

# COMMAND ----------

def get_historical_data(tickers_df) -> pd.DataFrame:
    raw_data = pd.DataFrame()
    for ticker in tickers_df['yfinance_ticker'].to_list():
        try:
            tmp = get_raw_data_ticker(ticker)
            raw_data = pd.concat([raw_data, tmp])
        except Exception as err:
            print(err)
            print(f"{ticker} was not uplaoded")
    return raw_data

# COMMAND ----------

hist_data = get_historical_data(tickers_df)

# COMMAND ----------

(spark
    .createDataFrame(hist_data)
    .select('ticker', 'date', 'weekday', 'day', 'hour', 'minute', 'open', 'high','low', 'close', 'delta', 'volume')
    .write
#     .partitionBy('ticker') 
    .format('delta')
    .mode('overwrite')
    .option("overwriteSchema", "true")
    .saveAsTable("historical_data")
)
display(spark.read.table("historical_data"))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select ticker, count(*) from historical_data
# MAGIC group by ticker
