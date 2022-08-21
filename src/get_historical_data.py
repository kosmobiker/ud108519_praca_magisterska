"""
this module is used to get historical data for 
selected cryptocurrencies using Cryptocompare API
with 1 hour interval

It consists of 3 steps:

1. Extract - get info from Crytocurrency API
2. Transform - make data transformation 
3. Load - upload it to HIVE table

"""
import os
import boto3
import cryptocompare
import pandas as pd
import awswrangler as wr
from datetime import datetime
from typing import List
from utils.read_config import read_toml_config
from utils.logger import setup_applevel_logger


ROOT_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.abspath(os.path.join(ROOT_DIR, '..', 'config', 'config.toml'))

config = read_toml_config(CONFIG_PATH)
CRYPTO_COMPARE_KEY = config['dev']['crypto_compare_key']
PATH_TO_LOGS = os.path.abspath(os.path.join(ROOT_DIR, '..', config['dev']['path_to_logs']))
PATH_COIN_LIST = config['dev']['path_coin_list']
PATH_DATA_LAKE =  config['dev']['path_data_lake']
OHLC_PATH = config['dev']['ohlc_path']
OHLC_TABLE_NAME = config['dev']['ohlc_table']
DB_NAME = config['dev']['db_name']
TODAY = datetime.today().strftime("%Y%m%d")
AWS_PROFILE = config['dev']['aws_profile']
AWS_BUCKET = config['dev']['aws_bucket']
LIST_OF_COINS = config['dev']['list_of_coins']
LIST_OF_CURRENCIES = config['dev']['list_of_currencies']

log = setup_applevel_logger(file_name = PATH_TO_LOGS + "/logging_{}".format(TODAY))
cryptocompare.cryptocompare._set_api_key_parameter(CRYPTO_COMPARE_KEY)
session = boto3.Session(profile_name=AWS_PROFILE)

#get dates of coin creation
coin_list =wr.s3.read_parquet(f"s3://{AWS_BUCKET}/{PATH_DATA_LAKE}/coin_info", boto3_session=session)
tmp_df = coin_list[coin_list['Name'].isin(LIST_OF_COINS)][['Name', 'ContentCreatedOn']]
created_on = dict(zip(tmp_df.Name, tmp_df.ContentCreatedOn))

def extract(coin:str,
            cur:str,
            created_on:dict,
            ts=int(datetime.now().timestamp()),
            limit=2000) -> pd.DataFrame:
    """EXTRACT step
    ===========================================
    This function extracts raw data from Cryptocompare API
    and transforms it into Pandas DataFrame

    Args:
        coin (str): name of the coin
        cur (str): name of the currency
        created_on (dict): a dict: where key is a name of cryptocurrency and values is a date of initial release (unix_timestamp) 
        ts (_type_, optional): date from which the historical data will be obtained. Defaults to current timestamp.
        limit (int, optional): Number of records obrtained per one call. Defaults to 2000 (max availible value in the free account).

    Returns:
        pd.DataFrame: DataFame with historical data
    """
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
    df = pd.DataFrame(data)
    df['coin'] = coin
    df['currency'] = cur
    return df 


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """TRANSFORM step
    ===========================================
    This function transforms the data in DataFrame and returns
    DataFrame ready for the load

    Args:
        df (pd.DataFrame): DataFrame with raw historical data

    Returns:
        pd.DataFrame: DataFrame after transformations
    """
    df['date_time'] = pd.to_datetime(df['time'], unit='s', errors='raise')
    df['year'] = df['date_time'].dt.year
    df['month'] = df['date_time'].dt.month
    df['day'] = df['date_time'].dt.day
    df['hour'] = df['date_time'].dt.hour
    df['minute'] = df['date_time'].dt.minute
    df['partition_col'] = df['date_time'].dt.strftime('%Y')
    df['delta'] = (df['close'] - df['open'])*100/df['open']
    df.rename(mapper={
            'time' : 'time_stamp',
            'volumefrom' : 'volume_fsym',
            'volumeto' : 'volume_tsym',
            'coin' : 'ticker'
            }, axis=1, inplace=True)
    df = df.drop(['conversion_type', 'conversion_symbol'], axis=1)
    return df

def load(pandas_df: pd.DataFrame,
        path_table: str,
        database_name: str,
        table_name: str,
        col_partition=None):
    """LOAD step
    ===========================================
    This functions uplaods DataFrame to HIVE table

    Args:
        pandas_df (pd.DataFrame): DataFrame after transformation step
        path_table (str): path for the table
        database_name (str): name of the database
        table_name (str): name of the table
        col_partition (_type_, optional): List of column names that will be used to create partitions. Defaults to None.
    """
    try:
        wr.s3.to_parquet(
            df=pandas_df,
            index=False,
            path=path_table,
            dataset=True,
            database=database_name,
            compression='snappy',
            table=table_name,
            mode="append",
            partition_cols=col_partition,
            use_threads=True,
            concurrent_partitioning=True,
            boto3_session=session
        )
    except Exception as err:
        print(err)


if __name__ == "__main__":
    log.info('starting upload the historical data!')
    for coin in LIST_OF_COINS:
        for cur in LIST_OF_CURRENCIES:
            if coin != cur:
                try:
                    df = extract(coin, cur, created_on)
                    log.debug(f'{coin} {cur} was extracted successful')
                    #save raw historical data without any transformation as csv
                    path = f's3://{AWS_BUCKET}/raw_hist_data/{coin}_{cur}_historical'
                    wr.s3.to_csv(df, path, index=False,
                                use_threads=True, boto3_session=session,
                                dataset=True, mode='overwrite')
                    log.debug(f'{coin} {cur} saved in the raw format')
                    transformed_df = transform(df)
                    log.debug(f'{coin} {cur} transformed! Almost done')
                    path_parquet = f"s3://{AWS_BUCKET}/{PATH_DATA_LAKE}/{OHLC_TABLE_NAME}"
                    load(df, path_parquet, DB_NAME, OHLC_TABLE_NAME, ['partition_col'])
                    log.info(f'{coin} {cur} was uploaded')
                except Exception as err:
                    log.error(err)
                    