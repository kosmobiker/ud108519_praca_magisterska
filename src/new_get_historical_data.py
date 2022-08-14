"""
this module is used to get historical data for 
selected cryptocurrencies using Cryptocompare API
with 1 hour interval

Contains 3 steps:

1. Extract - get info from Crytocurrency API
2. Load - save raw files to S3
3. Transform - make data dtransformation and upload it to HIVE table
"""
import os
import boto3
import cryptocompare
import pandas as pd
import awswrangler as wr
from datetime import datetime
from utils.read_config import read_toml_config
from utils.logger import setup_applevel_logger


ROOT_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.abspath(os.path.join(ROOT_DIR, '..', 'config', 'config.toml'))

config = read_toml_config(CONFIG_PATH)
CRYPTO_COMPARE_KEY = config['dev']['api_endpoint']
cryptocompare.cryptocompare._set_api_key_parameter(CRYPTO_COMPARE_KEY)
PATH_COIN_LIST = config['dev']['path_coin_list']
PATH_RAW_HIST_DATA = config['dev']['path_raw_hist_data']
PATH_TO_LOGS = os.path.abspath(os.path.join(ROOT_DIR, '..', config['dev']['path_to_logs']))
OHLC_PATH = config['dev']['ohlc_path']
OHLC_TABLE_NAME = config['dev']['ohlc_table']
DB_NAME = config['dev']['db_name']
TODAY = datetime.today().strftime("%Y%m%d")
AWS_PROFILE = config['dev']['aws_profile']
AWS_BUCKET = config['dev']['aws_bucket']
LIST_OF_COINS = config['dev']['list_of_coins']
LIST_OF_CURRENCIES = config['dev']['list_of_currencies']

log = setup_applevel_logger(file_name = PATH_TO_LOGS + "/logging_{}".format(TODAY))

session = boto3.Session(profile_name=AWS_PROFILE)

#get dates of coin creation
coin_list = wr.s3.read_csv(f's3://{AWS_BUCKET}/{PATH_COIN_LIST}', boto3_session=session)
tmp_df = coin_list[coin_list['Name'].isin(LIST_OF_COINS)][['Name', 'ContentCreatedOn']]
created_on = dict(zip(tmp_df.Name,tmp_df.ContentCreatedOn))

#"extract" step
def extract(coin:str,
            cur:str,
            created_on:dict,
            ts=int(datetime.now().timestamp()),
            limit=2000) -> pd.DataFrame:
    """
    there will be some doc strigns
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
    """
    there will be some doc strigns
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

def load(pandas_df: pd.DataFrame, path_table, database_name, table_name, col_partition):
    """
    there will be some doc strigns
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
    log.info('starting!')
    for coin in LIST_OF_COINS:
        for cur in LIST_OF_CURRENCIES:
            if coin != cur:
                try:
                    df = extract(coin, cur, created_on)
                    log.debug(f'{coin} {cur} was extracted successful')
                    path = f's3://{AWS_BUCKET}/{PATH_RAW_HIST_DATA}/{coin}_{cur}_historical'
                    wr.s3.to_csv(df, path, index=False,
                                use_threads=True, boto3_session=session,
                                dataset=True, mode='overwrite')
                    log.debug(f'{coin} {cur} saved in raw format')
                    transformed_df = transform(df)
                    log.debug(f'{coin} {cur} transformed! almost done')
                    path_parquet = f"s3://{AWS_BUCKET}/data/my_database/ohlc_data"
                    load(df, path_parquet, DB_NAME, OHLC_TABLE_NAME, ['partition_col'])
                    log.info(f'{coin} {cur} was uploaded')
                except Exception as err:
                    log.error(err)