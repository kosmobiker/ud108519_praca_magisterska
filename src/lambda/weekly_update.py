import os
import boto3
from datetime import datetime, timedelta
from typing import List
import logging
import pandas as pd
import boto3
import datetime
import json
import re
import awswrangler as wr
import pytz


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

UTC = pytz.UTC
BUCKET = os.environ['AWS_BUCKET']

def get_list_of_objects_s3(operation_parameters, days_update: int):
    """
    Generator that list files in s3 bucket that
    were placed not more the `days_update` days ago 
    """
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(**operation_parameters)
    for page in page_iterator:
        for content in page.get('Contents'):
            if content.get('LastModified') > UTC.localize(datetime.now() - timedelta(days=days_update)):
                yield content.get('Key')

def get_json_s3(bucket, key):
    """
    Function is used to get JSON file
    from S3 bucket
    """
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj['Body'].read().decode('utf-8')
    return json.loads(data)    

def write_to_glue(
    pandas_df: pd.DataFrame,
    path_table:str,
    database_name:str,
    table_name:str,
    col_partition: List[str],
    mode='overwrite_partitions'
        ):
    """
    This functions write pandas dataframe
    to the table in Glue catalogue
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
            mode=mode,
            partition_cols=col_partition,
            use_threads=True,
            concurrent_partitioning=True,
        )
    except Exception as err:
        logger.error(err)

def lambda_handler(event, context):
    try:
        ohlc_parameters = event['ohlc_parameters']
        tweets_parameters = event['tweets_parameters']
        days_update = event['days_update']
        ohlc_glue_parameters = event['ohlc_glue_parameters']
        tweets_glue_parameters = event['tweets_glue_parameters']
        logger.info('Fetching OHLC data...')
        ohlc_data = []
        for key in get_list_of_objects_s3(ohlc_parameters, days_update):
            try:
                if key.endswith('.json'):
                    coin_currency = re.findall("(?:[0-9]{4}_[0-9]{2}_[0-9]{2})_(.+)(?:.json)", key)[0]
                    tmp = get_json_s3(BUCKET, key)
                    res = [dict(item, **{'coin_currency':f'{coin_currency}'}) for item in tmp]
                    ohlc_data.extend(res)
            except Exception as err:
                logging.error(err)
        if len(ohlc_data) > 0:
            df = pd.DataFrame(ohlc_data)
            write_to_glue(df, ohlc_glue_parameters)
        logger.info('Fetching tweets data...')
        tweets_data = []
        for key in get_list_of_objects_s3(tweets_parameters, days_update):
            try:
                tmp = get_json_s3(BUCKET, key)
                tweets_data.extend(tmp)
            except Exception as err:
                logging.error(err)
        if len(tweets_data) > 0:
            df = pd.DataFrame(tweets_data)
            write_to_glue(df, tweets_glue_parameters)
        return True
    except Exception as err:
        logger.info(f"FAILED: update of tables wasn not performed")
        logger.error(err)
        return False