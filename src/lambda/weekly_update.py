import os
import boto3
from datetime import datetime, timedelta
import logging
import pandas as pd
import boto3
import datetime
import json
import numpy as np
import re
from datetime import datetime, timedelta
import awswrangler as wr
import pytz


logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

utc=pytz.UTC

bucket = os.environ['AWS_BUCKET']

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
            if content.get('LastModified') > utc.localize(datetime.now() - timedelta(days=days_update)):
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

def write_to_glue(pandas_df: pd.DataFrame, glue_parameters: Dict):
    try:
        wr.s3.to_parquet(
            df=pandas_df,
            index=False,
            path=glue_parameters['path_table'],
            dataset=True,
            database=glue_parameters['database_name'],
            compression='snappy',
            table=glue_parameters['table_name'],
            mode="append",
            # partition_cols=["coin"],
            use_threads=True,
            concurrent_partitioning=True,
            boto3_session=session
        )
    except Exception as err:
        print(err)