"""
Module for data transformation
steps:
    - scan dynamodb table
    - get appropriate json from s3
    - transfrom into dataframe
    - add data to glue table
    - update partitions
"""
import os
from datetime import datetime
from typing import List
import pandas as pd
import boto3
import awswrangler as wr
from boto3.dynamodb.conditions import Key
from utils.read_config import read_toml_config
from utils.aws_utils import get_json_s3
from utils.logger import setup_applevel_logger


ROOT_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.abspath(os.path.join(ROOT_DIR, '..', 'config', 'config.toml'))
config = read_toml_config(CONFIG_PATH)
PATH_TO_LOGS = os.path.abspath(os.path.join(ROOT_DIR, '..', config['dev']['path_to_logs']))
TODAY = datetime.today().strftime("%Y%m%d")
PATH_RAW_DATA = config['dev']['path_raw_data']
PATH_DATALAKE = config['dev']['path_datalake']
AWS_PROFILE = config['dev']['aws_profile']
AWS_BUCKET = config['dev']['aws_bucket']
DATABASE_NAME = config['dev']['database_name'] 
HISTORICAL_TABLE = config['dev']['historical_table']
ATHENA_OUTPUT = config['dev']['athena_output']
S3_OUTPUT = f"s3://{AWS_BUCKET}/{ATHENA_OUTPUT}/"
CHUNK_SIZE = config['dev']['chunk_size']

log = setup_applevel_logger(file_name = PATH_TO_LOGS + "/logging_{}".format(TODAY))


session = boto3.Session(profile_name=AWS_PROFILE)
dynamo = session.resource('dynamodb')
s3_client = session.client('s3')

def convert_lists(response) -> List:
    paths = [i['PathS3'] for i in response]
    tokens = [i['TokenName'] for i in response]
    log.debug('List converted')
    return [list(l) for l in zip(paths, tokens)]

def scan_paths_table(type: str, convert_lists, dynamodb=None) -> List:
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    log.debug('scan_paths_table started')
    table = dynamodb.Table('Pathes')
    scan_kwargs = {
        'FilterExpression': Key('TypeOfRecord').eq(type),
        'ProjectionExpression': "#pth, #tp, #tkn",
        'ExpressionAttributeNames': {"#pth": "PathS3", "#tp": "TypeOfRecord", "#tkn" : "TokenName"}
    }
    results = []
    done = False
    start_key = None
    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key
        response = table.scan(**scan_kwargs)
        results.extend(convert_lists(response.get('Items', [])))
        start_key = response.get('LastEvaluatedKey', None)
        done = start_key is None
        log.debug('DynamoDB is scanning....')
    log.debug('scan_paths_table finished')
    return results

def transform_json_dataframe(data: dict, token: str) -> pd.DataFrame:
    try:
        ts = [i[0] for i in data['prices']]
        prices = [i[1] for i in data['prices']]
        market_caps = [i[1] for i in data['market_caps']]
        total_volumes = [i[1] for i in data['total_volumes']]
        temp = pd.DataFrame({
            "timestamp" : ts,
            "prices" : prices,
            "market_caps" : market_caps,
            "total_volumes" : total_volumes,
            "formated_date" : [datetime.fromtimestamp(i/1000).strftime('%Y-%m-%d') for i in ts]
            })
        temp['coin'] = token
        temp['currency'] = 'usd'
        log.debug(f'{token} transformation was finished')
        return temp
    except Exception as err:
        log.error(err)
        log.error(f'{token} was not transformed')
        log.debug(data)

def chunk_dfs(file_names, chunk_size):
    """
    yields n dataframes at a time where n == chunksize
    """
    dfs = []
    for f in file_names:
        try:
            data = get_json_s3(AWS_BUCKET, f[0], s3_client)
            tmp_df = transform_json_dataframe(data, f[1])
            dfs.append(tmp_df)
            if len(dfs) == chunk_size:
                yield dfs
                dfs  = []
        except Exception as err:
            log.error(err)
            log.error(f"Didn't get df. Trouible with {f[1]}")
    if dfs:
        yield dfs


def write_to_glue(pandas_df: pd.DataFrame):
    log.debug(f'Write_to_glue started')
    try:
        wr.s3.to_parquet(
            df=pandas_df,
            index=False,
            path=f"s3://{AWS_BUCKET}/{PATH_DATALAKE}/{HISTORICAL_TABLE}/",
            dataset=True,
            database=DATABASE_NAME,
            compression='snappy',
            table=HISTORICAL_TABLE,
            mode="append",
            # partition_cols=["coin"],
            use_threads=True,
            concurrent_partitioning=True,
            boto3_session=session
        )
        log.debug(f"Data was added to {HISTORICAL_TABLE} table")
    except Exception as err:
        log.error(f"Data wasn't added to {HISTORICAL_TABLE} table")
        log.error(err)

def repair_partitions(database_name: str, table_name: str, s3_output_path: str, session):
    try:
        wr.athena.repair_table(table_name, database_name, s3_output_path, boto3_session=session)
        log.info('Partitions were updated')
    except Exception as err:
        log.error('Partitions were not updated')
        log.error(err)

if __name__ == "__main__":
    pathes_tokennames = scan_paths_table('good', convert_lists, dynamo)
    chunks = (pd.concat(dfs) for dfs in chunk_dfs(pathes_tokennames, 250))
    for chunk in chunks:
        try:
            write_to_glue(chunk)
        except Exception as err:
            log.error(err)
            log.error("Chunk is lost. It wasn't written to Hive")
    repair_partitions(DATABASE_NAME, HISTORICAL_TABLE, S3_OUTPUT, session)

    


