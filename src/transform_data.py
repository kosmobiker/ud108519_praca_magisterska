"""
Module for data transformation
scan dynamodb table ==> get appropriate json from s3 ==> transfrom into dataframe ==> add to glue table
"""
import os
from datetime import datetime
from typing import List
import pandas as pd
import boto3
from boto3.dynamodb.conditions import Key
from utils.read_config import read_toml_config
from utils.aws_utils import get_json_s3


ROOT_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.abspath(os.path.join(ROOT_DIR, '..', 'config', 'config.toml'))
config = read_toml_config(CONFIG_PATH)
PATH_TO_LOGS = os.path.abspath(os.path.join(ROOT_DIR, '..', config['dev']['path_to_logs']))
TODAY = datetime.today().strftime("%Y%m%d")
PATH_RAW_DATA = config['dev']['path_raw_data']
AWS_PROFILE = config['dev']['aws_profile']
AWS_BUCKET = config['dev']['aws_bucket']

session = boto3.Session(profile_name=AWS_PROFILE)
dynamo = session.resource('dynamodb')
s3_client = session.client('s3')

def convert_lists(response) -> List:
    paths = [i['PathS3'] for i in response]
    tokens = [i['TokenName'] for i in response]
    return [list(l) for l in zip(paths, tokens)]


def scan_paths_table(type: str, convert_lists, dynamodb=None) -> List:
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')

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

    return results


def transform_json_dataframe(data: dict, token: str) -> pd.DataFrame:
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
    return temp

def write_to_glue(pandas_df: pd.DataFrame):
    print('I am a bimbo')
    pass


if __name__ == "__main__":
    pathes_tokennames = scan_paths_table('good', convert_lists, dynamo)
    for element in pathes_tokennames:
        data = get_json_s3(AWS_BUCKET, element[0], s3_client)
        pandas_df = transform_json_dataframe(data, element[1])
        write_to_glue(pandas_df)
