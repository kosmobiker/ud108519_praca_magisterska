"""
This module is used to get basic information about cryptocurrencies
It uses cryptocompare API
Data is saving HIVE table in database
"""
import cryptocompare
import boto3
import os
from datetime import datetime
import pandas as pd
import awswrangler as wr


from utils.aws_utils import create_bucket
from utils.read_config import read_toml_config
from utils.logger import setup_applevel_logger


ROOT_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.abspath(os.path.join(ROOT_DIR, '..', 'config', 'config.toml'))

config = read_toml_config(CONFIG_PATH)
PATH_TO_LOGS = os.path.abspath(os.path.join(ROOT_DIR, '..', config['dev']['path_to_logs']))
PATH_DATA_LAKE =  config['dev']['path_data_lake']
AWS_PROFILE = config['dev']['aws_profile']
AWS_BUCKET = config['dev']['aws_bucket']
DB_NAME = config['dev']['db_name']
TODAY = datetime.today().strftime("%Y%m%d")

log = setup_applevel_logger(file_name = PATH_TO_LOGS + "/logging_{}".format(TODAY))
session = boto3.Session(profile_name=AWS_PROFILE)
s3_client = session.client('s3')


def get_info_about_coins():
    """
    This function is used to get basic information about cryptocurrencies
    and save it as HIVE table

    """
    pd_data = pd.DataFrame(columns=[
                        'Id',
                        'Url',
                        'ImageUrl',
                        'ContentCreatedOn',
                        'Name',
                        'Symbol',
                        'CoinName',
                        'FullName',
                        'Description',
                        'AssetTokenStatus',
                        'Algorithm',
                        'ProofType',
                        'SortOrder',
                        'Sponsored',
                        'Taxonomy.Access',
                        'Taxonomy.FCA',
                        'Taxonomy.FINMA',
                        'Taxonomy.Industry',
                        'Taxonomy.CollateralizedAsset',
                        'Taxonomy.CollateralizedAssetType',
                        'Taxonomy.CollateralType',
                        'Taxonomy.CollateralInfo',
                        'Rating.Weiss.Rating',
                        'Rating.Weiss.TechnologyAdoptionRating',
                        'Rating.Weiss.MarketPerformanceRating'])
    try:
        create_bucket(AWS_BUCKET, s3_client=s3_client)
        raw_data = cryptocompare.get_coin_list()
        for key in raw_data.keys():
            row = pd.json_normalize(raw_data[key])
            pd_data = pd.concat([pd_data, row])
        pd_data = pd_data.reset_index(drop = True)
        path = f"s3://{AWS_BUCKET}/{PATH_DATA_LAKE}/coin_info"
        wr.s3.to_parquet(pd_data, index=False, path=path,
                    dataset=True, database=DB_NAME,
                    table="coin_info",
                    mode="overwrite",
                    boto3_session=session)
        log.info('List of coins was uploaded to data lake')
    except Exception as err:
        log.info('List of coins was not uploaded to data lake')
        log.error(err)

if __name__ == "__main__":
    get_info_about_coins()