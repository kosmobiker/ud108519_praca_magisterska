"""
Reading of the config files
"""
import yaml
import toml
from utils.logger import get_logger


log = get_logger(__name__)

def read_yaml_config(path: str) -> dict:
    """
    This function is used to read config
    constants from yaml files.
    
    Parameters:
        path (str): path to the file with configs
    
    Return:
        dictionary with configuration settings
    """
    with open(path, "r") as conf:
        try:
            return yaml.safe_load(conf)
        except yaml.YAMLError as exc:
            log.error(exc)

def read_toml_config(path: str) -> dict:
    """
    This function is used to read config
    constants from toml files.
    
    Parameters:
        path (str): path to the file with configs
    
    Return:
        dictionary with configuration settings
    """
    with open(path, 'r') as conf:
        try:
            return toml.load(conf)
        except Exception as err:
            log.error(err)

