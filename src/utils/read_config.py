"""
Reading of the config files
"""
import yaml
from logger import get_logger


log = get_logger(__name__)

def read_config(path: str) -> dict:
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
