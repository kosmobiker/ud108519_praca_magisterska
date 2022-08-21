"""
Standard logger 
it is used to get Loggings
"""
import logging
import sys
APP_LOGGER_NAME = 'ud108519'


def setup_applevel_logger(logger_name = APP_LOGGER_NAME, file_name=None):
    """
    This function is used for logging. It saves the logs
    into the log files in appropriate folder.
    Default log level is *DEBUG*

    Parameters:
        logger_name (str): name of the app used in logging
        file_name (str): path for logfiles
    """ 
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    logger.handlers.clear()
    logger.addHandler(sh)
    if file_name:
        fh = logging.FileHandler(file_name)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    return logger

def get_logger(module_name):
    """
    It is used when there is a need to embed the logging into the function
    """    
    return logging.getLogger(APP_LOGGER_NAME).getChild(module_name)