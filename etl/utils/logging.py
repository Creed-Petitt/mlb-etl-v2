import logging
import sys

def get_etl_logger(name):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

def get_processor_logger(name):
    return get_etl_logger(f"processor.{name}")

def get_api_client_logger(name):
    return get_etl_logger(f"api.{name}")