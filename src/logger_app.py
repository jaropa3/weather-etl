import logging
import sys
from datetime import datetime

def setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    
    # konsola
    handler = logging.StreamHandler(sys.stdout)
    
    # plik
    file_handler = logging.FileHandler(f"logs/etl_{datetime.now():%Y%m%d}.log")
    
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    logger.addHandler(handler)
    logger.addHandler(file_handler)
    
    return logger