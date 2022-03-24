import os, logging
from pathlib import Path
from logging.handlers import RotatingFileHandler

def error_log(message, ROOT_DIR=None, filename='error.log', filefolder='log', maxBytes=5*1024*1024, setLevel='info', name='root',
              formatter='%(asctime)s %(name)s %(message)s'):
    log_formatter = logging.Formatter(formatter)
    if ROOT_DIR==None:
        ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
    folder_path = os.path.join(ROOT_DIR, filefolder)
    log_path = os.path.join(folder_path, filename)
    if not os.path.exists(os.path.join(ROOT_DIR, filefolder)):
        Path(folder_path).mkdir(parents=True, exist_ok=True)
    my_handler = RotatingFileHandler(log_path, mode='a', maxBytes=maxBytes,
                                     backupCount=2, encoding=None, delay=0)
    my_handler.setFormatter(log_formatter)
    if setLevel=='info':
        my_handler.setLevel(logging.INFO)
    else:
        my_handler.setLevel(logging.WARNING)
    logger = logging.getLogger(name=name)
    logger.setLevel(logging.INFO)
    logger.addHandler(my_handler)
    if setLevel=='info':
        logger.info(message)
    else:
        logger.warning(message)