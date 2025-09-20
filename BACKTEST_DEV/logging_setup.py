# logging_setup.py
import os, time, logging, sys
from logging.handlers import TimedRotatingFileHandler
from pythonjsonlogger import jsonlogger

def build_logger(log_root="./logs/backtest", log_level="I"):
    log_map = {"D":logging.DEBUG,"I":logging.INFO,"W":logging.WARN,"E":logging.ERROR}
    path = os.path.join(log_root, "log.log")
    os.makedirs(os.path.dirname(path), exist_ok=True)

    logger = logging.getLogger('trades')
    logger.setLevel(log_map[log_level])

    file_out = TimedRotatingFileHandler(filename=path, when="midnight", interval=1, backupCount=0, utc=True)

    # explicitly tell handler to add suffix to rolled files
    file_out.suffix = "%Y-%m-%d"
    file_out.extMatch = r"^\d{4}-\d{2}-\d{2}$"   # regex for rollover
    file_out.setFormatter(logging.Formatter("FILE: %(asctime)s %(levelname)s ::  %(message)s"))

    # console output
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter("STDOUT: %(asctime)s %(levelname)s ::  %(message)s"))
    
    if not logger.handlers:
        logger.addHandler(console)
        logger.addHandler(file_out)
        
    return logger
