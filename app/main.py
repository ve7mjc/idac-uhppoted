from configuration import get_config
from adapter import IdacUhppotedAdapter

import asyncio
import logging
from logging.handlers import RotatingFileHandler
import os

#
# Configure logging
#
logger = logging.getLogger()

logs_path = os.environ.get("LOGS_PATH", "./logs")
if not os.path.exists(logs_path):
    os.mkdir(logs_path)

log_file = os.path.join(logs_path, 'idac-uhppote.log')

# 5 MB per file, keep 5 old copies
file_handler = RotatingFileHandler(log_file, maxBytes=1024 * 1024 * 5,
                                   backupCount=5)

console_handler = logging.StreamHandler()

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# set levels
logger.setLevel(logging.DEBUG)
console_handler.setLevel(logging.WARNING)
file_handler.setLevel(logging.DEBUG)

try:
    config = get_config()
except FileNotFoundError:
    logger.error("cannot locate config file!")
    exit(1)

adapter = IdacUhppotedAdapter(config)
asyncio.run(adapter.start())
