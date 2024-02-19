"""crawler_llama_index"""

import logging
import os

ROOT_LOG_LEVEL = "DEBUG"

LOG_FORMAT = (
    "%(asctime)s.%(msecs)03d [%(levelname)-8s] %(name)+25s - %(message)s"
)
logging.basicConfig(level=ROOT_LOG_LEVEL, format=LOG_FORMAT, datefmt="%H:%M:%S")
logging.captureWarnings(True)

logging.getLogger('scrapy').propagate = False