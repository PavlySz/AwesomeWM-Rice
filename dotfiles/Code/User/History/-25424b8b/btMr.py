"""Read config"""

# %%
import logging
import json
import argparse

# Set up logger
LOGGING_FORMAT = "[%(levelname)s] %(message)s"
logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)
logger = logging.getLogger("my_logger")


# %%
# Take config file path as an argument
# default config path: "../config/bright_us_prod_config.json"
parser = argparse.ArgumentParser(description="Copy queries to S3 and RedShift")
parser.add_argument(
    "-c",
    "--config_file_path",
    type=str,
    required=True,
    help="path to config file containing S3 and RedShift configuration",
)

args = parser.parse_args()

# load config
with open(args.config_file_path, "r", encoding="UTF-8") as ifs:
    bright_config = json.load(ifs)

logger.info("BRIGHT config: \n%s\n", bright_config)


# %%
# Stuff
# 172.16.28.245 db02
# 172.20.14.77 stagedb01
# 172.20.24.220  stagedb02
# 172.20.20.90 stagedb03
# conn = psycopg2.connect(dbname='flo_stage_us', user='flo',
#                         password="pvHZ&5y3A&nT8TQN", host='stagedb02.fl3xx.us')
# US source: db04.fl3xx.us  172.16.24.143 time: daily at 5AM UTC
# US internal 172.16.24.143
