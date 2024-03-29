"""Read config"""

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
# default AWS credentials path: "../config/aws_credentials.json"
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
