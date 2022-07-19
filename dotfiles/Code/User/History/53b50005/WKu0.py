"""
# ETL script. Responsible for fetching queries and copying them to S3 and then loading them to RedShift.
# Author: Pavly Salah
# Launch on psalah@stagespark.fl3xx.us
"""

# TODO Format this shit

# %%
# Importing libraries
import os

# import sys
import time
import logging
import json
import argparse

import boto3
import redshift_connector
from botocore.exceptions import ClientError


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

# %%
# Set up RedShift client region
# ? Prod RS_HOST  'rs-ds-prod.cebwpov4juzc.us-west-1.redshift.amazonaws.com'
# ? Stage RS_HOST 'rs-ds-stage.cebwpov4juzc.us-west-1.redshift.amazonaws.com'
redshift = boto3.client(
    "redshift",
    region_name="us-west-1",
    aws_access_key_id=bright_config["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=bright_config["AWS_SECRET_ACCESS_KEY"],
)

# %%
# Read the SQL scripts
start_time = time.time()

scripts = {"booking": "./src/booking_report.sql", "flight": "./src/flight_report.sql"}
sql_exec = {}

for key, val in scripts.items():
    print(f"read {val}")
    with open(val, "r", encoding="UTF-8") as ifs:
        sql_exec[key] = ifs.read()
        logger.debug("Loaded SQL Query: %s", key)
# sys.exit(0)


# %%
# SQL queries to execute
# These queries fetch different information about the booking and the flights from the database
sql2exec = {}
for root, _, file_names in os.walk("queries/"):
    for file_name in file_names:
        if file_name.endswith(".sql"):
            with open(f"{root}/{file_name}", "r", encoding="UTF-8") as ifs:
                sql2exec[file_name[:-4]] = ifs.read()
                logger.info("Loaded SQL Query: %s", file_name[:-4])

# %%
# Define the resource to load - S3
resource = boto3.resource("s3")

# 172.16.28.245 db02
# 172.20.14.77 stagedb01
# 172.20.24.220  stagedb02
# 172.20.20.90 stagedb03
# conn = psycopg2.connect(dbname='flo_stage_us', user='flo', password="pvHZ&5y3A&nT8TQN", host='stagedb02.fl3xx.us')
# US- source: db04.fl3xx.us  172.16.24.143 time: daily at 5AM UTC
# US  internal 172.16.24.143


def copy2s3(bucket, select_query, filename) -> int:
    """
    Copy file with name `filename` to the S3 bucket

    Args:
        bucket (string): S3 bucket to copy the files to
        select_query (string): query to add
        filename (string): name of the input file to copy

    Returns:
        status_code (int): status code of the command.
                           0: the command was executed successfully
                           1: there was an error executing the command
    """
    # Command to copy tables to S3
    copy_command = f"""psql -U {bright_config['PG_USER']} -d {bright_config['PG_DIR']} -h {bright_config['PG_HOST']} \
                    -c "COPY ({select_query}) TO STDOUT WITH (FORMAT csv, DELIMITER ',', QUOTE '\\\"', HEADER TRUE);\" \
                    | aws s3 cp - s3://{bucket}/{filename}"""
    # logger.debug("Copy command to be executed: \n%s\n", copy_command)
    return os.system(copy_command)


def pause_instance(instance_id):
    """
    Pause a Redhist  an instance. The request returns immediately.
    To wait for the instance to start, use the Instance.wait_until_running() function.

    Args:
        instance_id (int): The ID of the instance to start.

    Returns:
        The response to the start request. This includes both the previous and
        current state of the instance.
    """
    try:
        response = redshift.pause_cluster(ClusterIdentifier=instance_id)
        logger.info("Started instance %s.", instance_id)
    except ClientError:
        logger.exception("Couldn't pause instance %s.", instance_id)
        raise
    else:
        return response


def resume_instance(instance_id):
    """
    Resume  an instance. The request returns immediately.
    To wait for the instance to stop, use the Instance.wait_until_stopped() function.

    Args:
        instance_id (int): The ID of the instance to stop.

    Returns:
        The response to the stop request. This includes both the previous and
        current state of the instance.
    """
    try:
        response = redshift.resume_cluster(ClusterIdentifier=instance_id)
        logger.info("Resumed instance %s.", instance_id)
    except ClientError:
        logger.exception("Couldn't resume instance %s.", instance_id)
        raise
    else:
        return response


# %%
# Resume the RedShift instance
logger.info("Resuming RedShift...")
try:
    res = resume_instance(bright_config["RS_ENDPOINT"])
    logger.info("Cluster resumed!")
except ClientError:
    logger.debug("Client is already running. No need to resume.")

# Basically creates a mini loading thingy
while (
    redshift.describe_clusters(ClusterIdentifier=bright_config["RS_ENDPOINT"])["Clusters"][0]["ClusterStatus"]
    != "available"
):
    print(".", end=" ", flush=True)
    time.sleep(5)

logger.info("Cluster described!")
print(f"\n--- Resuming done: {(time.time() - start_time):.3f} sec ---\n")

# %%
# Connect to Redshift cluster using AWS credentials
# ! Cannot execute from local hast. Is IP restricted to the server.
connector = redshift_connector.connect(
    host=bright_config["RS_HOST"],
    database=bright_config["RS_DATABASE"],
    user=bright_config["RS_USER"],
    password=bright_config["RS_PASSWORD"],
)
connector.autocommit = True
cursor: redshift_connector.Cursor = connector.cursor()


# %%
# Copy the SQL queries to S3 and Redshift.
# Create a CSV file based on these queries
for i, _ in sql2exec.items():
    tstart_time = time.time()
    # Get the target file and table
    target_file = bright_config["BASE_DIR"] + i + "/" + i + ".csv"
    target_table = "public." + i

    # Truncate the target table
    cursor.execute("truncate table " + target_table)
    logger.info("Truncated %s", target_table)

    # Copy the query to the target S3 bucket
    exit_code = copy2s3(bright_config["TARGET_BUCKET"], sql2exec[i], target_file)
    logger.info("Unload %s to s3 done.", i)

    # If the copy is successful, copy the tables to RedShift
    if exit_code == 0:
        # Query to copy the table from S3 to RedShift using IAM role
        # Query parameters MUST BE single-quoted
        query = f"""COPY {target_table} FROM 's3://{bright_config["TARGET_BUCKET"]}/{target_file}' \
            iam_role 'arn:aws:iam::{bright_config["IAM_ID"]}:role/rs-spectrum' DELIMITER ',' \
            MAXERROR as 100 IGNOREHEADER 1 CSV QUOTE '"' ACCEPTINVCHARS AS '?' TRUNCATECOLUMNS """

        logger.debug("Query to be executed: \n %s\n", query)
        logger.info("Loading data to %s", i)

        # Excute the query for copying the files to RedShift
        # ! These table take a very long time
        # ! booking_aud 419    flightexecution 124    leg 324    workflowstate 1881
        cursor.execute(query)

        # Log the process into `etl_log` file as a success
        etl_log = f"""INSERT INTO public.etl_log (proc_id, operation_details, operation_type) \
            VALUES ('{bright_config['PROC_ID']}',
            '{target_table} extracted and loaded from S3 in {time.time() - tstart_time} sec', 'PSQLSYNC')"""
        cursor.execute(etl_log)
    else:
        # Otherwise, if the copy is unseccessful, just log an error message
        logger.exception("!!!!  S3 copy failed !!!")

        # Log the process into `etl_log` file as a failed query
        etl_log = f"""INSERT INTO public.etl_log (proc_id, operation_details, operation_type) \
            VALUES ({bright_config['PROC_ID']}, {target_table}, 'PSQLSYNC_FAILED')"""
        cursor.execute(etl_log)

    print(f"---***--- {i} {(time.time() - tstart_time)} sec ---***---\n")

print(f"\n--- Sync DB Total: {(time.time() - start_time)} sec ---\n")


# %%
# Compute measure
logger.info("Computing resources...")
compute_start_time = time.time()

# ? Here, we basically execute the SQL commands for the booking only, not the flight
sql_queries_to_be_executed = ["booking", "flight"]

for query_to_be_executed in sql_queries_to_be_executed:
    logger.debug("Computing %s", query_to_be_executed)
    commands = sql_exec[query_to_be_executed].split(";")

    for command_number, command in enumerate(commands):
        # ! Why?
        if len(command) < 10:
            continue

        logger.info("Executing command #%d", command_number)
        logger.debug("Command: %s", command)

        cursor.execute(command)

    print(" ")

print(f"--- w Compute Total / Current phase: {(time.time() - compute_start_time)} sec ---")


# Back to pause instance
# res = pause_instance(RS_ENDPOINT)
print(f"--- Grand Total: {(time.time() - start_time)} sec ---")