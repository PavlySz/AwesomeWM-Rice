"""ETL script.

# Responsible for fetching queries and copying them to S3 and then loading them to RedShift.
# Author: Pavly Salah
# Launch on psalah@stagespark.fl3xx.us
# Ran from app/run_us_stage.sh
# All paths are reltive to the `/app` directory
"""

# ! Okay, so..
# ! The booking report went into s3-data-layer/prod/

# %%
# Importing libraries
import os
import sys
import time
import re

import boto3
import redshift_connector
from botocore.exceptions import ClientError

from helpers.helpers import resume_instance, pause_instance, copy2s3
from helpers.read_config import bright_config, logger

# %%
# Read the SQL scripts
start_time = time.time()

scripts = {
    "booking": "etl/src/booking_report.sql",
    "flight": "etl/src/flight_report.sql",
}
sql_exec = {}

# Read the SQL scripts
for key, val in scripts.items():
    print(f"read {val}")
    with open(val, "r", encoding="UTF-8") as ifs:
        sql_exec[key] = ifs.read()
        logger.debug("Loaded SQL Query: %s", key)

# %%
# SQL queries to execute
# These queries fetch different information about the booking and the flights from the database
sql2exec = {}
for root, _, file_names in os.walk("etl/queries/"):
    for file_name in file_names:
        if file_name.endswith(".sql"):
            with open(f"{root}/{file_name}", "r", encoding="UTF-8") as ifs:
                sql2exec[file_name[:-4]] = ifs.read()
                logger.info("Loaded SQL Query: %s", file_name[:-4])


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
# Resume the RedShift instance
logger.info("Resuming RedShift...")
try:
    _ = resume_instance(redshift, bright_config["RS_ENDPOINT"])
except ClientError:
    logger.debug("Client is already running. No need to resume.")

# Basically creates a mini loading thingy
while (
    redshift.describe_clusters(ClusterIdentifier=bright_config["RS_ENDPOINT"])[
        "Clusters"
    ][0]["ClusterStatus"]
    != "available"
):
    print(".", end=" ", flush=True)
    print("")
    time.sleep(5)

logger.info("Cluster available!")
print(f"\n--- Resuming done: {(time.time() - start_time):.3f} sec ---\n")


# %%
# Connect to Redshift cluster using AWS credentials
# ! Cannot execute from local hast. Is IP restricted to the server.
logger.info("Trying to connect to %s...", bright_config["RS_HOST"])

try:
    connector = redshift_connector.connect(
        host=bright_config["RS_HOST"],
        database=bright_config["RS_DATABASE"],
        user=bright_config["RS_USER"],
        password=bright_config["RS_PASSWORD"],
    )

    connector.autocommit = True
    cursor: redshift_connector.Cursor = connector.cursor()
    logger.info("Connected to Redshift host: %s", bright_config["RS_HOST"])

except redshift_connector.error.InterfaceError:
    CONNECTION_ERROR = f"""Could not connect to {bright_config['RS_HOST']}. \n \
        Make sure you are running the script from the server and not from a local machine."""
    logger.error(re.sub(r"\s+", " ", CONNECTION_ERROR))
    sys.exit(1)


# %%
# Copy the SQL queries to S3 and Redshift.
# Create a CSV file based on these queries
for i, val in sql2exec.items():
    tstart_time = time.time()
    # Get the target file and table
    target_file = bright_config["BASE_DIR"] + i + "/" + i + ".csv"
    target_table = "public." + i

    # Truncate the target table
    cursor.execute("truncate table " + target_table)
    logger.info("Truncated %s", target_table)

    # Copy the query to the target S3 bucket
    exit_code = copy2s3(bright_config["TARGET_BUCKET"], val, target_file)
    logger.info("Unload %s to s3 done.", i)

    # If the copy is successful, copy the tables to RedShift
    if exit_code == 0:
        # Query to copy the table from S3 to RedShift using IAM role
        # Query parameters MUST BE single-quoted
        query = f"""
            COPY {target_table} FROM 's3://{bright_config["TARGET_BUCKET"]}/{target_file}' \
            iam_role 'arn:aws:iam::{bright_config["IAM_ID"]}:role/rs-spectrum' DELIMITER ',' \
            MAXERROR as 100 IGNOREHEADER 1 CSV QUOTE '"' ACCEPTINVCHARS AS '?' TRUNCATECOLUMNS
        """

        query = re.sub(r"\s+", " ", query)
        logger.debug("Query to be executed: \n %s\n", query)
        logger.info("Loading data to %s", i)

        # Excute the query for copying the files to RedShift
        # ! These table take a very long time
        # ! booking_aud 419    flightexecution 124    leg 324    workflowstate 1881
        cursor.execute(query)

        # Log the process into `etl_log` file as a success
        etl_log = f"""
            INSERT INTO public.etl_log (proc_id, operation_details, operation_type) \
            VALUES ('{bright_config['PROC_ID']}',
            '{target_table} extracted and loaded from S3 in {time.time() - tstart_time} sec', \
            'PSQLSYNC')
        """

        etl_log = re.sub(r"\s+", " ", etl_log)
        cursor.execute(etl_log)
    else:
        # Otherwise, if the copy is unseccessful, just log an error message
        logger.exception("!!!!  S3 copy failed !!!")

        # Log the process into `etl_log` file as a failed query
        etl_log = f"""
            INSERT INTO public.etl_log (proc_id, operation_details, operation_type) \
            VALUES ({bright_config['PROC_ID']}, {target_table}, 'PSQLSYNC_FAILED')
        """

        etl_log = re.sub(r"\s+", " ", etl_log)
        cursor.execute(etl_log)

    print(f"---***--- {i} {(time.time() - tstart_time)} sec ---***---\n")

print(f"\n--- Sync DB Total: {(time.time() - start_time)} sec ---\n")


# %%
# Compute measure
logger.info("Computing resources...")
compute_start_time = time.time()

# Added the Flight queries to be executed
sql_queries_to_be_executed = ["booking", "flight"]

for query_to_be_executed in sql_queries_to_be_executed:
    logger.info("Computing %s", query_to_be_executed)
    commands = sql_exec[query_to_be_executed].split(";")

    for command_number, command in enumerate(commands):
        # This was probably made just for debugging.
        # I am going to leave it be just in case.
        if len(command) < 10:
            continue

        logger.info("Executing command #%d", command_number)
        logger.debug("Command: %s", command)

        cursor.execute(command)

    print(" ")

print(
    f"--- w Compute Total / Current phase: {(time.time() - compute_start_time)} sec ---"
)


# %%
# Back to pause instance
_ = pause_instance(redshift, bright_config["RS_ENDPOINT"])
print(f"--- Grand Total: {(time.time() - start_time)} sec ---")
