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
