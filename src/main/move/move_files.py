"""
move_files.py
=============

Module Purpose:
This module provides functions to move files either within S3 buckets
or between local directories (future extensibility).

Currently implemented:
1. move_s3_to_s3: Moves files from one S3 prefix to another within the same bucket.
"""

import traceback
from src.main.utility.logging_config import logger


def move_s3_to_s3(s3_client, bucket_name, source_prefix, destination_prefix, file_name=None):
    """
    Move files from one S3 folder (prefix) to another within the same bucket.

    Args:
        s3_client: boto3 S3 client object
        bucket_name (str): Name of the S3 bucket
        source_prefix (str): Source folder/prefix in S3
        destination_prefix (str): Destination folder/prefix in S3
        file_name (str, optional): Specific file to move. If None, move all files in the source_prefix.

    Returns:
        str: Success message

    Raises:
        Exception: Raises exception if move fails
    """
    try:
        # List all objects in the source prefix
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix)

        if file_name is None:
            # Move all files from source_prefix to destination_prefix
            for obj in response.get('Contents', []):
                source_key = obj['Key']
                destination_key = destination_prefix + source_key[len(source_prefix):]

                # Copy file to destination
                s3_client.copy_object(
                    Bucket=bucket_name,
                    CopySource={'Bucket': bucket_name, 'Key': source_key},
                    Key=destination_key
                )

                # Delete original file
                s3_client.delete_object(Bucket=bucket_name, Key=source_key)

        else:
            # Move only the file that matches file_name
            for obj in response.get('Contents', []):
                source_key = obj['Key']

                if source_key.endswith(file_name):
                    destination_key = destination_prefix + source_key[len(source_prefix):]

                    s3_client.copy_object(
                        Bucket=bucket_name,
                        CopySource={'Bucket': bucket_name, 'Key': source_key},
                        Key=destination_key
                    )

                    s3_client.delete_object(Bucket=bucket_name, Key=source_key)
                    logger.info(f"Moved file: {source_key} to {destination_key}")

        return f"Data moved successfully from {source_prefix} to {destination_prefix}"

    except Exception as e:
        logger.error(f"Error moving file: {str(e)}")
        traceback_message = traceback.format_exc()
        print(traceback_message)
        raise e


def move_local_to_local():
    """
    Placeholder for future functionality to move files between local directories.
    """
    pass
