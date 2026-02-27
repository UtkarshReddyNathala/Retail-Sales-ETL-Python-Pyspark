"""
aws_read.py
===========

Module Purpose:
This module provides functionality to read/list files from an S3 bucket.
It is primarily used in ETL pipelines to discover available files in S3 folders.
"""

import boto3
import traceback
from src.main.utility.logging_config import logger


class S3Reader:
    """
    S3Reader class provides methods to list files in S3 buckets.
    """

    def list_files(self, s3_client, bucket_name: str, folder_path: str) -> list:
        """
        List all files in a specific S3 folder (excluding directories).

        Args:
            s3_client: boto3 S3 client object
            bucket_name (str): Name of the S3 bucket
            folder_path (str): Prefix/folder path in the bucket

        Returns:
            list: List of S3 file paths

        Raises:
            Exception: Raises exception if listing fails
        """
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)

            if 'Contents' in response:
                files = [
                    f"s3://{bucket_name}/{obj['Key']}"
                    for obj in response['Contents']
                    if not obj['Key'].endswith('/')
                ]
                logger.info(
                    "Total files available in folder '%s' of bucket '%s': %d",
                    folder_path, bucket_name, len(files)
                )
                return files

            # Return empty list if folder has no files
            return []

        except Exception as e:
            error_message = f"Error listing files: {e}"
            traceback_message = traceback.format_exc()
            logger.error("Got this error: %s", error_message)
            print(traceback_message)
            raise

    ################### Future Extension: list all files in bucket ###########
    # def list_files(self, bucket_name):
    #     """
    #     List all files in an S3 bucket without folder filtering.
    #     """
    #     try:
    #         response = self.s3_client.list_objects_v2(Bucket=bucket_name)
    #         if 'Contents' in response:
    #             files = [f"s3://{bucket_name}/{obj['Key']}" for obj in response['Contents']]
    #             return files
    #         else:
    #             return []
    #     except Exception as e:
    #         print(f"Error listing files: {e}")
    #         return []
