"""
aws_file_download.py
====================

Module Purpose:
This module provides a class to download files from an AWS S3 bucket
to a local directory. Useful in ETL pipelines for:
1. Fetching raw data files from S3
2. Preparing local data for processing
3. Automating file retrieval in batch jobs
"""

import boto3
import traceback
import os
from src.main.utility.logging_config import logger


class S3FileDownloader:
    """
    Class to handle downloading files from an S3 bucket to local storage.
    """

    def __init__(self, s3_client, bucket_name: str, local_directory: str):
        """
        Initialize the downloader with S3 client, bucket name, and local directory.

        Args:
            s3_client: boto3 S3 client object
            bucket_name (str): Name of the S3 bucket
            local_directory (str): Path to local directory to store files
        """
        self.bucket_name = bucket_name
        self.local_directory = local_directory
        self.s3_client = s3_client

    def download_files(self, list_files: list):
        """
        Download all files from the list of S3 keys to the local directory.

        Args:
            list_files (list): List of S3 keys (file paths) to download

        Raises:
            Exception: Raises exception if download fails
        """
        logger.info("Running download for these files: %s", list_files)

        for key in list_files:
            file_name = os.path.basename(key)  # Extract file name from S3 key
            logger.info("Downloading file: %s", file_name)

            download_file_path = os.path.join(self.local_directory, file_name)

            try:
                self.s3_client.download_file(self.bucket_name, key, download_file_path)
            except Exception as e:
                error_message = f"Error downloading file '{key}': {str(e)}"
                traceback_message = traceback.format_exc()
                print(error_message)
                print(traceback_message)
                raise e
