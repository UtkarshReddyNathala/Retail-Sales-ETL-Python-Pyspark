"""
upload_to_s3.py
===============

Module Purpose:
This module provides functionality to upload local files or directories to an S3 bucket.
Each upload is organized under a timestamped prefix to avoid overwriting previous uploads.
"""

from src.main.utility.logging_config import logger
import traceback
import datetime
import os


class UploadToS3:
    """
    Handles uploading files or directories to an S3 bucket.
    """

    def __init__(self, s3_client):
        """
        Initialize with an active S3 client.

        Args:
            s3_client: boto3 S3 client object
        """
        self.s3_client = s3_client

    def upload_to_s3(self, s3_directory, s3_bucket, local_file_path):
        """
        Upload all files from a local directory to a specified S3 bucket and folder.

        Args:
            s3_directory: S3 folder/prefix where files will be uploaded
            s3_bucket: Name of the target S3 bucket
            local_file_path: Path to the local directory containing files

        Returns:
            Success message string
        """

        # Generate a timestamped prefix to avoid overwriting existing files
        current_epoch = int(datetime.datetime.now().timestamp()) * 1000
        s3_prefix = f"{s3_directory}/{current_epoch}/"

        try:
            # Walk through all files in the local directory
            for root, dirs, files in os.walk(local_file_path):
                for file in files:
                    file_full_path = os.path.join(root, file)  # Full path to local file
                    s3_key = f"{s3_prefix}/{file}"            # S3 key including prefix

                    # Upload file to S3
                    self.s3_client.upload_file(file_full_path, s3_bucket, s3_key)
                    logger.info(f"Uploaded file: {file_full_path} → s3://{s3_bucket}/{s3_key}")

            return f"Data successfully uploaded to {s3_directory} data mart."

        except Exception as e:
            logger.error(f"Error uploading file: {str(e)}")
            print(traceback.format_exc())
            raise e
