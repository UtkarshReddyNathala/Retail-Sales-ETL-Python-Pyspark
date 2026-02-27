# sales_data_uploads_s3.py
"""
Upload Local Sales Data to S3

Purpose:
Uploads all files from a local directory to a specified S3 bucket and directory.
Useful for ETL pipelines to push sales data to the cloud for processing.

Example Usage:
    python sales_data_uploads_s3.py
"""

import os
from resources.dev import config
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.encrypt_decrypt import decrypt
from src.main.utility.logging_config import logger

# -------------------------------
# Initialize S3 client
# -------------------------------
s3_client_provider = S3ClientProvider(
    decrypt(config.aws_access_key),
    decrypt(config.aws_secret_key)
)
s3_client = s3_client_provider.get_client()

# -------------------------------
# Local data path
# -------------------------------
local_base_path = "C:\\Users\\Utkarsh\\Documents\\data_engineering\\spark_data\\sales_data_to_s3\\"


def upload_to_s3(s3_directory: str, s3_bucket: str, local_base_path: str):
    """
    Upload all files from local_base_path to the given S3 bucket and directory.

    Args:
        s3_directory: Target folder/prefix in S3 bucket
        s3_bucket: S3 bucket name
        local_base_path: Local directory containing files to upload

    Returns:
        str: Success message with bucket and folder info
    """
    s3_prefix = s3_directory.rstrip("/")  # remove trailing slash if any

    try:
        # Traverse all files in local directory
        for root, dirs, files in os.walk(local_base_path):
            for file in files:
                file_full_path = os.path.join(root, file)
                s3_key = f"{s3_prefix}/{file}"  # full path in S3

                # Upload file to S3
                s3_client.upload_file(file_full_path, s3_bucket, s3_key)
                logger.info(f"Uploaded {file_full_path} → s3://{s3_bucket}/{s3_key}")

        return f"Files uploaded successfully to s3://{s3_bucket}/{s3_prefix}"

    except Exception as e:
        logger.error(f"Error uploading files: {str(e)}")
        raise e


# -------------------------------
# Example Usage
# -------------------------------
if __name__ == "__main__":
    s3_directory = "sales_data"
    s3_bucket = "retail-sales-data-bucket"

    result = upload_to_s3(s3_directory, s3_bucket, local_base_path)
    logger.info(result)
