# scratch_pad.py
"""
S3 File Listing Utility

Purpose:
This script provides a simple utility to list all files in a given S3 bucket folder.
It excludes directories and only lists actual files. Useful for ETL pipelines where
we need to fetch or process files from S3.

Example Usage:
    python scratch_pad.py
"""

import boto3
import traceback
from src.main.utility.logging_config import logger
from resources.dev import config
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.encrypt_decrypt import decrypt

# -------------------------------
# Initialize S3 client
# -------------------------------
# Use decrypted AWS credentials to create an S3 client
s3_client_provider = S3ClientProvider(
    decrypt(config.aws_access_key),
    decrypt(config.aws_secret_key)
)
s3_client = s3_client_provider.get_client()


def list_s3_files(s3_client, bucket_name: str, folder_path: str) -> list:
    """
    List all files in a specific S3 bucket folder (excluding directories).

    Args:
        s3_client: boto3 S3 client
        bucket_name: Name of the S3 bucket
        folder_path: S3 folder path (prefix)

    Returns:
        List[str]: List of S3 file paths
    """
    try:
        # Fetch all objects with the given prefix
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
        
        if 'Contents' in response:
            # Filter out directories and keep only files
            files = [
                f"s3://{bucket_name}/{obj['Key']}"
                for obj in response['Contents']
                if not obj['Key'].endswith('/')
            ]
            logger.info(
                "Total files in '%s' of bucket '%s': %d",
                folder_path,
                bucket_name,
                len(files)
            )
            return files
        
        # Return empty list if no files found
        return []

    except Exception as e:
        logger.error("Error listing files in '%s': %s", folder_path, str(e))
        traceback_message = traceback.format_exc()
        print(traceback_message)
        raise


# -------------------------------
# Example usage
# -------------------------------
if __name__ == "__main__":
    folder_path = "sales_data/"
    s3_files = list_s3_files(s3_client, config.bucket_name, folder_path)
    logger.info("Files available on S3: %s", s3_files)
