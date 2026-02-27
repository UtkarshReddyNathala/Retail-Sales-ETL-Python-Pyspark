"""
local_file_delete.py
====================

Module Purpose:
This module provides a function to delete all files and folders 
inside a specified local directory. Useful in ETL pipelines for:
1. Cleaning up temporary directories
2. Removing processed or error files
3. Resetting folders before re-running data processing
"""

import os
import shutil
import traceback
from src.main.utility.logging_config import logger


def delete_local_file(delete_file_path: str):
    """
    Delete all files and folders inside the given local directory.

    Args:
        delete_file_path (str): Path to the directory to be cleaned.

    Raises:
        Exception: Raises the exception if deletion fails.
    """
    try:
        # List all files and directories inside the target folder
        files_to_delete = [
            os.path.join(delete_file_path, filename)
            for filename in os.listdir(delete_file_path)
        ]

        # Iterate and delete each file or folder
        for item in files_to_delete:
            if os.path.isfile(item):
                os.remove(item)
                print(f"Deleted file: {item}")
            elif os.path.isdir(item):
                shutil.rmtree(item)
                print(f"Deleted folder: {item}")

        logger.info(f"All files and folders deleted from: {delete_file_path}")

    except Exception as e:
        # Log the error and raise exception for upstream handling
        logger.error(f"Error deleting local files: {str(e)}")
        traceback_message = traceback.format_exc()
        print(traceback_message)
        raise e
