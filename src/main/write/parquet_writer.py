"""
parquet_writer.py
=================

Parquet Writer Utility

Purpose:
Provides a method to write Spark DataFrames into Parquet files.
Used in ETL pipelines for persisting processed data in Parquet format.

Usage:
    parquet_writer = ParquetWriter(mode="overwrite", data_format="parquet")
    parquet_writer.dataframe_writer(df, "C:/data/output/")
"""

import traceback
from src.main.utility.logging_config import logger


class ParquetWriter:
    def __init__(self, mode, data_format):
        """
        Initialize ParquetWriter with mode and file format.

        Args:
            mode (str): Write mode, e.g., "overwrite" or "append"
            data_format (str): File format, e.g., "parquet"
        """
        self.mode = mode
        self.data_format = data_format

    def dataframe_writer(self, df, file_path):
        """
        Write a Spark DataFrame to the specified file path in the given format.

        Args:
            df (DataFrame): Spark DataFrame to write
            file_path (str): Destination file path
        """
        try:
            df.write.format(self.data_format) \
                .option("header", "true") \
                .mode(self.mode) \
                .option("path", file_path) \
                .save()

            logger.info(f"Data successfully written to {file_path} in {self.data_format} format")

        except Exception as e:
            logger.error(f"Error writing the data: {str(e)}")
            traceback_message = traceback.format_exc()
            print(traceback_message)
            raise e
