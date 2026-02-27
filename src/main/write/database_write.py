"""
database_writer.py
==================

Database Writer Utility

Purpose:
Provides a method to write Spark DataFrames into MySQL tables.
Used across the ETL pipeline for persisting processed data.

Usage:
    db_writer = DatabaseWriter(url=config.url, properties=config.properties)
    db_writer.write_dataframe(df, "table_name")
"""

from src.main.utility.logging_config import logger


class DatabaseWriter:
    def __init__(self, url, properties):
        """
        Initialize DatabaseWriter with JDBC URL and connection properties.

        Args:
            url (str): JDBC connection URL
            properties (dict): Dictionary with connection properties (user, password, driver)
        """
        self.url = url
        self.properties = properties

    def write_dataframe(self, df, table_name):
        """
        Write a Spark DataFrame into a specified MySQL table using JDBC.

        Args:
            df (DataFrame): Spark DataFrame to write
            table_name (str): Target table name in MySQL
        """
        try:
            print("inside write_dataframe")
            df.write.jdbc(
                url=self.url,
                table=table_name,
                mode="append",  # Append mode to avoid overwriting existing data
                properties=self.properties
            )
            logger.info(f"Data successfully written into {table_name} table")

        except Exception as e:
            logger.error(f"Error writing to {table_name}: {e}")
            raise e
