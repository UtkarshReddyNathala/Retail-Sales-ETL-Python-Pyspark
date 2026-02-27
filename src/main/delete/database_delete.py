"""
database_delete.py
==================

Database Delete Operations Module

Purpose:
This module provides basic delete operations for MySQL tables.

Why this module exists:
In ETL pipelines, sometimes we need to:
1. Delete old records before reprocessing data
2. Clean staging tables before loading fresh data
3. Remove corrupted or duplicate records
4. Perform incremental load cleanup

Currently, this module is kept for future extensibility.
It can be used when delete operations are required in the pipeline.
"""

from src.main.utility.logging_config import logger


class DatabaseDelete:
    """
    Provides methods to delete records or truncate tables in a MySQL database.
    """

    def __init__(self, connection):
        """
        Initialize with an active MySQL connection.

        :param connection: MySQL connection object
        """
        self.connection = connection
        logger.info("DatabaseDelete initialized.")

    def delete_records(self, table_name: str, condition: str):
        """
        Delete records from a table based on a condition.

        Example Usage:
            delete_records("sales_table", "sales_date < '2024-01-01'")

        :param table_name: Name of the table
        :param condition: SQL condition (WITHOUT 'WHERE')
        """
        try:
            cursor = self.connection.cursor()

            query = f"DELETE FROM {table_name} WHERE {condition}"
            logger.info(f"Executing delete query: {query}")

            cursor.execute(query)
            self.connection.commit()

            logger.info(f"Records deleted successfully from {table_name}")

        except Exception as e:
            logger.error(f"Error while deleting from {table_name}: {str(e)}")
            self.connection.rollback()
            raise e

        finally:
            cursor.close()

    def truncate_table(self, table_name: str):
        """
        Truncate entire table.

        When to use:
        - When refreshing staging tables
        - When doing full reload of a table
        - When clearing temporary data

        Example Usage:
            truncate_table("staging_sales_table")

        :param table_name: Name of the table
        """
        try:
            cursor = self.connection.cursor()

            query = f"TRUNCATE TABLE {table_name}"
            logger.info(f"Executing truncate query: {query}")

            cursor.execute(query)
            self.connection.commit()

            logger.info(f"Table {table_name} truncated successfully")

        except Exception as e:
            logger.error(f"Error while truncating {table_name}: {str(e)}")
            self.connection.rollback()
            raise e

        finally:
            cursor.close()
