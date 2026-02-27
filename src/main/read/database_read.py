"""
database_read.py
================

Module Purpose:
This module provides functionality to read data from MySQL tables
into Spark DataFrames for ETL and analytics purposes.
"""

class DatabaseReader:
    """
    DatabaseReader handles reading tables from a relational database
    into Spark DataFrames.
    """

    def __init__(self, url: str, properties: dict):
        """
        Initialize DatabaseReader with database connection details.

        Args:
            url (str): JDBC URL for the database connection
            properties (dict): Dictionary containing user, password, and driver
        """
        self.url = url
        self.properties = properties

    def create_dataframe(self, spark, table_name: str):
        """
        Read a table from the database and return it as a Spark DataFrame.

        Args:
            spark: SparkSession object
            table_name (str): Name of the table to read

        Returns:
            DataFrame: Spark DataFrame containing the table data
        """
        df = spark.read.jdbc(
            url=self.url,
            table=table_name,
            properties=self.properties
        )
        return df
