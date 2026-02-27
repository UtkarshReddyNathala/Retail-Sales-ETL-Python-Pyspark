"""
spark_session.py
================

Spark Session Utility

Purpose:
Initializes and returns a SparkSession for local ETL processing.
Includes imports and configurations for MySQL connector usage.

Usage:
    spark = spark_session()
"""

import findspark
findspark.init()  # Initialize findspark to locate Spark installation

from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from src.main.utility.logging_config import logger


def spark_session():
    """
    Create and return a SparkSession configured for local ETL tasks.

    Returns:
        SparkSession: Configured SparkSession instance
    """
    spark = (
        SparkSession.builder
        .master("local[*]")  # Use all local cores
        .appName("retail_sales_etl")  # Name of the Spark application
        .config(
            "spark.driver.extraClassPath",
            "C:\\my_sql_jar\\mysql-connector-java-8.0.26.jar"  # MySQL connector path
        )
        .getOrCreate()
    )

    logger.info("Spark session initialized: %s", spark)
    return spark
