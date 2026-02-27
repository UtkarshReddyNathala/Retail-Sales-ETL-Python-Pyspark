"""
sql_session.py
===============

MySQL Connection Utility

Purpose:
Creates and returns a MySQL database connection.
Used across the project for read, write, and delete operations.
"""

import mysql.connector
from src.main.utility.logging_config import logger


def get_mysql_connection():
    """
    Establish and return a MySQL connection.

    Note:
    - Replace placeholder values with actual credentials
      in dev/qa/prod config files.
    - Ensure MySQL server is running and accessible.
    
    Returns:
        connection (mysql.connector.connection.MySQLConnection): Active MySQL connection object
    """

    try:
        connection = mysql.connector.connect(
            host="localhost",       # MySQL server host
            user="root",            # MySQL username
            password="password",    # MySQL password
            database="retail_sales_db"   # Database name
        )

        if connection.is_connected():
            logger.info("Connected to MySQL database successfully.")

        return connection

    except Exception as e:
        logger.error(f"Error connecting to MySQL: {str(e)}")
        raise e
