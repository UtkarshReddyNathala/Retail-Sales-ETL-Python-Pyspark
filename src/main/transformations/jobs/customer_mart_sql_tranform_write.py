"""
customer_mart_sql_transform_write.py
====================================

Module Purpose:
This module calculates monthly total purchases for each customer
and writes the results into the MySQL customers_data_mart table.
"""

from pyspark.sql.functions import col, concat, lit, substring, sum
from pyspark.sql.window import Window
from resources.dev import config
from src.main.write.database_write import DatabaseWriter


def customer_mart_calculation_table_write(final_customer_data_mart_df):
    """
    Calculate monthly total purchases for each customer
    and write the results to the customers_data_mart MySQL table.

    Args:
        final_customer_data_mart_df: Spark DataFrame containing raw customer purchase data
    """

    # --------------------------------------
    # Step 1: Define window for monthly aggregation
    # --------------------------------------
    window = Window.partitionBy(
        "customer_id",
        "sales_date_month"
    )

    # --------------------------------------
    # Step 2: Aggregate total sales per customer per month
    # --------------------------------------
    final_customer_data_mart = (
        final_customer_data_mart_df
        # Extract month from sales_date (YYYY-MM)
        .withColumn(
            "sales_date_month",
            substring(col("sales_date"), 1, 7)
        )
        # Calculate total purchase per customer per month
        .withColumn(
            "total_sales_every_month_by_each_customer",
            sum("total_cost").over(window)
        )
        # Select relevant columns and concatenate full name
        .select(
            "customer_id",
            concat(col("first_name"), lit(" "), col("last_name")).alias("full_name"),
            "address",
            "phone_number",
            "sales_date_month",
            col("total_sales_every_month_by_each_customer").alias("total_sales")
        )
        .distinct()
    )

    # --------------------------------------
    # Step 3: Write final DataFrame to MySQL
    # --------------------------------------
    db_writer = DatabaseWriter(
        url=config.url,
        properties=config.properties
    )

    db_writer.write_dataframe(
        final_customer_data_mart,
        config.customer_data_mart_table
    )
