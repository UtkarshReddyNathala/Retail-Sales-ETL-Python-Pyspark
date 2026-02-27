"""
sales_mart_sql_transform_write.py
=================================

Module Purpose:
This module performs the transformation and calculation for the sales team
data mart, including monthly total sales, ranking, and incentives. 
Finally, it writes the processed data into the MySQL sales_team_data_mart table.
"""

from pyspark.sql.functions import (
    col,
    substring,
    sum,
    concat,
    lit,
    rank,
    when,
    round
)
from pyspark.sql.window import Window

from resources.dev import config
from src.main.write.database_write import DatabaseWriter


def sales_mart_calculation_table_write(final_sales_team_data_mart_df):
    """
    Calculate monthly sales, rank salespersons, calculate incentives,
    and write the results to the sales_team_data_mart MySQL table.

    Args:
        final_sales_team_data_mart_df: Spark DataFrame containing raw sales team data
    """

    # --------------------------------------
    # Step 1: Monthly total sales per salesperson
    # --------------------------------------
    window = Window.partitionBy(
        "store_id",
        "sales_person_id",
        "sales_month"
    )

    final_sales_team_data_mart = (
        final_sales_team_data_mart_df
        # Extract month from sales_date (YYYY-MM)
        .withColumn(
            "sales_month",
            substring(col("sales_date"), 1, 7)
        )
        # Calculate total sales for the month per salesperson
        .withColumn(
            "total_sales_every_month",
            sum(col("total_cost")).over(window)
        )
        # Select relevant columns with full_name concatenation
        .select(
            "store_id",
            "sales_person_id",
            concat(
                col("sales_person_first_name"),
                lit(" "),
                col("sales_person_last_name")
            ).alias("full_name"),
            "sales_month",
            "total_sales_every_month"
        )
        .distinct()
    )

    # --------------------------------------
    # Step 2: Ranking salespersons within store & month
    # --------------------------------------
    rank_window = Window.partitionBy(
        "store_id",
        "sales_month"
    ).orderBy(
        col("total_sales_every_month").desc()
    )

    final_sales_team_data_mart_table = (
        final_sales_team_data_mart
        # Rank salespersons
        .withColumn(
            "rnk",
            rank().over(rank_window)
        )
        # Calculate incentive: 1% for top-ranked salesperson
        .withColumn(
            "incentive",
            when(
                col("rnk") == 1,
                col("total_sales_every_month") * 0.01
            ).otherwise(0)
        )
        .withColumn(
            "incentive",
            round(col("incentive"), 2)
        )
        # Copy total_sales column
        .withColumn(
            "total_sales",
            col("total_sales_every_month")
        )
        # Select final columns for MySQL write
        .select(
            "store_id",
            "sales_person_id",
            "full_name",
            "sales_month",
            "total_sales",
            "incentive"
        )
    )

    # --------------------------------------
    # Step 3: Write final DataFrame to MySQL
    # --------------------------------------
    print("Writing the data into sales_team_data_mart table...")

    db_writer = DatabaseWriter(
        url=config.url,
        properties=config.properties
    )

    db_writer.write_dataframe(
        final_sales_team_data_mart_table,
        config.sales_team_data_mart_table
    )
