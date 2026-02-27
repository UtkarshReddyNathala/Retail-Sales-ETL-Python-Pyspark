"""
dimension_table_join.py
======================

Module Purpose:
This module enriches the main sales DataFrame by joining it with dimension tables:
1. Customer table
2. Store table
3. Sales team table

It also drops unnecessary columns and selects relevant sales team information.
"""

from pyspark.sql.functions import col
from src.main.utility.logging_config import logger


def dimensions_table_join(final_df_to_process,
                          customer_table_df,
                          store_table_df,
                          sales_team_table_df):
    """
    Enrich the main DataFrame by joining with customer, store, and sales team dimension tables.

    Args:
        final_df_to_process: Spark DataFrame with main sales data
        customer_table_df: Spark DataFrame with customer details
        store_table_df: Spark DataFrame with store details
        sales_team_table_df: Spark DataFrame with sales team details

    Returns:
        Spark DataFrame enriched with customer, store, and sales team info
    """

    # --------------------------------------
    # Step 1: Join with Customer Table
    # Drop unnecessary columns after the join
    # --------------------------------------
    logger.info("Joining final_df_to_process with customer_table_df")
    s3_customer_df_join = final_df_to_process.alias("s3_data") \
        .join(customer_table_df.alias("ct"),
              col("s3_data.customer_id") == col("ct.customer_id"), "inner") \
        .drop(
            "product_name",
            "price",
            "quantity",
            "additional_column",
            "s3_data.customer_id",
            "customer_joining_date"
        )

    # --------------------------------------
    # Step 2: Join with Store Table
    # Drop unnecessary store-related columns
    # --------------------------------------
    logger.info("Joining s3_customer_df_join with store_table_df")
    s3_customer_store_df_join = s3_customer_df_join.join(
        store_table_df,
        store_table_df["id"] == s3_customer_df_join["store_id"],
        "inner"
    ).drop(
        "id",
        "store_pincode",
        "store_opening_date",
        "reviews"
    )

    # --------------------------------------
    # Step 3: Join with Sales Team Table
    # Add sales team info columns and drop redundant ones
    # --------------------------------------
    logger.info("Joining s3_customer_store_df_join with sales_team_table_df")
    s3_customer_store_sales_df_join = s3_customer_store_df_join.join(
        sales_team_table_df.alias("st"),
        col("st.id") == s3_customer_store_df_join["sales_person_id"],
        "inner"
    ).withColumn("sales_person_first_name", col("st.first_name")) \
     .withColumn("sales_person_last_name", col("st.last_name")) \
     .withColumn("sales_person_address", col("st.address")) \
     .withColumn("sales_person_pincode", col("st.pincode")) \
     .drop(
         "id",
         "st.first_name",
         "st.last_name",
         "st.address",
         "st.pincode"
     )

    return s3_customer_store_sales_df_join
