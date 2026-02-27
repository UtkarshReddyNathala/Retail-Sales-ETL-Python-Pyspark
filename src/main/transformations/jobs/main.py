# ==============================================================
# Main ETL Orchestration Script
# --------------------------------------------------------------
# Executes the complete sales data pipeline:
# 1. Connects to AWS S3 securely
# 2. Downloads raw sales files
# 3. Validates schema
# 4. Enriches data using MySQL dimension tables
# 5. Creates customer & sales data marts
# 6. Uploads processed data back to S3
# 7. Updates staging status and performs cleanup
# ==============================================================

import os
import sys
import datetime

from pyspark.sql.functions import expr

from resources.dev import config
from src.main.utility.encrypt_decrypt import decrypt
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.logging_config import logger
from src.main.utility.spark_session import spark_session
from database_read import DatabaseReader
from parquet_writer import ParquetWriter
from s3_uploader import UploadToS3
from database_write import (
    customer_mart_calculation_table_write,
    sales_team_mart_calculation_table_write
)
from dimension_tables_join import dimensions_table_join
from src.main.utility.move_files import move_s3_to_s3
from src.main.utility.local_file_delete import delete_local_file
from src.main.utility.mysql_connection import get_mysql_connection

# ==============================================================
# Step 1: Create Secure S3 Client
# ==============================================================
s3_client_provider = S3ClientProvider(
    decrypt(config.aws_access_key),
    decrypt(config.aws_secret_key)
)
s3_client = s3_client_provider.get_client()

response = s3_client.list_buckets()
logger.info("List of Buckets: %s", response["Buckets"])

# ==============================================================
# Step 2: Read Available Files from S3
# ==============================================================
from src.main.utility.s3_client_object import S3Reader, S3FileDownloader

try:
    s3_reader = S3Reader()
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(
        s3_client,
        config.bucket_name,
        folder_path=folder_path
    )

    logger.info("Absolute S3 file paths: %s", s3_absolute_file_path)
    if not s3_absolute_file_path:
        raise Exception("No Data available to process")

except Exception as e:
    logger.error("Exited with error: %s", e)
    raise e

# ==============================================================
# Step 3: Download Files Locally
# ==============================================================
bucket_name = config.bucket_name
local_directory = config.local_directory
prefix = f"s3://{bucket_name}/"

file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
logger.info("Files available in bucket '%s': %s", bucket_name, file_paths)

try:
    downloader = S3FileDownloader(s3_client, bucket_name, local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error("File download error: %s", e)
    raise e

# ==============================================================
# Step 4: Filter Valid CSV Files
# ==============================================================
all_files = os.listdir(local_directory)
logger.info("Files in local directory: %s", all_files)

csv_files = []
error_files = []

for f in all_files:
    if f.endswith(".csv"):
        csv_files.append(os.path.abspath(os.path.join(local_directory, f)))
    else:
        error_files.append(os.path.abspath(os.path.join(local_directory, f)))

if not csv_files:
    raise Exception("No CSV data available to process")

logger.info("CSV files to process: %s", csv_files)

# ==============================================================
# Step 5: Initialize Spark Session
# ==============================================================
spark = spark_session()
logger.info("Spark session created successfully")

# ==============================================================
# Step 6: Schema Validation
# ==============================================================
correct_files = []
error_files = []

logger.info("Checking schema for data loaded from S3")
logger.info("Mandatory columns: %s", config.mandatory_columns)

for data in csv_files:
    data_schema = spark.read.format("csv").option("header", "true").load(data).columns
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    if missing_columns:
        error_files.append(data)
    else:
        correct_files.append(data)

logger.info("Correct files: %s", correct_files)
logger.info("Error files: %s", error_files)

# Load all correct CSVs into a single Spark DataFrame
final_df_to_process = spark.read.option("header", "true").csv(correct_files)

# ==============================================================
# Step 7: Load Dimension Tables from MySQL
# ==============================================================
database_client = DatabaseReader(config.url, config.properties)

customer_table_df = database_client.create_dataframe(spark, config.customer_table_name)
product_table_df = database_client.create_dataframe(spark, config.product_table)
product_staging_table_df = database_client.create_dataframe(spark, config.product_staging_table)
sales_team_table_df = database_client.create_dataframe(spark, config.sales_team_table)
store_table_df = database_client.create_dataframe(spark, config.store_table)

# ==============================================================
# Step 8: Join Fact with Dimension Tables
# ==============================================================
s3_customer_store_sales_df_join = dimensions_table_join(
    final_df_to_process,
    customer_table_df,
    store_table_df,
    sales_team_table_df
)
logger.info("Final enriched dataset created")

# ==============================================================
# Step 9: Customer Data Mart
# ==============================================================
final_customer_data_mart_df = s3_customer_store_sales_df_join.select(
    "ct.customer_id",
    "ct.first_name",
    "ct.last_name",
    "ct.address",
    "ct.pincode",
    "phone_number",
    "sales_date",
    "total_cost"
)

parquet_writer = ParquetWriter("overwrite", "parquet")
parquet_writer.dataframe_writer(final_customer_data_mart_df, config.customer_data_mart_local_file)

s3_uploader = UploadToS3(s3_client)
s3_uploader.upload_to_s3(config.s3_customer_datamart_directory, config.bucket_name, config.customer_data_mart_local_file)

# ==============================================================
# Step 10: Sales Team Data Mart
# ==============================================================
final_sales_team_data_mart_df = s3_customer_store_sales_df_join.select(
    "store_id",
    "sales_person_id",
    "sales_person_first_name",
    "sales_person_last_name",
    "store_manager_name",
    "manager_id",
    "is_manager",
    "sales_person_address",
    "sales_person_pincode",
    "sales_date",
    "total_cost",
    expr("SUBSTRING(sales_date, 1,7) as sales_month")
)

parquet_writer.dataframe_writer(final_sales_team_data_mart_df, config.sales_team_data_mart_local_file)
s3_uploader.upload_to_s3(config.s3_sales_datamart_directory, config.bucket_name, config.sales_team_data_mart_local_file)

# ==============================================================
# Step 11: Partitioned Write
# ==============================================================
final_sales_team_data_mart_df.write.format("parquet") \
    .mode("overwrite") \
    .partitionBy("sales_month", "store_id") \
    .option("path", config.sales_team_data_mart_partitioned_local_file) \
    .save()

# ==============================================================
# Step 12: Business Calculations
# ==============================================================
customer_mart_calculation_table_write(final_customer_data_mart_df)
sales_team_mart_calculation_table_write(final_sales_team_data_mart_df)

# ==============================================================
# Step 13: Move Processed Files & Cleanup
# ==============================================================
move_s3_to_s3(s3_client, config.bucket_name, config.s3_source_directory, config.s3_processed_directory)

delete_local_file(config.local_directory)
delete_local_file(config.customer_data_mart_local_file)
delete_local_file(config.sales_team_data_mart_local_file)
delete_local_file(config.sales_team_data_mart_partitioned_local_file)

# ==============================================================
# Step 14: Update Staging Table
# ==============================================================
db_name = config.database_name
formatted_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

update_statements = []

if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statement = (
            f"UPDATE {db_name}.{config.product_staging_table} "
            f"SET status = 'I', updated_date = '{formatted_date}' "
            f"WHERE file_name = '{filename}'"
        )
        update_statements.append(statement)

    connection = get_mysql_connection()
    cursor = connection.cursor()
    for statement in update_statements:
        cursor.execute(statement)
    connection.commit()
    cursor.close()
    connection.close()
else:
    sys.exit()

input("Press enter to terminate ")
