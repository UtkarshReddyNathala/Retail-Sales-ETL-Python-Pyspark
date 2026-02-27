import os

# -------------------------------
# Encryption Settings
# -------------------------------
# These are used to encrypt/decrypt sensitive data like AWS credentials or files
key = "retail_sales_key"
iv = "retail_sales_iv"
salt = "retail_sales_salt"

# -------------------------------
# AWS Credentials
# -------------------------------
# These are placeholders; in real projects, use encrypted secrets or environment variables
aws_access_key = "AWS_ENCRYPTED_ACCESS_KEY"
aws_secret_key = "AWS_ENCRYPTED_SECRET_KEY"

# S3 Bucket name for storing files
bucket_name = "retail-sales-data-bucket"

# S3 directories for different data categories
s3_customer_datamart_directory = "customer_data_mart"  # Customer data mart files
s3_sales_datamart_directory = "sales_data_mart"        # Sales data mart files
s3_source_directory = "sales_data/"                    # Raw source sales data
s3_error_directory = "sales_data_error/"              # Files with errors
s3_processed_directory = "sales_data_processed/"      # Processed files

# -------------------------------
# Database Configuration
# -------------------------------
# MySQL database configuration for connecting from Spark or Python
database_name = "retail_sales_db"
url = f"jdbc:mysql://localhost:3306/{database_name}"

properties = {
    "user": "MYSQL_USER",                    # Database username
    "password": "MYSQL_PASSWORD",            # Database password
    "driver": "com.mysql.cj.jdbc.Driver"     # JDBC driver for MySQL
}

# -------------------------------
# Table Names
# -------------------------------
# Core tables in the database
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

# Data Mart Tables
customer_data_mart_table = "customer_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# -------------------------------
# Mandatory Columns
# -------------------------------
# Columns expected in the input files to ensure schema consistency
mandatory_columns = [
    "customer_id",
    "store_id",
    "product_name",
    "sales_date",
    "sales_person_id",
    "price",
    "quantity",
    "total_cost"
]

# -------------------------------
# Local File Paths
# -------------------------------
# Base path where all local files are stored
base_local_path = "C:\\data_engineering\\spark_data\\"

# Specific directories for different stages of processing
local_directory = os.path.join(base_local_path, "file_from_s3")                       # Raw files from S3
customer_data_mart_local_file = os.path.join(base_local_path, "customer_data_mart")   # Customer data mart files
sales_team_data_mart_local_file = os.path.join(base_local_path, "sales_team_data_mart")  # Sales team data mart files
sales_team_data_mart_partitioned_local_file = os.path.join(base_local_path, "sales_partition_data")  # Partitioned sales data
error_folder_path_local = os.path.join(base_local_path, "error_files")               # Files that failed processing
