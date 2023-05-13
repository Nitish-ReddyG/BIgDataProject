import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import SparkSession

# Set the job parameters directly
EMR_CLUSTER_MASTER = "ec2-54-195-143-56.eu-west-1.compute.amazonaws.com" # Replace with your EMR cluster's master address
HIVE_DATABASE_NAME = "hive_ny_analysis_db"
S3_CSV_FOLDER = ["s3://crimes-processed/files1/"]

# Set up the GlueContext and SparkContext objects
glueContext = GlueContext(SparkContext.getOrCreate())

# Create a new Spark session with the necessary configurations to connect to the EMR cluster
spark = (SparkSession.builder
         .appName("EMR Hive Table Creation")
         .config("spark.master", f"yarn")
         .config("spark.submit.deployMode", "client")
         .config("spark.hadoop.yarn.resourcemanager.address", f"{EMR_CLUSTER_MASTER}:8032")
         .config("spark.hadoop.fs.defaultFS", f"hdfs://{EMR_CLUSTER_MASTER}:8020")
         .config("spark.hadoop.fs.s3a.access.key", "XXXXXXXXXXXXXXXX") # Replace with your AWS Access Key
         .config("spark.hadoop.fs.s3a.secret.key", "XXXXXXXXXXXXXXXX") # Replace with your AWS Secret Key
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .getOrCreate())
         
# Create a new Hive database if it doesn't already exist
hive_database_name = HIVE_DATABASE_NAME
spark.sql(f"DROP DATABASE IF EXISTS {hive_database_name}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {hive_database_name}")
spark.sql(f"USE {hive_database_name}")

# Define the column definitions, replace these with your actual column names and data types
# Define the schema for the Hive table
column_definitions = """
    CMPLNT_NUM STRING,
    CMPLNT_FR_DT STRING,
    CMPLNT_FR_TM STRING,
    ADDR_PCT_CD STRING,
    RPT_DT STRING,
    KY_CD STRING,
    OFNS_DESC STRING,
    PD_CD STRING,
    PD_DESC STRING,
    CRM_ATPT_CPTD_CD STRING,
    LAW_CAT_CD STRING,
    BORO_NM STRING,
    PREM_TYP_DESC STRING,
    JURIS_DESC STRING,
    JURISDICTION_CODE STRING,
    X_COORD_CD STRING,
    Y_COORD_CD STRING,
    Latitude STRING,
    Longitude STRING,
    PATROL_BORO STRING,
    VIC_RACE STRING,
    VIC_SEX STRING,
    CMPLNT_FR_TM_HOUR STRING
"""

# Create the Hive table if it doesn't exist
spark.sql(f"CREATE TABLE IF NOT EXISTS ny_table ({column_definitions})")

# Read data from the S3 input path and convert it to a DynamicFrame
input_dyf = glueContext.create_dynamic_frame_from_options(
    connection_type='s3',
    connection_options={'paths': S3_CSV_FOLDER},
    format='csv',
    format_options={'separator': ',', 'quoteChar': '"', 'header': True, 'inferSchema': True}
)

# Convert the DynamicFrame to a DataFrame
input_df = input_dyf.toDF()

# Write the DataFrame to the Hive table
input_df.write.mode("append").insertInto("ny_table")

