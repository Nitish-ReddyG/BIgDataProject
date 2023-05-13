import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def main():

    # Create a SparkSession
    spark = SparkSession.builder.appName("Data Processing Job").getOrCreate()

    # Read the CSV file from S3 into a DataFrame
    data = spark.read.csv("s3://crime-data-raw/crime-city-files/Chicago_Crimes.csv", header = True, inferSchema=True)

    # Check the percentage of missing values in each column
    missing_percentages = data.select([(count(when(col(c).isNull(), c)) / count(c)).alias(c) for c in data.columns])

    # Drop columns with more than 5% missing values
    data2 = data.select([c for c in data.columns if missing_percentages.select(c).collect()[0][0] < 0.05])

    # Drop rows with missing values
    data3 = data2.dropna()

    # Convert the 'Date' column to a DateType object and extract the year
    data3 = data3.withColumn("Date", data3["Date"].cast(DateType())) \
                 .withColumn("Year_from_Date", year("Date"))

    # Convert the 'Date' column to a datetime object
    data3 = data3.withColumn('Date', to_date(col('Date'), 'MM/dd/yyyy hh:mm:ss a'))

    # Create a new column for the hour of the day
    data3 = data3.withColumn('Hour', hour(col('Date')))

    # Create a new column for the day of the week
    data3 = data3.withColumn('Day_of_Week', dayofweek(col('Date')))

    # Extract only the date from the 'Date' column
    data3 = data3.withColumn('Date', col('Date').cast('date'))

    # Save the DataFrame as csv files in S3
    data3.coalesce(1).write.mode('overwrite').option("header", "true").csv("s3://crime-data-processed/chicago-processed/")

# Call the main function
main()
