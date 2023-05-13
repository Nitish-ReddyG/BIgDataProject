# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def main():


    # Create a SparkSession
    spark = SparkSession.builder.appName("data_cleaning").getOrCreate()

    # Read the input data from an S3 path
    data = spark.read.csv("s3://crimes-raw/files/newyork.csv", header = True, inferSchema=True)

    # Calculate the 5% threshold for missing values
    threshold = data.count() * 0.05

    # Get the columns with missing values exceeding the threshold
    cols_to_drop = [col for col in data.columns if data.filter(data[col].isNull()).count() > threshold]

    # Drop the columns from the dataframe
    data2 = data.drop(*cols_to_drop)

    # Drop rows with missing values
    data3 = data2.na.drop()

    # Define a schema for the date and time columns
    date_schema = StructType([StructField('CMPLNT_FR_DT', DateType()), StructField('RPT_DT', DateType())])
    time_schema = StructType([StructField('CMPLNT_FR_TM', TimestampType())])

    # Convert date and time columns to datetime format using the defined schemas
    data4 = data3 \
        .withColumn('CMPLNT_FR_DT', to_date('CMPLNT_FR_DT', 'MM/dd/yyyy').cast(date_schema['CMPLNT_FR_DT'].dataType)) \
        .withColumn('CMPLNT_FR_TM', to_timestamp('CMPLNT_FR_TM', 'HH:mm:ss').cast(time_schema['CMPLNT_FR_TM'].dataType)) \
        .withColumn('RPT_DT', to_date('RPT_DT', 'MM/dd/yyyy').cast(date_schema['RPT_DT'].dataType)) \
        .select('CMPLNT_FR_DT', 'CMPLNT_FR_TM', 'RPT_DT',
                *list(set(data.columns) - set(cols_to_drop) - set(['CMPLNT_FR_DT', 'CMPLNT_FR_TM', 'RPT_DT'])))

    # Extract hour from CMPLNT_FR_TM and create new column CMPLNT_FR_TM_HOUR
    data5 = data4.withColumn('CMPLNT_FR_TM_HOUR', hour('CMPLNT_FR_TM'))


    data5 = data5.withColumn('PD_DESC', split(data3['PD_DESC'], ',').getItem(0))

    data5 = data5.drop('Lat_Lon')

    # # Save the DataFrame as csv files in S3
    data5.coalesce(1).write.mode('overwrite').option("header", "true").csv("s3://crimes-processed/files1/")




# Press the green button in the gutter to run the script.
main()
