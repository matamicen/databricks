# Databricks notebook source
# MAGIC %sql
# MAGIC     CREATE TABLE IF NOT EXISTS DimCustomerTable2
# MAGIC     (
# MAGIC     surrogateKey BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     CustomerID INT,
# MAGIC     CustomerName STRING,
# MAGIC     City STRING,
# MAGIC     State STRING,
# MAGIC     Country STRING,
# MAGIC     startDate DATE,
# MAGIC     endDate DATE,
# MAGIC     active BOOLEAN
# MAGIC     )
# MAGIC     LOCATION 'dbfs:/mnt/skynet/output/adb_customer_delta2';

# COMMAND ----------

from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from pyspark.sql import functions as F

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Delta Table Merge with CSV input") \
    .getOrCreate()

# Define the paths for the Delta tables and CSV input
delta_table_people_path = "dbfs:/mnt/skynet/output/adb_customer_delta2"
input_csv_path = 'dbfs:/mnt/skynetdev/sales_star_schema/customer2.csv'

# Read the CSV file into a DataFrame
df_updates = spark.read.csv(input_csv_path, header=True, inferSchema=True)


# Initialize DeltaTable instance or create it if it doesn't exist
#if DeltaTable.isDeltaTable(spark, delta_table_people_path):
delta_table_people = DeltaTable.forPath(spark, delta_table_people_path)


# Merge the data from the CSV DataFrame (df_updates) into the Delta table (delta_table_people)
delta_table_people.alias('people') \
    .merge(
        df_updates.alias('updates'),
        'people.CustomerID = updates.CustomerID'
    ) \
    .whenMatchedUpdate(set=
        {
            "endDate": current_date(),  # Set the endDate to null on update
            "active" : "false"
        }
    ) \
    .whenNotMatchedInsert(values=
        {
            "CustomerID": "updates.CustomerID",
            "CustomerName": "updates.CustomerName",
            "City": "updates.City",
            "State": "updates.State",
            "Country": "updates.Country",
            "startDate": current_date(),  # Set the startDate to the current date on insert
            "endDate": F.lit("9999-12-31"),  # Set the endDate to null on insert
            "active" : "true"
        }
    ) \
    .execute()

