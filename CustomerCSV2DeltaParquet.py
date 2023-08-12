# Databricks notebook source
# Define the mount point you want to check
mount_point_to_check = '/mnt/skynetdev'

# Get the list of currently mounted file systems
mounted_file_systems = dbutils.fs.mounts()

# Check if the mount point already exists in the mounted file systems
is_mounted = any(mount_point_to_check == mount_info.mountPoint for mount_info in mounted_file_systems)

# Perform the mount only if it's not already mounted
if not is_mounted:
    dbutils.fs.mount(
        source='wasbs://input@skynetdev.blob.core.windows.net',
        mount_point='/mnt/skynetdev',
        extra_configs={'fs.azure.account.key.skynetdev.blob.core.windows.net':'Fxcl6jPU7hOWgCToYUH4sA2RFUqJJ7rHHx9rA7euvHDxNgCeyfxHjRBxdRRIAwPktQlepHw0/zm7+AStZ/7g3A=='}
    )
else:
    print("The mount point is already mounted.")

# COMMAND ----------

df = spark.read.format("csv").option("header", "true").load("dbfs:/mnt/skynetdev/sales_star_schema/customer.csv")
df.show()


# COMMAND ----------

# Define the mount point you want to check
mount_point_to_check = '/mnt/skynet/output'

# Get the list of currently mounted file systems
mounted_file_systems = dbutils.fs.mounts()

# Check if the mount point already exists in the mounted file systems
is_mounted = any(mount_point_to_check == mount_info.mountPoint for mount_info in mounted_file_systems)

if not is_mounted:
    # Define the client secret as a Databricks secret (Optional: if you are using a secret)
    client_secret = dbutils.secrets.get(scope="enter-your-key-vault-secret-scope-name-here", key="enter-the-secret")

    # Define the configurations for the mount
    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": "d32332e2-ee48-4254-95b9-eb9d7ead2b1c",
        "fs.azure.account.oauth2.client.secret": client_secret,  # Use the Databricks secret here
        "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/26c2ce68-82a1-455e-981c-ef9bd4dffc2f/oauth2/token"
    }

    # Mount the ADLS Gen2 container
    dbutils.fs.mount(
        source="abfss://output@skynetdev.dfs.core.windows.net",
        mount_point="/mnt/skynet/output",
        extra_configs=configs
    )
else:
    print("The mount point is already mounted.")


# COMMAND ----------

# from pyspark.sql import SparkSession
# from delta import *

# # Initialize SparkSession
# spark = SparkSession.builder \
#     .appName("Save CSV to Delta Parquet") \
#     .getOrCreate()

# # Load the customer.csv file
# input_path = "dbfs:/mnt/skynetdev/sales_star_schema/customer.csv"
# df = spark.read.format("csv").option("header", "true").load(input_path)

# # Show the loaded data (optional)
# df.show()

# # Save the DataFrame in Delta Parquet format
# output_path = "dbfs:/mnt/skynet/output/adb_customer"
# df.write.format("delta").mode("overwrite").save(output_path)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import *

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Save CSV to Delta Parquet with SCD Type 2") \
    .getOrCreate()

# Load the customer.csv file
input_path = "dbfs:/mnt/skynetdev/sales_star_schema/customer.csv"
df = spark.read.format("csv").option("header", "true").load(input_path)

# Show the loaded data (optional)
df.show()

# Define the output path for Delta Lake
output_path = "dbfs:/mnt/skynet/output/adb_customer_delta"

# Check if the Delta table already exists
if DeltaTable.isDeltaTable(spark, output_path):
    # If the Delta table exists, read the current data from the table
    delta_table = DeltaTable.forPath(spark, output_path)
    current_data = delta_table.toDF()
    # print("current records:")
    current_data.show()

    # Get the new records to be inserted
    new_records = df.alias("new").join(current_data.alias("current"), "CustomerID", "left_outer") \
        .where(F.col("current.CustomerID").isNull()) \
        .selectExpr("new.*")

    # Get the records to be updated
    records_to_update = df.alias("new").join(current_data.alias("current"), "CustomerID") \
        .where(
            (F.col("new.CustomerName") != F.col("current.CustomerName")) |
            (F.col("new.City") != F.col("current.City")) |
            (F.col("new.State") != F.col("current.State")) |
            (F.col("new.Country") != F.col("current.Country"))
        ) \
        .selectExpr("new.*")


    # Set the end date for the current version of the records to be updated
    records_to_update = records_to_update.withColumn("EndDate", F.current_date())

    records_to_update = records_to_update.withColumn("CustomerID", F.col("CustomerID").cast("string"))


    # Update the current version of the records in the Delta table
    delta_table.alias("current").merge(
        records_to_update.alias("updates"),
        "current.CustomerID = updates.CustomerID"
    ).whenMatchedUpdate(set={
        "EndDate": F.col("updates.EndDate"),
        "CustomerName": F.col("updates.CustomerName")
    }).execute()

    # Insert the new records into the Delta table
    delta_table.alias("current").merge(
        new_records.alias("new"),
        "current.CustomerID = new.CustomerID"
    ).whenNotMatchedInsert(values={
        "CustomerID": F.col("new.CustomerID"),
        "CustomerName": F.col("new.CustomerName"),
        "City": F.col("new.City"),
        "State": F.col("new.State"),
        "Country": F.col("new.Country"),
        "StartDate": F.current_date(),
        "EndDate": F.lit("9999-12-31")
    }).execute()



    
    # delta_table.toDF().show()
    display(delta_table.toDF())


else:
    # If the Delta table does not exist, save the DataFrame as a new Delta table
    # Adding the "StartDate" and "EndDate" columns to the schema
    df = df.withColumn("StartDate", F.current_date())
    df = df.withColumn("EndDate", F.lit("9999-12-31"))
    df.write.format("delta").mode("overwrite").save(output_path)

