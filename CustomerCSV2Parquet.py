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

df_1 = spark.read.format("csv").option("header", "true").load("dbfs:/mnt/skynetdev/sales_star_schema/customer.csv")
df_1.show()


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

from pyspark.sql import SparkSession


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Save CSV to Parquet") \
    .getOrCreate()

# Load the customer.csv file
input_path = "dbfs:/mnt/skynetdev/sales_star_schema/customer.csv"
df = spark.read.format("csv").option("header", "true").load(input_path)

# Show the loaded data (optional)
df.show()

# Save the DataFrame in Parquet format
output_path = "dbfs:/mnt/skynet/output/adb_customer"
# df.write.parquet(output_path, mode="overwrite")
df.write.parquet(output_path, mode="append")


# Stop the SparkSession (optional if you don't need it for further processing)
# spark.stop()

