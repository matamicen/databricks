# Databricks notebook source
# MAGIC %sql
# MAGIC     CREATE TABLE IF NOT EXISTS DimCustomerTable3
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

# MAGIC %sql
# MAGIC
# MAGIC -- Assuming the table `adb_customer_delta2` already exists in the database and has the appropriate schema.
# MAGIC -- Drop the temporary table if it exists
# MAGIC DROP TABLE IF EXISTS tmp_updates;
# MAGIC -- Read the CSV file into a temporary table
# MAGIC CREATE TEMPORARY TABLE tmp_updates
# MAGIC USING csv
# MAGIC OPTIONS (
# MAGIC   path 'dbfs:/mnt/skynetdev/sales_star_schema/customer3.csv',
# MAGIC   header 'true',
# MAGIC   inferSchema 'true'
# MAGIC );
# MAGIC
# MAGIC
# MAGIC MERGE INTO DimCustomerTable3 AS a
# MAGIC USING
# MAGIC (
# MAGIC   SELECT 
# MAGIC     CustomerID as mergeKey,
# MAGIC     CustomerID,
# MAGIC     CustomerName,
# MAGIC     City,
# MAGIC     State,
# MAGIC     Country
# MAGIC   FROM tmp_updates
# MAGIC   UNION ALL
# MAGIC   SELECT 
# MAGIC     NULL as mergeKey,
# MAGIC     c.CustomerID,
# MAGIC     c.CustomerName,
# MAGIC     c.City,
# MAGIC     c.State,
# MAGIC     c.Country
# MAGIC   FROM tmp_updates as c
# MAGIC   JOIN DimCustomerTable3 as d ON ((c.CustomerID = d.CustomerID) and (c.CustomerName != d.CustomerName) and (d.active=true))
# MAGIC ) AS b
# MAGIC ON a.CustomerID = b.mergeKey 
# MAGIC WHEN MATCHED and ((b.CustomerName != a.CustomerName) and (a.active=true)) THEN
# MAGIC update SET a.endDate = current_date() - 1, a.active = 'false'
# MAGIC WHEN NOT MATCHED
# MAGIC THEN 
# MAGIC   INSERT (
# MAGIC     CustomerID,
# MAGIC     CustomerName,
# MAGIC     City,
# MAGIC     State,
# MAGIC     Country,
# MAGIC     startDate,
# MAGIC     endDate,
# MAGIC     active
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     b.CustomerID,
# MAGIC     b.CustomerName,
# MAGIC     b.City,
# MAGIC     b.State,
# MAGIC     b.Country,
# MAGIC     CURRENT_DATE(),  -- Set the startDate to the current date on insert
# MAGIC     '9999-12-31',    -- Set the endDate to null on insert
# MAGIC     'true'
# MAGIC   );
# MAGIC
