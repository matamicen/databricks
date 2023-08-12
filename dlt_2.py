# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# df = spark.read.format("csv").option("header", "true").load("dbfs:/mnt/skynetdev/sales_star_schema/customer.csv")
# df.show()

json_path = "/databricks-datasets/wikipedia-datasets/data-001/clickstream/raw-uncompressed-json/2015_2_clickstream.json"
@dlt.create_table(
  comment="The raw wikipedia clickstream dataset, ingested from /databricks-datasets."
)
def clickstream_raw():          
  return (
    # spark.read.json(json_path)
    spark.read.format("csv").option("header", "true").load("dbfs:/mnt/skynetdev/sales_star_schema/customer.csv")
  )


# @dlt.table(
#   comment="Wikipedia clickstream data cleaned and prepared for analysis."
# )
# @dlt.expect("valid_current_page_title", "current_page_title IS NOT NULL")
# @dlt.expect_or_fail("valid_count", "click_count > 0")
# def clickstream_prepared():
#   return (
#     dlt.read("clickstream_raw")
#       .withColumn("click_count", expr("CAST(n AS INT)"))
#       .withColumnRenamed("curr_title", "current_page_title")
#       .withColumnRenamed("prev_title", "previous_page_title")
#       .select("current_page_title", "click_count", "previous_page_title")
#   )


# @dlt.table(
#   comment="A table containing the top pages linking to the Apache Spark page."
# )
# def top_spark_referrers():
#   return (
#     dlt.read("clickstream_prepared")
#       .filter(expr("current_page_title == 'Apache_Spark'"))
#       .withColumnRenamed("previous_page_title", "referrer")
#       .sort(desc("click_count"))
#       .select("referrer", "click_count")
#       .limit(10)
#   )
