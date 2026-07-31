# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

customers = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/Volumes/bronze/bronze1/rani/Telecom_DataLakehouse_Dataset/customers.csv")

display(customers)

# COMMAND ----------

plans = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/Volumes/bronze/bronze1/rani/Telecom_DataLakehouse_Dataset/plans.csv")

display(plans)

# COMMAND ----------

usage = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/Volumes/bronze/bronze1/rani/Telecom_DataLakehouse_Dataset/internet_usage.csv")

display(usage)

# COMMAND ----------

recharge = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/Volumes/bronze/bronze1/rani/Telecom_DataLakehouse_Dataset/recharge.csv")

display(recharge)

# COMMAND ----------

complaints = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("/Volumes/bronze/bronze1/rani/Telecom_DataLakehouse_Dataset/complaints.csv")

display(complaints)

# COMMAND ----------

customers = customers.withColumn("load_date", current_timestamp()) \
    .withColumn("source", lit("customers.csv"))

plans = plans.withColumn("load_date", current_timestamp()) \
    .withColumn("source", lit("plans.csv"))

usage = usage.withColumn("load_date", current_timestamp()) \
    .withColumn("source", lit("internet_usage.csv"))

recharge = recharge.withColumn("load_date", current_timestamp()) \
    .withColumn("source", lit("recharge.csv"))

complaints = complaints.withColumn("load_date", current_timestamp()) \
    .withColumn("source", lit("complaints.csv"))

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS bronze_usage")

# COMMAND ----------

customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_customers")

plans.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_plans")

usage.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_usage")

recharge.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_recharge")

complaints.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_complaints")

# COMMAND ----------

spark.sql("SHOW TABLES").show(truncate=False)

# COMMAND ----------

display(spark.table("bronze_customers"))

# COMMAND ----------

display(spark.table("bronze_usage"))

# COMMAND ----------

spark.table("bronze_customers").printSchema()

spark.table("bronze_plans").printSchema()

spark.table("bronze_usage").printSchema()

spark.table("bronze_recharge").printSchema()

spark.table("bronze_complaints").printSchema()