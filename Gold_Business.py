# Databricks notebook source

from pyspark.sql.functions import *
from pyspark.sql.functions import when

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS telecom_gold;
# MAGIC
# MAGIC SHOW DATABASES;

# COMMAND ----------

customers = spark.table("silver_customers")

plans = spark.table("silver_plans")

# COMMAND ----------

customer_plan = customers.join(
    plans,
    "plan_id",
    "left"
)

# COMMAND ----------

gold_customer_churn = customer_plan.select(
    "customer_id",
    "first_name",
    "last_name",
    "gender",
    "age",
    "plan_name",
    "plan_type",
    "monthly_price",
    "is_active",
    "churn_flag",
    "customer_status",
    "join_date",
    "device_type",
    "preferred_channel"
)

# COMMAND ----------

gold_customer_churn = gold_customer_churn.withColumn(
    "churn_category",
    when(
        col("churn_flag")==1,
        "High Risk Customer"
    )
    .otherwise(
        "Active Customer"
    )
)

# COMMAND ----------

gold_customer_churn.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
"telecom_gold.gold_customer_churn_analysis"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM telecom_gold.gold_customer_churn_analysis
# MAGIC LIMIT 10;

# COMMAND ----------

recharge_df = spark.table("silver_recharge")
plans_df = spark.table("silver_plans")
customers_df = spark.table("silver_customers")

# COMMAND ----------

customers_clean = customers_df.drop("plan_id")

# COMMAND ----------

recharge_plan = recharge_df.join(
    plans_df,
    recharge_df["plan_id"] == plans_df["plan_id"],
    "left"
)

# COMMAND ----------

recharge_plan = recharge_plan.select(
    recharge_df["recharge_id"],
    recharge_df["customer_id"],
    recharge_df["plan_id"],
    plans_df["plan_name"],
    plans_df["plan_type"],
    plans_df["monthly_price"],
    recharge_df["amount"],
    recharge_df["cashback"],
    recharge_df["net_revenue"],
    recharge_df["channel"],
    recharge_df["status"],
    recharge_df["recharge_day"]
)

# COMMAND ----------

gold_revenue_analysis = recharge_plan.join(
    customers_clean,
    "customer_id",
    "left"
)

# COMMAND ----------

gold_revenue_analysis.printSchema()

# COMMAND ----------

gold_revenue_analysis = gold_revenue_analysis.filter(
    col("status") == "SUCCESS"
)

# COMMAND ----------

gold_revenue_analysis = gold_revenue_analysis.withColumn(
    "revenue_category",
    when(
        col("net_revenue") < 200,
        "Low Revenue"
    )
    .when(
        col("net_revenue") < 700,
        "Medium Revenue"
    )
    .otherwise(
        "High Revenue"
    )
)

# COMMAND ----------

display(
    gold_revenue_analysis.limit(10)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS telecom_gold;

# COMMAND ----------

gold_revenue_analysis.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
"telecom_gold.gold_revenue_analysis"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM telecom_gold.gold_revenue_analysis
# MAGIC LIMIT 10;

# COMMAND ----------

gold_revenue_analysis.printSchema()

# COMMAND ----------

usage_df = spark.table("silver_usage")

customers_df = spark.table("silver_customers")

plans_df = spark.table("silver_plans")

usage_df.printSchema()

# COMMAND ----------

customers_clean = customers_df.drop("plan_id")

# COMMAND ----------

usage_customer = usage_df.join(
    customers_clean,
    "customer_id",
    "left"
)

# COMMAND ----------

gold_usage_analysis = usage_customer.join(
    plans_df,
    usage_customer["plan_id"] == plans_df["plan_id"],
    "left"
)

# COMMAND ----------

gold_usage_analysis = gold_usage_analysis.select(
    usage_df["usage_id"],
    usage_df["customer_id"],
    usage_df["data_used_gb"],
    usage_df["session_duration_min"],
    usage_df["network_type"],
    usage_df["usage_date"],
    usage_df["usage_category"],
    customers_clean["first_name"],
    customers_clean["last_name"],
    customers_clean["device_type"],
    plans_df["plan_name"],
    plans_df["plan_type"]
)

# COMMAND ----------

gold_usage_analysis = gold_usage_analysis.withColumn(
    "customer_usage_segment",
    when(
        col("data_used_gb") < 1,
        "Low User"
    )
    .when(
        col("data_used_gb") < 5,
        "Medium User"
    )
    .otherwise(
        "Heavy User"
    )
)

# COMMAND ----------

display(
    gold_usage_analysis.limit(10)
)

# COMMAND ----------

gold_usage_analysis.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
"telecom_gold.gold_usage_analysis"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM telecom_gold.gold_usage_analysis
# MAGIC LIMIT 10;

# COMMAND ----------

complaints_df = spark.table("silver_complaints")

customers_df = spark.table("silver_customers")

complaints_df.printSchema()

# COMMAND ----------

customers_clean = customers_df.drop("plan_id")

# COMMAND ----------

complaint_customer = complaints_df.join(
    customers_clean,
    "customer_id",
    "left"
)

# COMMAND ----------

gold_complaint_analysis = complaint_customer.select(
    complaints_df["complaint_id"],
    complaints_df["customer_id"],
    complaints_df["complaint_type"],
    complaints_df["status"],
    complaints_df["priority"],
    complaints_df["resolution_days"],
    complaints_df["resolution_category"],
    complaints_df["priority_score"],
    complaints_df["complaint_day"],
    customers_clean["first_name"],
    customers_clean["last_name"],
    customers_clean["device_type"]
)

# COMMAND ----------

gold_complaint_analysis = gold_complaint_analysis.withColumn(
    "complaint_severity",
    when(
        col("priority_score")==3,
        "Critical"
    )
    .when(
        col("priority_score")==2,
        "Moderate"
    )
    .otherwise(
        "Low"
    )
)

# COMMAND ----------

gold_complaint_analysis = gold_complaint_analysis.withColumn(
    "service_performance",
    when(
        col("resolution_days") <= 2,
        "Excellent"
    )
    .when(
        col("resolution_days") <= 7,
        "Average"
    )
    .otherwise(
        "Poor"
    )
)

# COMMAND ----------

display(
    gold_complaint_analysis.limit(10)
)

# COMMAND ----------

gold_complaint_analysis.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable(
"telecom_gold.gold_complaint_analysis"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM telecom_gold.gold_complaint_analysis
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN telecom_gold;