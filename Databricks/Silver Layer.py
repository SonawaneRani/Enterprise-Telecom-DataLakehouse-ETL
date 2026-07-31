# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.functions import col

# COMMAND ----------

customers_bronze = spark.table("bronze_customers")

plans_bronze = spark.table("bronze_plans")

usage_bronze = spark.table("bronze_usage")

recharge_bronze = spark.table("bronze_recharge")

complaints_bronze = spark.table("bronze_complaints")

# COMMAND ----------

customers_bronze.printSchema()

# COMMAND ----------

silver_customers = customers_bronze.dropDuplicates(
    ["customer_id"]
)

# COMMAND ----------

silver_customers = silver_customers.filter(
    col("customer_id").isNotNull()
)

# COMMAND ----------

silver_customers = silver_customers.withColumn(
    "first_name",
    initcap(trim(col("first_name")))
)

# COMMAND ----------

silver_customers = silver_customers.withColumn(
    "last_name",
    initcap(trim(col("last_name")))
)

# COMMAND ----------

silver_customers = silver_customers.withColumn(
    "email",
    lower(trim(col("email")))
)

# COMMAND ----------

silver_customers = silver_customers.fillna(
{
    "gender":"Unknown",
    "device_type":"Unknown",
    "preferred_channel":"Unknown"
}
)

# COMMAND ----------

silver_customers = silver_customers.withColumn(
    "join_date",
    to_date(col("join_date"))
)

# COMMAND ----------

silver_customers = silver_customers.withColumn(
    "customer_status",
    when(
        col("churn_flag")==1,
        "Churned"
    )
    .otherwise("Active")
)

# COMMAND ----------

silver_customers.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("silver_customers")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM silver_customers
# MAGIC LIMIT 10;

# COMMAND ----------

plans_bronze = spark.table("bronze_plans")

plans_bronze.printSchema()

# COMMAND ----------

silver_plans = plans_bronze.dropDuplicates(
    ["plan_id"]
)

# COMMAND ----------

silver_plans = silver_plans.filter(
    col("plan_id").isNotNull()
)

# COMMAND ----------

silver_plans = silver_plans.withColumn(
    "plan_name",
    initcap(trim(col("plan_name")))
)

# COMMAND ----------

silver_plans = silver_plans.withColumn(
    "plan_type",
    initcap(trim(col("plan_type")))
)

# COMMAND ----------

silver_plans = silver_plans.fillna(
{
    "monthly_price":0,
    "data_quota_gb":0,
    "validity_days":0,
    "voice_minutes":"0",
    "sms_per_day":"0"
}
)

# COMMAND ----------

silver_plans = silver_plans.withColumn(
    "price_category",
    when(col("monthly_price") < 300,"Low")
    .when(col("monthly_price") < 700,"Medium")
    .otherwise("Premium")
)

# COMMAND ----------

silver_plans = silver_plans.withColumn(
    "network_category",
    when(
        col("is_5g_enabled")==True,
        "5G"
    )
    .otherwise("4G")
)

# COMMAND ----------

silver_plans.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("silver_plans")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_plans
# MAGIC LIMIT 10;

# COMMAND ----------

usage_bronze = spark.table("bronze_usage")

usage_bronze.printSchema()

# COMMAND ----------

silver_usage = usage_bronze.dropDuplicates(
    ["usage_id"]
)

# COMMAND ----------

silver_usage = silver_usage.filter(
    col("customer_id").isNotNull()
)

# COMMAND ----------

silver_usage = silver_usage.filter(
    col("data_used_mb") >= 0
)

# COMMAND ----------

silver_usage = silver_usage.filter(
    col("session_duration_min") >= 0
)

# COMMAND ----------

silver_usage = silver_usage.withColumn(
    "data_used_gb",
    round(col("data_used_mb") / 1024, 2)
)

# COMMAND ----------

silver_usage = silver_usage.withColumn(
    "usage_date",
    to_date(col("session_date"))
)

# COMMAND ----------

silver_usage = silver_usage.withColumn(
    "network_type",
    upper(trim(col("network_type")))
)

# COMMAND ----------

silver_usage = silver_usage.withColumn(
    "usage_category",
    when(
        col("data_used_gb") < 1,
        "Low Usage"
    )
    .when(
        col("data_used_gb") < 5,
        "Medium Usage"
    )
    .otherwise(
        "High Usage"
    )
)

# COMMAND ----------

silver_usage.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("silver_usage")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_usage
# MAGIC LIMIT 10;

# COMMAND ----------

recharge_bronze = spark.table("bronze_recharge")

recharge_bronze.printSchema()

# COMMAND ----------

silver_recharge = recharge_bronze.dropDuplicates(
    ["recharge_id"]
)

# COMMAND ----------

silver_recharge = silver_recharge.filter(
    col("customer_id").isNotNull()
)

# COMMAND ----------

silver_recharge = silver_recharge.filter(
    col("amount") > 0
)

# COMMAND ----------

silver_recharge = silver_recharge.fillna(
{
    "cashback":0,
    "channel":"Unknown",
    "status":"Pending"
}
)

# COMMAND ----------

silver_recharge = silver_recharge.withColumn(
    "channel",
    upper(trim(col("channel")))
)

# COMMAND ----------

silver_recharge = silver_recharge.withColumn(
    "status",
    upper(trim(col("status")))
)

# COMMAND ----------

silver_recharge = silver_recharge.withColumn(
    "net_revenue",
    col("amount") - col("cashback")
)

# COMMAND ----------

silver_recharge = silver_recharge.withColumn(
    "recharge_day",
    to_date(col("recharge_date"))
)

# COMMAND ----------

silver_recharge = silver_recharge.withColumn(
    "recharge_category",
    when(
        col("amount") < 200,
        "Small"
    )
    .when(
        col("amount") < 700,
        "Medium"
    )
    .otherwise(
        "Large"
    )
)

# COMMAND ----------

silver_recharge.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("silver_recharge")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_recharge
# MAGIC LIMIT 10;

# COMMAND ----------

complaints_bronze = spark.table("bronze_complaints")

complaints_bronze.printSchema()

# COMMAND ----------

silver_complaints = complaints_bronze.dropDuplicates(
    ["complaint_id"]
)

# COMMAND ----------

silver_complaints = silver_complaints.filter(
    col("customer_id").isNotNull()
)

# COMMAND ----------

silver_complaints = silver_complaints.withColumn(
    "complaint_type",
    initcap(trim(col("complaint_type")))
)

# COMMAND ----------

silver_complaints = silver_complaints.withColumn(
    "status",
    upper(trim(col("status")))
)

# COMMAND ----------

silver_complaints = silver_complaints.fillna(
{
    "status":"OPEN",
    "priority":"MEDIUM",
    "resolution_days":0
}
)

# COMMAND ----------

silver_complaints = silver_complaints.withColumn(
    "resolution_category",
    when(
        col("resolution_days") <= 2,
        "Fast"
    )
    .when(
        col("resolution_days") <= 7,
        "Medium"
    )
    .otherwise(
        "Delayed"
    )
)

# COMMAND ----------

silver_complaints = silver_complaints.withColumn(
    "complaint_day",
    to_date(col("complaint_date"))
)

# COMMAND ----------

silver_complaints = silver_complaints.withColumn(
    "priority_score",
    when(col("priority")=="HIGH",3)
    .when(col("priority")=="MEDIUM",2)
    .otherwise(1)
)

# COMMAND ----------

silver_complaints.write \
.format("delta") \
.mode("overwrite") \
.saveAsTable("silver_complaints")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM silver_complaints
# MAGIC LIMIT 10;