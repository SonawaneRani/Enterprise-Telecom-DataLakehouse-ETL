# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_customers,
# MAGIC     SUM(churn_flag) AS churned_customers,
# MAGIC     ROUND(
# MAGIC         SUM(churn_flag)*100.0/COUNT(*),
# MAGIC         2
# MAGIC     ) AS churn_rate
# MAGIC FROM telecom_gold.gold_customer_churn_analysis;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     plan_name,
# MAGIC     COUNT(*) AS customers,
# MAGIC     SUM(churn_flag) AS churned
# MAGIC FROM telecom_gold.gold_customer_churn_analysis
# MAGIC GROUP BY plan_name
# MAGIC ORDER BY churned DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     SUM(net_revenue) AS total_revenue
# MAGIC FROM telecom_gold.gold_revenue_analysis;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     plan_name,
# MAGIC     SUM(net_revenue) AS revenue
# MAGIC FROM telecom_gold.gold_revenue_analysis
# MAGIC GROUP BY plan_name
# MAGIC ORDER BY revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name,
# MAGIC     SUM(data_used_gb) AS total_data
# MAGIC FROM telecom_gold.gold_usage_analysis
# MAGIC GROUP BY
# MAGIC     customer_id,
# MAGIC     first_name,
# MAGIC     last_name
# MAGIC ORDER BY total_data DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     network_type,
# MAGIC     SUM(data_used_gb) AS total_usage
# MAGIC FROM telecom_gold.gold_usage_analysis
# MAGIC GROUP BY network_type;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     service_performance,
# MAGIC     COUNT(*) AS complaints
# MAGIC FROM telecom_gold.gold_complaint_analysis
# MAGIC GROUP BY service_performance;

# COMMAND ----------

spark.table("telecom_gold.gold_customer_churn_analysis").show(5)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN telecom_gold;