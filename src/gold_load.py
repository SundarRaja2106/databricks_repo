# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Gold Load: Monthly Sales by Market Segment
# MAGIC
# MAGIC Aggregates `testing.silver.silver_customer_orders` into `testing.gold.gold_monthly_segment_sales`: one row per order year, month, market segment and order status with order counts, unique customers and revenue metrics.

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import functions as F

catalog = "testing"
silver_table = f"{catalog}.silver.silver_customer_orders"
gold_schema = "gold"
gold_table = f"{catalog}.{gold_schema}.gold_monthly_segment_sales"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{gold_schema}")

# COMMAND ----------

# DBTITLE 1,Aggregate silver and write gold table
silver = spark.read.table(silver_table)

gold_df = (
    silver.groupBy("order_year", "order_month", "market_segment", "order_status")
    .agg(
        F.count("order_key").alias("order_count"),
        F.countDistinct("customer_key").alias("unique_customers"),
        F.sum("total_price").alias("total_revenue"),
        F.round(F.avg("total_price"), 2).cast("decimal(18,2)").alias("avg_order_value"),
        F.min("total_price").alias("min_order_value"),
        F.max("total_price").alias("max_order_value"),
        F.sum(F.when(F.col("order_priority").isin("1-URGENT", "2-HIGH"), 1).otherwise(0)).alias("high_priority_orders"),
    )
    .withColumn("month_start", F.make_date("order_year", "order_month", F.lit(1)))
    .withColumn("_gold_load_timestamp", F.current_timestamp())
    .select(
        "month_start", "order_year", "order_month", "market_segment", "order_status",
        "order_count", "unique_customers", "total_revenue", "avg_order_value",
        "min_order_value", "max_order_value", "high_priority_orders", "_gold_load_timestamp",
    )
)

(
    gold_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_table)
)
print(f"Loaded {gold_table}")

# COMMAND ----------

# DBTITLE 1,Validate gold table
gold = spark.read.table(gold_table)

# Reconcile gold totals against silver
check = (
    gold.agg(F.count("*").alias("gold_rows"), F.sum("order_count").alias("gold_orders"), F.sum("total_revenue").alias("gold_revenue"))
    .crossJoin(silver.agg(F.count("*").alias("silver_orders"), F.sum("total_price").alias("silver_revenue")))
)
display(check)
display(gold.orderBy("month_start", "market_segment", "order_status").limit(20))