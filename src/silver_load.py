# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Silver Load: Customer Orders
# MAGIC
# MAGIC Joins `testing.bronze.bronze_tpch_orders` with `testing.bronze.bronze_tpch_customer` into a single cleaned, deduplicated silver table `testing.silver.silver_customer_orders` (one row per order, enriched with customer attributes).

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog = "testing"
bronze_schema = "bronze"
silver_schema = "silver"

bronze_customer = f"{catalog}.{bronze_schema}.bronze_tpch_customer"
bronze_orders = f"{catalog}.{bronze_schema}.bronze_tpch_orders"
silver_table = f"{catalog}.{silver_schema}.silver_customer_orders"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema}")

# COMMAND ----------

# DBTITLE 1,Clean and deduplicate bronze tables
# Keep the latest ingested record per business key
def latest_per_key(df, key):
    w = Window.partitionBy(key).orderBy(F.col("_ingest_timestamp").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

customers = (
    latest_per_key(spark.read.table(bronze_customer), "c_custkey")
    .select(
        F.col("c_custkey").alias("customer_key"),
        F.trim("c_name").alias("customer_name"),
        F.trim("c_address").alias("customer_address"),
        F.col("c_nationkey").alias("nation_key"),
        F.trim("c_phone").alias("customer_phone"),
        F.col("c_acctbal").alias("account_balance"),
        F.upper(F.trim("c_mktsegment")).alias("market_segment"),
    )
)

orders = (
    latest_per_key(spark.read.table(bronze_orders).filter(F.col("o_orderkey").isNotNull()), "o_orderkey")
    .select(
        F.col("o_orderkey").alias("order_key"),
        F.col("o_custkey").alias("customer_key"),
        F.upper(F.trim("o_orderstatus")).alias("order_status"),
        F.col("o_totalprice").alias("total_price"),
        F.col("o_orderdate").alias("order_date"),
        F.trim("o_orderpriority").alias("order_priority"),
        F.trim("o_clerk").alias("clerk"),
        F.col("o_shippriority").alias("ship_priority"),
        F.trim("o_comment").alias("order_comment"),
    )
)

# COMMAND ----------

# DBTITLE 1,Join and write silver table
# One row per order, enriched with customer attributes (left join keeps orders without a matching customer)
silver_df = (
    orders.join(customers, on="customer_key", how="left")
    .withColumn("order_year", F.year("order_date"))
    .withColumn("order_month", F.month("order_date"))
    .withColumn("_silver_load_timestamp", F.current_timestamp())
    .select(
        "order_key", "customer_key", "customer_name", "market_segment", "nation_key",
        "customer_address", "customer_phone", "account_balance",
        "order_status", "order_date", "order_year", "order_month", "total_price",
        "order_priority", "clerk", "ship_priority", "order_comment", "_silver_load_timestamp",
    )
)

(
    silver_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)
print(f"Loaded {silver_table}")

# COMMAND ----------

# DBTITLE 1,Validate silver table
silver = spark.read.table(silver_table)

validation = silver.agg(
    F.count("*").alias("silver_rows"),
    F.countDistinct("order_key").alias("distinct_orders"),
    F.sum(F.when(F.col("customer_name").isNull(), 1).otherwise(0)).alias("orders_without_customer"),
).withColumn("bronze_distinct_orders", F.lit(spark.read.table(bronze_orders).select("o_orderkey").distinct().count()))

display(validation)
display(silver.limit(5))