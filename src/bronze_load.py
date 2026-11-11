# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Bronze Load from TPC-H Samples
# MAGIC
# MAGIC Loads two raw bronze tables (`bronze_tpch_customer`, `bronze_tpch_orders`) into `testing.bronze` from the built-in `samples.tpch` dataset, adding ingestion metadata columns.

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import functions as F

# Target location for bronze tables
schema = "bronze"

dbutils.widgets.text("catalog", "testing")
catalog = dbutils.widgets.get("catalog")

# Source (built-in sample dataset) -> target table name
sources = {
    "samples.tpch.customer": "bronze_tpch_customer",
    "samples.tpch.orders": "bronze_tpch_orders",
}

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# DBTITLE 1,Load bronze tables
# Raw copy of source data plus ingestion metadata columns
for source_table, target_name in sources.items():
    target_table = f"{catalog}.{schema}.{target_name}"
    df = (
        spark.read.table(source_table)
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_table", F.lit(source_table))
    )
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
    print(f"Loaded {source_table} -> {target_table}")

# COMMAND ----------

# DBTITLE 1,Validate row counts
# Compare source vs bronze row counts
counts = spark.createDataFrame(
    [
        (
            src,
            f"{catalog}.{schema}.{tgt}",
            spark.read.table(src).count(),
            spark.read.table(f"{catalog}.{schema}.{tgt}").count(),
        )
        for src, tgt in sources.items()
    ],
    "source_table string, bronze_table string, source_rows long, bronze_rows long",
).withColumn("match", F.col("source_rows") == F.col("bronze_rows"))

display(counts)

# COMMAND ----------

# DBTITLE 1,Preview bronze orders
display(spark.read.table(f"{catalog}.{schema}.bronze_tpch_orders").limit(5))