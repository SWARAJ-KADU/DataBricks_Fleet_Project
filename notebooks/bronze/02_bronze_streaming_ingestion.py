# Databricks notebook source
# MAGIC %md
# MAGIC # 02_bronze_streaming_ingestion
# MAGIC
# MAGIC Phase 2: Bronze layer ingestion for the 3 streaming sources using
# MAGIC Structured Streaming and Auto Loader.

# COMMAND ----------
# CATLOG_NAME = dbutils.widgets.get("CATLOG_NAME")
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--CATALOG_NAME", required=True)

args = parser.parse_args()
CATALOG_NAME = args.CATALOG_NAME

STREAM_LANDING = f"/Volumes/{CATALOG_NAME}/bronze/landing_zone/streaming"
CHECKPOINT_BASE = f"/Volumes/{CATALOG_NAME}/bronze/landing_zone/_checkpoints"

print("Streaming landing zone:", STREAM_LANDING)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, col

# Import your shared schemas
from utils.schemas import SCHEMAS

# COMMAND ----------

def start_stream_ingestion(
    source_name: str,
    bronze_table: str,
    trigger_seconds: int = 15,
):
    """
    Starts a Structured Streaming query that continuously ingests JSON files
    from the landing zone into a Bronze Delta table.
    """

    source_path = f"{STREAM_LANDING}/{source_name}"
    checkpoint_path = f"{CHECKPOINT_BASE}/{source_name}"

    df = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .schema(SCHEMAS[source_name])
            .load(source_path)
    )

    df_with_metadata = (
        df
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_source_file", col("_metadata.file_path"))
            .withColumn("_source_name", lit(source_name))
    )

    query = (
        df_with_metadata.writeStream
            .format("delta")
            .option("checkpointLocation", checkpoint_path)
            .outputMode("append")
            .trigger(availableNow=True)
            .queryName(f"bronze_{source_name}")
            .toTable(bronze_table)
    )

    print(f"Started streaming query: bronze_{source_name} -> {bronze_table}")

    return query

# COMMAND ----------

# MAGIC %md
# MAGIC ## Start all streams

# COMMAND ----------

gps_query = start_stream_ingestion(
    "gps_events",
    f"{CATALOG_NAME}.bronze.gps_events"
)

sensor_query = start_stream_ingestion(
    "sensor_events",
    f"{CATALOG_NAME}.bronze.sensor_events"
)

delivery_query = start_stream_ingestion(
    "delivery_events",
    f"{CATALOG_NAME}.bronze.delivery_events"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream Status

# COMMAND ----------

for q in [gps_query, sensor_query, delivery_query]:
    batch_id = (
        q.lastProgress["batchId"]
        if q.lastProgress
        else "none yet"
    )

    print(
        f"{q.name}: active={q.isActive}, last batch={batch_id}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Row Counts

# COMMAND ----------

for tbl in [
    "gps_events",
    "sensor_events",
    "delivery_events",
]:
    try:
        count = spark.table(
            f"{CATALOG_NAME}.bronze.{tbl}"
        ).count()

        print(
            f"{CATALOG_NAME}.bronze.{tbl}: {count} rows"
        )

    except Exception as e:
        print(
            f"{CATALOG_NAME}.bronze.{tbl}: not created yet ({e})"
        )

# COMMAND ----------

display(
    spark.table(f"{CATALOG_NAME}.bronze.gps_events")
         .orderBy("timestamp", ascending=False)
         .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stop Streams

# COMMAND ----------

for q in [gps_query, sensor_query, delivery_query]:
    q.stop()
    print(f"Stopped {q.name}")