# Databricks notebook source
# MAGIC %md
# MAGIC # 01_bronze_batch_autoloader
# MAGIC
# MAGIC Phase 2: Bronze layer ingestion for the 4 batch sources using Auto Loader.
# MAGIC
# MAGIC Sources:
# MAGIC   - shipment_manifests (JSON)
# MAGIC   - invoices (CSV)
# MAGIC   - maintenance_logs (CSV)
# MAGIC   - driver_vehicle_master (CSV)

# COMMAND ----------
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--CATALOG_NAME", required=True)

args = parser.parse_args()
CATALOG_NAME = args.CATALOG_NAME

LANDING_VOLUME = f"/Volumes/{CATALOG_NAME}/bronze/landing_zone/batch"
CHECKPOINT_BASE = f"/Volumes/{CATALOG_NAME}/bronze/landing_zone/_checkpoints"

from pyspark.sql.functions import current_timestamp, col, lit
from pyspark.sql.types import *
from utils.schemas import SCHEMAS

def ingest_batch_source(source_name: str, file_format: str, bronze_table: str):

    source_path = f"{LANDING_VOLUME}/{source_name}"
    checkpoint_path = f"{CHECKPOINT_BASE}/{source_name}"

    reader = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", file_format)
            .schema(SCHEMAS[source_name])
    )

    if file_format == "csv":
        reader = (
            reader
                .option("header", "true")
        )

    df = reader.load(source_path)

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
            .toTable(bronze_table)
    )

    query.awaitTermination()

    print(f"Ingested {source_name} -> {bronze_table}")

# -----------------------------------------------------------------------------
# Ingest Bronze Tables
# -----------------------------------------------------------------------------

ingest_batch_source(
    "shipment_manifests",
    "json",
    f"{CATALOG_NAME}.bronze.shipment_manifests"
)

ingest_batch_source(
    "invoices",
    "csv",
    f"{CATALOG_NAME}.bronze.invoices"
)

ingest_batch_source(
    "maintenance_logs",
    "csv",
    f"{CATALOG_NAME}.bronze.maintenance_logs"
)

ingest_batch_source(
    "driver_vehicle_master",
    "csv",
    f"{CATALOG_NAME}.bronze.driver_vehicle_master"
)

# -----------------------------------------------------------------------------
# Validate Counts
# -----------------------------------------------------------------------------

for tbl in [
    "shipment_manifests",
    "invoices",
    "maintenance_logs",
    "driver_vehicle_master",
]:
    count = spark.table(f"{CATALOG_NAME}.bronze.{tbl}").count()
    print(f"{CATALOG_NAME}.bronze.{tbl}: {count} rows")