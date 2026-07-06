# Databricks notebook source
# MAGIC %md
# MAGIC # 00_generate_batch_data
# MAGIC Phase 1: Run the batch data generator directly inside Databricks and land
# MAGIC the output in the DEV landing zone, ready for Auto Loader (Phase 2).
# MAGIC
# MAGIC This notebook lives in `notebooks/00_setup/` and imports the generator
# MAGIC code from `data_generator/` (both synced via Databricks Repos).

# COMMAND ----------

# MAGIC %pip install Faker --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
%pip install faker

import sys
import os

# Explicit path is more robust than auto-detecting the notebook's own path.
# Replace "your_username" with your actual Databricks username/email, and
# confirm the repo folder name matches what you see under Workspace > Repos.
REPO_ROOT = "/Workspace/Users/swarajunofficial0@gmail.com/DataBricks_Fleet_Project"
GENERATOR_PATH = f"{REPO_ROOT}/data_generator"
print("Generator path:", GENERATOR_PATH)

sys.path.append(GENERATOR_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Landing zone definition
# MAGIC
# MAGIC We write generated files to a DEV landing path in the workspace's
# MAGIC default storage (Unity Catalog volume). This is the path Auto Loader
# MAGIC will point at in Phase 2.

# COMMAND ----------

# Using a Unity Catalog managed Volume as the landing zone -- this is the
# modern, correct way to land files for Auto Loader on Unity-Catalog-enabled
# workspaces (replaces the older DBFS-mount pattern).
CATALOG = "fleet_uat"
SCHEMA = "bronze"
VOLUME = "landing_zone"

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/batch"
print("Landing zone path:", LANDING_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the generator
# MAGIC
# MAGIC Writes 45 days of historical batch data directly to the Volume.
# MAGIC Safe to re-run -- batch_generator.py overwrites by date, and the
# MAGIC underlying random seed means output is reproducible.

# COMMAND ----------

from batch_generator import main as run_batch_generator
import argparse

# Simulate CLI args programmatically since we're calling from a notebook
sys.argv = [
    "batch_generator.py",
    "--days", "45",
    "--start-date", "2026-05-01",
    "--output", LANDING_PATH,
    "--num-trucks", "12",
]
run_batch_generator()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify

# COMMAND ----------

display(dbutils.fs.ls(LANDING_PATH))

# COMMAND ----------

for subfolder in ["shipment_manifests", "invoices", "maintenance_logs", "driver_vehicle_master"]:
    files = dbutils.fs.ls(f"{LANDING_PATH}/{subfolder}")
    print(f"{subfolder}: {len(files)} files")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample read-back check (confirms files are valid and readable by Spark)

# COMMAND ----------

df = spark.read.json(f"{LANDING_PATH}/shipment_manifests/")
print("Total manifest rows across all days:", df.count())
display(df.limit(5))

# COMMAND ----------

df_master = spark.read.option("header", True).csv(f"{LANDING_PATH}/driver_vehicle_master/")
print("Total driver_vehicle_master snapshot rows:", df_master.count())
display(df_master.limit(5))
