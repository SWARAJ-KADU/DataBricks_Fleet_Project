# Databricks notebook source
# MAGIC %md
# MAGIC # 01_generate_streaming_data
# MAGIC Phase 1: Run the streaming producer inside Databricks for a short burst,
# MAGIC landing GPS/sensor/delivery events into the DEV landing zone.
# MAGIC
# MAGIC Run this notebook live during a session, right before/while you run your
# MAGIC Structured Streaming ingestion notebook (Phase 2), so there's fresh data
# MAGIC for the stream to pick up.
# MAGIC
# MAGIC NOTE: this runs synchronously for `DURATION_MINUTES` -- the notebook cell
# MAGIC will block until it finishes (or you cancel the cell). That's intentional:
# MAGIC keeps this simple and avoids needing a background process on serverless
# MAGIC compute.

# COMMAND ----------

# MAGIC %pip install Faker --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
%pip install faker

import sys
import os

# Replace "your_username" with your actual Databricks username/email, and
# confirm the repo folder name matches what you see under Workspace > Repos.
REPO_ROOT = "/Workspace/Users/swarajunofficial0@gmail.com/DataBricks_Fleet_Project"
GENERATOR_PATH = f"{REPO_ROOT}/data_generator"
sys.path.append(GENERATOR_PATH)

# COMMAND ----------

CATALOG = "fleet_uat"
SCHEMA = "bronze"
VOLUME = "landing_zone"

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
STREAM_LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/streaming"
print("Streaming landing zone path:", STREAM_LANDING_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configure burst length
# MAGIC Keep this short (5-15 min) per session -- enough to demo Structured
# MAGIC Streaming picking up new files live, without burning your session time.

# COMMAND ----------

dbutils.widgets.text("duration_minutes", "10")
dbutils.widgets.text("tick_seconds", "10")
dbutils.widgets.text("num_trucks", "12")

DURATION_MINUTES = int(dbutils.widgets.get("duration_minutes"))
TICK_SECONDS = int(dbutils.widgets.get("tick_seconds"))
NUM_TRUCKS = int(dbutils.widgets.get("num_trucks"))

# COMMAND ----------

from fleet_simulator import generate_fleet
from streaming_producer import run_file_mode
from pathlib import Path

fleet = generate_fleet(NUM_TRUCKS)

# dbutils.fs paths (dbfs:/, /Volumes/...) aren't directly usable with plain
# Python `open()` in all Databricks runtime configs -- Unity Catalog Volumes
# ARE POSIX-path accessible, so this works directly:
run_file_mode(
    fleet=fleet,
    output_dir=Path(STREAM_LANDING_PATH),
    duration_minutes=DURATION_MINUTES,
    tick_seconds=TICK_SECONDS,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify events landed

# COMMAND ----------

for subfolder in ["gps_events", "sensor_events", "delivery_events"]:
    path = f"{STREAM_LANDING_PATH}/{subfolder}"
    try:
        files = dbutils.fs.ls(path)
        print(f"{subfolder}: {len(files)} files")
    except Exception as e:
        print(f"{subfolder}: not found yet ({e})")

# COMMAND ----------

df_gps = spark.read.json(f"{STREAM_LANDING_PATH}/gps_events/")
display(df_gps.orderBy("timestamp", ascending=False).limit(10))
