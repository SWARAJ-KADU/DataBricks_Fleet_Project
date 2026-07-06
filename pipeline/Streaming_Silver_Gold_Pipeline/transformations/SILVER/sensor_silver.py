from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_timestamp
from transformations._pipeline_utils import bronze_table, VALID_ENGINE_TEMP_RANGE, VALID_FUEL_RANGE, VALID_CARGO_TEMP_RANGE
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

CATALOG_NAME = spark.conf.get("CATALOG_NAME")

@dp.table(
    name=f"{CATALOG_NAME}.silver.sensor_events_silver",
    comment="Cleaned sensor events with anomaly detection and watermarking."
)
@dp.expect_or_drop("valid_engine_temp", f"engine_temp_c BETWEEN {VALID_ENGINE_TEMP_RANGE[0]} AND {VALID_ENGINE_TEMP_RANGE[1]}")
@dp.expect_or_drop("valid_fuel_level", f"fuel_level_litres BETWEEN {VALID_FUEL_RANGE[0]} AND {VALID_FUEL_RANGE[1]}")
@dp.expect_or_drop("valid_cargo_temp", f"cargo_temp_c IS NULL OR cargo_temp_c BETWEEN {VALID_CARGO_TEMP_RANGE[0]} AND {VALID_CARGO_TEMP_RANGE[1]}")

def sensor_events_silver():
    df = (
        spark.readStream.table(bronze_table("sensor_events"))
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        .withColumn("is_critical_anomaly", col("anomaly_flag").isNotNull())
        .withWatermark("event_timestamp", "2 minutes")
    )
    df = df.dropDuplicatesWithinWatermark(["trip_id", "event_timestamp"])

    return df
