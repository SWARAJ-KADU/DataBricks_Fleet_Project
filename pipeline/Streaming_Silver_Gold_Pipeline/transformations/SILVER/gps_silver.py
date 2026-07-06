from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_timestamp
from transformations._pipeline_utils import bronze_table, VALID_LAT_RANGE, VALID_LON_RANGE
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

CATALOG_NAME = spark.conf.get("CATALOG_NAME")

@dp.table(
    name=f"{CATALOG_NAME}.silver.gps_events_silver",
    comment="Cleaned, deduplicated GPS pings with watermarking applied.",
)
@dp.expect_or_drop("valid_latitude", f"lat BETWEEN {VALID_LAT_RANGE[0]} AND {VALID_LAT_RANGE[1]}")
@dp.expect_or_drop("valid_longitude", f"lon BETWEEN {VALID_LON_RANGE[0]} AND {VALID_LON_RANGE[1]}")
@dp.expect_or_drop("valid_speed", "speed_kmph >= 0 AND speed_kmph <= 150")
@dp.expect("has_trip_id", "trip_id IS NOT NULL") 

def gps_events_silver():
    df = (
        spark.readStream.table(bronze_table("gps_events"))
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        .withWatermark("event_timestamp", "2 minutes")
    )
    df = df.dropDuplicatesWithinWatermark(["trip_id", "event_timestamp"])

    return df
