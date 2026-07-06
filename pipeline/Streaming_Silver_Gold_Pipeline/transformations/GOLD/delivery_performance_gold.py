from pyspark import pipelines as dp
from pyspark.sql.functions import window, col, count
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

CATALOG_NAME = spark.conf.get("CATALOG_NAME")
 
@dp.table(
    name=f"{CATALOG_NAME}.gold.delivery_performance_gold",
    comment="Hourly count of delivery events by status, using tumbling window aggregation. Powers on-time delivery rate trends.",
)
def delivery_performance_gold():
    # Ensure event_timestamp is not null and status is present
    df = spark.readStream.table(f"{CATALOG_NAME}.silver.delivery_events_silver")
    df = df.filter(col("event_timestamp").isNotNull() & col("status").isNotNull())
    return (
        df.withWatermark("event_timestamp", "10 minutes")
        .groupBy(
            window(col("event_timestamp"), "1 hour"),
            col("status"),
        )
        .agg(count("*").alias("event_count"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("status"),
            col("event_count"),
        )
    )
