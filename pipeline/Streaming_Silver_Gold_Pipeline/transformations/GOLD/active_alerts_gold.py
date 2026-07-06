from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()
 
CATALOG_NAME = spark.conf.get("CATALOG_NAME")

@dp.table(
    name=f"{CATALOG_NAME}.gold.active_alerts_gold",
    comment="Live feed of sensor anomalies (engine overheat, cold-chain breach, fuel drop) as they occur.",
)
def active_alerts_gold():
    return (
        spark.readStream.table(f"{CATALOG_NAME}.silver.sensor_events_silver")
        .filter(col("is_critical_anomaly") == True)
        .withColumn("alert_detected_at", current_timestamp())
        .select(
            "vehicle_id",
            "trip_id",
            "event_timestamp",
            "anomaly_flag",
            "engine_temp_c",
            "fuel_level_litres",
            "cargo_temp_c",
            "alert_detected_at",
        )
    )
