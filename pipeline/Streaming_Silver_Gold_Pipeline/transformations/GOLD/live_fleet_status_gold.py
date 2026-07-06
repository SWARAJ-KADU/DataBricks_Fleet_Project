from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

CATALOG_NAME = spark.conf.get("CATALOG_NAME")

@dp.table(
    name=f"{CATALOG_NAME}.gold.live_fleet_status_gold",
    comment="Real-time fleet position + health status, one row per truck per tick. Powers the live fleet map dashboard.",
)
def live_fleet_status_gold():
    gps = spark.readStream.table(f"{CATALOG_NAME}.silver.gps_events_silver").withWatermark("event_timestamp", "2 minutes")
    sensor = spark.readStream.table(f"{CATALOG_NAME}.silver.sensor_events_silver").withWatermark("event_timestamp", "2 minutes")

    return (
        gps.alias("g")
        .join(
            sensor.alias("s"),
            expr("""
                g.vehicle_id = s.vehicle_id AND
                g.trip_id = s.trip_id AND
                g.event_timestamp BETWEEN s.event_timestamp - INTERVAL 30 SECONDS
                                       AND s.event_timestamp + INTERVAL 30 SECONDS
            """),
            "inner",
        )
        .select(
            col("g.vehicle_id"),
            col("g.trip_id"),
            col("g.event_timestamp"),
            col("g.lat"),
            col("g.lon"),
            col("g.speed_kmph"),
            col("g.heading"),
            col("g.route_name"),
            col("s.engine_temp_c"),
            col("s.fuel_level_litres"),
            col("s.cargo_temp_c"),
            col("s.anomaly_flag"),
            col("s.is_critical_anomaly"),
        )
    )
