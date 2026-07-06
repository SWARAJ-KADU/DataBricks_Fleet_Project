from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_date, first, last, max as spark_max, min as spark_min, sum as spark_sum
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

CATALOG_NAME = spark.conf.get("CATALOG_NAME")

@dp.table(
    name=f"{CATALOG_NAME}.gold.fuel_cost_efficiency_gold",
    comment="Daily fuel consumption approximation per route, derived from sensor + GPS data. See code comments for the trip-boundary simplification this relies on.",
)
def fuel_cost_efficiency_gold():
    sensor = spark.read.table(f"{CATALOG_NAME}.silver.sensor_events_silver")
    gps = spark.readStream.table(f"{CATALOG_NAME}.silver.gps_events_silver")

    joined = (
        sensor.alias("s")
        .join(
            gps.alias("g"),
            (col("s.vehicle_id") == col("g.vehicle_id")) & (col("s.trip_id") == col("g.trip_id")),
            "inner",
        )
        .select(
            col("s.vehicle_id"),
            col("s.trip_id"),
            col("s.event_timestamp"),
            col("s.fuel_level_litres"),
            col("g.route_name"),
            col("g.distance_traveled_km"),
        )
        .withColumn("event_date", to_date(col("event_timestamp")))
    )

    return (
        joined.groupBy("trip_id")
        .agg(
            spark_max("fuel_level_litres").alias("fuel_start_litres"),
            spark_min("fuel_level_litres").alias("fuel_end_litres"),
            spark_max("distance_traveled_km").alias("max_distance_km"),
            spark_min("distance_traveled_km").alias("min_distance_km"),
        )
        .withColumn("estimated_fuel_consumed_litres", col("fuel_start_litres") - col("fuel_end_litres"))
        .withColumn("estimated_distance_km", col("max_distance_km") - col("min_distance_km"))
    )
