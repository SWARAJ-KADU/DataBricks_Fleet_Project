from pyspark.sql.types import *

shipment_manifest_schema = StructType([
    StructField("manifest_id", StringType(), True),
    StructField("shipment_date", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("route_name", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("destination_city", StringType(), True),
    StructField("package_count", IntegerType(), True),
    StructField("total_weight_kg", DoubleType(), True),
    StructField("is_cold_chain", BooleanType(), True),
    StructField("priority", StringType(), True)
])

invoice_schema = StructType([
    StructField("invoice_id", StringType(), True),
    StructField("manifest_id", StringType(), True),
    StructField("invoice_date", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("amount_inr", DoubleType(), True),
    StructField("payment_status", StringType(), True)
])

maintenance_schema = StructType([
    StructField("log_id", StringType(), True),
    StructField("service_date", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("issue_type", StringType(), True),
    StructField("cost_inr", DoubleType(), True),
    StructField("odometer_km", IntegerType(), True),
    StructField("service_center", StringType(), True)
])

driver_vehicle_master_schema = StructType([
    StructField("snapshot_date", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("license_plate", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("driver_name", StringType(), True),
    StructField("driver_phone", StringType(), True),
    StructField("driver_license_no", StringType(), True),
    StructField("home_route", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("last_change_type", StringType(), True)
])

gps_event_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("trip_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("timestamp", StringType(), True),   # Can be TimestampType() after parsing
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("speed_kmph", DoubleType(), True),
    StructField("heading", StringType(), True),
    StructField("distance_traveled_km", DoubleType(), True),
    StructField("route_name", StringType(), True),
])

sensor_event_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("trip_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("engine_temp_c", DoubleType(), True),
    StructField("fuel_level_litres", DoubleType(), True),
    StructField("cargo_temp_c", DoubleType(), True),   # Nullable for non-refrigerated trucks
    StructField("anomaly_flag", StringType(), True),   # Nullable when no anomaly
])

delivery_event_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("trip_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("status", StringType(), True),
    StructField("location_city", StringType(), True),
])

SCHEMAS = {
    "shipment_manifests": shipment_manifest_schema,
    "invoices": invoice_schema,
    "maintenance_logs": maintenance_schema,
    "driver_vehicle_master": driver_vehicle_master_schema,
    "gps_events": gps_event_schema,
    "sensor_events": sensor_event_schema,
    "delivery_events": delivery_event_schema
}