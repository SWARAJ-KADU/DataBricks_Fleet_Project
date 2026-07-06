from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from transformations._pipeline_utils import bronze_table

spark = SparkSession.getActiveSession()

CATALOG_NAME = spark.conf.get("CATALOG_NAME")

@dp.temporary_view(name="driver_vehicle_master_bronze_stream")
def driver_vehicle_master_bronze_stream():
    return spark.readStream.table(bronze_table("driver_vehicle_master"))

dp.create_streaming_table(
    name=f"{CATALOG_NAME}.silver.driver_vehicle_master_silver",
    comment="SCD Type 2 history of driver/vehicle assignments, derived from daily master snapshots."
)

dp.create_auto_cdc_flow(
    target=f"{CATALOG_NAME}.silver.driver_vehicle_master_silver",
    source="driver_vehicle_master_bronze_stream",
    keys=["vehicle_id"],
    sequence_by="snapshot_date",
    stored_as_scd_type=2,
)