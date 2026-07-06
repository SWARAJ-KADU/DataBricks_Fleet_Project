from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim
from transformations._pipeline_utils import bronze_table
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

CATALOG_NAME = spark.conf.get("CATALOG_NAME")

@dp.table(
    name=f"{CATALOG_NAME}.silver.maintenance_logs_silver",
    comment="Cleaned maintenance log records, processed incrementally.",
)
@dp.expect_or_drop("valid_cost", "cost_inr > 0")
@dp.expect_or_drop("valid_odometer_km", "odometer_km > 0")
def maintenance_logs_silver():
    return (
        spark.readStream.table(bronze_table("maintenance_logs"))
        .withColumn("issue_type", trim(col("issue_type")))
        .withColumn("service_center", trim(col("service_center")))
    )