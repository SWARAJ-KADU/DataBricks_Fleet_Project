from pyspark import pipelines as dp
from pyspark.sql.functions import col, sum as spark_sum, count
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

def _get_catalog_name() -> str:
    """Get catalog name from pipeline configuration."""
    spark = SparkSession.getActiveSession()
    if spark is None:
        return "fleet_dev"
    try:
        return spark.conf.get("CATALOG_NAME")
    except Exception:
        return "fleet_dev"

CATALOG_NAME = _get_catalog_name()
 
@dp.table(
    name=f"{CATALOG_NAME}.gold.vehicle_scorecard_gold",
    comment="Maintenance cost summary per vehicle, joined with CURRENT driver/route assignment from the SCD2 dimension table. NOTE: uses current-version assignment, not as-of-date assignment -- see code comment.",
)
def vehicle_scorecard_gold():
    catalog_name = _get_catalog_name()
    maintenance = spark.readStream.table(f"{catalog_name}.silver.maintenance_logs_silver")
    current_master = (
        spark.read.table(f"{catalog_name}.silver.driver_vehicle_master_silver")
        .filter(col("__END_AT").isNull())
    )
 
    maintenance_costs = (
        maintenance.groupBy("vehicle_id")
        .agg(
            spark_sum("cost_inr").alias("total_maintenance_cost_inr"),
            count("log_id").alias("maintenance_event_count"),
        )
    )
 
    return (
        maintenance_costs.alias("mc")
        .join(current_master.alias("dm"), col("mc.vehicle_id") == col("dm.vehicle_id"), "left")
        .select(
            col("mc.vehicle_id"),
            col("dm.driver_name"),
            col("dm.vehicle_type"),
            col("dm.home_route"),
            col("dm.is_active"),
            col("mc.total_maintenance_cost_inr"),
            col("mc.maintenance_event_count"),
        )
    )