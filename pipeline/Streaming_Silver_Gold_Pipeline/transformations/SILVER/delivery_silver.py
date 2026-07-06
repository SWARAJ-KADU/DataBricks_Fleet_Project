from pyspark import pipelines as dp
from pyspark.sql.functions import col, to_timestamp, element_at, create_map, lit
from itertools import chain
from transformations._pipeline_utils import bronze_table, DELIVERY_STATE_ORDER
from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

CATALOG_NAME = spark.conf.get("CATALOG_NAME")

valid_states_sql = ", ".join(f"'{s}'" for s in DELIVERY_STATE_ORDER.keys())

status_map = create_map(
    *list(chain.from_iterable((lit(k), lit(v)) for k, v in DELIVERY_STATE_ORDER.items()))
)

@dp.table(
    name= f"{CATALOG_NAME}.silver.delivery_events_silver",
    comment="Cleaned delivery status events with validated status values and numeric state ordering.",
)
@dp.expect_or_drop("valid_status", f"status IN ({valid_states_sql})")
def delivery_events_silver():
    df = (
        spark.readStream.table(bronze_table("delivery_events"))
        .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        .withColumn("status_order", element_at(status_map, col("status")))
        .withWatermark("event_timestamp", "10 minutes")
    )
    df = df.dropDuplicatesWithinWatermark(["trip_id", "event_timestamp"])
    return df